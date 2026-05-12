"""
PULSAR Queue Manager

Multi-tenant GPU workload queue system with per-user FIFO queues,
priority queue support, and fairness-weighted dequeuing.
"""

import logging
import heapq
import threading
from collections import defaultdict, OrderedDict
from typing import Optional, List, Dict

from pulsar.pulsar_types import GPUJob, JobStatus, JobPriority, PulsarEvent

logger = logging.getLogger("pulsar.queue")


class QueueManager:
    """
    Multi-tenant queue manager for GPU workloads.

    Supports per-user FIFO queues with fairness-weighted or priority-based
    dequeuing. Thread-safe for concurrent API access.
    """

    def __init__(self):
        self._lock = threading.RLock()
        self._user_queues: Dict[str, List[GPUJob]] = defaultdict(list)
        self._job_index: Dict[str, GPUJob] = OrderedDict()
        self._user_order: List[str] = []
        self._rr_index: int = 0
        self._events: List[PulsarEvent] = []

    def submit_job(self, job: GPUJob) -> PulsarEvent:
        """Submit a job to the user's queue."""
        with self._lock:
            job.status = JobStatus.QUEUED
            self._user_queues[job.user].append(job)
            self._job_index[job.job_id] = job

            if job.user not in self._user_order:
                self._user_order.append(job.user)

            position = len(self._user_queues[job.user])
            event = PulsarEvent(
                event_type="QUEUE",
                message=f"Job {job.job_id} queued (position {position})",
                user=job.user,
                job_id=job.job_id,
                metadata={
                    "position": position,
                    "total_queued": len(self._job_index),
                    "gpus_requested": job.gpu_required,
                    "priority": job.priority.name,
                    "workload_type": job.workload_type,
                },
            )
            self._events.append(event)
            logger.info(event.format())
            return event

    def requeue_job(self, job: GPUJob) -> PulsarEvent:
        """Re-queue a preempted job at the front of its user queue."""
        with self._lock:
            job.status = JobStatus.QUEUED
            self._user_queues[job.user].insert(0, job)
            self._job_index[job.job_id] = job

            if job.user not in self._user_order:
                self._user_order.append(job.user)

            event = PulsarEvent(
                event_type="QUEUE",
                message=f"Job {job.job_id} re-queued at front (preempted)",
                user=job.user,
                job_id=job.job_id,
                metadata={"preemption_count": job.preemption_count},
            )
            self._events.append(event)
            logger.info(event.format())
            return event

    def get_next_job(
        self,
        fairness_scores: Optional[Dict[str, float]] = None,
        policy: str = "fair_share",
    ) -> Optional[GPUJob]:
        """
        Dequeue the next job.

        Selection strategy depends on policy:
        - fair_share: user with highest fairness score
        - priority: highest priority job across all queues
        - fifo: strict global submission order
        - backfill: delegates to caller
        """
        with self._lock:
            if not self._job_index:
                return None

            # Clean empty queues
            empty = [u for u, q in self._user_queues.items() if not q]
            for u in empty:
                del self._user_queues[u]
                if u in self._user_order:
                    self._user_order.remove(u)

            if not self._user_queues:
                return None

            selected_user = None
            job = None

            if policy == "priority":
                # Pick highest-priority job across all queues
                best_job = None
                for user, queue in self._user_queues.items():
                    for j in queue:
                        if best_job is None or j.priority.value > best_job.priority.value:
                            best_job = j
                        elif (
                            j.priority.value == best_job.priority.value
                            and j.submitted_at < best_job.submitted_at
                        ):
                            best_job = j
                if best_job:
                    self._user_queues[best_job.user].remove(best_job)
                    del self._job_index[best_job.job_id]
                    job = best_job
                    selected_user = best_job.user

            elif policy == "fifo":
                # Strict submission order
                oldest = None
                for j in self._job_index.values():
                    if oldest is None or j.submitted_at < oldest.submitted_at:
                        oldest = j
                if oldest:
                    self._user_queues[oldest.user].remove(oldest)
                    del self._job_index[oldest.job_id]
                    job = oldest
                    selected_user = oldest.user

            else:
                # fair_share / default: use fairness scores
                if fairness_scores:
                    candidates = [
                        (u, fairness_scores.get(u, 0.5))
                        for u in self._user_queues
                        if self._user_queues[u]
                    ]
                    if candidates:
                        candidates.sort(key=lambda x: x[1], reverse=True)
                        selected_user = candidates[0][0]
                else:
                    # Round-robin fallback
                    if self._user_order:
                        self._rr_index = self._rr_index % len(self._user_order)
                        selected_user = self._user_order[self._rr_index]
                        self._rr_index += 1

                if selected_user and self._user_queues.get(selected_user):
                    job = self._user_queues[selected_user].pop(0)
                    del self._job_index[job.job_id]

            if job is None:
                return None

            event = PulsarEvent(
                event_type="QUEUE",
                message=f"Dequeued {job.job_id}",
                user=job.user,
                job_id=job.job_id,
                metadata={
                    "remaining_user": len(self._user_queues.get(job.user, [])),
                    "total_remaining": len(self._job_index),
                    "policy": policy,
                },
            )
            self._events.append(event)
            logger.info(event.format())
            return job

    def get_backfill_candidates(self, available_gpus: int) -> List[GPUJob]:
        """Return small jobs that fit into available GPU slots (for backfill policy)."""
        with self._lock:
            candidates = []
            for user, queue in self._user_queues.items():
                for job in queue:
                    if job.gpu_required <= available_gpus:
                        candidates.append(job)
            candidates.sort(key=lambda j: (j.gpu_required, j.submitted_at))
            return candidates

    def remove_backfill_job(self, job_id: str) -> Optional[GPUJob]:
        """Remove a specific job selected by backfill."""
        with self._lock:
            job = self._job_index.pop(job_id, None)
            if job:
                q = self._user_queues.get(job.user, [])
                self._user_queues[job.user] = [j for j in q if j.job_id != job_id]
            return job

    def cancel_job(self, job_id: str) -> Optional[GPUJob]:
        """Cancel a queued job."""
        with self._lock:
            job = self._job_index.pop(job_id, None)
            if job:
                q = self._user_queues.get(job.user, [])
                self._user_queues[job.user] = [j for j in q if j.job_id != job_id]
                job.status = JobStatus.CANCELLED
                logger.info("[PULSAR] [QUEUE] Job %s cancelled", job_id)
            return job

    def peek_next(self, user: Optional[str] = None) -> Optional[GPUJob]:
        with self._lock:
            if user:
                q = self._user_queues.get(user, [])
                return q[0] if q else None
            for q in self._user_queues.values():
                if q:
                    return q[0]
            return None

    def get_job(self, job_id: str) -> Optional[GPUJob]:
        with self._lock:
            return self._job_index.get(job_id)

    def get_queue_status(self) -> Dict[str, dict]:
        with self._lock:
            status = {}
            for user, queue in self._user_queues.items():
                if queue:
                    status[user] = {
                        "depth": len(queue),
                        "total_gpus_queued": sum(j.gpu_required for j in queue),
                        "dgpu_queued": sum(j.gpu_required for j in queue if getattr(j, 'preferred_gpu_class', 'dgpu') == 'dgpu'),
                        "igpu_queued": sum(j.gpu_required for j in queue if getattr(j, 'preferred_gpu_class', 'dgpu') == 'igpu'),
                        "jobs": [j.to_dict() for j in queue],
                    }
            return status

    @property
    def total_queued(self) -> int:
        with self._lock:
            return len(self._job_index)

    @property
    def total_gpus_queued(self) -> int:
        with self._lock:
            return sum(j.gpu_required for j in self._job_index.values())

    @property
    def users_with_jobs(self) -> List[str]:
        with self._lock:
            return [u for u, q in self._user_queues.items() if q]

    @property
    def events(self) -> List[PulsarEvent]:
        return self._events
