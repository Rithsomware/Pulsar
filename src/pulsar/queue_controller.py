"""
PULSAR Real-Time Queue Controller

Enhanced queue management with:
- Priority aging: auto-boost priority after configurable interval (default 60s)
- Starvation prevention: force-promote jobs waiting beyond threshold (default 300s)
- Preemption signals: emit Kubernetes preemption signals for queued workloads
- Real-time queue depth and wait time tracking
"""

import logging
import threading
import time
from collections import defaultdict
from datetime import datetime
from typing import Dict, List, Optional, Callable

from pulsar.pulsar_types import GPUJob, JobStatus, JobPriority, PulsarEvent
from pulsar.queue_manager import QueueManager
from pulsar.metrics import MetricsCollector

logger = logging.getLogger("pulsar.queue_controller")


class QueueController:
    """
    Real-time queue controller managing job lifecycle in queue.

    Features:
      * Aging: every N seconds, boost priority of waiting jobs by one level.
      * Starvation prevention: jobs waiting longer than threshold are force-promoted.
      * Preemption signals: annotate pods with preemption candidates for K8s scheduler.
      * Metrics: queue_depth, wait_time, per-tenant counters.
    """

    def __init__(
        self,
        queue_manager: Optional[QueueManager] = None,
        aging_enabled: bool = True,
        aging_boost_interval_seconds: float = 60.0,
        aging_boost_amount: int = 1,
        starvation_threshold_seconds: float = 300.0,
        preemption_signals: bool = True,
        max_queue_depth_per_tenant: int = 50,
        on_preemption_signal: Optional[Callable[[GPUJob, Dict], None]] = None,
        metrics: Optional[MetricsCollector] = None,
    ):
        self.queue_manager = queue_manager or QueueManager()
        self.aging_enabled = aging_enabled
        self.aging_boost_interval_seconds = aging_boost_interval_seconds
        self.aging_boost_amount = aging_boost_amount
        self.starvation_threshold_seconds = starvation_threshold_seconds
        self.preemption_signals = preemption_signals
        self.max_queue_depth_per_tenant = max_queue_depth_per_tenant
        self.on_preemption_signal = on_preemption_signal
        self.metrics = metrics

        self._lock = threading.RLock()
        self._running = False
        self._thread: Optional[threading.Thread] = None
        self._stop_event = threading.Event()

        # Aging state: job_id -> last_boosted_timestamp
        self._last_boosted: Dict[str, datetime] = {}
        # Starvation tracking: job_id -> submitted_timestamp
        self._starvation_logged: set = set()
        # Preemption signal tracking
        self._preemption_signals_sent: set = set()
        # Events
        self._events: List[PulsarEvent] = []

    def start(self):
        """Start the background queue controller loop."""
        if self._running:
            return
        self._running = True
        self._stop_event.clear()
        self._thread = threading.Thread(target=self._controller_loop, daemon=True)
        self._thread.start()
        logger.info(
            "Queue controller started: aging=%s, boost_interval=%.1fs, starvation=%.1fs, preemption_signals=%s",
            self.aging_enabled,
            self.aging_boost_interval_seconds,
            self.starvation_threshold_seconds,
            self.preemption_signals,
        )

    def stop(self):
        """Stop the background loop gracefully."""
        self._running = False
        self._stop_event.set()
        if self._thread:
            self._thread.join(timeout=5.0)
        logger.info("Queue controller stopped")

    def submit_job(self, job: GPUJob) -> PulsarEvent:
        """Submit job with queue-depth guard."""
        with self._lock:
            user_depth = sum(
                1 for q in self.queue_manager._user_queues.values()
                for j in q if j.user == job.user
            )
            if user_depth >= self.max_queue_depth_per_tenant:
                job.status = JobStatus.REJECTED
                event = PulsarEvent(
                    event_type="QUEUE_REJECTED",
                    message=f"Job {job.job_id} rejected: tenant queue depth limit",
                    user=job.user,
                    job_id=job.job_id,
                    metadata={"max_depth": self.max_queue_depth_per_tenant},
                )
                self._events.append(event)
                logger.warning(event.format())
                return event

        event = self.queue_manager.submit_job(job)
        with self._lock:
            self._last_boosted[job.job_id] = datetime.now()
        return event

    def get_next_job(self, fairness_scores, policy="fair_share") -> Optional[GPUJob]:
        """Dequeue with starvation bypass."""
        # Check for starved jobs first
        starved = self._get_starved_jobs()
        if starved:
            job = self._force_promote_starved(starved)
            if job:
                return job
        return self.queue_manager.get_next_job(fairness_scores, policy)

    def requeue_job(self, job: GPUJob) -> PulsarEvent:
        """Re-queue a job, resetting aging state."""
        event = self.queue_manager.requeue_job(job)
        with self._lock:
            self._last_boosted[job.job_id] = datetime.now()
            self._starvation_logged.discard(job.job_id)
            self._preemption_signals_sent.discard(job.job_id)
        return event

    def cancel_job(self, job_id: str) -> Optional[GPUJob]:
        """Cancel and clean up tracking state."""
        job = self.queue_manager.cancel_job(job_id)
        if job:
            with self._lock:
                self._last_boosted.pop(job_id, None)
                self._starvation_logged.discard(job_id)
                self._preemption_signals_sent.discard(job_id)
        return job

    def _controller_loop(self):
        interval = min(self.aging_boost_interval_seconds, 5.0)
        while self._running and not self._stop_event.is_set():
            try:
                self._apply_aging()
                self._check_starvation()
                if self.preemption_signals:
                    self._emit_preemption_signals()
            except Exception:
                logger.exception("Queue controller loop error")
            self._stop_event.wait(timeout=interval)

    def _apply_aging(self):
        """Boost priority of jobs that have been waiting longer than the aging interval."""
        if not self.aging_enabled:
            return
        now = datetime.now()
        with self._lock:
            for job_id, last_boosted in list(self._last_boosted.items()):
                elapsed = (now - last_boosted).total_seconds()
                if elapsed >= self.aging_boost_interval_seconds:
                    job = self.queue_manager.get_job(job_id)
                    if job and job.status == JobStatus.QUEUED:
                        old_priority = job.priority
                        new_value = min(
                            job.priority.value + self.aging_boost_amount,
                            JobPriority.CRITICAL.value,
                        )
                        job.priority = JobPriority(new_value)
                        self._last_boosted[job_id] = now
                        if self.metrics:
                            self.metrics.record_aging_boost()
                        event = PulsarEvent(
                            event_type="AGING",
                            message=f"Job {job_id} priority boosted: {old_priority.name} -> {job.priority.name}",
                            user=job.user,
                            job_id=job_id,
                            metadata={
                                "wait_seconds": round(elapsed, 1),
                                "boost_amount": self.aging_boost_amount,
                            },
                        )
                        self._events.append(event)
                        logger.info(event.format())
                    else:
                        self._last_boosted.pop(job_id, None)

    def _get_starved_jobs(self) -> List[GPUJob]:
        """Return jobs that have exceeded the starvation threshold."""
        now = datetime.now()
        starved = []
        with self._lock:
            for user, queue in self.queue_manager._user_queues.items():
                for job in queue:
                    if job.submitted_at:
                        wait = (now - job.submitted_at).total_seconds()
                        if wait >= self.starvation_threshold_seconds:
                            starved.append(job)
        return starved

    def _check_starvation(self):
        """Process the queues and boost starving jobs."""
        with self._lock:
            now = datetime.now()
            
            # 1. Starvation check & boosting
            for user, queue in self.queue_manager._user_queues.items():
                for job in queue:
                    if not job.submitted_at:
                        continue
                    
                    wait_time = (now - job.submitted_at).total_seconds()
                    
                    if wait_time > self.starvation_threshold_seconds:
                        # Boost priority if starving
                        if job.priority != JobPriority.CRITICAL:
                            logger.info("[QUEUE] Starvation detected for %s (%ds) — boosting to CRITICAL", job.job_id, int(wait_time))
                            job.priority = JobPriority.CRITICAL
                            # Re-building the queue structure if necessary
                            if hasattr(self.queue_manager, '_rebuild_queue'):
                                self.queue_manager._rebuild_queue(user)
                            break # Handle one boost per user per cycle
            
            # 2. Aging logic (periodic boost for long-waiting jobs)
            # This is already partially handled by incremental boosting in _apply_aging

    def _force_promote_starved(self, starved: List[GPUJob]) -> Optional[GPUJob]:
        """Remove the most-starved job from queue and return it directly."""
        if not starved:
            return None
        # Pick the one waiting the longest
        starved.sort(key=lambda j: j.submitted_at or datetime.now())
        victim = starved[0]
        # Remove from queue_manager
        with self._lock:
            job = self.queue_manager.cancel_job(victim.job_id)
            if job:
                self._starvation_logged.discard(job.job_id)
                self._last_boosted.pop(job.job_id, None)
                event = PulsarEvent(
                    event_type="FORCE_PROMOTE",
                    message=f"Job {job.job_id} force-promoted due to starvation",
                    user=job.user,
                    job_id=job.job_id,
                    metadata={"wait_seconds": self.starvation_threshold_seconds},
                )
                self._events.append(event)
                logger.info(event.format())
                return job
        return None

    def _emit_preemption_signals(self):
        """Emit preemption signal metadata for queued jobs that are candidates."""
        now = datetime.now()
        with self._lock:
            for user, queue in self.queue_manager._user_queues.items():
                for job in queue:
                    if not job.submitted_at:
                        continue
                    wait = (now - job.submitted_at).total_seconds()
                    if wait >= self.aging_boost_interval_seconds and job.job_id not in self._preemption_signals_sent:
                        self._preemption_signals_sent.add(job.job_id)
                        if self.metrics:
                            self.metrics.record_preemption_signal()
                        signal_data = {
                            "job_id": job.job_id,
                            "user": job.user,
                            "wait_seconds": round(wait, 1),
                            "priority": job.priority.name,
                            "gpu_required": job.gpu_required,
                            "preemptible": job.preemptible,
                        }
                        event = PulsarEvent(
                            event_type="PREEMPTION_SIGNAL",
                            message=f"Preemption signal for {job.job_id}",
                            user=job.user,
                            job_id=job.job_id,
                            metadata=signal_data,
                        )
                        self._events.append(event)
                        if self.on_preemption_signal:
                            try:
                                self.on_preemption_signal(job, signal_data)
                            except Exception:
                                logger.exception("Preemption signal callback failed")

    @property
    def total_queued(self) -> int:
        return self.queue_manager.total_queued

    @property
    def total_gpus_queued(self) -> int:
        return self.queue_manager.total_gpus_queued

    @property
    def users_with_jobs(self) -> List[str]:
        return self.queue_manager.users_with_jobs

    def get_queue_status(self) -> Dict[str, dict]:
        status = self.queue_manager.get_queue_status()
        now = datetime.now()
        with self._lock:
            for user, data in status.items():
                depth = data.get("depth", 0)
                max_wait = 0.0
                for j in data.get("jobs", []):
                    submitted = j.get("submitted_at")
                    if submitted:
                        try:
                            dt = datetime.fromisoformat(submitted)
                            wait = (now - dt).total_seconds()
                            max_wait = max(max_wait, wait)
                            if self.metrics:
                                self.metrics.record_wait_time_bucket(wait)
                        except Exception:
                            pass
                data["max_wait_seconds"] = round(max_wait, 1)
                data["starved_jobs"] = sum(
                    1 for q in self.queue_manager._user_queues.get(user, [])
                    if q.submitted_at and (now - q.submitted_at).total_seconds() >= self.starvation_threshold_seconds
                )
                if self.metrics:
                    gpu_class = "dgpu"
                    if data.get("jobs"):
                        first_pref = data["jobs"][0].get("preferred_gpu_class", "dgpu")
                        gpu_class = first_pref if first_pref else "dgpu"
                    self.metrics.record_queue_depth(user, gpu_class, depth)
        return status

    def get_aging_status(self) -> Dict[str, dict]:
        """Return current aging state for all queued jobs."""
        now = datetime.now()
        with self._lock:
            result = {}
            for job_id, last_boosted in self._last_boosted.items():
                job = self.queue_manager.get_job(job_id)
                if job:
                    result[job_id] = {
                        "user": job.user,
                        "priority": job.priority.name,
                        "seconds_since_last_boost": round((now - last_boosted).total_seconds(), 1),
                        "submitted_at": job.submitted_at.isoformat() if job.submitted_at else None,
                    }
            return result

    @property
    def events(self) -> List[PulsarEvent]:
        return self._events
