"""
PULSAR Preemption Engine

Priority-based job preemption for GPU workloads. Evicts lower-priority
preemptible jobs to make room for higher-priority workloads.
"""

import logging
from datetime import datetime
from typing import List, Optional, Tuple

from pulsar.pulsar_types import GPUJob, JobStatus, PulsarEvent

logger = logging.getLogger("pulsar.preemption")


class PreemptionEngine:
    """
    Manages priority-based preemption of GPU workloads.

    When a high-priority job cannot be admitted due to capacity,
    the preemption engine identifies lower-priority victims to evict.
    """

    def __init__(self, enabled: bool = True, max_per_cycle: int = 3):
        self.enabled = enabled
        self.max_per_cycle = max_per_cycle
        self._preemption_count: int = 0
        self._events: List[PulsarEvent] = []

    def should_preempt(self, job: GPUJob, available_gpus: int) -> bool:
        """Check if preemption should be attempted for this job."""
        if not self.enabled:
            return False
        if job.gpu_required <= available_gpus:
            return False
        if job.priority.value < 2:  # Only HIGH and CRITICAL can preempt
            return False
        return True

    def select_victims(
        self,
        job: GPUJob,
        active_jobs: dict,
        available_gpus: int,
    ) -> Tuple[List[GPUJob], int]:
        """
        Select victim jobs to preempt.

        Selection criteria:
        1. Only preemptible jobs with strictly lower priority
        2. Sorted by: lowest priority first, then oldest allocation first
        3. Selects minimum victims needed to free enough GPUs

        Returns:
            Tuple of (victim_list, total_gpus_freed)
        """
        needed = job.gpu_required - available_gpus
        if needed <= 0:
            return [], 0

        candidates = [
            j for j in active_jobs.values()
            if j.preemptible
            and j.priority.value < job.priority.value
            and j.job_id != job.job_id
        ]

        # Sort: lowest priority first, then oldest admitted first
        candidates.sort(
            key=lambda j: (
                j.priority.value,
                (j.admitted_at or datetime.now()).timestamp(),
            )
        )

        victims = []
        freed = 0
        for candidate in candidates:
            if freed >= needed:
                break
            if len(victims) >= self.max_per_cycle:
                break
            victims.append(candidate)
            freed += candidate.gpu_required

        if freed >= needed:
            self._preemption_count += len(victims)
            for v in victims:
                event = PulsarEvent(
                    event_type="PREEMPTION",
                    message=f"Selected victim {v.job_id} ({v.gpu_required} GPUs, priority={v.priority.name})",
                    user=v.user,
                    job_id=v.job_id,
                    metadata={
                        "preempted_by": job.job_id,
                        "preempted_by_user": job.user,
                        "preempted_by_priority": job.priority.name,
                    },
                )
                self._events.append(event)
                logger.info(event.format())

            event = PulsarEvent(
                event_type="PREEMPTION",
                message=f"Preempting {len(victims)} jobs to free {freed} GPUs for {job.job_id}",
                user=job.user,
                job_id=job.job_id,
                metadata={
                    "victims": len(victims),
                    "gpus_freed": freed,
                    "gpus_needed": job.gpu_required,
                },
            )
            self._events.append(event)
            logger.info(event.format())
            return victims, freed

        logger.info(
            "[PULSAR] [PREEMPTION] Cannot preempt enough: need %d GPUs, can free %d",
            needed, freed,
        )
        return [], 0

    @property
    def preemption_count(self) -> int:
        return self._preemption_count

    @property
    def events(self) -> List[PulsarEvent]:
        return self._events
