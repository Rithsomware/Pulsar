"""
PULSAR Admission Controller

Production resource admission control with GPU capacity enforcement,
per-user quotas, GPU memory tracking, and preemption support.
"""

import logging
import threading
from datetime import datetime
from typing import Dict, List, Optional, Tuple

from pulsar.pulsar_types import GPUJob, JobStatus, UserQuota, PulsarEvent

logger = logging.getLogger("pulsar.admission")


class AdmissionController:
    """
    GPU resource admission controller.

    Enforces cluster-wide capacity and per-user quotas. Supports
    preemption by identifying victim jobs when capacity is insufficient.
    """

    def __init__(self, total_gpus: int, gpu_memory_gb: int = 80,
                 quotas: Optional[Dict[str, UserQuota]] = None):
        self._lock = threading.Lock()
        self._total_gpus = total_gpus
        self._available_gpus = total_gpus
        self._gpu_memory_gb = gpu_memory_gb
        self._total_memory_gb = total_gpus * gpu_memory_gb
        self._available_memory_gb = self._total_memory_gb
        self._quotas: Dict[str, UserQuota] = quotas or {}
        self._active_jobs: Dict[str, GPUJob] = {}
        self._events: List[PulsarEvent] = []

    def set_quota(self, user: str, max_gpus: int = 8, max_jobs: int = 10,
                  weight: float = 1.0):
        with self._lock:
            if user in self._quotas:
                self._quotas[user].max_gpus = max_gpus
                self._quotas[user].max_jobs = max_jobs
                self._quotas[user].weight = weight
            else:
                self._quotas[user] = UserQuota(
                    user=user, max_gpus=max_gpus, max_jobs=max_jobs, weight=weight
                )

    def can_admit(self, job: GPUJob) -> Tuple[bool, str]:
        """Check if a job can be admitted. Thread-safe."""
        with self._lock:
            return self._can_admit_unlocked(job)

    def _can_admit_unlocked(self, job: GPUJob) -> Tuple[bool, str]:
        # Absolute limits (permanent rejection)
        if job.gpu_required > self._total_gpus:
            return False, f"PERMANENT_REJECT: Job requires {job.gpu_required} GPUs, exceeding total cluster capacity ({self._total_gpus})."

        quota = self._quotas.get(job.user)
        if quota:
            if job.gpu_required > quota.max_gpus:
                return False, f"PERMANENT_REJECT: Job requires {job.gpu_required} GPUs, exceeding absolute quota limit ({quota.max_gpus})."

        # Temporary limits (requeue)
        if job.gpu_required > self._available_gpus:
            return False, (
                f"Insufficient cluster GPUs: need {job.gpu_required}, "
                f"available {self._available_gpus}/{self._total_gpus}"
            )

        if job.gpu_memory_gb > 0:
            needed_mem = job.gpu_required * job.gpu_memory_gb
            if needed_mem > self._available_memory_gb:
                return False, (
                    f"Insufficient GPU memory: need {needed_mem}GB, "
                    f"available {self._available_memory_gb}GB"
                )

        if quota:
            if job.gpu_required > quota.gpu_available:
                return False, (
                    f"User quota temporarily exceeded: need {job.gpu_required}, "
                    f"quota allows {quota.gpu_available} more (limit: {quota.max_gpus})"
                )
            if not quota.can_submit:
                return False, (
                    f"Job limit temporarily reached: {quota.current_job_count}/{quota.max_jobs} jobs"
                )
        return True, "OK"

    def allocate(self, job: GPUJob) -> PulsarEvent:
        """Allocate GPU resources for a job.

        IMPORTANT: Call can_admit() first to check before allocating.
        This method assumes the job has already been approved for admission.
        """
        with self._lock:
            # Double-check before allocating (belt and suspenders)
            can, reason = self._can_admit_unlocked(job)
            if not can:
                job.status = JobStatus.REJECTED
                event = PulsarEvent(
                    event_type="ADMISSION",
                    message=f"REJECTED {job.job_id}: {reason}",
                    user=job.user, job_id=job.job_id,
                    metadata={"gpus_requested": job.gpu_required,
                              "available": self._available_gpus,
                              "total": self._total_gpus},
                )
                self._events.append(event)
                logger.warning(event.format())
                return event

            self._available_gpus -= job.gpu_required
            mem_used = job.gpu_required * (job.gpu_memory_gb or self._gpu_memory_gb)
            self._available_memory_gb -= mem_used
            job.status = JobStatus.ADMITTED
            job.admitted_at = datetime.now()
            self._active_jobs[job.job_id] = job

            quota = self._quotas.get(job.user)
            if quota:
                quota.current_gpu_usage += job.gpu_required
                quota.current_job_count += 1

            event = PulsarEvent(
                event_type="ADMISSION",
                message=f"ADMITTED {job.job_id}: {job.gpu_required} GPUs",
                user=job.user, job_id=job.job_id,
                metadata={
                    "gpus_allocated": job.gpu_required,
                    "cluster_utilization": f"{self._utilization_unlocked():.0%}",
                    "cluster_used": f"{self._total_gpus - self._available_gpus}/{self._total_gpus}",
                },
            )
            self._events.append(event)
            logger.info(event.format())
            return event

    def release(self, job_id: str) -> Optional[PulsarEvent]:
        """Release GPU resources when a job completes."""
        with self._lock:
            job = self._active_jobs.pop(job_id, None)
            if not job:
                return None

            self._available_gpus += job.gpu_required
            mem_freed = job.gpu_required * (job.gpu_memory_gb or self._gpu_memory_gb)
            self._available_memory_gb += mem_freed
            job.status = JobStatus.COMPLETED
            job.completed_at = datetime.now()

            quota = self._quotas.get(job.user)
            if quota:
                quota.current_gpu_usage = max(0, quota.current_gpu_usage - job.gpu_required)
                quota.current_job_count = max(0, quota.current_job_count - 1)
                if job.admitted_at:
                    hours = (datetime.now() - job.admitted_at).total_seconds() / 3600
                    quota.total_gpu_hours += job.gpu_required * hours

            event = PulsarEvent(
                event_type="ADMISSION",
                message=f"RELEASED {job_id}: freed {job.gpu_required} GPUs",
                user=job.user, job_id=job_id,
                metadata={
                    "gpus_freed": job.gpu_required,
                    "cluster_utilization": f"{self._utilization_unlocked():.0%}",
                },
            )
            self._events.append(event)
            logger.info(event.format())
            return event

    def find_preemption_victims(self, job: GPUJob) -> List[GPUJob]:
        """
        Find preemptible jobs that can be evicted to make room.

        Selects victims with: lower priority first, then oldest first.
        Returns enough victims to free the required GPUs.
        """
        with self._lock:
            needed = job.gpu_required - self._available_gpus
            if needed <= 0:
                return []

            candidates = [
                j for j in self._active_jobs.values()
                if j.preemptible and j.priority.value < job.priority.value
            ]
            candidates.sort(key=lambda j: (j.priority.value, -(j.admitted_at or datetime.now()).timestamp()))

            victims = []
            freed = 0
            for c in candidates:
                victims.append(c)
                freed += c.gpu_required
                if freed >= needed:
                    break

            return victims if freed >= needed else []

    def force_release(self, job_id: str) -> Optional[GPUJob]:
        """Force-release a job for preemption (returns the job object)."""
        with self._lock:
            job = self._active_jobs.pop(job_id, None)
            if not job:
                return None

            self._available_gpus += job.gpu_required
            mem_freed = job.gpu_required * (job.gpu_memory_gb or self._gpu_memory_gb)
            self._available_memory_gb += mem_freed
            job.status = JobStatus.PREEMPTED
            job.preemption_count += 1

            quota = self._quotas.get(job.user)
            if quota:
                quota.current_gpu_usage = max(0, quota.current_gpu_usage - job.gpu_required)
                quota.current_job_count = max(0, quota.current_job_count - 1)

            logger.info("[PULSAR] [PREEMPTION] Force-released %s (%d GPUs)",
                        job_id, job.gpu_required)
            return job

    def _utilization_unlocked(self) -> float:
        return (self._total_gpus - self._available_gpus) / self._total_gpus if self._total_gpus > 0 else 0.0

    @property
    def utilization(self) -> float:
        with self._lock:
            return self._utilization_unlocked()

    @property
    def available_gpus(self) -> int:
        with self._lock:
            return self._available_gpus

    @property
    def total_gpus(self) -> int:
        return self._total_gpus

    def get_active_jobs(self) -> Dict[str, GPUJob]:
        with self._lock:
            return dict(self._active_jobs)

    def get_quota(self, user: str) -> Optional[UserQuota]:
        return self._quotas.get(user)

    def get_all_quotas(self) -> Dict[str, UserQuota]:
        return dict(self._quotas)

    def get_cluster_status(self) -> dict:
        with self._lock:
            return {
                "total_gpus": self._total_gpus,
                "available_gpus": self._available_gpus,
                "used_gpus": self._total_gpus - self._available_gpus,
                "utilization": round(self._utilization_unlocked(), 4),
                "utilization_pct": f"{self._utilization_unlocked():.0%}",
                "gpu_memory_gb_per_device": self._gpu_memory_gb,
                "total_memory_gb": self._total_memory_gb,
                "available_memory_gb": self._available_memory_gb,
                "active_jobs": len(self._active_jobs),
                "quotas": {u: q.to_dict() for u, q in self._quotas.items()},
            }

    @property
    def events(self) -> List[PulsarEvent]:
        return self._events
