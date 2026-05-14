"""
PULSAR Control Plane

Production orchestrator. When a job is admitted, it is executed via the
JobExecutor (creates K8s pods with GPU resource requests — nvidia.com/gpu
or amd.com/gpu) and monitored by the JobWatcher (auto-completes when
pods finish).

On startup, recovers state from SQLite. On shutdown, drains gracefully.
"""

import logging
import time
import signal
import threading
from datetime import datetime
from typing import Dict, List, Optional

from pulsar.pulsar_types import GPUJob, JobStatus, UserQuota, PulsarEvent, SchedulingPolicy
from pulsar.queue_manager import QueueManager
from pulsar.queue_controller import QueueController
from pulsar.fair_scheduler import FairScheduler
from pulsar.admission_controller import AdmissionController
from pulsar.preemption import PreemptionEngine
from pulsar.metrics import MetricsCollector
from pulsar.persistence import JobStore
from pulsar.config import PulsarConfig
from pulsar.executor import JobExecutor, JobWatcher

logger = logging.getLogger("pulsar.control_plane")


class PulsarControlPlane:
    """
    PULSAR GPU Queue & Fairness Control Plane.

    Production control plane for shared AI clusters. When connected to
    Kubernetes, jobs create real GPU pods. When standalone, operates as
    a local queue & fairness system.

    Lifecycle:
        1. Load config from YAML
        2. Recover state from SQLite (if crash-restarted)
        3. Start background scheduler (processes queue every N seconds)
        4. Start job watcher (polls K8s pod status, auto-completes)
        5. Accept jobs via API / CLI / CRD
        6. On shutdown: drain scheduler, save state, stop watcher
    """

    def __init__(self, config: Optional[PulsarConfig] = None):
        self.config = config or PulsarConfig()
        self.config.setup_logging()

        # Core components
        self.metrics = MetricsCollector()
        self.queue_manager = QueueManager()
        self.queue_controller = QueueController(
            queue_manager=self.queue_manager,
            aging_enabled=self.config.scheduling.queue_controller.aging_enabled,
            aging_boost_interval_seconds=self.config.scheduling.queue_controller.aging_boost_interval_seconds,
            starvation_threshold_seconds=self.config.scheduling.queue_controller.starvation_threshold_seconds,
            preemption_signals=self.config.scheduling.preemption.enabled,
            max_queue_depth_per_tenant=self.config.scheduling.queue_controller.max_queue_depth_per_tenant,
            metrics=self.metrics,
        )
        self.fair_scheduler = FairScheduler()
        self.admission_controller = AdmissionController(
            total_gpus=self.config.cluster.total_gpus,
            gpu_memory_gb=self.config.cluster.gpu_memory_gb,
        )
        self.preemption_engine = PreemptionEngine(
            enabled=self.config.scheduling.preemption.enabled,
            max_per_cycle=self.config.scheduling.preemption.max_preemptions_per_cycle,
        )
        self.policy = self.config.scheduling.policy

        # Execution layer — creates real K8s pods when available
        # In standalone mode, jobs auto-complete via timer callbacks
        self.executor = JobExecutor(
            gpu_resource_name=self.config.cluster.gpu_resource_name,
            dgpu_resource_name=self.config.cluster.dgpu_resource_name,
            igpu_resource_name=self.config.cluster.igpu_resource_name,
            on_complete=self._on_job_completed,
            on_fail=self._on_job_failed,
            config=self.config,
        )
        self.watcher = JobWatcher(
            self.executor,
            on_complete=self._on_job_completed,
            on_fail=self._on_job_failed,
        )

        # Persistence
        self._store: Optional[JobStore] = None
        if self.config.persistence.enabled:
            self._store = JobStore(self.config.persistence.database)

        # State
        self._scheduled_jobs: Dict[str, GPUJob] = {}
        self._completed_jobs: List[GPUJob] = []
        self._all_jobs: Dict[str, GPUJob] = {}
        self._terminated_log: List[dict] = []  # recent termination events for dashboard
        self._lock = threading.RLock()
        self._scheduler_running = False

        # Apply quotas and weights from config
        for user, qcfg in self.config.quotas.items():
            self.admission_controller.set_quota(
                user, max_gpus=qcfg.max_gpus, max_jobs=qcfg.max_jobs, weight=qcfg.weight
            )
            self.fair_scheduler.set_weight(user, qcfg.weight)

        # Set total cluster resources for DRF
        self.fair_scheduler.set_total_resources(
            self.config.cluster.total_gpus,
            self.config.cluster.total_gpus * self.config.cluster.gpu_memory_gb,
        )

        # Recover state from persistence
        self._recover_state()

        mode = "K8s" if self.executor.is_k8s_available else "standalone"
        logger.info(
            "PULSAR Control Plane initialized: %d GPUs, policy=%s, "
            "preemption=%s, mode=%s",
            self.config.cluster.total_gpus, self.policy,
            self.config.scheduling.preemption.enabled, mode,
        )

    def _recover_state(self):
        """Recover active jobs from SQLite on restart."""
        if not self._store:
            return
        try:
            running = self._store.list_jobs(status="RUNNING", limit=1000)
            queued = self._store.list_jobs(status="QUEUED", limit=1000)
            recovered = 0

            for job in running:
                # Re-track as active (resource accounting)
                self._all_jobs[job.job_id] = job
                self._scheduled_jobs[job.job_id] = job
                # Re-apply to admission controller
                self.admission_controller.allocate(job)
                self.fair_scheduler.update_usage(job.user, job.gpu_required)
                self.fair_scheduler.update_resource_usage(job.user, job.gpu_required, job.gpu_memory_gb)
                
                # Restart the execution process so it can complete
                success, _ = self.executor.execute(job)
                if not success:
                    self.admission_controller.release(job.job_id)
                    self.fair_scheduler.release_usage(job.user, job.gpu_required)
                    self.fair_scheduler.release_resource_usage(job.user, job.gpu_required, job.gpu_memory_gb)
                    job.status = JobStatus.FAILED
                recovered += 1

            for job in queued:
                self._all_jobs[job.job_id] = job
                self.queue_controller.submit_job(job)
                recovered += 1

            if recovered > 0:
                logger.info("State recovered: %d jobs (%d running, %d queued)",
                            recovered, len(running), len(queued))
        except Exception:
            logger.exception("State recovery failed — starting fresh")

    # ─── Quota Management ──────────────────────────────────────

    def set_quota(self, user: str, max_gpus: int = 8, max_jobs: int = 10,
                  weight: float = 1.0):
        """Set per-user GPU quota and fair-share weight."""
        self.admission_controller.set_quota(user, max_gpus, max_jobs, weight)
        self.fair_scheduler.set_weight(user, weight)
        logger.info("Quota set for %s: max_gpus=%d, max_jobs=%d, weight=%.2f",
                     user, max_gpus, max_jobs, weight)

    # ─── Job Lifecycle ──────────────────────────────────────────

    def submit_job(self, job: GPUJob) -> GPUJob:
        """Submit a GPU job to the PULSAR queue."""
        if not job.preferred_gpu_class:
            job.preferred_gpu_class = self.config.scheduling.fallback.preferred_gpu_class
        job.preferred_gpu_class = job.preferred_gpu_class.lower()
        with self._lock:
            self._all_jobs[job.job_id] = job
        self.metrics.record_submission(job.user)
        self.queue_controller.submit_job(job)
        if self._store:
            self._store.save_job(job)
        logger.info("Job %s submitted by %s (%d GPUs, %s, priority=%s)",
                     job.job_id, job.user, job.gpu_required,
                     job.workload_type, job.priority.name)
        return job

    def cancel_job(self, job_id: str):
        """Cancel a job (queued or running) and stop its process."""
        with self._lock:
            job = self._all_jobs.get(job_id)
            if not job:
                logger.warning("Cannot cancel job %s — not found", job_id)
                return None

            logger.info("Cancelling job %s (status=%s)", job_id, job.status.value)

            # Stop the process if it's running
            self.executor.terminate(job)

            # Update status
            job.status = JobStatus.CANCELLED
            job.completed_at = datetime.now()

            # Release resources if the job was scheduled/running
            if job_id in self._scheduled_jobs:
                self._scheduled_jobs.pop(job_id)
                self.admission_controller.release(job_id)
                self.fair_scheduler.release_usage(job.user, job.gpu_required)
                self.fair_scheduler.release_resource_usage(job.user, job.gpu_required, job.gpu_memory_gb)
                logger.info("Released resources for job %s", job_id)

            # Notify queue to remove if queued
            self.queue_controller.cancel_job(job_id)

            # Persist
            if self._store:
                self._store.save_job(job)

            self._log_termination(job, "CANCELLED", "user cancelled")
            return job

    def get_job(self, job_id: str) -> Optional[GPUJob]:
        """Get a job by ID (in-memory first, then persistence)."""
        with self._lock:
            job = self._all_jobs.get(job_id)
            if job:
                return job
        if self._store:
            return self._store.load_job(job_id)
        return None

    def list_jobs(self, user: Optional[str] = None,
                  status: Optional[str] = None) -> List[GPUJob]:
        """List jobs with optional filters."""
        with self._lock:
            jobs = list(self._all_jobs.values())
        if user:
            jobs = [j for j in jobs if j.user == user]
        if status:
            jobs = [j for j in jobs if j.status.value == status]
        jobs.sort(key=lambda j: j.submitted_at, reverse=True)
        return jobs

    def complete_job(self, job_id: str) -> Optional[GPUJob]:
        """Mark a job as completed and release resources."""
        with self._lock:
            job = self._scheduled_jobs.pop(job_id, None)
        if not job:
            return None

        # Terminate K8s resources if any
        self.executor.terminate(job)
        self.admission_controller.release(job_id)
        self.fair_scheduler.release_usage(job.user, job.gpu_required)
        self.fair_scheduler.release_resource_usage(job.user, job.gpu_required, job.gpu_memory_gb)

        gpu_hours = 0.0
        if job.admitted_at:
            gpu_hours = job.gpu_required * (datetime.now() - job.admitted_at).total_seconds() / 3600
        self.metrics.record_completion(job.user, job.gpu_required, gpu_hours)

        job.status = JobStatus.COMPLETED
        job.completed_at = datetime.now()
        self._completed_jobs.append(job)
        self._log_termination(job, "COMPLETED")

        if self._store:
            self._store.save_job(job)

        logger.info("Job %s completed (%.2f GPU-hours)", job_id, gpu_hours)
        return job

    def fail_job(self, job_id: str, reason: str = "unknown") -> Optional[GPUJob]:
        """Mark a job as failed and release resources."""
        with self._lock:
            job = self._scheduled_jobs.pop(job_id, None)
        if not job:
            return None

        self.executor.terminate(job)
        self.admission_controller.release(job_id)
        self.fair_scheduler.release_usage(job.user, job.gpu_required)
        self.fair_scheduler.release_resource_usage(job.user, job.gpu_required, job.gpu_memory_gb)

        job.status = JobStatus.FAILED
        job.completed_at = datetime.now()
        self.metrics.record_rejection(job.user)
        self._log_termination(job, "FAILED", reason)

        if self._store:
            self._store.save_job(job)

        logger.error("Job %s FAILED: %s", job_id, reason)
        return job

    # ─── Callbacks from JobWatcher ──────────────────────────────

    def _on_job_completed(self, job_id: str):
        """Called by JobWatcher when a K8s pod succeeds."""
        self.complete_job(job_id)

    def _on_job_failed(self, job_id: str, reason: str):
        """Called by JobWatcher when a K8s pod fails."""
        self.fail_job(job_id, reason)

    def _apply_gpu_fallback_policy(self, job: GPUJob):
        """Decide assigned GPU class for this scheduling cycle."""
        cfg = self.config.scheduling.fallback
        preferred = (job.preferred_gpu_class or cfg.preferred_gpu_class or "dgpu").lower()
        fallback = (cfg.fallback_gpu_class or "igpu").lower()

        wait_seconds = 0.0
        if job.submitted_at:
            wait_seconds = (datetime.now() - job.submitted_at).total_seconds()

        assigned = preferred
        fallback_applied = False
        fallback_reason = None

        timeout = cfg.max_dgpu_wait_seconds or 0.0
        if (
            preferred == "dgpu"
            and fallback == "igpu"
            and timeout > 0
            and wait_seconds >= timeout
        ):
            assigned = "igpu"
            fallback_applied = True
            fallback_reason = "queue_timeout"

        if not self.executor.is_k8s_available and not fallback_applied:
            if assigned == "dgpu" and not self.executor.has_dgpu:
                if self.executor.has_igpu and fallback == "igpu":
                    assigned = "igpu"
                    fallback_applied = True
                    fallback_reason = "no_capacity"
                else:
                    assigned = "cpu"
                    fallback_applied = True
                    fallback_reason = "no_capacity"
            elif assigned == "igpu" and not self.executor.has_igpu:
                if self.executor.has_dgpu:
                    assigned = "dgpu"
                    fallback_applied = True
                    fallback_reason = "no_capacity"
                else:
                    assigned = "cpu"
                    fallback_applied = True
                    fallback_reason = "no_capacity"

        job.preferred_gpu_class = preferred
        job.assigned_gpu_class = assigned
        job.fallback_applied = fallback_applied
        job.fallback_reason = fallback_reason
        job.fallback_decided_at = datetime.now()

    # ─── Scheduling Loop ────────────────────────────────────────

    def process_queue(self) -> Optional[GPUJob]:
        """
        Process the next job from the queue. Core scheduling cycle.

        Flow: Queue → Fairness Selection → Admission → Execution → Track
        """
        while True:
            if self.queue_controller.total_queued == 0:
                return None

            start_time = time.time()

            users_with_jobs = self.queue_controller.users_with_jobs
            if not users_with_jobs:
                logger.debug("No users with jobs despite total_queued=%d", self.queue_controller.total_queued)
                return None

            fairness_scores = {
                user: self.fair_scheduler.get_priority(user)
                for user in users_with_jobs
            }

            # Handle backfill policy
            if self.policy == "backfill":
                return self._process_backfill(fairness_scores, start_time)

            # Dequeue using configured policy
            job = self.queue_controller.get_next_job(fairness_scores, policy=self.policy)
            if not job:
                return None

            # Admission check
            can_admit, reason = self.admission_controller.can_admit(job)

            if not can_admit:
                # Try preemption for high-priority jobs
                if self.preemption_engine.should_preempt(
                    job, self.admission_controller.available_gpus
                ):
                    active = self.admission_controller.get_active_jobs()
                    victims, freed = self.preemption_engine.select_victims(
                        job, active, self.admission_controller.available_gpus
                    )
                    if victims:
                        for v in victims:
                            evicted = self.admission_controller.force_release(v.job_id)
                            if evicted:
                                # Terminate the K8s pod
                                self.executor.terminate(evicted)
                                with self._lock:
                                    self._scheduled_jobs.pop(v.job_id, None)
                                self.fair_scheduler.release_usage(v.user, v.gpu_required)
                                self.fair_scheduler.release_resource_usage(v.user, v.gpu_required, v.gpu_memory_gb)
                                self.metrics.record_preemption(v.user)
                                self.queue_controller.requeue_job(evicted)
                                if self._store:
                                    self._store.save_job(evicted)

                        can_admit, reason = self.admission_controller.can_admit(job)

                if not can_admit:
                    if "PERMANENT_REJECT" not in reason:
                        self.queue_controller.requeue_job(job)
                        return None
                    else:
                        job.status = JobStatus.REJECTED
                        self.metrics.record_rejection(job.user)
                        self.admission_controller.allocate(job)
                        if self._store:
                            self._store.save_job(job)
                        continue  # Process the next job in the queue instead of returning None

            # Admit the job
            self.admission_controller.allocate(job)
            self.fair_scheduler.update_usage(job.user, job.gpu_required)
            self.fair_scheduler.update_resource_usage(job.user, job.gpu_required, job.gpu_memory_gb)
            self.metrics.record_admission(job.user, job.gpu_required)

            # Track fairness
            fi = self.fair_scheduler.compute_jains_fairness_index()
            self.metrics.record_fairness_index(fi)
            drf_shares = self.fair_scheduler.compute_drf_shares()
            for user, drf_data in drf_shares.items():
                self.metrics.record_drf_dominant_share(user, drf_data.get("dominant_share", 0.0))
                # approximate GPU share from active allocations
                active = self.fair_scheduler._active_allocations.get(user, 0)
                total_gpus = max(1, self.config.cluster.total_gpus)
                self.metrics.record_gpu_share(user, active / total_gpus)

            # Queue wait time
            if job.submitted_at:
                wait = (datetime.now() - job.submitted_at).total_seconds()
                self.metrics.record_queue_wait(wait)

            # Decide execution lane (dgpu / igpu / cpu) with fallback policy.
            self._apply_gpu_fallback_policy(job)

            # ═══ EXECUTE: create real K8s pod ═══
            success, exec_msg = self.executor.execute(job)
            if not success:
                # Execution failed — release resources and requeue
                self.admission_controller.release(job.job_id)
                self.fair_scheduler.release_usage(job.user, job.gpu_required)
                self.fair_scheduler.release_resource_usage(job.user, job.gpu_required, job.gpu_memory_gb)
                job.status = JobStatus.FAILED
                if self._store:
                    self._store.save_job(job)
                logger.error("Job %s execution failed: %s", job.job_id, exec_msg)
                return None

            # Track as running
            with self._lock:
                self._scheduled_jobs[job.job_id] = job

            if self._store:
                self._store.save_job(job)

            self.metrics.record_execution_lane(
                gpu_class=job.assigned_gpu_class or "unknown",
                fallback_applied=job.fallback_applied,
                fallback_reason=job.fallback_reason or "",
            )
            if job.assigned_gpu_class:
                self.metrics.record_gpu_class_job(job.assigned_gpu_class)

            latency_ms = (time.time() - start_time) * 1000
            self.metrics.record_scheduling_latency(latency_ms)

            logger.info(
                "[SCHEDULE] %s → %d GPUs (user=%s, policy=%s, mode=%s, latency=%.1fms)",
                job.job_id, job.gpu_required, job.user, self.policy,
                "k8s" if self.executor.is_k8s_available else "standalone",
                latency_ms,
            )
            return job

    def _process_backfill(self, fairness_scores, start_time) -> Optional[GPUJob]:
        """Backfill scheduling: fit small jobs into available capacity."""
        available = self.admission_controller.available_gpus
        candidates = self.queue_controller.queue_manager.get_backfill_candidates(available)

        if not candidates:
            job = self.queue_controller.get_next_job(fairness_scores, policy="fair_share")
            if job:
                can, _ = self.admission_controller.can_admit(job)
                if not can:
                    self.queue_controller.requeue_job(job)
                    return None
                self.admission_controller.allocate(job)
                self.fair_scheduler.update_usage(job.user, job.gpu_required)
                self.fair_scheduler.update_resource_usage(job.user, job.gpu_required, job.gpu_memory_gb)
                self.metrics.record_admission(job.user, job.gpu_required)
                self._apply_gpu_fallback_policy(job)
                success, _ = self.executor.execute(job)
                if not success:
                    self.admission_controller.release(job.job_id)
                    self.fair_scheduler.release_usage(job.user, job.gpu_required)
                    self.fair_scheduler.release_resource_usage(job.user, job.gpu_required, job.gpu_memory_gb)
                    job.status = JobStatus.FAILED
                    if self._store:
                        self._store.save_job(job)
                    return None
                self.metrics.record_execution_lane(
                    gpu_class=job.assigned_gpu_class or "unknown",
                    fallback_applied=job.fallback_applied,
                    fallback_reason=job.fallback_reason or "",
                )
                with self._lock:
                    self._scheduled_jobs[job.job_id] = job
                return job
            return None

        best = None
        best_score = -1
        for c in candidates:
            score = fairness_scores.get(c.user, 0.5)
            if score > best_score:
                best = c
                best_score = score

        if best:
            job = self.queue_controller.queue_manager.remove_backfill_job(best.job_id)
            if job:
                self.admission_controller.allocate(job)
                self.fair_scheduler.update_usage(job.user, job.gpu_required)
                self.fair_scheduler.update_resource_usage(job.user, job.gpu_required, job.gpu_memory_gb)
                self.metrics.record_admission(job.user, job.gpu_required)
                self._apply_gpu_fallback_policy(job)
                success, _ = self.executor.execute(job)
                if not success:
                    self.admission_controller.release(job.job_id)
                    self.fair_scheduler.release_usage(job.user, job.gpu_required)
                    self.fair_scheduler.release_resource_usage(job.user, job.gpu_required, job.gpu_memory_gb)
                    job.status = JobStatus.FAILED
                    if self._store:
                        self._store.save_job(job)
                    return None
                self.metrics.record_execution_lane(
                    gpu_class=job.assigned_gpu_class or "unknown",
                    fallback_applied=job.fallback_applied,
                    fallback_reason=job.fallback_reason or "",
                )
                with self._lock:
                    self._scheduled_jobs[job.job_id] = job
                logger.info("[BACKFILL] %s (%d GPUs)", job.job_id, job.gpu_required)
                return job

        return None

    def process_all(self) -> List[GPUJob]:
        """Process all queued jobs until queue is empty or no capacity."""
        scheduled = []
        max_iters = self.queue_controller.total_queued + 10
        iters = 0
        while self.queue_controller.total_queued > 0 and iters < max_iters:
            job = self.process_queue()
            if job:
                scheduled.append(job)
            else:
                break
            iters += 1
        return scheduled

    # ─── Background Scheduler ──────────────────────────────────

    def start_scheduler(self):
        """Start the background scheduling loop and job watcher."""
        if self._scheduler_running:
            return
        self._scheduler_running = True

        # Queue controller (aging, starvation prevention, preemption signals)
        self.queue_controller.start()

        # Scheduler thread
        sched_thread = threading.Thread(target=self._scheduler_loop, daemon=True)
        sched_thread.start()

        # Job watcher (monitors K8s pod status)
        self.watcher.start()

        logger.info("Background scheduler started (interval=%.1fs)",
                     self.config.scheduling.scheduling_interval_seconds)

    def stop_scheduler(self):
        """Graceful shutdown: stop scheduler, watcher, save state."""
        logger.info("Shutting down PULSAR control plane...")
        self._scheduler_running = False
        self.queue_controller.stop()
        self.watcher.stop()

        # Save all active job state
        if self._store:
            with self._lock:
                for job in self._scheduled_jobs.values():
                    self._store.save_job(job)
            logger.info("Active job state saved to persistence")

        logger.info("PULSAR control plane stopped")

    def _scheduler_loop(self):
        interval = self.config.scheduling.scheduling_interval_seconds
        while self._scheduler_running:
            try:
                self.process_all()
            except Exception:
                logger.exception("Scheduler loop error")
            time.sleep(interval)

    # ─── Health Checks ─────────────────────────────────────────

    def health_check(self) -> dict:
        """Liveness check — returns basic health status."""
        return {
            "status": "healthy",
            "scheduler_running": self._scheduler_running,
            "k8s_connected": self.executor.is_k8s_available,
            "persistence_enabled": self._store is not None,
            "total_gpus": self.config.cluster.total_gpus,
            "policy": self.policy,
        }

    def readiness_check(self) -> dict:
        """Readiness check — verifies the system can accept workloads."""
        ready = True
        checks = {}

        checks["scheduler"] = self._scheduler_running
        if not self._scheduler_running:
            ready = False

        checks["admission_controller"] = self.admission_controller.available_gpus >= 0
        checks["queue_manager"] = True

        if self._store:
            try:
                self._store.get_job_count_by_status()
                checks["persistence"] = True
            except Exception:
                checks["persistence"] = False
                ready = False

        return {"ready": ready, "checks": checks}

    def _log_termination(self, job: GPUJob, status: str, reason: Optional[str] = None):
        """Log a job termination event for the dashboard."""
        self._terminated_log.append({
            "job_id": job.job_id,
            "user": job.user,
            "status": status,
            "reason": reason,
            "time": datetime.now().isoformat()
        })
        if len(self._terminated_log) > 100:
            self._terminated_log.pop(0)

    # ─── Dashboard & Status ────────────────────────────────────

    def get_dashboard(self) -> dict:
        with self._lock:
            active = {
                jid: j.to_dict() for jid, j in self._scheduled_jobs.items()
            }
        return {
            "cluster": self.admission_controller.get_cluster_status(),
            "queues": self.queue_controller.get_queue_status(),
            "fairness": self.fair_scheduler.get_fairness_report(),
            "fairness_index": self.fair_scheduler.compute_jains_fairness_index(),
            "metrics": self.metrics.export_metrics(),
            "active_jobs": active,
            "active_job_count": len(active),
            "completed_jobs": len(self._completed_jobs),
            "scheduling_policy": self.policy,
            "preemption_enabled": self.preemption_engine.enabled,
            "preemption_count": self.preemption_engine.preemption_count,
            "queue_controller": {
                "aging_enabled": self.queue_controller.aging_enabled,
                "aging_interval_seconds": self.queue_controller.aging_boost_interval_seconds,
                "starvation_threshold_seconds": self.queue_controller.starvation_threshold_seconds,
                "preemption_signals": self.queue_controller.preemption_signals,
                "aging_status": self.queue_controller.get_aging_status(),
            },
            "execution_mode": "kubernetes" if self.executor.is_k8s_available else "standalone",
            "has_dgpu": self.executor.has_dgpu,
            "has_igpu": self.executor.has_igpu,
            "dgpu_name": getattr(self.executor, '_dgpu_name', ''),
            "igpu_name": getattr(self.executor, '_igpu_name', ''),
            "cpu_mode": not (self.executor.has_dgpu or self.executor.has_igpu),
            "dgpu_resource_name": self.executor.dgpu_resource_name,
            "igpu_resource_name": self.executor.igpu_resource_name,
            "nvidia_smi": self.executor.get_nvidia_smi(),
            "active_pids": self.executor.get_active_pids(),
            "terminated_log": list(self._terminated_log),
        }

    def get_prometheus_metrics(self) -> str:
        cluster = self.admission_controller.get_cluster_status()
        return self.metrics.generate_prometheus_metrics(cluster)
