"""Unit tests for PULSAR control plane components."""

import sys
import os
import pytest
from datetime import datetime, timedelta

# Add src to path
sys.path.insert(0, os.path.join(os.path.dirname(__file__), "..", "src"))

from pulsar.pulsar_types import GPUJob, JobStatus, JobPriority, UserQuota
from pulsar.queue_manager import QueueManager
from pulsar.fair_scheduler import FairScheduler
from pulsar.admission_controller import AdmissionController
from pulsar.preemption import PreemptionEngine
from pulsar.config import PulsarConfig
from pulsar.control_plane import PulsarControlPlane
from pulsar.metrics import MetricsCollector
from pulsar.executor import JobExecutor


# ─── Types ─────────────────────────────────────────────────

class TestGPUJob:
    def test_create_defaults(self):
        job = GPUJob(user="alice", gpu_required=4)
        assert job.user == "alice"
        assert job.gpu_required == 4
        assert job.status == JobStatus.QUEUED
        assert job.priority == JobPriority.NORMAL
        assert job.job_id.startswith("job-")

    def test_serialization(self):
        job = GPUJob(
            user="bob",
            gpu_required=2,
            priority=JobPriority.HIGH,
            preferred_gpu_class="dgpu",
            assigned_gpu_class="igpu",
            fallback_applied=True,
            fallback_reason="queue_timeout",
        )
        d = job.to_dict()
        assert d["user"] == "bob"
        assert d["gpu_required"] == 2
        assert d["priority"] == "HIGH"
        assert d["assigned_gpu_class"] == "igpu"
        assert d["fallback_applied"] is True

        restored = GPUJob.from_dict(d)
        assert restored.user == "bob"
        assert restored.priority == JobPriority.HIGH
        assert restored.assigned_gpu_class == "igpu"
        assert restored.fallback_reason == "queue_timeout"

    def test_user_quota(self):
        q = UserQuota(user="team-a", max_gpus=8, max_jobs=5)
        assert q.gpu_available == 8
        assert q.can_submit is True
        q.current_gpu_usage = 8
        assert q.gpu_available == 0
        q.current_job_count = 5
        assert q.can_submit is False


# ─── Queue Manager ─────────────────────────────────────────

class TestQueueManager:
    def test_submit_and_dequeue(self):
        qm = QueueManager()
        job = GPUJob(user="alice", gpu_required=2)
        qm.submit_job(job)
        assert qm.total_queued == 1

        dequeued = qm.get_next_job()
        assert dequeued is not None
        assert dequeued.job_id == job.job_id
        assert qm.total_queued == 0

    def test_fifo_ordering(self):
        qm = QueueManager()
        j1 = GPUJob(user="alice", gpu_required=1)
        j2 = GPUJob(user="alice", gpu_required=2)
        qm.submit_job(j1)
        qm.submit_job(j2)

        first = qm.get_next_job(policy="fifo")
        assert first.job_id == j1.job_id

    def test_priority_ordering(self):
        qm = QueueManager()
        j_low = GPUJob(user="alice", gpu_required=1, priority=JobPriority.LOW)
        j_high = GPUJob(user="bob", gpu_required=1, priority=JobPriority.HIGH)
        qm.submit_job(j_low)
        qm.submit_job(j_high)

        first = qm.get_next_job(policy="priority")
        assert first.job_id == j_high.job_id

    def test_cancel_job(self):
        qm = QueueManager()
        job = GPUJob(user="alice", gpu_required=2)
        qm.submit_job(job)
        cancelled = qm.cancel_job(job.job_id)
        assert cancelled is not None
        assert cancelled.status == JobStatus.CANCELLED
        assert qm.total_queued == 0

    def test_requeue(self):
        qm = QueueManager()
        j1 = GPUJob(user="alice", gpu_required=1)
        j2 = GPUJob(user="alice", gpu_required=2)
        qm.submit_job(j1)
        qm.submit_job(j2)

        dequeued = qm.get_next_job()
        qm.requeue_job(dequeued)
        front = qm.peek_next(user="alice")
        assert front.job_id == dequeued.job_id

    def test_fairness_dequeue(self):
        qm = QueueManager()
        qm.submit_job(GPUJob(user="alice", gpu_required=1))
        qm.submit_job(GPUJob(user="bob", gpu_required=1))

        scores = {"alice": 0.3, "bob": 0.9}
        first = qm.get_next_job(fairness_scores=scores)
        assert first.user == "bob"  # higher fairness score


# ─── Fair Scheduler ────────────────────────────────────────

class TestFairScheduler:
    def test_priority_computation(self):
        fs = FairScheduler()
        assert fs.get_priority("new_user") == 1.0
        fs.update_usage("alice", 4)
        assert fs.get_priority("alice") == 1.0 / 5.0

    def test_weighted_priority(self):
        fs = FairScheduler()
        fs.set_weight("alice", 2.0)
        fs.set_weight("bob", 1.0)
        fs.update_usage("alice", 4)
        fs.update_usage("bob", 4)
        # alice: 2.0 / 5.0 = 0.4, bob: 1.0 / 5.0 = 0.2
        assert fs.get_priority("alice") > fs.get_priority("bob")

    def test_jains_index(self):
        fs = FairScheduler()
        fs.update_usage("alice", 4)
        fs.update_usage("bob", 4)
        fi = fs.compute_jains_fairness_index()
        assert fi == 1.0  # equal usage = perfect fairness

    def test_jains_index_unequal(self):
        fs = FairScheduler()
        fs.update_usage("alice", 10)
        fs.update_usage("bob", 1)
        fi = fs.compute_jains_fairness_index()
        assert fi < 1.0

    def test_select_user(self):
        fs = FairScheduler()
        fs.update_usage("alice", 10)
        fs.update_usage("bob", 2)
        selected, score = fs.select_user(["alice", "bob"])
        assert selected == "bob"


# ─── Admission Controller ─────────────────────────────────

class TestAdmissionController:
    def test_admit_within_capacity(self):
        ac = AdmissionController(total_gpus=8)
        job = GPUJob(user="alice", gpu_required=4)
        can, reason = ac.can_admit(job)
        assert can is True

    def test_reject_over_capacity(self):
        ac = AdmissionController(total_gpus=4)
        job = GPUJob(user="alice", gpu_required=8)
        can, reason = ac.can_admit(job)
        assert can is False
        assert "Insufficient" in reason

    def test_quota_enforcement(self):
        ac = AdmissionController(total_gpus=16)
        ac.set_quota("alice", max_gpus=4, max_jobs=2)
        j1 = GPUJob(user="alice", gpu_required=3)
        ac.allocate(j1)
        j2 = GPUJob(user="alice", gpu_required=3)
        can, reason = ac.can_admit(j2)
        assert can is False
        assert "quota" in reason.lower()

    def test_allocate_and_release(self):
        ac = AdmissionController(total_gpus=8)
        job = GPUJob(user="alice", gpu_required=4)
        ac.allocate(job)
        assert ac.available_gpus == 4
        ac.release(job.job_id)
        assert ac.available_gpus == 8

    def test_preemption_victims(self):
        ac = AdmissionController(total_gpus=8)
        low = GPUJob(user="alice", gpu_required=4, priority=JobPriority.LOW, preemptible=True)
        ac.allocate(low)
        high = GPUJob(user="bob", gpu_required=8, priority=JobPriority.HIGH)
        victims = ac.find_preemption_victims(high)
        assert len(victims) == 1
        assert victims[0].job_id == low.job_id


# ─── Preemption Engine ─────────────────────────────────────

class TestPreemptionEngine:
    def test_should_preempt(self):
        pe = PreemptionEngine(enabled=True)
        high = GPUJob(user="bob", gpu_required=4, priority=JobPriority.HIGH)
        assert pe.should_preempt(high, available_gpus=2) is True

    def test_should_not_preempt_low_priority(self):
        pe = PreemptionEngine(enabled=True)
        low = GPUJob(user="bob", gpu_required=4, priority=JobPriority.LOW)
        assert pe.should_preempt(low, available_gpus=2) is False

    def test_select_victims(self):
        pe = PreemptionEngine(enabled=True)
        high = GPUJob(user="bob", gpu_required=4, priority=JobPriority.CRITICAL)
        active = {
            "j1": GPUJob(user="alice", gpu_required=2, priority=JobPriority.LOW, preemptible=True, job_id="j1"),
            "j2": GPUJob(user="alice", gpu_required=2, priority=JobPriority.NORMAL, preemptible=True, job_id="j2"),
        }
        victims, freed = pe.select_victims(high, active, available_gpus=0)
        assert freed >= 4
        assert len(victims) >= 1


# ─── Control Plane ─────────────────────────────────────────

class TestControlPlane:
    def _make_cp(self):
        config = PulsarConfig()
        config.cluster.total_gpus = 8
        config.persistence.enabled = False
        return PulsarControlPlane(config)

    def test_submit_and_process(self):
        cp = self._make_cp()
        cp.set_quota("alice", max_gpus=8, max_jobs=5)
        job = GPUJob(user="alice", gpu_required=2)
        cp.submit_job(job)
        scheduled = cp.process_all()
        assert len(scheduled) == 1
        assert scheduled[0].status == JobStatus.RUNNING

    def test_complete_job(self):
        cp = self._make_cp()
        cp.set_quota("alice", max_gpus=8, max_jobs=5)
        job = GPUJob(user="alice", gpu_required=2)
        cp.submit_job(job)
        cp.process_all()
        result = cp.complete_job(job.job_id)
        assert result is not None
        assert result.status == JobStatus.COMPLETED
        assert cp.admission_controller.available_gpus == 8

    def test_cancel_queued(self):
        cp = self._make_cp()
        job = GPUJob(user="alice", gpu_required=2)
        cp.submit_job(job)
        result = cp.cancel_job(job.job_id)
        assert result is not None
        assert result.status == JobStatus.CANCELLED

    def test_dashboard(self):
        cp = self._make_cp()
        d = cp.get_dashboard()
        assert "cluster" in d
        assert "fairness" in d
        assert "metrics" in d
        assert d["cluster"]["total_gpus"] == 8

    def test_prometheus_metrics(self):
        cp = self._make_cp()
        cp.set_quota("alice", max_gpus=8, max_jobs=5)
        cp.submit_job(GPUJob(user="alice", gpu_required=2))
        cp.process_all()
        prom = cp.get_prometheus_metrics()
        assert "pulsar_jobs_total" in prom
        assert "pulsar_cluster_gpus" in prom

    def test_fallback_queue_timeout(self):
        cp = self._make_cp()
        cp.set_quota("alice", max_gpus=8, max_jobs=5)
        cp.config.scheduling.fallback.max_dgpu_wait_seconds = 1
        cp.executor._has_dgpu = True
        cp.executor._has_igpu = True
        job = GPUJob(user="alice", gpu_required=1, preferred_gpu_class="dgpu")
        job.submitted_at = datetime.now() - timedelta(seconds=10)
        cp.submit_job(job)
        scheduled = cp.process_all()
        assert len(scheduled) == 1
        assert scheduled[0].assigned_gpu_class == "igpu"
        assert scheduled[0].fallback_applied is True
        assert scheduled[0].fallback_reason == "queue_timeout"


# ─── Metrics ───────────────────────────────────────────────

class TestMetrics:
    def test_export(self):
        mc = MetricsCollector()
        mc.record_submission("alice")
        mc.record_admission("alice", 4)
        mc.record_completion("alice", 4, 2.5)
        data = mc.export_metrics()
        assert data["jobs_submitted_total"] == 1
        assert data["jobs_admitted_total"] == 1
        assert data["jobs_completed_total"] == 1

    def test_prometheus_format(self):
        mc = MetricsCollector()
        mc.record_submission("alice")
        text = mc.generate_prometheus_metrics()
        assert "pulsar_jobs_total" in text

    def test_fallback_metrics(self):
        mc = MetricsCollector()
        mc.record_execution_lane("igpu", fallback_applied=True, fallback_reason="queue_timeout")
        data = mc.export_metrics()
        assert data["fallback_total"] == 1
        assert data["gpu_class_assignments"]["igpu"] == 1
        assert data["fallback_by_reason"]["queue_timeout"] == 1


# ─── Config ────────────────────────────────────────────────

class TestConfig:
    def test_defaults(self):
        cfg = PulsarConfig()
        assert cfg.cluster.total_gpus == 16
        assert cfg.scheduling.policy == "fair_share"
        assert cfg.scheduling.preemption.enabled is True

    def test_load_from_yaml(self, tmp_path):
        yaml_content = """
cluster:
  total_gpus: 32
  gpu_memory_gb: 80
  dgpu_resource_name: nvidia.com/gpu
  igpu_resource_name: gpu.intel.com/i915
scheduling:
  policy: priority
  fallback:
    preferred_gpu_class: dgpu
    fallback_gpu_class: igpu
    max_dgpu_wait_seconds: 30
  preemption:
    enabled: false
quotas:
  team-x:
    max_gpus: 16
    max_jobs: 10
    weight: 2.0
"""
        p = tmp_path / "test.yaml"
        p.write_text(yaml_content)
        cfg = PulsarConfig.from_yaml(str(p))
        assert cfg.cluster.total_gpus == 32
        assert cfg.scheduling.policy == "priority"
        assert cfg.scheduling.fallback.max_dgpu_wait_seconds == 30
        assert cfg.cluster.dgpu_resource_name == "nvidia.com/gpu"
        assert cfg.cluster.igpu_resource_name == "gpu.intel.com/i915"
        assert cfg.scheduling.preemption.enabled is False
        assert "team-x" in cfg.quotas
        assert cfg.quotas["team-x"].weight == 2.0


# ─── Executor ──────────────────────────────────────────────

class TestExecutor:
    def test_standalone_mode(self):
        ex = JobExecutor()
        job = GPUJob(user="alice", gpu_required=2)
        success, msg = ex.execute(job)
        assert success is True
        assert job.status == JobStatus.RUNNING
        assert "PID" in msg or "standalone" in msg
        ex.shutdown()  # kill the spawned process

    def test_terminate_standalone(self):
        ex = JobExecutor()
        job = GPUJob(user="alice", gpu_required=2)
        ex.execute(job)
        success, msg = ex.terminate(job)
        assert success is True
        ex.shutdown()

    def test_execution_info(self):
        ex = JobExecutor()
        job = GPUJob(user="alice", gpu_required=2)
        ex.execute(job)
        info = ex.get_execution_info(job.job_id)
        assert info is not None
        assert info["mode"] == "standalone"
        ex.shutdown()


# ─── Health ────────────────────────────────────────────────

class TestHealth:
    def _make_cp(self):
        config = PulsarConfig()
        config.cluster.total_gpus = 8
        config.persistence.enabled = False
        cp = PulsarControlPlane(config)
        cp._scheduler_running = True
        return cp

    def test_health_check(self):
        cp = self._make_cp()
        h = cp.health_check()
        assert h["status"] == "healthy"
        assert h["total_gpus"] == 8

    def test_readiness_check(self):
        cp = self._make_cp()
        r = cp.readiness_check()
        assert r["ready"] is True

    def test_fail_job(self):
        cp = self._make_cp()
        cp.set_quota("alice", max_gpus=8, max_jobs=5)
        job = GPUJob(user="alice", gpu_required=2)
        cp.submit_job(job)
        cp.process_all()
        result = cp.fail_job(job.job_id, "OOM")
        assert result is not None
        assert result.status == JobStatus.FAILED

    def test_dashboard_has_execution_mode(self):
        cp = self._make_cp()
        d = cp.get_dashboard()
        assert "execution_mode" in d
        assert d["execution_mode"] == "standalone"
