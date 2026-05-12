"""
PULSAR Metrics Collector

Production metrics with Prometheus-format export, histograms,
per-user tracking, and fairness index monitoring.
"""

import time
import threading
from collections import defaultdict
from datetime import datetime
from typing import Dict, List


class MetricsCollector:
    """Collects and exports PULSAR control plane metrics."""

    def __init__(self):
        self._lock = threading.Lock()
        self.jobs_submitted: int = 0
        self.jobs_admitted: int = 0
        self.jobs_rejected: int = 0
        self.jobs_completed: int = 0
        self.jobs_preempted: int = 0
        self.jobs_cancelled: int = 0
        self.total_gpus_allocated: int = 0
        self.total_gpus_released: int = 0
        self._per_user_submissions: Dict[str, int] = defaultdict(int)
        self._per_user_gpu_hours: Dict[str, float] = defaultdict(float)
        self._gpu_class_assignments: Dict[str, int] = defaultdict(int)
        self._fallback_reasons: Dict[str, int] = defaultdict(int)
        self._fallback_total: int = 0
        self._scheduling_latencies: List[float] = []
        self._queue_wait_times: List[float] = []
        self._started_at: datetime = datetime.now()
        self._fairness_index_history: List[float] = []

        # New metrics: queue depth, wait time histograms, gpu share, per-tenant preemptions
        self._queue_depth_by_tenant: Dict[str, int] = defaultdict(int)
        self._queue_depth_by_gpu_class: Dict[str, int] = defaultdict(int)
        self._wait_time_buckets: Dict[str, int] = defaultdict(int)
        self._gpu_share_by_tenant: Dict[str, float] = defaultdict(float)
        self._per_tenant_preemptions: Dict[str, int] = defaultdict(int)
        self._aging_boosts_total: int = 0
        self._starvation_events_total: int = 0
        self._preemption_signals_total: int = 0
        self._drf_dominant_share_by_tenant: Dict[str, float] = defaultdict(float)
        self._dgpu_jobs_total: int = 0
        self._igpu_jobs_total: int = 0

        # Scheduler Extender metrics
        self._extender_filter_latencies: List[float] = []
        self._extender_bind_latencies: List[float] = []
        self._pod_annotations_injected: int = 0

    def record_submission(self, user: str):
        with self._lock:
            self.jobs_submitted += 1
            self._per_user_submissions[user] += 1

    def record_admission(self, user: str, gpus: int):
        with self._lock:
            self.jobs_admitted += 1
            self.total_gpus_allocated += gpus

    def record_rejection(self, user: str):
        with self._lock:
            self.jobs_rejected += 1

    def record_completion(self, user: str, gpus: int, gpu_hours: float):
        with self._lock:
            self.jobs_completed += 1
            self.total_gpus_released += gpus
            self._per_user_gpu_hours[user] += gpu_hours

    def record_preemption(self, user: str):
        with self._lock:
            self.jobs_preempted += 1
            self._per_tenant_preemptions[user] += 1

    def record_cancellation(self):
        with self._lock:
            self.jobs_cancelled += 1

    def record_aging_boost(self):
        with self._lock:
            self._aging_boosts_total += 1

    def record_starvation_event(self):
        with self._lock:
            self._starvation_events_total += 1

    def record_preemption_signal(self):
        with self._lock:
            self._preemption_signals_total += 1

    def record_queue_depth(self, tenant: str, gpu_class: str, depth: int):
        with self._lock:
            self._queue_depth_by_tenant[tenant] = depth
            self._queue_depth_by_gpu_class[gpu_class] += depth

    def record_wait_time_bucket(self, seconds: float):
        with self._lock:
            if seconds < 10:
                self._wait_time_buckets["le_10s"] += 1
            elif seconds < 60:
                self._wait_time_buckets["le_60s"] += 1
            elif seconds < 300:
                self._wait_time_buckets["le_300s"] += 1
            elif seconds < 600:
                self._wait_time_buckets["le_600s"] += 1
            else:
                self._wait_time_buckets["le_inf"] += 1

    def record_gpu_share(self, tenant: str, share: float):
        with self._lock:
            self._gpu_share_by_tenant[tenant] = share

    def record_drf_dominant_share(self, tenant: str, share: float):
        with self._lock:
            self._drf_dominant_share_by_tenant[tenant] = share

    def record_gpu_class_job(self, gpu_class: str):
        with self._lock:
            if gpu_class == "dgpu":
                self._dgpu_jobs_total += 1
            elif gpu_class == "igpu":
                self._igpu_jobs_total += 1

    def record_extender_filter_latency(self, latency_ms: float):
        with self._lock:
            self._extender_filter_latencies.append(latency_ms)

    def record_extender_bind_latency(self, latency_ms: float):
        with self._lock:
            self._extender_bind_latencies.append(latency_ms)

    def record_pod_annotation_injected(self):
        with self._lock:
            self._pod_annotations_injected += 1

    def record_scheduling_latency(self, latency_ms: float):
        with self._lock:
            self._scheduling_latencies.append(latency_ms)

    def record_queue_wait(self, wait_seconds: float):
        with self._lock:
            self._queue_wait_times.append(wait_seconds)

    def record_fairness_index(self, index: float):
        with self._lock:
            self._fairness_index_history.append(index)

    def record_execution_lane(
        self,
        gpu_class: str,
        fallback_applied: bool = False,
        fallback_reason: str = "",
    ):
        with self._lock:
            lane = gpu_class or "unknown"
            self._gpu_class_assignments[lane] += 1
            if fallback_applied:
                self._fallback_total += 1
                reason = fallback_reason or "unknown"
                self._fallback_reasons[reason] += 1

    def export_metrics(self) -> dict:
        with self._lock:
            uptime = (datetime.now() - self._started_at).total_seconds()
            avg_wait = (
                sum(self._queue_wait_times) / len(self._queue_wait_times)
                if self._queue_wait_times else 0.0
            )
            avg_latency = (
                sum(self._scheduling_latencies) / len(self._scheduling_latencies)
                if self._scheduling_latencies else 0.0
            )
            p99_latency = (
                sorted(self._scheduling_latencies)[int(len(self._scheduling_latencies) * 0.99)]
                if self._scheduling_latencies else 0.0
            )
            avg_filter_latency = (
                sum(self._extender_filter_latencies) / len(self._extender_filter_latencies)
                if self._extender_filter_latencies else 0.0
            )
            avg_bind_latency = (
                sum(self._extender_bind_latencies) / len(self._extender_bind_latencies)
                if self._extender_bind_latencies else 0.0
            )
            current_fairness = (
                self._fairness_index_history[-1]
                if self._fairness_index_history else 1.0
            )

            return {
                "pulsar_uptime_seconds": round(uptime, 1),
                "jobs_submitted_total": self.jobs_submitted,
                "jobs_admitted_total": self.jobs_admitted,
                "jobs_rejected_total": self.jobs_rejected,
                "jobs_completed_total": self.jobs_completed,
                "jobs_preempted_total": self.jobs_preempted,
                "jobs_cancelled_total": self.jobs_cancelled,
                "admission_rate": round(
                    self.jobs_admitted / self.jobs_submitted if self.jobs_submitted > 0 else 0.0, 4
                ),
                "gpus_allocated_total": self.total_gpus_allocated,
                "gpus_released_total": self.total_gpus_released,
                "avg_queue_wait_seconds": round(avg_wait, 3),
                "avg_scheduling_latency_ms": round(avg_latency, 3),
                "p99_scheduling_latency_ms": round(p99_latency, 3),
                "jains_fairness_index": round(current_fairness, 4),
                "fallback_total": self._fallback_total,
                "gpu_class_assignments": dict(self._gpu_class_assignments),
                "fallback_by_reason": dict(self._fallback_reasons),
                "per_user_gpu_hours": {k: round(v, 2) for k, v in self._per_user_gpu_hours.items()},
                "per_user_submissions": dict(self._per_user_submissions),
                # New metrics
                "queue_depth_by_tenant": dict(self._queue_depth_by_tenant),
                "queue_depth_by_gpu_class": dict(self._queue_depth_by_gpu_class),
                "wait_time_buckets": dict(self._wait_time_buckets),
                "gpu_share_by_tenant": {k: round(v, 6) for k, v in self._gpu_share_by_tenant.items()},
                "per_tenant_preemptions": dict(self._per_tenant_preemptions),
                "aging_boosts_total": self._aging_boosts_total,
                "starvation_events_total": self._starvation_events_total,
                "preemption_signals_total": self._preemption_signals_total,
                "drf_dominant_share_by_tenant": {k: round(v, 6) for k, v in self._drf_dominant_share_by_tenant.items()},
                "dgpu_jobs_total": self._dgpu_jobs_total,
                "igpu_jobs_total": self._igpu_jobs_total,
                "avg_extender_filter_latency_ms": round(avg_filter_latency, 3),
                "avg_extender_bind_latency_ms": round(avg_bind_latency, 3),
                "pod_annotations_injected_total": self._pod_annotations_injected,
            }

    def generate_prometheus_metrics(self, cluster_status: dict = None) -> str:
        """Generate Prometheus text-format metrics."""
        data = self.export_metrics()
        lines = [
            "# HELP pulsar_uptime_seconds Time since PULSAR started",
            "# TYPE pulsar_uptime_seconds gauge",
            f'pulsar_uptime_seconds {data["pulsar_uptime_seconds"]}',
            "",
            "# HELP pulsar_jobs_total Total jobs by status",
            "# TYPE pulsar_jobs_total counter",
            f'pulsar_jobs_total{{status="submitted"}} {data["jobs_submitted_total"]}',
            f'pulsar_jobs_total{{status="admitted"}} {data["jobs_admitted_total"]}',
            f'pulsar_jobs_total{{status="rejected"}} {data["jobs_rejected_total"]}',
            f'pulsar_jobs_total{{status="completed"}} {data["jobs_completed_total"]}',
            f'pulsar_jobs_total{{status="preempted"}} {data["jobs_preempted_total"]}',
            f'pulsar_jobs_total{{status="cancelled"}} {data["jobs_cancelled_total"]}',
            "",
            "# HELP pulsar_admission_rate Ratio of admitted to submitted jobs",
            "# TYPE pulsar_admission_rate gauge",
            f'pulsar_admission_rate {data["admission_rate"]}',
            "",
            "# HELP pulsar_gpus_allocated_total Total GPUs allocated over time",
            "# TYPE pulsar_gpus_allocated_total counter",
            f'pulsar_gpus_allocated_total {data["gpus_allocated_total"]}',
            "",
            "# HELP pulsar_scheduling_latency_ms Average scheduling latency",
            "# TYPE pulsar_scheduling_latency_ms gauge",
            f'pulsar_scheduling_latency_ms{{quantile="avg"}} {data["avg_scheduling_latency_ms"]}',
            f'pulsar_scheduling_latency_ms{{quantile="p99"}} {data["p99_scheduling_latency_ms"]}',
            "",
            "# HELP pulsar_jains_fairness_index Current Jain\'s Fairness Index",
            "# TYPE pulsar_jains_fairness_index gauge",
            f'pulsar_jains_fairness_index {data["jains_fairness_index"]}',
            "",
            "# HELP pulsar_gpu_class_assignments_total Jobs assigned by GPU class",
            "# TYPE pulsar_gpu_class_assignments_total counter",
        ]
        for gpu_class, count in data.get("gpu_class_assignments", {}).items():
            lines.append(
                f'pulsar_gpu_class_assignments_total{{gpu_class="{gpu_class}"}} {count}'
            )

        lines.extend([
            "",
            "# HELP pulsar_gpu_fallback_total Jobs where fallback policy was applied",
            "# TYPE pulsar_gpu_fallback_total counter",
            f'pulsar_gpu_fallback_total {data.get("fallback_total", 0)}',
            "",
            "# HELP pulsar_gpu_fallback_reason_total Fallback count by reason",
            "# TYPE pulsar_gpu_fallback_reason_total counter",
        ])
        for reason, count in data.get("fallback_by_reason", {}).items():
            lines.append(
                f'pulsar_gpu_fallback_reason_total{{reason="{reason}"}} {count}'
            )

        # Per-user GPU hours
        lines.append("")
        lines.append("# HELP pulsar_user_gpu_hours_total GPU-hours consumed per user")
        lines.append("# TYPE pulsar_user_gpu_hours_total counter")
        for user, hours in data.get("per_user_gpu_hours", {}).items():
            lines.append(f'pulsar_user_gpu_hours_total{{user="{user}"}} {hours}')

        # Queue depth by tenant (gauge)
        lines.append("")
        lines.append("# HELP pulsar_queue_depth Current queue depth per tenant")
        lines.append("# TYPE pulsar_queue_depth gauge")
        for tenant, depth in data.get("queue_depth_by_tenant", {}).items():
            lines.append(f'pulsar_queue_depth{{tenant="{tenant}"}} {depth}')

        # Queue depth by GPU class (gauge)
        lines.append("")
        lines.append("# HELP pulsar_queue_depth_by_gpu_class Current queue depth by GPU class")
        lines.append("# TYPE pulsar_queue_depth_by_gpu_class gauge")
        for gpu_class, depth in data.get("queue_depth_by_gpu_class", {}).items():
            lines.append(f'pulsar_queue_depth_by_gpu_class{{gpu_class="{gpu_class}"}} {depth}')

        # Wait time buckets (histogram)
        lines.append("")
        lines.append("# HELP pulsar_wait_time_seconds Histogram of queue wait times")
        lines.append("# TYPE pulsar_wait_time_seconds histogram")
        for bucket, count in data.get("wait_time_buckets", {}).items():
            lines.append(f'pulsar_wait_time_seconds_bucket{{le="{bucket}"}} {count}')
        total_waits = sum(data.get("wait_time_buckets", {}).values())
        lines.append(f'pulsar_wait_time_seconds_count {total_waits}')

        # GPU share by tenant (gauge)
        lines.append("")
        lines.append("# HELP pulsar_gpu_share Tenant GPU share fraction")
        lines.append("# TYPE pulsar_gpu_share gauge")
        for tenant, share in data.get("gpu_share_by_tenant", {}).items():
            lines.append(f'pulsar_gpu_share{{tenant="{tenant}"}} {share}')

        # Per-tenant preemptions (counter)
        lines.append("")
        lines.append("# HELP pulsar_preemptions_by_tenant_total Preemptions per tenant")
        lines.append("# TYPE pulsar_preemptions_by_tenant_total counter")
        for tenant, count in data.get("per_tenant_preemptions", {}).items():
            lines.append(f'pulsar_preemptions_by_tenant_total{{tenant="{tenant}"}} {count}')

        # Aging boosts (counter)
        lines.append("")
        lines.append("# HELP pulsar_aging_boosts_total Total priority aging boosts")
        lines.append("# TYPE pulsar_aging_boosts_total counter")
        lines.append(f'pulsar_aging_boosts_total {data.get("aging_boosts_total", 0)}')

        # Starvation events (counter)
        lines.append("")
        lines.append("# HELP pulsar_starvation_events_total Total starvation prevention events")
        lines.append("# TYPE pulsar_starvation_events_total counter")
        lines.append(f'pulsar_starvation_events_total {data.get("starvation_events_total", 0)}')

        # Preemption signals (counter)
        lines.append("")
        lines.append("# HELP pulsar_preemption_signals_total Total preemption signals emitted")
        lines.append("# TYPE pulsar_preemption_signals_total counter")
        lines.append(f'pulsar_preemption_signals_total {data.get("preemption_signals_total", 0)}')

        # DRF dominant share by tenant (gauge)
        lines.append("")
        lines.append("# HELP pulsar_drf_dominant_share Tenant DRF dominant resource share")
        lines.append("# TYPE pulsar_drf_dominant_share gauge")
        for tenant, share in data.get("drf_dominant_share_by_tenant", {}).items():
            lines.append(f'pulsar_drf_dominant_share{{tenant="{tenant}"}} {share}')

        # dGPU / iGPU job counters
        lines.append("")
        lines.append("# HELP pulsar_gpu_class_jobs_total Jobs assigned by GPU class (cumulative)")
        lines.append("# TYPE pulsar_gpu_class_jobs_total counter")
        lines.append(f'pulsar_gpu_class_jobs_total{{gpu_class="dgpu"}} {data.get("dgpu_jobs_total", 0)}')
        lines.append(f'pulsar_gpu_class_jobs_total{{gpu_class="igpu"}} {data.get("igpu_jobs_total", 0)}')

        # Extender Metrics
        lines.append("")
        lines.append("# HELP pulsar_extender_filter_latency_ms Average latency of extender filter calls")
        lines.append("# TYPE pulsar_extender_filter_latency_ms gauge")
        lines.append(f'pulsar_extender_filter_latency_ms {data.get("avg_extender_filter_latency_ms", 0)}')
        
        lines.append("")
        lines.append("# HELP pulsar_extender_bind_latency_ms Average latency of extender bind calls")
        lines.append("# TYPE pulsar_extender_bind_latency_ms gauge")
        lines.append(f'pulsar_extender_bind_latency_ms {data.get("avg_extender_bind_latency_ms", 0)}')
        
        lines.append("")
        lines.append("# HELP pulsar_pod_annotations_injected_total Total pods successfully annotated")
        lines.append("# TYPE pulsar_pod_annotations_injected_total counter")
        lines.append(f'pulsar_pod_annotations_injected_total {data.get("pod_annotations_injected_total", 0)}')

        # Cluster metrics if available
        if cluster_status:
            lines.extend([
                "",
                "# HELP pulsar_cluster_gpus GPU counts",
                "# TYPE pulsar_cluster_gpus gauge",
                f'pulsar_cluster_gpus{{state="total"}} {cluster_status.get("total_gpus", 0)}',
                f'pulsar_cluster_gpus{{state="available"}} {cluster_status.get("available_gpus", 0)}',
                f'pulsar_cluster_gpus{{state="used"}} {cluster_status.get("used_gpus", 0)}',
                "",
                "# HELP pulsar_cluster_utilization GPU utilization ratio",
                "# TYPE pulsar_cluster_utilization gauge",
                f'pulsar_cluster_utilization {cluster_status.get("utilization", 0)}',
                "",
                "# HELP pulsar_active_jobs Current running jobs",
                "# TYPE pulsar_active_jobs gauge",
                f'pulsar_active_jobs {cluster_status.get("active_jobs", 0)}',
            ])

        lines.append("")
        return "\n".join(lines)
