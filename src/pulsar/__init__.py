"""
PULSAR — GPU Queue & Fairness Control Plane

Production control plane for managing GPU workloads in shared AI clusters.
Supports NVIDIA (nvidia.com/gpu) and AMD (amd.com/gpu) GPUs via configurable
device plugin resource names. Operates standalone when K8s is unavailable.
"""

from pulsar.pulsar_types import GPUJob, JobStatus, JobPriority, UserQuota, PulsarEvent, SchedulingPolicy
from pulsar.queue_manager import QueueManager
from pulsar.fair_scheduler import FairScheduler
from pulsar.admission_controller import AdmissionController
from pulsar.preemption import PreemptionEngine
from pulsar.executor import JobExecutor, JobWatcher
from pulsar.control_plane import PulsarControlPlane
from pulsar.metrics import MetricsCollector
from pulsar.config import PulsarConfig
from pulsar.persistence import JobStore

__version__ = "2.0.0"
__all__ = [
    "GPUJob", "JobStatus", "JobPriority", "UserQuota", "PulsarEvent", "SchedulingPolicy",
    "QueueManager", "FairScheduler", "AdmissionController", "PreemptionEngine",
    "JobExecutor", "JobWatcher",
    "PulsarControlPlane", "MetricsCollector", "PulsarConfig", "JobStore",
]
