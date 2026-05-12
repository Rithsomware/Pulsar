"""
PULSAR Core Data Types

Defines the fundamental data structures used across the PULSAR control plane.
"""

import uuid
from dataclasses import dataclass, field, asdict
from datetime import datetime
from enum import Enum
from typing import Optional, Dict, List


class JobStatus(Enum):
    QUEUED = "QUEUED"
    ADMITTED = "ADMITTED"
    RUNNING = "RUNNING"
    COMPLETED = "COMPLETED"
    REJECTED = "REJECTED"
    PREEMPTED = "PREEMPTED"
    CANCELLED = "CANCELLED"
    FAILED = "FAILED"


class JobPriority(Enum):
    LOW = 0
    NORMAL = 1
    HIGH = 2
    CRITICAL = 3


class SchedulingPolicy(Enum):
    FIFO = "fifo"
    FAIR_SHARE = "fair_share"
    PRIORITY = "priority"
    BACKFILL = "backfill"
    DRF = "drf"


@dataclass
class GPUJob:
    """Represents a GPU workload submitted to the PULSAR control plane."""
    user: str
    gpu_required: int
    gpu_memory_gb: int = 0
    namespace: str = "default"
    job_id: str = field(default_factory=lambda: f"job-{uuid.uuid4().hex[:8]}")
    priority: JobPriority = JobPriority.NORMAL
    preemptible: bool = True
    workload_type: str = "Training"
    framework: str = "PyTorch"
    estimated_duration_minutes: float = 60.0
    status: JobStatus = JobStatus.QUEUED
    submitted_at: datetime = field(default_factory=datetime.now)
    admitted_at: Optional[datetime] = None
    started_at: Optional[datetime] = None
    completed_at: Optional[datetime] = None
    preferred_gpu_class: str = "dgpu"
    assigned_gpu_class: Optional[str] = None
    assigned_gpu_resource: Optional[str] = None
    fallback_applied: bool = False
    fallback_reason: Optional[str] = None
    fallback_decided_at: Optional[datetime] = None
    assigned_node: Optional[str] = None
    assigned_gpus: List[str] = field(default_factory=list)
    preemption_count: int = 0

    def to_dict(self) -> dict:
        """Serialize to dictionary for API/persistence."""
        return {
            "job_id": self.job_id,
            "user": self.user,
            "namespace": self.namespace,
            "gpu_required": self.gpu_required,
            "gpu_memory_gb": self.gpu_memory_gb,
            "priority": self.priority.name,
            "preemptible": self.preemptible,
            "workload_type": self.workload_type,
            "framework": self.framework,
            "estimated_duration_minutes": self.estimated_duration_minutes,
            "status": self.status.value,
            "submitted_at": self.submitted_at.isoformat() if self.submitted_at else None,
            "admitted_at": self.admitted_at.isoformat() if self.admitted_at else None,
            "started_at": self.started_at.isoformat() if self.started_at else None,
            "completed_at": self.completed_at.isoformat() if self.completed_at else None,
            "preferred_gpu_class": self.preferred_gpu_class,
            "assigned_gpu_class": self.assigned_gpu_class,
            "assigned_gpu_resource": self.assigned_gpu_resource,
            "fallback_applied": self.fallback_applied,
            "fallback_reason": self.fallback_reason,
            "fallback_decided_at": self.fallback_decided_at.isoformat() if self.fallback_decided_at else None,
            "assigned_node": self.assigned_node,
            "assigned_gpus": self.assigned_gpus,
            "preemption_count": self.preemption_count,
        }

    @classmethod
    def from_dict(cls, data: dict) -> "GPUJob":
        """Deserialize from dictionary."""
        def parse_dt(v):
            if v is None:
                return None
            if isinstance(v, datetime):
                return v
            return datetime.fromisoformat(v)

        return cls(
            job_id=data.get("job_id", f"job-{uuid.uuid4().hex[:8]}"),
            user=data["user"],
            namespace=data.get("namespace", "default"),
            gpu_required=data["gpu_required"],
            gpu_memory_gb=data.get("gpu_memory_gb", 0),
            priority=JobPriority[data.get("priority", "NORMAL")],
            preemptible=data.get("preemptible", True),
            workload_type=data.get("workload_type", "Training"),
            framework=data.get("framework", "PyTorch"),
            estimated_duration_minutes=data.get("estimated_duration_minutes", 60.0),
            status=JobStatus(data.get("status", "QUEUED")),
            submitted_at=parse_dt(data.get("submitted_at")) or datetime.now(),
            admitted_at=parse_dt(data.get("admitted_at")),
            started_at=parse_dt(data.get("started_at")),
            completed_at=parse_dt(data.get("completed_at")),
            preferred_gpu_class=data.get("preferred_gpu_class", "dgpu"),
            assigned_gpu_class=data.get("assigned_gpu_class"),
            assigned_gpu_resource=data.get("assigned_gpu_resource"),
            fallback_applied=data.get("fallback_applied", False),
            fallback_reason=data.get("fallback_reason"),
            fallback_decided_at=parse_dt(data.get("fallback_decided_at")),
            assigned_node=data.get("assigned_node"),
            assigned_gpus=data.get("assigned_gpus", []),
            preemption_count=data.get("preemption_count", 0),
        )

    def __repr__(self):
        return (
            f"GPUJob(id={self.job_id}, user={self.user}, "
            f"gpus={self.gpu_required}, status={self.status.value}, "
            f"priority={self.priority.name})"
        )


@dataclass
class UserQuota:
    """Per-user GPU resource quota enforced by the admission controller."""
    user: str
    max_gpus: int = 8
    max_jobs: int = 10
    weight: float = 1.0
    current_gpu_usage: int = 0
    current_job_count: int = 0
    total_gpu_hours: float = 0.0

    @property
    def gpu_available(self) -> int:
        return max(0, self.max_gpus - self.current_gpu_usage)

    @property
    def can_submit(self) -> bool:
        return self.current_job_count < self.max_jobs

    def to_dict(self) -> dict:
        return {
            "user": self.user,
            "max_gpus": self.max_gpus,
            "max_jobs": self.max_jobs,
            "weight": self.weight,
            "current_gpu_usage": self.current_gpu_usage,
            "current_job_count": self.current_job_count,
            "gpu_available": self.gpu_available,
            "total_gpu_hours": round(self.total_gpu_hours, 2),
        }

    def __repr__(self):
        return (
            f"UserQuota({self.user}: {self.current_gpu_usage}/{self.max_gpus} GPUs, "
            f"{self.current_job_count}/{self.max_jobs} jobs, weight={self.weight})"
        )


@dataclass
class PulsarEvent:
    """Structured event for PULSAR control plane logging."""
    event_type: str
    message: str
    user: Optional[str] = None
    job_id: Optional[str] = None
    metadata: Dict = field(default_factory=dict)
    timestamp: datetime = field(default_factory=datetime.now)

    def format(self) -> str:
        parts = [f"[PULSAR] [{self.event_type}]"]
        if self.user:
            parts.append(f"[{self.user}]")
        parts.append(self.message)
        if self.metadata:
            meta_str = ", ".join(f"{k}={v}" for k, v in self.metadata.items())
            parts.append(f"({meta_str})")
        return " ".join(parts)

    def to_dict(self) -> dict:
        return {
            "event_type": self.event_type,
            "message": self.message,
            "user": self.user,
            "job_id": self.job_id,
            "metadata": self.metadata,
            "timestamp": self.timestamp.isoformat(),
        }

    def __repr__(self):
        return self.format()
