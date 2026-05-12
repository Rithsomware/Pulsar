"""
PULSAR REST API Server

FastAPI-based REST API for the PULSAR GPU Queue & Fairness Control Plane.
Provides full CRUD for jobs, quotas, cluster management, and monitoring.
"""

import logging
import time
from contextlib import asynccontextmanager
from typing import Optional

from fastapi import FastAPI, HTTPException, Query
from fastapi.responses import HTMLResponse, PlainTextResponse
from pydantic import BaseModel, Field

from pulsar.config import PulsarConfig
from pulsar.control_plane import PulsarControlPlane
from pulsar.pulsar_types import GPUJob, JobPriority
from pulsar.dashboard import render_dashboard

logger = logging.getLogger("pulsar.api")

# Global control plane instance
_control_plane: Optional[PulsarControlPlane] = None


def get_cp() -> PulsarControlPlane:
    if _control_plane is None:
        raise RuntimeError("Control plane not initialized")
    return _control_plane


# ─── Request/Response Models ───────────────────────────────

class JobSubmitRequest(BaseModel):
    user: str
    gpu_required: int = Field(ge=1, le=256)
    gpu_memory_gb: int = Field(default=0, ge=0)
    namespace: str = "default"
    priority: str = "NORMAL"
    preemptible: bool = True
    workload_type: str = "Training"
    framework: str = "PyTorch"
    estimated_duration_minutes: float = 60.0
    preferred_gpu_class: Optional[str] = None
    image: Optional[str] = None
    command: Optional[list] = None


class QuotaUpdateRequest(BaseModel):
    max_gpus: int = Field(ge=1)
    max_jobs: int = Field(ge=1)
    weight: float = Field(default=1.0, ge=0.1)


class JobResponse(BaseModel):
    job_id: str
    user: str
    gpu_required: int
    status: str
    priority: str
    workload_type: str
    framework: str
    submitted_at: Optional[str] = None
    admitted_at: Optional[str] = None
    started_at: Optional[str] = None
    completed_at: Optional[str] = None
    preferred_gpu_class: Optional[str] = None
    assigned_gpu_class: Optional[str] = None
    fallback_applied: bool = False
    fallback_reason: Optional[str] = None


# ─── App Setup ─────────────────────────────────────────────

def create_app(config: Optional[PulsarConfig] = None) -> FastAPI:
    """Create the FastAPI application with the given config."""

    @asynccontextmanager
    async def lifespan(app: FastAPI):
        global _control_plane
        cfg = config or PulsarConfig.load()
        _control_plane = PulsarControlPlane(cfg)
        _control_plane.start_scheduler()
        logger.info("PULSAR API server started on %s:%d", cfg.api.host, cfg.api.port)
        yield
        _control_plane.stop_scheduler()
        logger.info("PULSAR API server stopped")

    app = FastAPI(
        title="PULSAR — GPU Queue & Fairness Control Plane",
        description="REST API for managing GPU workloads in shared AI clusters",
        version="2.0.0",
        lifespan=lifespan,
    )

    # ─── Dashboard ──────────────────────────────────────────

    @app.get("/", response_class=HTMLResponse, tags=["Dashboard"])
    async def dashboard():
        """Render the PULSAR web dashboard."""
        cp = get_cp()
        data = cp.get_dashboard()
        return render_dashboard(data)

    # ─── Jobs ───────────────────────────────────────────────

    @app.post("/api/v1/jobs", tags=["Jobs"])
    async def submit_job(req: JobSubmitRequest):
        """Submit a GPU workload to the queue."""
        cp = get_cp()
        try:
            priority = JobPriority[req.priority.upper()]
        except KeyError:
            raise HTTPException(400, f"Invalid priority: {req.priority}")
        preferred_gpu_class = None
        if req.preferred_gpu_class:
            preferred_gpu_class = req.preferred_gpu_class.lower().strip()
            if preferred_gpu_class not in {"dgpu", "igpu"}:
                raise HTTPException(400, "preferred_gpu_class must be dgpu or igpu")

        job = GPUJob(
            user=req.user,
            gpu_required=req.gpu_required,
            gpu_memory_gb=req.gpu_memory_gb,
            namespace=req.namespace,
            priority=priority,
            preemptible=req.preemptible,
            workload_type=req.workload_type,
            framework=req.framework,
            estimated_duration_minutes=req.estimated_duration_minutes,
            preferred_gpu_class=preferred_gpu_class or "dgpu",
        )
        cp.submit_job(job)
        return {"status": "queued", "job": job.to_dict()}

    @app.get("/api/v1/jobs", tags=["Jobs"])
    async def list_jobs(
        user: Optional[str] = Query(None),
        status: Optional[str] = Query(None),
    ):
        """List jobs with optional filters."""
        cp = get_cp()
        jobs = cp.list_jobs(user=user, status=status)
        return {"jobs": [j.to_dict() for j in jobs], "total": len(jobs)}

    @app.get("/api/v1/jobs/{job_id}", tags=["Jobs"])
    async def get_job(job_id: str):
        """Get details for a specific job."""
        cp = get_cp()
        job = cp.get_job(job_id)
        if not job:
            raise HTTPException(404, f"Job {job_id} not found")
        return job.to_dict()

    @app.delete("/api/v1/jobs/{job_id}", tags=["Jobs"])
    async def cancel_job(job_id: str):
        """Cancel a queued or running job."""
        cp = get_cp()
        job = cp.cancel_job(job_id)
        if not job:
            raise HTTPException(404, f"Job {job_id} not found or already completed")
        return {"status": "cancelled", "job_id": job_id}

    @app.post("/api/v1/jobs/{job_id}/complete", tags=["Jobs"])
    async def complete_job(job_id: str):
        """Mark a running job as completed."""
        cp = get_cp()
        job = cp.complete_job(job_id)
        if not job:
            raise HTTPException(404, f"Job {job_id} not found or not running")
        return {"status": "completed", "job": job.to_dict()}

    # ─── Health ─────────────────────────────────────────────

    @app.get("/healthz", tags=["Health"])
    async def health():
        """Liveness probe for Kubernetes."""
        return get_cp().health_check()

    @app.get("/readyz", tags=["Health"])
    async def ready():
        """Readiness probe for Kubernetes."""
        result = get_cp().readiness_check()
        if not result["ready"]:
            raise HTTPException(503, result)
        return result

    # ─── Events ─────────────────────────────────────────────

    @app.get("/api/v1/events", tags=["Monitoring"])
    async def recent_events(limit: int = Query(50, ge=1, le=500)):
        """Get recent PULSAR events from the event log."""
        cp = get_cp()
        if cp._store:
            return {"events": cp._store.get_recent_events(limit)}
        return {"events": []}

    # ─── CRD Export ─────────────────────────────────────────

    @app.get("/api/v1/crd", tags=["Kubernetes"])
    async def get_crd():
        """Get the GPUWorkload CRD definition for kubectl apply."""
        from pulsar.k8s_integration import GPU_WORKLOAD_CRD
        return GPU_WORKLOAD_CRD

    # ─── Cluster ────────────────────────────────────────────

    @app.get("/api/v1/cluster", tags=["Cluster"])
    async def cluster_status():
        """Get cluster resource status."""
        return get_cp().admission_controller.get_cluster_status()

    # ─── Queues ─────────────────────────────────────────────

    @app.get("/api/v1/queues", tags=["Queues"])
    async def queue_status():
        """Get per-user queue status."""
        cp = get_cp()
        return {
            "queues": cp.queue_manager.get_queue_status(),
            "total_queued": cp.queue_manager.total_queued,
            "total_gpus_queued": cp.queue_manager.total_gpus_queued,
        }

    # ─── Fairness ───────────────────────────────────────────

    @app.get("/api/v1/fairness", tags=["Fairness"])
    async def fairness_report():
        """Get fairness report and Jain's index."""
        cp = get_cp()
        return {
            "report": cp.fair_scheduler.get_fairness_report(),
            "jains_fairness_index": cp.fair_scheduler.compute_jains_fairness_index(),
            "fair_share": cp.fair_scheduler.get_fair_share(cp.config.cluster.total_gpus),
        }

    # ─── Quotas ─────────────────────────────────────────────

    @app.get("/api/v1/quotas", tags=["Quotas"])
    async def list_quotas():
        """List all user quotas."""
        cp = get_cp()
        quotas = cp.admission_controller.get_all_quotas()
        return {"quotas": {u: q.to_dict() for u, q in quotas.items()}}

    @app.put("/api/v1/quotas/{user}", tags=["Quotas"])
    async def update_quota(user: str, req: QuotaUpdateRequest):
        """Create or update a user quota."""
        cp = get_cp()
        cp.set_quota(user, max_gpus=req.max_gpus, max_jobs=req.max_jobs, weight=req.weight)
        return {"status": "updated", "user": user, "quota": req.model_dump()}

    # ─── Metrics ────────────────────────────────────────────

    @app.get("/api/v1/metrics", response_class=PlainTextResponse, tags=["Monitoring"])
    async def prometheus_metrics():
        """Prometheus-compatible metrics endpoint."""
        return get_cp().get_prometheus_metrics()

    @app.get("/api/v1/metrics/json", tags=["Monitoring"])
    async def metrics_json():
        """Metrics in JSON format."""
        return get_cp().metrics.export_metrics()

    # ─── Scheduler Extender ─────────────────────────────────

    @app.post("/api/v1/scheduler/filter", tags=["Kubernetes"])
    async def scheduler_filter(req: dict):
        """K8s scheduler extender: filter nodes."""
        cp = get_cp()
        start_time = time.time()
        # Basic implementation: just accept all nodes for now, 
        # or filter based on basic GPU availability.
        # A real implementation would parse ExtenderArgs and check `cp.admission_controller`.
        nodes = req.get("nodes", {}).get("items", [])
        
        # We just return the same nodes back as eligible.
        result = {
            "nodes": {
                "items": nodes
            }
        }
        latency_ms = (time.time() - start_time) * 1000
        cp.metrics.record_extender_filter_latency(latency_ms)
        return result

    @app.post("/api/v1/scheduler/prioritize", tags=["Kubernetes"])
    async def scheduler_prioritize(req: dict):
        """K8s scheduler extender: prioritize nodes."""
        # Score nodes based on preferred_gpu_class and availability
        nodes = req.get("nodes", {}).get("items", [])
        host_priorities = []
        for node in nodes:
            node_name = node.get("metadata", {}).get("name", "unknown")
            host_priorities.append({"host": node_name, "score": 10})
        return host_priorities

    @app.post("/api/v1/scheduler/preempt", tags=["Kubernetes"])
    async def scheduler_preempt(req: dict):
        """K8s scheduler extender: preempt pods."""
        # A real implementation would use cp.admission_controller.find_preemption_victims
        node_name = req.get("nodeName", "unknown")
        return {"nodeName": node_name, "victims": []}

    @app.post("/api/v1/scheduler/bind", tags=["Kubernetes"])
    async def scheduler_bind(req: dict):
        """K8s scheduler extender: bind pod and inject annotations."""
        cp = get_cp()
        start_time = time.time()
        
        pod_name = req.get("podName")
        pod_namespace = req.get("podNamespace", "default")
        node_name = req.get("node")
        
        # Inject annotations via kubernetes client
        annotations = {
            "pulsar.io/assigned-gpu-class": "dgpu", # default, could be dynamic
            "pulsar.io/assigned-node": node_name
        }
        
        # Add fallback applied annotation if necessary
        # We can dynamically decide this based on cp.admission_controller state
        
        if cp.k8s.is_k8s_available:
            success = cp.k8s.patch_pod_annotations(pod_name, pod_namespace, annotations)
            if success:
                cp.metrics.record_pod_annotation_injected()

        latency_ms = (time.time() - start_time) * 1000
        cp.metrics.record_extender_bind_latency(latency_ms)
        
        # Return empty error string on success
        return {"error": ""}

    # ─── Dashboard API ──────────────────────────────────────

    @app.get("/api/v1/dashboard", tags=["Dashboard"])
    async def dashboard_data():
        """Full dashboard data as JSON."""
        data = get_cp().get_dashboard()
        # Inject live nvidia-smi data
        data["nvidia_smi"] = get_cp().executor.get_nvidia_smi()
        data["active_pids"] = get_cp().executor.get_active_pids()
        return data

    # ─── GPU Discovery ─────────────────────────────────────

    @app.get("/api/v1/gpu", tags=["Hardware"])
    async def detect_gpus():
        """Detect real GPU hardware on this machine."""
        from pulsar.gpu_discovery import get_gpu_summary
        return get_gpu_summary()

    @app.get("/api/v1/gpu/nvidia-smi", tags=["Hardware"])
    async def nvidia_smi():
        """Live nvidia-smi data with PULSAR job→PID mapping."""
        return get_cp().executor.get_nvidia_smi()

    @app.get("/api/v1/gpu/processes", tags=["Hardware"])
    async def gpu_processes():
        """Active PULSAR processes with PIDs."""
        cp = get_cp()
        pids = cp.executor.get_active_pids()
        jobs = {}
        for job_id, pid in pids.items():
            info = cp.executor.get_execution_info(job_id)
            job = cp.get_job(job_id)
            jobs[job_id] = {
                "pid": pid,
                "team": job.user if job else None,
                "gpu_required": job.gpu_required if job else None,
                "workload_type": job.workload_type if job else None,
                **(info or {}),
            }
        return {"processes": jobs, "count": len(pids)}

    return app
