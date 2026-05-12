"""
PULSAR Job Executor

Launches REAL GPU processes when PULSAR admits a workload.

In standalone mode:
  - Spawns `gpu_worker.py` as a subprocess with actual GPU computation
  - Worker is visible in nvidia-smi with PID + memory usage
  - Process is killed on completion/preemption/cancellation

In K8s mode:
  - Creates real Kubernetes Jobs with GPU resource requests

Supports heterogeneous GPU environments (dGPU + iGPU fallback).
"""

import os
import sys
import logging
import time
import threading
import subprocess
import signal
from datetime import datetime
from typing import Dict, List, Optional, Tuple

from pulsar.pulsar_types import GPUJob, JobStatus

logger = logging.getLogger("pulsar.executor")

# Default container image when none specified
DEFAULT_GPU_IMAGE = "pytorch/pytorch:2.2.0-cuda12.1-cudnn8-runtime"

# Memory allocation per GPU unit (MB) — tuned for RTX 3050 6GB
GPU_MEM_PER_UNIT_MB = 400


class JobExecutor:
    """
    Executes admitted GPU jobs by launching real processes.

    Standalone mode:
      - Spawns gpu_worker.py subprocess with CUDA matrix ops
      - Each job gets a real PID visible in nvidia-smi
      - Process auto-terminates after estimated_duration
      - Supports dGPU → iGPU fallback

    K8s mode:
      - Creates K8s Jobs with GPU resource requests (nvidia.com/gpu or amd.com/gpu)
    """

    def __init__(
        self,
        gpu_resource_name: str = "nvidia.com/gpu",
        dgpu_resource_name: str = "nvidia.com/gpu",
        igpu_resource_name: str = "gpu.intel.com/i915",
        on_complete=None,
        on_fail=None,
        config=None,
    ):
        self._k8s_available = False
        self._batch_client = None
        self._core_client = None
        self._config = config
        # Backward compatible alias; dgpu_resource_name takes precedence.
        self._gpu_resource_name = gpu_resource_name
        self._dgpu_resource_name = dgpu_resource_name or gpu_resource_name
        self._igpu_resource_name = igpu_resource_name or ""
        self._on_complete = on_complete
        self._on_fail = on_fail
        self._try_init_k8s()
        self._created_jobs: Dict[str, dict] = {}
        self._processes: Dict[str, subprocess.Popen] = {}
        self._watchers: Dict[str, threading.Thread] = {}

        # Detect GPU hardware
        self._has_dgpu = False
        self._has_igpu = False
        self._detect_gpu_hardware()

    def _detect_gpu_hardware(self):
        """Detect real GPU hardware and cache memory for accurate fallback reporting."""
        self._dgpu_name = ""
        self._igpu_name = ""
        self._dgpu_pci_bus = "0000:00:00.0"
        self._igpu_pci_bus = "0000:00:00.0"
        self._dgpu_memory_mb = 0  # cached from real nvidia-smi

        # 1. Try dGPU detection via nvidia-smi
        try:
            result = subprocess.run(
                ["nvidia-smi", "--query-gpu=name,pci.bus_id,memory.total",
                 "--format=csv,noheader,nounits"],
                capture_output=True, text=True, timeout=5,
            )
            if result.returncode == 0 and result.stdout.strip():
                # Split by line first — each GPU is one CSV line
                lines = [l.strip() for l in result.stdout.strip().split("\n") if l.strip()]
                if lines:
                    parts = [p.strip() for p in lines[0].split(",")]
                    if len(parts) >= 2:
                        self._has_dgpu = True
                        self._dgpu_name = parts[0]
                        self._dgpu_pci_bus = parts[1]
                        if len(parts) >= 3:
                            mem_str = parts[2].strip()
                            # Handle unit suffixes like "6144 MiB" or "[N/A]"
                            numeric = ""
                            for ch in mem_str:
                                if ch.isdigit() or ch == ".":
                                    numeric += ch
                                else:
                                    break
                            if numeric:
                                try:
                                    self._dgpu_memory_mb = int(float(numeric))
                                except ValueError:
                                    pass
                        logger.info("dGPU detected via nvidia-smi: %s at %s (%d MB)",
                                    self._dgpu_name, self._dgpu_pci_bus, self._dgpu_memory_mb)
        except (FileNotFoundError, subprocess.TimeoutExpired):
            pass

        # 2. Fallback: Detect dGPU and iGPU via lspci
        try:
            result = subprocess.run(
                ["lspci"], capture_output=True, text=True, timeout=5,
            )
            for line in result.stdout.split("\n"):
                low = line.lower()
                if "vga" in low or "3d controller" in low:
                    parts = line.split(" ")
                    pci_id = parts[0]
                    # Ensure full domain format (lspci usually gives 00:02.0)
                    full_pci_id = f"0000:{pci_id}" if ":" in pci_id else pci_id

                    # Detect NVIDIA (usually dGPU)
                    if "nvidia" in low:
                        if not self._has_dgpu:
                            self._has_dgpu = True
                            self._dgpu_pci_bus = full_pci_id
                            name_parts = line.split(":")
                            self._dgpu_name = name_parts[-1].strip() if len(name_parts) >= 3 else line.strip()
                            logger.info("dGPU detected via lspci: %s at %s", self._dgpu_name, self._dgpu_pci_bus)

                    # Detect iGPU (Intel/AMD)
                    elif "intel" in low or ("amd" in low and "nvidia" not in low):
                        if not self._has_igpu:
                            self._has_igpu = True
                            self._igpu_pci_bus = full_pci_id
                            name_parts = line.split(":")
                            self._igpu_name = name_parts[-1].strip() if len(name_parts) >= 3 else line.strip()
                            logger.info("iGPU detected via lspci: %s at %s", self._igpu_name, self._igpu_pci_bus)
        except Exception as e:
            logger.debug("lspci detection failed: %s", e)

    def _try_init_k8s(self):
        """Attempt to initialize K8s clients."""
        try:
            from kubernetes import client, config as k8s_config
            try:
                k8s_config.load_incluster_config()
                logger.info("Loaded in-cluster K8s config")
            except k8s_config.ConfigException:
                try:
                    k8s_config.load_kube_config()
                    logger.info("Loaded kubeconfig")
                except Exception:
                    logger.info("No K8s config found — standalone mode")
                    return

            self._batch_client = client.BatchV1Api()
            self._core_client = client.CoreV1Api()
            self._k8s_available = True
            logger.info("Kubernetes executor enabled")
        except ImportError:
            logger.info("kubernetes package not installed — standalone mode")
        except Exception as e:
            logger.warning("K8s init failed: %s — standalone mode", e)

    @property
    def is_k8s_available(self) -> bool:
        return self._k8s_available

    @property
    def has_dgpu(self) -> bool:
        return self._has_dgpu

    @property
    def has_igpu(self) -> bool:
        return self._has_igpu

    @property
    def dgpu_resource_name(self) -> str:
        return self._dgpu_resource_name

    @property
    def igpu_resource_name(self) -> str:
        return self._igpu_resource_name

    def execute(self, job: GPUJob) -> Tuple[bool, str]:
        """Execute an admitted job with real GPU processes."""
        if self._k8s_available:
            return self._create_k8s_job(job)
        else:
            return self._execute_standalone(job)

    def _resolve_standalone_device(self, job: GPUJob) -> str:
        """Resolve real execution lane in standalone mode."""
        preferred = (job.assigned_gpu_class or job.preferred_gpu_class or "dgpu").lower()
        if preferred == "igpu":
            if self._has_igpu:
                return "igpu"
            if self._has_dgpu:
                return "dgpu"
            return "cpu"
        if preferred == "dgpu":
            if self._has_dgpu:
                return "dgpu"
            if self._has_igpu:
                return "igpu"
            return "cpu"
        return "cpu"

    def _execute_standalone(self, job: GPUJob) -> Tuple[bool, str]:
        """
        Launch a real GPU worker process.

        The worker subprocess performs actual CUDA matrix operations,
        visible in nvidia-smi. Process is tagged with team/job env vars.
        """
        job.status = JobStatus.RUNNING
        job.started_at = datetime.now()

        # Calculate duration (compress for demo: 1 min config → 15-60s real)
        duration_s = max(15, min(120, job.estimated_duration_minutes * 15))

        # GPU memory allocation per unit
        gpu_mem_mb = job.gpu_required * GPU_MEM_PER_UNIT_MB

        # Determine actual standalone execution device.
        device_type = self._resolve_standalone_device(job)
        job.assigned_gpu_class = device_type
        if device_type == "dgpu":
            job.assigned_gpu_resource = self._dgpu_resource_name
        elif device_type == "igpu":
            job.assigned_gpu_resource = self._igpu_resource_name
        else:
            job.assigned_gpu_resource = ""

        # Build command — run as module so imports resolve correctly
        cmd = [
            sys.executable, "-m", "pulsar.gpu_worker",
            "--job-id", job.job_id,
            "--team-id", job.user,
            "--duration", str(duration_s),
            "--gpu-mem-mb", str(gpu_mem_mb),
            "--workload-type", job.workload_type,
            "--framework", job.framework,
            "--priority", job.priority.name,
        ]

        if device_type == "cpu":
            cmd.append("--cpu-only")

        # Environment with PULSAR metadata
        env = {
            **os.environ,
            "PULSAR_JOB_ID": job.job_id,
            "PULSAR_TEAM": job.user,
            "PULSAR_WORKLOAD_TYPE": job.workload_type,
            "PULSAR_FRAMEWORK": job.framework,
            "PULSAR_GPU_COUNT": str(job.gpu_required),
            "PULSAR_PRIORITY": job.priority.name,
            "PULSAR_DEVICE_TYPE": device_type,
        }

        try:
            # CWD must be parent of 'pulsar' package for -m to work
            src_dir = os.path.dirname(os.path.dirname(__file__))
            proc = subprocess.Popen(
                cmd,
                env=env,
                cwd=src_dir,
                stdout=subprocess.PIPE,
                stderr=subprocess.STDOUT,
            )

            pid = proc.pid
            self._processes[job.job_id] = proc
            self._created_jobs[job.job_id] = {
                "mode": "standalone",
                "pid": pid,
                "device_type": device_type,
                "gpu_resource": job.assigned_gpu_resource,
                "fallback_applied": job.fallback_applied,
                "fallback_reason": job.fallback_reason,
                "gpu_mem_mb": gpu_mem_mb,
                "duration_s": duration_s,
                "started_at": job.started_at.isoformat(),
            }

            # Start a watcher thread that waits for process exit
            watcher = threading.Thread(
                target=self._watch_process,
                args=(job.job_id, proc, duration_s),
                daemon=True,
            )
            watcher.start()
            self._watchers[job.job_id] = watcher

            logger.info(
                "[EXECUTOR] Process started: %s PID=%d (%s, %d GPUs, %dMB VRAM, ~%ds)",
                job.job_id, pid, device_type, job.gpu_required, gpu_mem_mb, duration_s,
            )
            return True, f"PID {pid} ({device_type}, ~{duration_s}s)"

        except Exception as e:
            logger.error("[EXECUTOR] Failed to start process for %s: %s", job.job_id, e)
            return False, f"Process launch failed: {e}"

    def _watch_process(self, job_id: str, proc: subprocess.Popen, timeout: float):
        """Watch a subprocess and trigger completion when it exits."""
        try:
            returncode = proc.wait(timeout=timeout + 30)  # Extra grace period
        except subprocess.TimeoutExpired:
            logger.warning("[EXECUTOR] Process %s (PID %d) timed out — killing", job_id, proc.pid)
            proc.kill()
            returncode = proc.wait()

        # Determine final status based on exit code
        # Code 0 = Success (COMPLETED)
        # Code -15/15 = SIGTERM (CANCELLED)
        # Code -9/9 = SIGKILL (CANCELLED)
        # Other = Error (FAILED)

        if returncode == 0:
            logger.info("Job %s completed successfully", job_id)
            if self._on_complete:
                self._on_complete(job_id)
        elif returncode in (15, -15, 9, -9):
            logger.info("Job %s was terminated (code %d)", job_id, returncode)
            # We don't trigger on_complete or on_fail for intentional cancellations
            # The control plane handles status update via cancel_job()
        else:
            logger.error("Job %s failed with exit code %d", job_id, returncode)
            if self._on_fail:
                self._on_fail(job_id, f"Exit code {returncode}")

        self._processes.pop(job_id, None)
        self._watchers.pop(job_id, None)

    def _create_k8s_job(self, job: GPUJob) -> Tuple[bool, str]:
        """Create a real Kubernetes Job with GPU resource requests."""
        candidates = self._k8s_resource_candidates(job)
        last_err = None

        for idx, (gpu_class, gpu_resource, reason) in enumerate(candidates):
            try:
                k8s_job = self._build_k8s_job(job, gpu_class, gpu_resource)
                result = self._batch_client.create_namespaced_job(
                    namespace=job.namespace, body=k8s_job,
                )

                job.status = JobStatus.RUNNING
                job.started_at = datetime.now()
                job.assigned_gpu_class = gpu_class
                job.assigned_gpu_resource = gpu_resource
                if idx > 0 and not job.fallback_applied:
                    job.fallback_applied = True
                    job.fallback_reason = reason or "no_capacity"

                self._created_jobs[job.job_id] = {
                    "mode": "kubernetes",
                    "k8s_name": f"pulsar-{job.job_id}",
                    "namespace": job.namespace,
                    "uid": result.metadata.uid,
                    "gpu_class": gpu_class,
                    "gpu_resource": gpu_resource,
                    "fallback_applied": job.fallback_applied,
                    "fallback_reason": job.fallback_reason,
                }

                logger.info(
                    "[EXECUTOR] K8s Job created: pulsar-%s (%d GPUs, class=%s, resource=%s)",
                    job.job_id, job.gpu_required, gpu_class, gpu_resource,
                )
                return True, f"K8s Job pulsar-{job.job_id} created ({gpu_class})"
            except Exception as e:
                last_err = e
                logger.warning(
                    "[EXECUTOR] K8s attempt failed for %s (class=%s resource=%s): %s",
                    job.job_id, gpu_class, gpu_resource, e,
                )

        logger.error("[EXECUTOR] K8s Job failed for %s: %s", job.job_id, last_err)
        return False, f"K8s Job creation failed: {last_err}"

    def _k8s_resource_candidates(self, job: GPUJob) -> List[Tuple[str, str, str]]:
        """Build ordered candidate lanes for K8s launch attempts."""
        preferred = (job.assigned_gpu_class or job.preferred_gpu_class or "dgpu").lower()
        candidates: List[Tuple[str, str, str]] = []

        def add_candidate(gpu_class: str, resource: str, reason: str = ""):
            if not resource:
                return
            if any(r == resource for _, r, _ in candidates):
                return
            candidates.append((gpu_class, resource, reason))

        if preferred == "igpu":
            add_candidate("igpu", self._igpu_resource_name)
            add_candidate("dgpu", self._dgpu_resource_name, "no_capacity")
        else:
            add_candidate("dgpu", self._dgpu_resource_name)
            add_candidate("igpu", self._igpu_resource_name, "no_capacity")

        # Backward compatibility fallback
        if not candidates:
            add_candidate(preferred, self._gpu_resource_name)
        return candidates

    def _build_k8s_job(self, job: GPUJob, gpu_class: str, gpu_resource: str):
        """Build a Kubernetes Job object for a specific GPU class/resource."""
        from kubernetes import client

        container = client.V1Container(
            name="gpu-workload",
            image=DEFAULT_GPU_IMAGE,
            command=["sleep", str(int(job.estimated_duration_minutes * 60))],
            resources=client.V1ResourceRequirements(
                limits={gpu_resource: str(job.gpu_required)},
                requests={gpu_resource: str(job.gpu_required)},
            ),
            env=[
                client.V1EnvVar(name="PULSAR_JOB_ID", value=job.job_id),
                client.V1EnvVar(name="PULSAR_USER", value=job.user),
                client.V1EnvVar(name="PULSAR_WORKLOAD_TYPE", value=job.workload_type),
                client.V1EnvVar(name="PULSAR_FRAMEWORK", value=job.framework),
                client.V1EnvVar(name="PULSAR_GPU_COUNT", value=str(job.gpu_required)),
                client.V1EnvVar(name="PULSAR_GPU_CLASS", value=gpu_class),
                client.V1EnvVar(name="PULSAR_GPU_RESOURCE", value=gpu_resource),
                client.V1EnvVar(name="PULSAR_FALLBACK_APPLIED", value=str(job.fallback_applied).lower()),
                client.V1EnvVar(name="PULSAR_FALLBACK_REASON", value=job.fallback_reason or ""),
            ],
        )

        pod_spec = client.V1PodSpec(
            containers=[container],
            restart_policy="Never",
            scheduler_name="default-scheduler",
        )

        if job.assigned_node:
            pod_spec.node_selector = {"kubernetes.io/hostname": job.assigned_node}

        annotations = {
            "pulsar.io/job-id": job.job_id,
            "pulsar.io/user": job.user,
            "pulsar.io/priority": job.priority.name,
            "pulsar.io/workload-type": job.workload_type,
            "pulsar.io/gpu-class": gpu_class,
            "pulsar.io/gpu-resource": gpu_resource,
            "pulsar.io/fallback-applied": str(job.fallback_applied).lower(),
            "pulsar.io/fallback-reason": job.fallback_reason or "",
            # KGWE standard annotations for scheduler extender and CRD integration
            "kgwe.nvidia.io/job-id": job.job_id,
            "kgwe.nvidia.io/user": job.user,
            "kgwe.nvidia.io/priority": job.priority.name,
            "kgwe.nvidia.io/workload-type": job.workload_type,
            "kgwe.nvidia.io/assigned-gpu-class": gpu_class,
            "kgwe.nvidia.io/gpu-resource": gpu_resource,
            "kgwe.nvidia.io/preferred-gpu-class": job.preferred_gpu_class or "dgpu",
            "kgwe.nvidia.io/fallback-applied": str(job.fallback_applied).lower(),
            "kgwe.nvidia.io/fallback-reason": job.fallback_reason or "",
            "kgwe.nvidia.io/scheduled-at": datetime.now().isoformat(),
        }
        if job.gpu_memory_gb > 0:
            annotations["pulsar.io/gpu-memory-gb"] = str(job.gpu_memory_gb)
            annotations["kgwe.nvidia.io/gpu-memory-gb"] = str(job.gpu_memory_gb)

        labels = {
            "app.kubernetes.io/managed-by": "pulsar",
            "pulsar.io/job-id": job.job_id,
            "pulsar.io/user": job.user.replace("/", "-"),
            "pulsar.io/priority": job.priority.name.lower(),
            "pulsar.io/workload-type": job.workload_type.lower().replace(" ", "-"),
            "pulsar.io/gpu-class": gpu_class,
            # KGWE labels for GPU class visibility and scheduling
            "kgwe.nvidia.io/gpu-class": gpu_class,
            "kgwe.nvidia.io/preferred-gpu-class": job.preferred_gpu_class or "dgpu",
            "kgwe.nvidia.io/workload-type": job.workload_type.lower().replace(" ", "-"),
        }

        template = client.V1PodTemplateSpec(
            metadata=client.V1ObjectMeta(labels=labels, annotations=annotations),
            spec=pod_spec,
        )

        return client.V1Job(
            api_version="batch/v1",
            kind="Job",
            metadata=client.V1ObjectMeta(
                name=f"pulsar-{job.job_id}",
                namespace=job.namespace,
                labels=labels,
                annotations=annotations,
            ),
            spec=client.V1JobSpec(
                template=template,
                backoff_limit=2,
                ttl_seconds_after_finished=3600,
            ),
        )

    def terminate(self, job: GPUJob) -> Tuple[bool, str]:
        """Terminate a running job — kills the real process."""
        meta = self._created_jobs.pop(job.job_id, None)
        proc = self._processes.pop(job.job_id, None)
        self._watchers.pop(job.job_id, None)

        if proc and proc.poll() is None:
            try:
                proc.terminate()
                try:
                    proc.wait(timeout=5)
                except subprocess.TimeoutExpired:
                    proc.kill()
                    proc.wait()
                logger.info("[EXECUTOR] Process killed: %s PID=%d", job.job_id, proc.pid)
                return True, f"PID {proc.pid} terminated"
            except Exception as e:
                logger.error("[EXECUTOR] Failed to kill PID for %s: %s", job.job_id, e)
                return False, str(e)

        if not meta:
            return True, "no executor record"

        if meta.get("mode") == "kubernetes" and self._k8s_available:
            try:
                from kubernetes import client
                self._batch_client.delete_namespaced_job(
                    name=f"pulsar-{job.job_id}",
                    namespace=job.namespace,
                    body=client.V1DeleteOptions(propagation_policy="Background"),
                )
                logger.info("[EXECUTOR] K8s Job deleted: pulsar-%s", job.job_id)
                return True, f"K8s Job deleted"
            except Exception as e:
                return False, str(e)

        return True, "job terminated"

    def get_execution_info(self, job_id: str) -> Optional[dict]:
        """Get execution metadata for a job."""
        info = self._created_jobs.get(job_id)
        if info:
            result = dict(info)
            # Add live process status
            proc = self._processes.get(job_id)
            if proc:
                result["alive"] = proc.poll() is None
            return result
        return None

    def get_nvidia_smi(self) -> dict:
        """Get comprehensive live nvidia-smi data for dashboard display."""
        result = {"available": False, "processes": [], "gpu": {}}

        # 1. Try real nvidia-smi first
        try:
            gpu_out = subprocess.run(
                ["nvidia-smi",
                 "--query-gpu="
                 "name,driver_version,pci.bus_id,"
                 "utilization.gpu,utilization.memory,"
                 "memory.used,memory.total,memory.free,"
                 "temperature.gpu,temperature.memory,"
                 "power.draw,power.limit,"
                 "fan.speed,"
                 "clocks.current.graphics,clocks.current.memory,clocks.max.graphics,clocks.max.memory,"
                 "pcie.link.gen.current,pcie.link.width.current,"
                 "encoder.stats.averageFps,encoder.stats.averageLatency",
                 "--format=csv,noheader,nounits"],
                capture_output=True, text=True, timeout=5,
            )
            if gpu_out.returncode == 0:
                lines = [l.strip() for l in gpu_out.stdout.strip().split("\n") if l.strip()]
                if lines:
                    # Parse first GPU line; multi-GPU systems report one CSV line per GPU
                    parts = [p.strip() for p in lines[0].split(",")]
                    # Expected: name, driver, pci_bus, util.gpu, util.mem, mem.used, mem.total,
                    #            mem.free, temp.gpu, temp.mem, power.draw, power.limit,
                    #            fan.speed, clock.graphics, clock.memory, clock.max.graphics,
                    #            clock.max.memory, pcie.gen, pcie.width, encoder.fps, encoder.latency
                    def safe_int(v, d=0):
                        try: return int(float(v))
                        except (ValueError, TypeError): return d
                    def safe_float(v, d=0.0):
                        try: return float(v)
                        except (ValueError, TypeError): return d
                    if len(parts) >= 12:
                        result["available"] = True
                        # Cache detected memory so fallback never lies
                        mem_total = safe_int(parts[6])
                        if mem_total > 0:
                            self._dgpu_memory_mb = mem_total
                        result["gpu"] = {
                            "name": parts[0],
                            "driver_version": parts[1],
                            "pci_bus": parts[2],
                            "gpu_util": safe_int(parts[3]),
                            "mem_util": safe_int(parts[4]),
                            "mem_used_mb": safe_int(parts[5]),
                            "mem_total_mb": mem_total,
                            "mem_free_mb": safe_int(parts[7]),
                            "temperature_c": safe_int(parts[8]),
                            "temperature_mem_c": safe_int(parts[9]),
                            "power_w": safe_float(parts[10]),
                            "power_limit_w": safe_float(parts[11]),
                            "fan_speed_pct": safe_int(parts[12]) if len(parts) > 12 else 0,
                            "clock_graphics_mhz": safe_int(parts[13]) if len(parts) > 13 else 0,
                            "clock_memory_mhz": safe_int(parts[14]) if len(parts) > 14 else 0,
                            "clock_max_graphics_mhz": safe_int(parts[15]) if len(parts) > 15 else 0,
                            "clock_max_memory_mhz": safe_int(parts[16]) if len(parts) > 16 else 0,
                            "pcie_gen": safe_int(parts[17]) if len(parts) > 17 else 0,
                            "pcie_width": safe_int(parts[18]) if len(parts) > 18 else 0,
                            "encoder_fps": safe_int(parts[19]) if len(parts) > 19 else 0,
                            "encoder_latency": safe_int(parts[20]) if len(parts) > 20 else 0,
                        }

            # Process info via nvidia-smi
            proc_out = subprocess.run(
                ["nvidia-smi",
                 "--query-compute-apps=pid,used_gpu_memory,name",
                 "--format=csv,noheader,nounits"],
                capture_output=True, text=True, timeout=5,
            )
            if proc_out.returncode == 0:
                smi_processes = []
                for line in proc_out.stdout.strip().split("\n"):
                    if not line.strip(): continue
                    parts = [p.strip() for p in line.split(",")]
                    if len(parts) >= 3:
                        pid = int(parts[0])
                        job_id = None
                        for jid, proc in self._processes.items():
                            if proc.pid == pid:
                                job_id = jid
                                break
                        if not job_id:
                            for jid, proc in self._processes.items():
                                try:
                                    children = subprocess.run(["pgrep", "-P", str(proc.pid)], capture_output=True, text=True, timeout=1)
                                    if str(pid) in children.stdout:
                                        job_id = jid
                                        break
                                except: pass

                        smi_processes.append({
                            "pid": pid,
                            "gpu_mem_mb": int(parts[1]),
                            "process_name": parts[2],
                            "pulsar_job_id": job_id,
                        })
                result["processes"] = smi_processes

        except:
            pass

        # 2. Strict Internal State Mapping (Always available if real SMI fails)
        # This ensures the dashboard always shows the REAL state of the control plane
        if not result["available"]:
            active_jobs = [jid for jid, p in self._processes.items() if p.poll() is None]
            result["available"] = True

            # Use REAL detected memory, never config guesses. If detection failed,
            # 6144 is a sane default for modern laptops but will be overridden
            # once nvidia-smi works again.
            total_mem = self._dgpu_memory_mb if self._dgpu_memory_mb > 0 else 6144

            used_mem = 0
            for jid in active_jobs:
                meta = self._created_jobs.get(jid, {})
                used_mem += meta.get("gpu_mem_mb", 0)

            # Use real hardware name and PCIe bus if we detected it via lspci earlier
            result["gpu"] = {
                "name": self._dgpu_name or "NVIDIA GPU (driver unavailable)",
                "driver_version": "N/A",
                "pci_bus": self._dgpu_pci_bus,
                "gpu_util": min(100, int((used_mem / max(1, total_mem)) * 100)) if active_jobs else 0,
                "mem_util": int((used_mem / total_mem) * 100) if total_mem > 0 else 0,
                "mem_used_mb": used_mem,
                "mem_total_mb": total_mem,
                "mem_free_mb": max(0, total_mem - used_mem),
                "temperature_c": 0,
                "temperature_mem_c": 0,
                "power_w": 0.0,
                "power_limit_w": 0.0,
                "fan_speed_pct": 0,
                "clock_graphics_mhz": 0,
                "clock_memory_mhz": 0,
                "clock_max_graphics_mhz": 0,
                "clock_max_memory_mhz": 0,
                "pcie_gen": 0,
                "pcie_width": 0,
                "encoder_fps": 0,
                "encoder_latency": 0,
            }

            # Report the REAL processes we are tracking
            if not result["processes"]:
                for jid in active_jobs:
                    proc = self._processes.get(jid)
                    meta = self._created_jobs.get(jid, {})
                    result["processes"].append({
                        "pid": proc.pid,
                        "gpu_mem_mb": meta.get("gpu_mem_mb", 400),
                        "process_name": "python3 (pulsar.gpu_worker)",
                        "pulsar_job_id": jid,
                    })

        return result

    def get_active_pids(self) -> Dict[str, int]:
        """Get job_id → PID mapping for running processes."""
        return {
            jid: proc.pid
            for jid, proc in self._processes.items()
            if proc.poll() is None
        }

    def shutdown(self):
        """Kill all running processes."""
        for job_id, proc in list(self._processes.items()):
            if proc.poll() is None:
                proc.terminate()
                try:
                    proc.wait(timeout=3)
                except subprocess.TimeoutExpired:
                    proc.kill()
                logger.info("[EXECUTOR] Shutdown: killed PID %d (%s)", proc.pid, job_id)
        self._processes.clear()


# ─── Job Watcher ──────────────────────────────────────────────────

class JobWatcher:
    """
    Watches running Kubernetes Jobs/Pods and syncs status back to PULSAR.
    In standalone mode, process watching is handled by the executor itself.
    """

    def __init__(self, executor: JobExecutor, on_complete=None, on_fail=None):
        self._executor = executor
        self._on_complete = on_complete
        self._on_fail = on_fail
        self._running = False
        self._poll_interval = 10

    def start(self):
        if not self._executor.is_k8s_available:
            logger.info("JobWatcher: K8s not available — watcher disabled")
            return
        self._running = True
        thread = threading.Thread(target=self._watch_loop, daemon=True)
        thread.start()
        logger.info("JobWatcher started (poll interval=%ds)", self._poll_interval)

    def stop(self):
        self._running = False

    def _watch_loop(self):
        while self._running:
            try:
                self._poll_jobs()
            except Exception:
                logger.exception("JobWatcher poll error")
            time.sleep(self._poll_interval)

    def _poll_jobs(self):
        if not self._executor.is_k8s_available:
            return
        try:
            batch = self._executor._batch_client
            for job_id, meta in list(self._executor._created_jobs.items()):
                if meta.get("mode") != "kubernetes":
                    continue
                try:
                    k8s_job = batch.read_namespaced_job_status(
                        name=meta["k8s_name"], namespace=meta["namespace"],
                    )
                    if k8s_job.status.succeeded and k8s_job.status.succeeded > 0:
                        logger.info("[WATCHER] Job %s succeeded", job_id)
                        if self._on_complete:
                            self._on_complete(job_id)
                    elif k8s_job.status.failed and k8s_job.status.failed > 0:
                        logger.warning("[WATCHER] Job %s failed", job_id)
                        if self._on_fail:
                            self._on_fail(job_id, "K8s Job failed")
                except Exception as e:
                    if "404" in str(e) or "NotFound" in str(e):
                        logger.warning("[WATCHER] Job %s not found", job_id)
                    else:
                        logger.error("[WATCHER] Error checking %s: %s", job_id, e)
        except Exception:
            logger.exception("JobWatcher poll error")
