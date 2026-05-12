"""
PULSAR Job Launcher

Lightweight module to launch, track, and kill real GPU worker processes.
Each launched job runs gpu_worker.py as a subprocess visible in nvidia-smi.

Usage:
    from pulsar.job_launcher import JobLauncher

    launcher = JobLauncher()
    pid = launcher.start_job("team-alpha", "job-001", duration=60)
    jobs = launcher.get_active_jobs()
    launcher.stop_job("job-001")
    launcher.stop_all()
"""

import os
import sys
import signal
import subprocess
import logging
from datetime import datetime
from typing import Dict, List, Optional

logging.basicConfig(
    format="[PULSAR] %(message)s",
    level=logging.INFO,
)
log = logging.getLogger("pulsar.launcher")

# CWD for subprocess: parent of the `pulsar` package so -m works
_SRC_DIR = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))


class JobLauncher:
    """
    Spawns and manages real GPU worker processes.

    Each job runs as a separate OS process performing CUDA matrix
    multiplications, fully visible in nvidia-smi.
    """

    def __init__(self):
        # job_id → {team_id, pid, process, started_at, duration}
        self._jobs: Dict[str, dict] = {}

    # ─── Start ──────────────────────────────────────────────────

    def start_job(
        self,
        team_id: str,
        job_id: str,
        duration: float = 60,
        gpu_mem_mb: int = 512,
        workload_type: str = "Training",
    ) -> int:
        """Launch a GPU worker subprocess.

        Returns the PID of the spawned process.

        Args:
            team_id:       Team or user identifier.
            job_id:        Unique job identifier.
            duration:      How long to run (seconds).
            gpu_mem_mb:    GPU memory to allocate (MB).
            workload_type: One of Training, Inference, FineTuning, DataPreprocessing.
        """
        if job_id in self._jobs:
            raise ValueError(f"Job {job_id} is already running")

        cmd = [
            sys.executable, "-m", "pulsar.gpu_worker",
            "--job-id", job_id,
            "--team-id", team_id,
            "--duration", str(duration),
            "--gpu-mem-mb", str(gpu_mem_mb),
            "--workload-type", workload_type,
        ]

        env = {
            **os.environ,
            "PULSAR_JOB_ID": job_id,
            "PULSAR_TEAM": team_id,
        }

        proc = subprocess.Popen(
            cmd,
            env=env,
            cwd=_SRC_DIR,
            stdout=subprocess.PIPE,
            stderr=subprocess.STDOUT,
        )

        pid = proc.pid

        self._jobs[job_id] = {
            "team_id": team_id,
            "pid": pid,
            "process": proc,
            "started_at": datetime.now(),
            "duration": duration,
            "gpu_mem_mb": gpu_mem_mb,
            "workload_type": workload_type,
        }

        log.info(
            "Started job %s (Team %s) with PID %d", job_id, team_id, pid
        )
        return pid

    # ─── Stop ───────────────────────────────────────────────────

    def stop_job(self, job_id: str) -> bool:
        """Kill a running job by its job_id.

        Sends SIGTERM first, escalates to SIGKILL after 5 seconds.
        Process will disappear from nvidia-smi after termination.

        Returns True if the process was successfully terminated.
        """
        entry = self._jobs.pop(job_id, None)
        if entry is None:
            log.warning("Job %s not found", job_id)
            return False

        proc = entry["process"]
        pid = entry["pid"]

        if proc.poll() is not None:
            log.info("Stopped job %s (PID %d) — already exited", job_id, pid)
            return True

        try:
            proc.terminate()  # SIGTERM
            try:
                proc.wait(timeout=5)
            except subprocess.TimeoutExpired:
                proc.kill()  # SIGKILL
                proc.wait()
            log.info("Stopped job %s (PID %d)", job_id, pid)
            return True
        except Exception as e:
            log.error("Failed to stop job %s (PID %d): %s", job_id, pid, e)
            return False

    def stop_all(self):
        """Kill all running jobs."""
        job_ids = list(self._jobs.keys())
        for job_id in job_ids:
            self.stop_job(job_id)
        log.info("All jobs stopped")

    # ─── Monitoring ─────────────────────────────────────────────

    def get_active_jobs(self) -> List[dict]:
        """Return a list of active jobs with their status.

        Returns:
            List of dicts with keys: job_id, team_id, pid, status
        """
        active = []
        expired = []

        for job_id, entry in self._jobs.items():
            proc = entry["process"]
            poll = proc.poll()

            if poll is None:
                status = "RUNNING"
            elif poll == 0:
                status = "COMPLETED"
                expired.append(job_id)
            else:
                status = f"EXITED (code {poll})"
                expired.append(job_id)

            active.append({
                "job_id": job_id,
                "team_id": entry["team_id"],
                "pid": entry["pid"],
                "status": status,
                "started_at": entry["started_at"].isoformat(),
                "duration": entry["duration"],
                "gpu_mem_mb": entry["gpu_mem_mb"],
                "workload_type": entry["workload_type"],
            })

        # Clean up finished jobs
        for jid in expired:
            self._jobs.pop(jid, None)

        return active

    def is_running(self, job_id: str) -> bool:
        """Check if a specific job is still running."""
        entry = self._jobs.get(job_id)
        if entry is None:
            return False
        return entry["process"].poll() is None

    def get_pid(self, job_id: str) -> Optional[int]:
        """Get the PID of a running job."""
        entry = self._jobs.get(job_id)
        if entry is None:
            return None
        return entry["pid"]

    @property
    def running_count(self) -> int:
        """Number of currently running jobs."""
        return sum(
            1 for e in self._jobs.values() if e["process"].poll() is None
        )

    def __del__(self):
        """Cleanup: kill all processes on garbage collection."""
        self.stop_all()
