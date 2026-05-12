"""
PULSAR GPU Worker

Real GPU workload process launched by the PULSAR executor / job launcher.
Performs actual GPU computation (matrix multiplications) that is visible
in nvidia-smi. Each worker is tagged with team and job metadata.

Usage (launched automatically by the job launcher):
    python gpu_worker.py --job-id JOB123 --team-id team-alpha \
        --duration 60 --gpu-mem-mb 512 --workload-type Training

Requirements:
    - PyTorch with CUDA support
    - NVIDIA GPU with working driver
"""

import os
import sys
import time
import signal
import argparse
import logging

logging.basicConfig(
    format="[PULSAR WORKER] %(message)s",
    level=logging.INFO,
)
log = logging.getLogger("worker")

# Graceful shutdown
_running = True


def _handle_signal(signum, frame):
    global _running
    _running = False
    log.info("Received signal %d — shutting down", signum)


signal.signal(signal.SIGTERM, _handle_signal)
signal.signal(signal.SIGINT, _handle_signal)


def run_gpu_workload(
    job_id: str,
    team_id: str,
    duration_s: float,
    gpu_mem_mb: int,
    workload_type: str,
    cpu_only: bool = False,
):
    """Run actual GPU computation visible in nvidia-smi.

    Allocates large tensors on cuda:0 and runs continuous matrix
    multiplications to keep GPU utilization >50%.
    """
    try:
        import torch
    except ImportError:
        log.error("PyTorch not installed — cannot run GPU workload")
        sys.exit(1)

    if cpu_only or not torch.cuda.is_available():
        if cpu_only:
            log.info("CPU-only mode requested — using CPU")
        else:
            log.warning("CUDA not available — falling back to CPU workload")
        device = torch.device("cpu")
    else:
        device = torch.device("cuda:0")
        gpu_name = torch.cuda.get_device_name(0)
        log.info("GPU: %s", gpu_name)

    # ── Allocate tensors ──────────────────────────────────────────
    # Each float32 = 4 bytes. For an NxN matrix: 4*N*N bytes.
    # We create 2 matrices for matmul, so each gets half the budget.
    mem_bytes = gpu_mem_mb * 1024 * 1024
    n = int((mem_bytes / (2 * 4)) ** 0.5)
    n = max(512, min(n, 8192))  # Clamp to safe range

    log.info(
        "Allocating %dx%d matrices (~%d MB VRAM)",
        n, n, (2 * n * n * 4) // (1024 * 1024),
    )

    try:
        a = torch.randn(n, n, device=device)
        b = torch.randn(n, n, device=device)
    except torch.cuda.OutOfMemoryError:
        # Fall back to smaller matrices
        n = 1024
        log.warning("OOM — falling back to %dx%d matrices", n, n)
        a = torch.randn(n, n, device=device)
        b = torch.randn(n, n, device=device)

    mem_alloc = torch.cuda.memory_allocated(0) / (1024 * 1024)
    log.info("VRAM allocated: %.0f MB", mem_alloc)

    # ── Sustained GPU compute loop ────────────────────────────────
    # No sleep between iterations — keeps GPU utilization high (>50%).
    log.info("Running job %s for team %s", job_id, team_id)
    log.info(
        "Workload=%s, Duration=%.0fs, PID=%d",
        workload_type, duration_s, os.getpid(),
    )

    start = time.time()
    iterations = 0

    is_cuda = device.type == "cuda"

    while _running and (time.time() - start) < duration_s:
        if workload_type in ("Training", "FineTuning"):
            # Heavy compute: chained matmul
            c = torch.mm(a, b)
            c = torch.mm(c, a)
            if is_cuda: torch.cuda.synchronize()
        elif workload_type == "Inference":
            # Medium compute: matmul + softmax
            c = torch.mm(a, b)
            c = torch.softmax(c, dim=1)
            if is_cuda: torch.cuda.synchronize()
        elif workload_type == "DataPreprocessing":
            # Light compute: element-wise ops + matmul to keep util up
            c = a * b + a
            c = torch.relu(c)
            c = torch.mm(a, b)
            if is_cuda: torch.cuda.synchronize()
        else:
            c = torch.mm(a, b)
            if is_cuda: torch.cuda.synchronize()

        iterations += 1

        # Log progress every 30 seconds
        if iterations % 500 == 0:
            elapsed = time.time() - start
            log.info(
                "  [%s] %d iterations, %.0fs elapsed, %.0fs remaining",
                job_id, iterations, elapsed, max(0, duration_s - elapsed),
            )
        
        # CPU workload: don't spin too hard if on CPU
        if not is_cuda:
            time.sleep(0.01)

    elapsed = time.time() - start
    log.info("Done: %d iterations in %.1fs", iterations, elapsed)

    # Explicit cleanup
    del a, b
    if "c" in dir():
        del c
    if is_cuda: torch.cuda.empty_cache()
    log.info("Resources released")


def main():
    parser = argparse.ArgumentParser(description="PULSAR GPU Worker")
    parser.add_argument("--job-id", required=True, help="PULSAR job ID")
    parser.add_argument("--team-id", required=True, help="Team/user ID")
    parser.add_argument(
        "--duration", type=float, default=60, help="Duration in seconds"
    )
    parser.add_argument(
        "--gpu-mem-mb", type=int, default=512, help="GPU memory to allocate (MB)"
    )
    parser.add_argument(
        "--workload-type", default="Training", help="Workload type"
    )
    parser.add_argument(
        "--framework", default="PyTorch", help="ML Framework"
    )
    parser.add_argument(
        "--priority", default="NORMAL", help="Job priority"
    )
    parser.add_argument(
        "--cpu-only", action="store_true", help="Force CPU-only mode"
    )
    args = parser.parse_args()

    log.info("=== PULSAR GPU Worker ===")
    log.info("Job:      %s", args.job_id)
    log.info("Team:     %s", args.team_id)
    log.info("Type:     %s", args.workload_type)
    log.info("Duration: %.0fs", args.duration)
    log.info("GPU Mem:  %d MB", args.gpu_mem_mb)
    log.info("CPU Only: %s", args.cpu_only)
    log.info("PID:      %d", os.getpid())

    run_gpu_workload(
        job_id=args.job_id,
        team_id=args.team_id,
        duration_s=args.duration,
        gpu_mem_mb=args.gpu_mem_mb,
        workload_type=args.workload_type,
        cpu_only=args.cpu_only,
    )

    log.info("Worker exiting cleanly")
    sys.exit(0)


if __name__ == "__main__":
    main()
