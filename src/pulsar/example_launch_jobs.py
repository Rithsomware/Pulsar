#!/usr/bin/env python3
"""
PULSAR Example: Launch Multiple Real GPU Jobs

Demonstrates the job launcher by starting multiple concurrent GPU
workloads that are visible in nvidia-smi.

Usage:
    python -m pulsar.example_launch_jobs

    # In a separate terminal, watch GPU usage:
    watch -n 1 nvidia-smi
"""

import time
import sys
import os

# Ensure the src/ directory is on the path
sys.path.insert(0, os.path.join(os.path.dirname(__file__), ".."))

from pulsar.job_launcher import JobLauncher


def main():
    launcher = JobLauncher()

    print("=" * 60)
    print("  PULSAR — Real GPU Job Launcher Demo")
    print("=" * 60)
    print()
    print("  Open another terminal and run:")
    print("    watch -n 1 nvidia-smi")
    print()
    print("  You will see real GPU processes with PIDs and VRAM usage.")
    print("=" * 60)
    print()

    # ── Launch 3 concurrent jobs from different teams ──────────

    jobs = [
        {
            "team_id": "team-alpha",
            "job_id": "job-001",
            "duration": 45,
            "gpu_mem_mb": 256,
            "workload_type": "Training",
        },
        {
            "team_id": "team-beta",
            "job_id": "job-002",
            "duration": 30,
            "gpu_mem_mb": 256,
            "workload_type": "Inference",
        },
        {
            "team_id": "team-gamma",
            "job_id": "job-003",
            "duration": 60,
            "gpu_mem_mb": 256,
            "workload_type": "FineTuning",
        },
    ]

    print("[DEMO] Starting 3 concurrent GPU jobs...\n")

    for spec in jobs:
        try:
            pid = launcher.start_job(**spec)
            print(
                f"  ✓ {spec['job_id']} ({spec['team_id']}) — "
                f"PID {pid}, {spec['workload_type']}, "
                f"{spec['gpu_mem_mb']}MB VRAM, {spec['duration']}s"
            )
        except Exception as e:
            print(f"  ✗ {spec['job_id']} failed: {e}")

    print()

    # ── Monitor loop ──────────────────────────────────────────────

    print("[DEMO] Monitoring active jobs (Ctrl+C to stop all)...\n")

    try:
        while launcher.running_count > 0:
            active = launcher.get_active_jobs()
            print(f"  [{time.strftime('%H:%M:%S')}] Active jobs: {len(active)}")
            for job in active:
                print(
                    f"    • {job['job_id']} | Team: {job['team_id']} | "
                    f"PID: {job['pid']} | Status: {job['status']} | "
                    f"Type: {job['workload_type']}"
                )
            print()
            time.sleep(5)

    except KeyboardInterrupt:
        print("\n[DEMO] Ctrl+C received — stopping all jobs...\n")
        launcher.stop_all()
        print()

    # ── Preemption demo ───────────────────────────────────────────

    print("[DEMO] Demonstrating job preemption...")
    print()

    try:
        pid = launcher.start_job(
            team_id="team-delta",
            job_id="job-preempt",
            duration=120,
            gpu_mem_mb=256,
            workload_type="Training",
        )
        print(f"  ✓ job-preempt started with PID {pid}")
        print("  Waiting 10 seconds before preempting...")
        time.sleep(10)

        # Verify it's running
        active = launcher.get_active_jobs()
        for job in active:
            if job["job_id"] == "job-preempt":
                print(f"  → job-preempt is {job['status']} (PID {job['pid']})")

        # Preempt (kill) the job
        launcher.stop_job("job-preempt")
        print("  ✓ job-preempt preempted — verify it disappeared from nvidia-smi")
        print()

    except Exception as e:
        print(f"  Preemption demo failed: {e}")

    # ── Final status ──────────────────────────────────────────────

    remaining = launcher.get_active_jobs()
    if remaining:
        print(f"[DEMO] Cleaning up {len(remaining)} remaining jobs...")
        launcher.stop_all()

    print("[DEMO] All done.")
    print()
    print("=" * 60)
    print("  Verify with: nvidia-smi")
    print("  All PULSAR processes should be gone.")
    print("=" * 60)


if __name__ == "__main__":
    main()
