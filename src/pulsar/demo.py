"""
PULSAR Demo

Runs through the full scheduling pipeline:
  config → submit → fair-share schedule → preempt → complete → reschedule

Run with:  cd src && python -m pulsar.demo
"""

from pulsar.pulsar_types import GPUJob, JobPriority
from pulsar.config import PulsarConfig
from pulsar.control_plane import PulsarControlPlane


def section(title):
    print(f"\n--- {title} ---\n")


def show_status(cp):
    d = cp.get_dashboard()
    cl = d["cluster"]
    m = d["metrics"]
    f = d.get("fairness", {})
    fi = d.get("fairness_index", 1.0)

    used = cl["used_gpus"]
    total = cl["total_gpus"]
    pct = round(cl.get("utilization", 0) * 100)
    bar_len = pct // 5

    print(f"  cluster: {used}/{total} GPUs ({pct}%) [{'█' * bar_len}{'·' * (20 - bar_len)}]")
    print(f"  active: {cl['active_jobs']}  policy: {cp.policy}  fairness: {fi:.3f}")

    if f:
        print(f"  per-team:")
        for user, data in f.items():
            s = data["usage_share"]
            bw = int(s * 20)
            print(f"    {user:<14} {'█' * bw}{'·' * (20 - bw)} {s:.0%}"
                  f"  active={data['active_gpus']}  w={data.get('weight', 1.0)}")

    active = d.get("active_jobs", {})
    if active:
        print(f"  jobs:")
        for jid, j in active.items():
            if isinstance(j, dict):
                print(f"    {j.get('job_id',''):<14} {j.get('user',''):<14} "
                      f"{j.get('gpu_required',0)} GPU  {j.get('priority',''):<8} "
                      f"{j.get('workload_type','')}")

    print(f"  metrics: submitted={m.get('jobs_submitted_total',0)} "
          f"admitted={m.get('jobs_admitted_total',0)} "
          f"rejected={m.get('jobs_rejected_total',0)} "
          f"completed={m.get('jobs_completed_total',0)}")


def main():
    print("\nPULSAR demo — GPU queue & fairness control plane\n")

    # 1. Config
    section("1. Init")
    config = PulsarConfig()
    config.cluster.total_gpus = 16
    config.cluster.gpu_memory_gb = 80
    config.scheduling.policy = "fair_share"
    config.scheduling.preemption.enabled = True
    config.persistence.enabled = False

    cp = PulsarControlPlane(config)
    cp.set_quota("team-alpha", max_gpus=8, max_jobs=6, weight=1.0)
    cp.set_quota("team-beta", max_gpus=6, max_jobs=5, weight=1.0)
    cp.set_quota("team-gamma", max_gpus=4, max_jobs=4, weight=0.5)
    print("  16 GPUs, fair_share policy, preemption on")
    print("  quotas: alpha=8, beta=6, gamma=4")

    # 2. Submit
    section("2. Submit jobs")
    jobs = [
        GPUJob(user="team-alpha", gpu_required=4, workload_type="Training", framework="PyTorch"),
        GPUJob(user="team-alpha", gpu_required=2, workload_type="FineTuning", framework="PyTorch"),
        GPUJob(user="team-beta", gpu_required=4, workload_type="Training", framework="TensorFlow"),
        GPUJob(user="team-beta", gpu_required=2, workload_type="Inference", framework="Triton"),
        GPUJob(user="team-gamma", gpu_required=2, workload_type="Training", framework="JAX"),
        GPUJob(user="team-gamma", gpu_required=1, workload_type="Inference", framework="Triton"),
        GPUJob(user="team-alpha", gpu_required=2, workload_type="DataPrep", framework="PyTorch"),
        GPUJob(user="team-beta", gpu_required=2, workload_type="FineTuning", framework="PyTorch"),
    ]
    for j in jobs:
        cp.submit_job(j)
    print(f"  {len(jobs)} jobs submitted")

    # 3. Schedule
    section("3. Schedule")
    scheduled = cp.process_all()
    print(f"  {len(scheduled)} jobs scheduled")
    show_status(cp)

    # 4. Preemption
    section("4. Preempt (CRITICAL job)")
    critical = GPUJob(
        user="team-beta", gpu_required=4, workload_type="Training",
        framework="PyTorch", priority=JobPriority.CRITICAL, preemptible=False,
    )
    cp.submit_job(critical)
    result = cp.process_all()
    print(f"  {len(result)} jobs scheduled after preemption")
    show_status(cp)

    # 5. Complete
    section("5. Complete first 3 jobs")
    for j in scheduled[:3]:
        cp.complete_job(j.job_id)
    print(f"  3 jobs completed, resources freed")

    # 6. Reschedule
    section("6. Reschedule")
    more = cp.process_all()
    print(f"  {len(more)} more jobs scheduled")
    show_status(cp)

    # 7. Metrics sample
    section("7. Prometheus metrics (first 15 lines)")
    prom = cp.get_prometheus_metrics()
    for line in prom.split("\n")[:15]:
        print(f"  {line}")

    print("\n--- done ---\n")
    print("  start the server: cd src && python -m pulsar.cli server")
    print("  dashboard:        http://localhost:8080/")
    print("  api docs:         http://localhost:8080/docs")
    print()


if __name__ == "__main__":
    main()
