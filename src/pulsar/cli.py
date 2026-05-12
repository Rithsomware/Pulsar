"""
PULSAR CLI

Command-line interface for managing the PULSAR GPU Queue & Fairness Control Plane.

Usage:
    pulsar server                              Start the API server
    pulsar submit --user X --gpus N            Submit a job
    pulsar jobs [--user X] [--status running]  List jobs
    pulsar status <job-id>                     Get job status
    pulsar cancel <job-id>                     Cancel a job
    pulsar cluster                             Show cluster status
    pulsar fairness                            Show fairness report
    pulsar quotas                              List quotas
    pulsar demo                                Run built-in demo
"""

import argparse
import json
import sys

import httpx


DEFAULT_URL = "http://localhost:8080"


def _api(method, path, url=DEFAULT_URL, data=None):
    """Make an API call and return the response."""
    full = f"{url}{path}"
    try:
        if method == "GET":
            r = httpx.get(full, timeout=10)
        elif method == "POST":
            r = httpx.post(full, json=data or {}, timeout=10)
        elif method == "PUT":
            r = httpx.put(full, json=data or {}, timeout=10)
        elif method == "DELETE":
            r = httpx.delete(full, timeout=10)
        else:
            raise ValueError(f"Unknown method: {method}")
        r.raise_for_status()
        if r.headers.get("content-type", "").startswith("text/"):
            return r.text
        return r.json()
    except httpx.ConnectError:
        print(f"Error: Cannot connect to PULSAR server at {url}")
        print("Start the server with: pulsar server")
        sys.exit(1)
    except httpx.HTTPStatusError as e:
        print(f"Error: {e.response.status_code} - {e.response.text}")
        sys.exit(1)


def cmd_server(args):
    """Start the PULSAR API server."""
    import uvicorn
    from pulsar.api_server import create_app
    from pulsar.config import PulsarConfig

    config = PulsarConfig.load(args.config)
    app = create_app(config)
    print(f"Starting PULSAR server on {config.api.host}:{config.api.port}")
    print(f"Dashboard: http://localhost:{config.api.port}/")
    print(f"API Docs:  http://localhost:{config.api.port}/docs")
    uvicorn.run(app, host=config.api.host, port=config.api.port, log_level="info")


def cmd_submit(args):
    """Submit a GPU job."""
    data = {
        "user": args.user,
        "gpu_required": args.gpus,
        "workload_type": args.type,
        "framework": args.framework,
        "priority": args.priority.upper(),
        "preemptible": not args.no_preempt,
    }
    if args.memory:
        data["gpu_memory_gb"] = args.memory
    result = _api("POST", "/api/v1/jobs", args.url, data)
    job = result.get("job", {})
    print(f"Job submitted: {job.get('job_id')} (status: {job.get('status')})")


def cmd_jobs(args):
    """List jobs."""
    params = []
    if args.user:
        params.append(f"user={args.user}")
    if args.status:
        params.append(f"status={args.status.upper()}")
    query = "?" + "&".join(params) if params else ""
    result = _api("GET", f"/api/v1/jobs{query}", args.url)
    jobs = result.get("jobs", [])
    if not jobs:
        print("No jobs found.")
        return
    print(f"{'JOB ID':<16} {'USER':<16} {'GPUs':>4} {'STATUS':<12} {'PRIORITY':<10} {'TYPE':<16}")
    print("-" * 80)
    for j in jobs:
        print(f"{j['job_id']:<16} {j['user']:<16} {j['gpu_required']:>4} "
              f"{j['status']:<12} {j['priority']:<10} {j['workload_type']:<16}")


def cmd_status(args):
    """Get job status."""
    result = _api("GET", f"/api/v1/jobs/{args.job_id}", args.url)
    print(json.dumps(result, indent=2))


def cmd_cancel(args):
    """Cancel a job."""
    result = _api("DELETE", f"/api/v1/jobs/{args.job_id}", args.url)
    print(f"Job {args.job_id}: {result.get('status', 'unknown')}")


def cmd_complete(args):
    """Complete a job (simulation)."""
    result = _api("POST", f"/api/v1/jobs/{args.job_id}/complete", args.url)
    print(f"Job {args.job_id}: {result.get('status', 'unknown')}")


def cmd_cluster(args):
    """Show cluster status."""
    result = _api("GET", "/api/v1/cluster", args.url)
    print(f"Cluster Status:")
    print(f"  Total GPUs:     {result.get('total_gpus', 0)}")
    print(f"  Available GPUs: {result.get('available_gpus', 0)}")
    print(f"  Used GPUs:      {result.get('used_gpus', 0)}")
    print(f"  Utilization:    {result.get('utilization_pct', '0%')}")
    print(f"  Active Jobs:    {result.get('active_jobs', 0)}")
    print(f"  Memory/Device:  {result.get('gpu_memory_gb_per_device', 0)}GB")


def cmd_fairness(args):
    """Show fairness report."""
    result = _api("GET", "/api/v1/fairness", args.url)
    print(f"Jain's Fairness Index: {result.get('jains_fairness_index', 0):.4f}")
    print()
    report = result.get("report", {})
    if report:
        print(f"{'USER':<16} {'ACTIVE':>6} {'CUMUL':>6} {'WEIGHT':>6} {'PRIORITY':>8} {'SHARE':>7}")
        print("-" * 55)
        for user, d in report.items():
            print(f"{user:<16} {d['active_gpus']:>6} {d['cumulative_gpu_usage']:>6.0f} "
                  f"{d['weight']:>6.1f} {d['fairness_priority']:>8.4f} {d['usage_share']:>6.0%}")


def cmd_quotas(args):
    """List quotas."""
    result = _api("GET", "/api/v1/quotas", args.url)
    quotas = result.get("quotas", {})
    if not quotas:
        print("No quotas configured.")
        return
    print(f"{'USER':<16} {'GPU USED':>8} {'GPU MAX':>7} {'JOBS':>5} {'GPU-HRS':>8}")
    print("-" * 50)
    for user, q in quotas.items():
        print(f"{user:<16} {q['current_gpu_usage']:>4}/{q['max_gpus']:<3} "
              f"{q['max_gpus']:>7} {q['current_job_count']:>3}/{q['max_jobs']:<2} "
              f"{q['total_gpu_hours']:>7.1f}")


def cmd_metrics(args):
    """Show Prometheus metrics."""
    result = _api("GET", "/api/v1/metrics", args.url)
    print(result)


def cmd_demo(args):
    """Run built-in demo."""
    from pulsar.demo import main as demo_main
    demo_main()


def main():
    parser = argparse.ArgumentParser(
        prog="pulsar",
        description="PULSAR — GPU Queue & Fairness Control Plane CLI",
    )
    sub = parser.add_subparsers(dest="command")

    # server
    p = sub.add_parser("server", help="Start the API server")
    p.add_argument("--config", "-c", help="Config file path")
    p.set_defaults(func=cmd_server)

    # submit
    p = sub.add_parser("submit", help="Submit a GPU job")
    p.add_argument("--user", "-u", required=True)
    p.add_argument("--gpus", "-g", type=int, required=True)
    p.add_argument("--type", "-t", default="Training")
    p.add_argument("--framework", "-f", default="PyTorch")
    p.add_argument("--priority", "-p", default="NORMAL")
    p.add_argument("--memory", "-m", type=int, default=0)
    p.add_argument("--no-preempt", action="store_true")
    p.add_argument("--url", default=DEFAULT_URL)
    p.set_defaults(func=cmd_submit)

    # jobs
    p = sub.add_parser("jobs", help="List jobs")
    p.add_argument("--user", "-u")
    p.add_argument("--status", "-s")
    p.add_argument("--url", default=DEFAULT_URL)
    p.set_defaults(func=cmd_jobs)

    # status
    p = sub.add_parser("status", help="Get job status")
    p.add_argument("job_id")
    p.add_argument("--url", default=DEFAULT_URL)
    p.set_defaults(func=cmd_status)

    # cancel
    p = sub.add_parser("cancel", help="Cancel a job")
    p.add_argument("job_id")
    p.add_argument("--url", default=DEFAULT_URL)
    p.set_defaults(func=cmd_cancel)

    # complete
    p = sub.add_parser("complete", help="Mark a job as completed")
    p.add_argument("job_id")
    p.add_argument("--url", default=DEFAULT_URL)
    p.set_defaults(func=cmd_complete)

    # cluster
    p = sub.add_parser("cluster", help="Show cluster status")
    p.add_argument("--url", default=DEFAULT_URL)
    p.set_defaults(func=cmd_cluster)

    # fairness
    p = sub.add_parser("fairness", help="Show fairness report")
    p.add_argument("--url", default=DEFAULT_URL)
    p.set_defaults(func=cmd_fairness)

    # quotas
    p = sub.add_parser("quotas", help="List quotas")
    p.add_argument("--url", default=DEFAULT_URL)
    p.set_defaults(func=cmd_quotas)

    # metrics
    p = sub.add_parser("metrics", help="Show Prometheus metrics")
    p.add_argument("--url", default=DEFAULT_URL)
    p.set_defaults(func=cmd_metrics)

    # demo
    p = sub.add_parser("demo", help="Run built-in demo")
    p.set_defaults(func=cmd_demo)

    args = parser.parse_args()
    if not args.command:
        parser.print_help()
        sys.exit(1)
    args.func(args)


if __name__ == "__main__":
    main()
