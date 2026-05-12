"""
PULSAR Simulation Script

Simulates multiple teams submitting GPU jobs concurrently to the PULSAR control plane.
Demonstrates fairness, queuing, and real process execution.
"""

import time
import random
import threading
import httpx

BASE_URL = "http://localhost:8081"
TEAMS = ["ml-team-alpha", "ml-team-beta", "ml-team-gamma"]
WORKLOAD_TYPES = ["Training", "FineTuning", "Inference", "DataPrep"]
FRAMEWORKS = ["PyTorch", "TensorFlow", "JAX"]

def submit_job(team):
    gpus = random.choice([1, 2, 4])
    duration = random.uniform(0.5, 2.0)
    workload = random.choice(WORKLOAD_TYPES)
    framework = random.choice(FRAMEWORKS)
    priority = random.choice(["NORMAL", "HIGH", "LOW"])

    payload = {
        "user": team,
        "gpu_required": gpus,
        "workload_type": workload,
        "framework": framework,
        "priority": priority,
        "estimated_duration_minutes": duration
    }

    try:
        response = httpx.post(f"{BASE_URL}/api/v1/jobs", json=payload, timeout=10)
        if response.status_code == 200:
            job = response.json().get("job", {})
            print(f"[SUBMIT] Team: {team:<15} Job: {job.get('job_id')} ({gpus} GPUs, {priority})")
        else:
            print(f"[ERROR] Failed to submit job for {team}: {response.text}")
    except Exception as e:
        print(f"[ERROR] Connection error for {team}: {e}")

def team_simulator(team, num_jobs):
    for _ in range(num_jobs):
        submit_job(team)
        time.sleep(random.uniform(1, 3))

def main():
    print("Starting PULSAR Simulation...")
    print(f"Target: {BASE_URL}")
    print(f"Teams: {', '.join(TEAMS)}")
    print("-" * 50)

    threads = []
    for team in TEAMS:
        t = threading.Thread(target=team_simulator, args=(team, 5))
        threads.append(t)
        t.start()

    for t in threads:
        t.join()

    print("-" * 50)
    print("Simulation complete. Check the dashboard at http://localhost:8081/")

if __name__ == "__main__":
    main()
