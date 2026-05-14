# PULSAR Complete Job Flow Analysis

## Executive Summary

Jobs submitted through the showcase script flow through 7 major stages:

1. **CLI Submission** → API endpoint
2. **Queue Management** → stored in per-user FIFO queues
3. **Fairness Selection** → chosen by fair_share algorithm
4. **Admission Control** → GPU resources checked and allocated
5. **GPU Fallback** → assigned to dgpu/igpu/cpu
6. **Execution** → launched as real subprocess via JobExecutor
7. **Completion Tracking** → resources released and job marked complete

---

## Stage 1: CLI Submit Command (src/pulsar/cli.py)

### cmd_submit()

```python
def cmd_submit(args):
    data = {
        "user": args.user,
        "gpu_required": args.gpus,
        "workload_type": args.type,
        "framework": args.framework,
        "priority": args.priority.upper(),
        "preemptible": not args.no_preempt,
    }
    result = _api("POST", "/api/v1/jobs", args.url, data)
    print(f"Job submitted: {job.get('job_id')} (status: {job.get('status')})")
```

**Flow:**

- Makes HTTP POST to `/api/v1/jobs` at `http://localhost:8080`
- Sends job metadata as JSON
- Expects response with job object including job_id and status

**Potential Issues:**

- ❌ Server not running at specified URL
- ❌ Server not ready (readiness check failing)
- ❌ Network/connection error

---

## Stage 2: API Server Job Submission (src/pulsar/api_server.py)

### @app.post("/api/v1/jobs")

```python
async def submit_job(req: JobSubmitRequest):
    cp = get_cp()
    job = GPUJob(
        user=req.user,
        gpu_required=req.gpu_required,
        ...
    )
    cp.submit_job(job)
    return {"status": "queued", "job": job.to_dict()}
```

**Flow:**

- Creates JobSubmitRequest model from HTTP body
- Creates GPUJob object with unique job_id (UUID)
- Calls `control_plane.submit_job(job)`
- Returns immediate response with job details

**Key Point:** Response is sent IMMEDIATELY - job status is "QUEUED" but not yet processed

---

## Stage 3: Control Plane Job Submission (src/pulsar/control_plane.py)

### control_plane.submit_job()

```python
def submit_job(self, job: GPUJob) -> GPUJob:
    if not job.preferred_gpu_class:
        job.preferred_gpu_class = self.config.scheduling.fallback.preferred_gpu_class
    with self._lock:
        self._all_jobs[job.job_id] = job
    self.metrics.record_submission(job.user)
    self.queue_controller.submit_job(job)  # ← Queue it
    if self._store:
        self._store.save_job(job)  # ← Persist to DB
    logger.info("Job %s submitted by %s (%d GPUs, %s, priority=%s)",
                 job.job_id, job.user, job.gpu_required,
                 job.workload_type, job.priority.name)
    return job
```

**Flow:**

1. Stores job in `_all_jobs` dict (for lookups)
2. Calls `queue_controller.submit_job(job)` → **QUEUES THE JOB**
3. Saves to SQLite persistence if enabled
4. Records metrics

---

## Stage 4: Queue Controller (src/pulsar/queue_controller.py)

### queue_controller.submit_job()

```python
def submit_job(self, job: GPUJob) -> PulsarEvent:
    with self._lock:
        user_depth = sum(...)
        if user_depth >= self.max_queue_depth_per_tenant:
            job.status = JobStatus.REJECTED  # ❌ REJECTED if queue full
            return event

    event = self.queue_manager.submit_job(job)
    with self._lock:
        self._last_boosted[job.job_id] = datetime.now()
    return event
```

**Flow:**

1. Checks per-tenant queue depth limit (default 50)
2. If at limit → **JOB IS REJECTED** 🔴
3. Otherwise → calls `queue_manager.submit_job(job)`

### queue_manager.submit_job()

```python
def submit_job(self, job: GPUJob) -> PulsarEvent:
    with self._lock:
        job.status = JobStatus.QUEUED  # ← Set to QUEUED
        self._user_queues[job.user].append(job)  # ← Add to user's FIFO queue
        self._job_index[job.job_id] = job
        if job.user not in self._user_order:
            self._user_order.append(job.user)
    event = PulsarEvent(...)
    return event
```

**Flow:**

- Job stored in `self._user_queues[user]` as a list
- Each user has their own FIFO queue
- Job status = `QUEUED`

**Key Point:** Job is now in queue waiting to be dequeued

### queue_controller Background Loop (CRITICAL!)

```python
def start(self):
    """Start the background queue controller loop."""
    if self._running:
        return
    self._running = True
    self._stop_event.clear()
    self._thread = threading.Thread(target=self._controller_loop, daemon=True)
    self._thread.start()

def _controller_loop(self):
    interval = min(self.aging_boost_interval_seconds, 5.0)
    while self._running and not self._stop_event.is_set():
        try:
            self._apply_aging()  # Boost priority for long-waiting jobs
            self._check_starvation()  # Force-promote starved jobs
            if self.preemption_signals:
                self._emit_preemption_signals()
        except Exception:
            logger.exception("Queue controller loop error")
        self._stop_event.wait(timeout=interval)
```

**CRITICAL:** This runs in background every 5 seconds (or aging_interval if smaller)

- Applies priority aging
- Prevents starvation
- Emits preemption signals

---

## Stage 5: Background Scheduler Loop (src/pulsar/control_plane.py)

### control_plane.start_scheduler() - **MUST BE CALLED**

```python
def start_scheduler(self):
    """Start the background scheduling loop and job watcher."""
    if self._scheduler_running:
        return
    self._scheduler_running = True

    # Queue controller (aging, starvation prevention, preemption signals)
    self.queue_controller.start()  # ← BACKGROUND AGING LOOP

    # Scheduler thread
    sched_thread = threading.Thread(target=self._scheduler_loop, daemon=True)
    sched_thread.start()  # ← BACKGROUND SCHEDULER LOOP

    # Job watcher (monitors K8s pod status)
    self.watcher.start()  # ← MONITORS RUNNING JOBS

    logger.info("Background scheduler started (interval=%.1fs)",
                 self.config.scheduling.scheduling_interval_seconds)
```

**CRITICAL REQUIREMENT:** This must be called in the lifespan handler!

### control_plane._scheduler_loop() - **THE MAIN ENGINE**

```python
def _scheduler_loop(self):
    interval = self.config.scheduling.scheduling_interval_seconds  # default 2s
    while self._scheduler_running:
        try:
            self.process_all()  # ← PROCESSES QUEUED JOBS
        except Exception:
            logger.exception("Scheduler loop error")
        time.sleep(interval)
```

**Runs every 2 seconds** (or configured interval)

- Continuously calls `process_all()` to dequeue and schedule jobs

### control_plane.process_all()

```python
def process_all(self) -> List[GPUJob]:
    """Process all queued jobs until queue is empty or no capacity."""
    scheduled = []
    max_iters = self.queue_controller.total_queued + 10
    iters = 0
    while self.queue_controller.total_queued > 0 and iters < max_iters:
        job = self.process_queue()  # ← PROCESS ONE JOB
        if job:
            scheduled.append(job)
        else:
            break
        iters += 1
    return scheduled
```

**Flow:**

1. Loops until queue empty or capacity exhausted
2. Each iteration calls `process_queue()`

---

## Stage 6: The Core Scheduling Cycle (control_plane.process_queue())

```python
def process_queue(self) -> Optional[GPUJob]:
    """Process the next job from the queue. Core scheduling cycle.

    Flow: Queue → Fairness Selection → Admission → Execution → Track
    """
    if self.queue_controller.total_queued == 0:
        return None  # ← No jobs, exit

    start_time = time.time()

    # 1. GET FAIRNESS SCORES
    users_with_jobs = self.queue_controller.users_with_jobs
    if not users_with_jobs:
        return None

    fairness_scores = {
        user: self.fair_scheduler.get_priority(user)
        for user in users_with_jobs
    }

    # 2. DEQUEUE JOB (using fairness scores)
    job = self.queue_controller.get_next_job(fairness_scores, policy=self.policy)
    if not job:
        return None

    # 3. ADMISSION CHECK
    can_admit, reason = self.admission_controller.can_admit(job)

    if not can_admit:
        # Try preemption if high-priority job
        if self.preemption_engine.should_preempt(...):
            # ... handle preemption ...

        if not can_admit:
            if "Insufficient cluster" in reason:
                self.queue_controller.requeue_job(job)  # ← Put back at front
                return None
            else:
                job.status = JobStatus.REJECTED  # ❌ REJECTED
                return None

    # 4. ADMIT THE JOB
    self.admission_controller.allocate(job)
    self.fair_scheduler.update_usage(job.user, job.gpu_required)
    self.fair_scheduler.update_resource_usage(job.user, job.gpu_required, job.gpu_memory_gb)

    # 5. DECIDE GPU CLASS (dgpu vs igpu vs cpu)
    self._apply_gpu_fallback_policy(job)

    # 6. EXECUTE THE JOB
    success, exec_msg = self.executor.execute(job)
    if not success:
        # Release and fail
        self.admission_controller.release(job.job_id)
        job.status = JobStatus.FAILED
        return None

    # 7. TRACK AS RUNNING
    with self._lock:
        self._scheduled_jobs[job.job_id] = job

    if self._store:
        self._store.save_job(job)

    logger.info("[SCHEDULE] %s → %d GPUs (user=%s, policy=%s)",
                job.job_id, job.gpu_required, job.user, self.policy)
    return job
```

### Key Sub-Steps

#### 6.1 - Fairness Selection (queue_controller.get_next_job)

```python
def get_next_job(self, fairness_scores, policy="fair_share") -> Optional[GPUJob]:
    """Dequeue with starvation bypass."""
    # First check for starved jobs (waiting > 60s)
    starved = self._get_starved_jobs()
    if starved:
        job = self._force_promote_starved(starved)
        if job:
            return job  # ← Force-promote starved job

    # Otherwise use fairness/priority
    return self.queue_manager.get_next_job(fairness_scores, policy)
```

**Flow in queue_manager.get_next_job():**

```python
def get_next_job(self, fairness_scores, policy="fair_share"):
    with self._lock:
        if not self._job_index:
            return None  # ← Queue empty

        # Remove job from both queue structures
        selected_user = None
        job = None

        if policy == "fair_share":
            # Select user with highest fairness score
            candidates = [(u, fairness_scores.get(u, 0.5)) for u in self._user_queues]
            candidates.sort(key=lambda x: x[1], reverse=True)
            selected_user = candidates[0][0]

            # Dequeue FIFO from that user's queue
            if selected_user and self._user_queues.get(selected_user):
                job = self._user_queues[selected_user].pop(0)  # ← POP FROM QUEUE
                del self._job_index[job.job_id]

        return job
```

**Result:** Job removed from queue, ready for admission check

#### 6.2 - Admission Control Check

```python
can_admit, reason = self.admission_controller.can_admit(job)

def can_admit(self, job: GPUJob) -> Tuple[bool, str]:
    with self._lock:
        return self._can_admit_unlocked(job)

def _can_admit_unlocked(self, job: GPUJob) -> Tuple[bool, str]:
    # Check 1: Cluster GPU availability
    if job.gpu_required > self._available_gpus:
        return False, f"Insufficient cluster GPUs: need {job.gpu_required}, available {self._available_gpus}/{self._total_gpus}"

    # Check 2: GPU memory availability
    if job.gpu_memory_gb > 0:
        needed_mem = job.gpu_required * job.gpu_memory_gb
        if needed_mem > self._available_memory_gb:
            return False, f"Insufficient GPU memory: need {needed_mem}GB, available {self._available_memory_gb}GB"

    # Check 3: User quota
    quota = self._quotas.get(job.user)
    if quota:
        if job.gpu_required > quota.gpu_available:
            return False, f"User quota exceeded: need {job.gpu_required}, quota allows {quota.gpu_available} more"
        if not quota.can_submit:
            return False, f"Job limit reached: {quota.current_job_count}/{quota.max_jobs} jobs"

    return True, "OK"
```

**Possible Rejection Reasons:**

1. ❌ **Insufficient cluster GPUs** → requeue and retry later
2. ❌ **Insufficient GPU memory** → requeue or reject
3. ❌ **User quota exceeded** → reject
4. ❌ **Job limit reached** → reject

#### 6.3 - Allocation

```python
def allocate(self, job: GPUJob) -> PulsarEvent:
    """Allocate GPU resources for a job."""
    with self._lock:
        can, reason = self._can_admit_unlocked(job)
        if not can:
            job.status = JobStatus.REJECTED  # ❌ REJECTED
            return event

        # Allocate from cluster pool
        self._available_gpus -= job.gpu_required
        mem_used = job.gpu_required * (job.gpu_memory_gb or self._gpu_memory_gb)
        self._available_memory_gb -= mem_used

        job.status = JobStatus.ADMITTED  # ✅ ADMITTED
        job.admitted_at = datetime.now()
        self._active_jobs[job.job_id] = job

        # Update user quota
        quota = self._quotas.get(job.user)
        if quota:
            quota.current_gpu_usage += job.gpu_required
            quota.current_job_count += 1

        return event
```

#### 6.4 - GPU Fallback Policy (dgpu vs igpu)

```python
def _apply_gpu_fallback_policy(self, job: GPUJob):
    cfg = self.config.scheduling.fallback
    preferred = (job.preferred_gpu_class or cfg.preferred_gpu_class or "dgpu").lower()

    # Timeout-based fallback: if waiting > X seconds on dgpu, use igpu
    timeout = cfg.max_dgpu_wait_seconds or 0.0
    if preferred == "dgpu" and timeout > 0 and wait_seconds >= timeout:
        job.assigned_gpu_class = "igpu"
        job.fallback_applied = True
        job.fallback_reason = "queue_timeout"

    # Hardware-based fallback: if no dgpu available, use igpu or cpu
    if not self.executor.is_k8s_available and not fallback_applied:
        if assigned == "dgpu" and not self.executor.has_dgpu:
            if self.executor.has_igpu:
                assigned = "igpu"
            else:
                assigned = "cpu"
```

#### 6.5 - Execution (Create Real Process)

```python
success, exec_msg = self.executor.execute(job)

def execute(self, job: GPUJob) -> Tuple[bool, str]:
    if self._k8s_available:
        return self._create_k8s_job(job)
    else:
        return self._execute_standalone(job)

def _execute_standalone(self, job: GPUJob) -> Tuple[bool, str]:
    job.status = JobStatus.RUNNING
    job.started_at = datetime.now()

    # Build command
    cmd = [
        sys.executable, "-m", "pulsar.gpu_worker",
        "--job-id", job.job_id,
        "--team-id", job.user,
        "--duration", str(duration_s),
        ...
    ]

    # Spawn subprocess
    proc = subprocess.Popen(cmd, env=env, cwd=src_dir, stdout=subprocess.PIPE, stderr=subprocess.STDOUT)

    pid = proc.pid
    self._processes[job.job_id] = proc

    # Start watcher thread
    watcher = threading.Thread(target=self._watch_process, args=(job.job_id, proc, duration_s), daemon=True)
    watcher.start()

    logger.info("[EXECUTOR] Process started: %s PID=%d", job.job_id, pid)
    return True, f"PID {pid}"
```

**Key Points:**

- Spawns real GPU worker subprocess
- Worker performs actual CUDA operations
- Process visible in `nvidia-smi`
- Watcher thread monitors exit
- On exit → calls `_on_complete()` or `_on_fail()` callback

---

## Stage 7: Completion & Resource Release

### On Successful Job Completion

```python
def _on_job_completed(self, job_id: str):
    """Called by JobWatcher when process/pod succeeds."""
    self.complete_job(job_id)

def complete_job(self, job_id: str) -> Optional[GPUJob]:
    with self._lock:
        job = self._scheduled_jobs.pop(job_id, None)  # ← Remove from active
    if not job:
        return None

    # Release resources
    self.executor.terminate(job)
    self.admission_controller.release(job_id)  # ← Free GPUs
    self.fair_scheduler.release_usage(job.user, job.gpu_required)

    # Mark complete
    job.status = JobStatus.COMPLETED
    job.completed_at = datetime.now()
    self._completed_jobs.append(job)

    if self._store:
        self._store.save_job(job)

    return job
```

### Admission Controller Release

```python
def release(self, job_id: str) -> Optional[PulsarEvent]:
    """Release GPU resources when a job completes."""
    with self._lock:
        job = self._active_jobs.pop(job_id, None)
        if not job:
            return None

        # Return GPUs to available pool
        self._available_gpus += job.gpu_required
        mem_freed = job.gpu_required * (job.gpu_memory_gb or self._gpu_memory_gb)
        self._available_memory_gb += mem_freed

        # Update user quota
        quota = self._quotas.get(job.user)
        if quota:
            quota.current_gpu_usage = max(0, quota.current_gpu_usage - job.gpu_required)
            quota.current_job_count = max(0, quota.current_job_count - 1)
```

---

## Showcase Script Flow

```bash
./showcase_judges.sh
  ↓
1. setup_environment()
   - Create venv
   - Install pulsar package
   - Clean stale processes
   - Clear bytecode cache

2. ensure_server()
   - Check if server running at localhost:8080
   - If not, start: pulsar.cli server --config .showcase_pulsar.yaml
   - Wait for /readyz endpoint

3. configure_showcase_quotas()
   - Set quotas for team-alpha, team-beta, team-gamma

4. inject_wave_one()
   - submit_job team-alpha 2 NORMAL Training PyTorch
   - submit_job team-beta 2 NORMAL Inference Triton
   - submit_job team-gamma 1 NORMAL Training JAX

5. Monitor dashboard for 8 seconds
   - Jobs should move: QUEUED → ADMITTED → RUNNING → COMPLETED

6. inject_wave_two() / inject_wave_three()
```

---

## CRITICAL ISSUES & FAILURE POINTS

### 🔴 ISSUE 1: start_scheduler() Not Called?

The scheduler loop WILL NOT RUN unless `start_scheduler()` is explicitly called.

**Location:** `src/pulsar/api_server.py` in the lifespan handler:

```python
@asynccontextmanager
async def lifespan(app: FastAPI):
    global _control_plane
    cfg = config or PulsarConfig.load()
    _control_plane = PulsarControlPlane(cfg)
    _control_plane.start_scheduler()  # ← MUST BE HERE!
    logger.info("PULSAR API server started on %s:%d", cfg.api.host, cfg.api.port)
    yield
    _control_plane.stop_scheduler()
    logger.info("PULSAR API server stopped")
```

**Check:** Verify this is called during server startup

### 🔴 ISSUE 2: Queue Full Rejection

If `max_queue_depth_per_tenant=50` and you submit >50 jobs per team, they'll be REJECTED at queue stage:

```python
def submit_job(self, job: GPUJob) -> PulsarEvent:
    with self._lock:
        user_depth = sum(1 for q in self.queue_manager._user_queues.values() for j in q if j.user == job.user)
        if user_depth >= self.max_queue_depth_per_tenant:
            job.status = JobStatus.REJECTED  # ❌ REJECTED
            return event
```

**Check:** Showcase config has `max_queue_depth_per_tenant: 50` - should be OK for demo

### 🔴 ISSUE 3: Admission Rejection - Quota

User quota might be limiting jobs:

```python
quota = self._quotas.get(job.user)
if quota:
    if job.gpu_required > quota.gpu_available:
        return False, "User quota exceeded"
    if not quota.can_submit:
        return False, "Job limit reached"
```

**Showcase config quotas:**

```yaml
quotas:
  team-alpha:
    max_gpus: 16
    max_jobs: 20
    weight: 1.0
  team-beta:
    max_gpus: 16
    max_jobs: 20
    weight: 1.0
  team-gamma:
    max_gpus: 16
    max_jobs: 20
    weight: 0.5
```

**Check:** Jobs should fit - each wave submits 2-4 GPUs for teams with 16 GPU limit

### 🔴 ISSUE 4: Executor Process Launch Failure

Subprocess might fail to start:

```python
proc = subprocess.Popen(
    cmd,
    env=env,
    cwd=src_dir,
    stdout=subprocess.PIPE,
    stderr=subprocess.STDOUT,
)
```

**Potential causes:**

- `pulsar.gpu_worker` module not importable
- CUDA/GPU not available
- Subprocess execution error

**Logs to check:**

- `"[EXECUTOR] Process started: ... PID=..."`
- `"[EXECUTOR] Failed to start process..."`

### 🔴 ISSUE 5: Threading/Daemon Issues

All background threads are daemon threads:

```python
sched_thread = threading.Thread(target=self._scheduler_loop, daemon=True)
sched_thread.start()

self._thread = threading.Thread(target=self._controller_loop, daemon=True)
self._thread.start()
```

**Risk:** If main thread exits before processing, daemon threads might be killed

### 🔴 ISSUE 6: Resource Pool Exhaustion

Cluster has 16 GPUs total. If all allocated, subsequent jobs are REQUEUED:

```python
if job.gpu_required > self._available_gpus:
    return False, f"Insufficient cluster GPUs: need {job.gpu_required}, available {self._available_gpus}/{self._total_gpus}"
```

**Showcase jobs:**

- Wave 1: 2+2+1 = 5 GPUs (11 available)
- Wave 2: 2+2+1 = 5 GPUs (6 available)
- Wave 3: 2 GPUs (4 available)

**Total: 14 GPUs used** - should fit in 16

---

## DEBUGGING CHECKLIST

### ✅ Verify Scheduler is Running

Check logs for:

```
"Background scheduler started (interval=..."
"Queue controller started: aging=..."
```

### ✅ Verify Jobs Are Queued

Check API:

```bash
curl http://localhost:8080/api/v1/jobs?status=QUEUED
```

Should return all queued jobs

### ✅ Verify Jobs Are Being Dequeued

Check logs for:

```
"[SCHEDULE] <job_id> → <N> GPUs"
```

One per job that was scheduled

### ✅ Verify Executor Launching Processes

Check logs for:

```
"[EXECUTOR] Process started: <job_id> PID=<N>"
```

One per running job

### ✅ Verify Completion

Check logs for:

```
"Job <job_id> completed successfully"
```

Or check API:

```bash
curl http://localhost:8080/api/v1/jobs?status=COMPLETED
```

### ✅ Dashboard Updates

Open browser to `http://localhost:8080/`

- Running Jobs section should show active jobs
- Queue section should show queued jobs
- Fairness metrics should update
- Metrics graph should show activity

---

## Expected Behavior Timeline

```
0s:   Job submitted → status = QUEUED
      Appears in dashboard Queue section

2-4s: Scheduler loop processes job
      Status: QUEUED → ADMITTED → RUNNING
      Process spawned (visible in nvidia-smi)

15-60s: Worker runs CUDA matrix operations
        Status: RUNNING
        PID visible in nvidia-smi

90-120s: Worker completes
         Status: COMPLETED
         Resources released
         Job moves to Completed section
```

---

## Summary of Complete Flow

```
┌─ SUBMISSION PHASE (immediate) ─┐
│                                 │
│ 1. CLI: pulsar submit           │
│ 2. HTTP: POST /api/v1/jobs      │
│ 3. API: Create GPUJob           │
│ 4. CP: submit_job()             │
│ 5. Queue: Add to user queue     │
│ 6. Status: QUEUED               │
│ 7. Response: Return to user     │
│                                 │
└─────────────────────────────────┘
        ↓ (via background threads)
┌─ SCHEDULING PHASE (continuous) ┐
│                                 │
│ 1. Scheduler loop (every 2s)    │
│ 2. process_queue()              │
│ 3. Get fairness scores          │
│ 4. Dequeue job (FIFO per user)  │
│ 5. Admission check              │
│ 6. Allocate resources           │
│ 7. Status: ADMITTED             │
│                                 │
└─────────────────────────────────┘
        ↓
┌─ EXECUTION PHASE ───────────────┐
│                                 │
│ 1. GPU fallback policy          │
│ 2. Spawn subprocess             │
│ 3. Execute GPU worker           │
│ 4. Status: RUNNING              │
│ 5. Process in nvidia-smi        │
│                                 │
└─────────────────────────────────┘
        ↓ (after worker finishes)
┌─ COMPLETION PHASE ──────────────┐
│                                 │
│ 1. Watcher detects exit         │
│ 2. Release resources            │
│ 3. Release to admission pool    │
│ 4. Update user quota            │
│ 5. Status: COMPLETED            │
│ 6. Persist to DB                │
│                                 │
└─────────────────────────────────┘
```

---

## Where Jobs Might Get Stuck

1. **QUEUED** → **Never dequeued**
   - Scheduler not running (start_scheduler not called)
   - total_queued = 0 (queue empty)
   - users_with_jobs empty

2. **QUEUED** → **Rejected at admission**
   - Not enough cluster GPUs
   - User quota exceeded
   - Job limit reached
   - Memory insufficient

3. **ADMITTED** → **Failed to execute**
   - Subprocess launch error
   - GPU/CUDA not available
   - Worker module import failed

4. **RUNNING** → **Never completes**
   - Watcher thread not running
   - Process hangs
   - Callback not triggered

5. **Any status** → **Not persisted**
   - Persistence disabled in config
   - DB write failed
   - Loss on restart
