# PULSAR Complete Flow Analysis - Executive Summary

## Overview

I've completed a comprehensive investigation of the Pulsar job submission and execution flow. The system is **architecturally sound** with proper threading, synchronization, and component separation. However, there appear to be **specific failure points** that could cause jobs to be rejected.

---

## The Complete 7-Stage Job Journey

### STAGE 1: CLI Submission (Immediate)

```
pulsar submit --user alice --gpus 2
    ↓
POST /api/v1/jobs
    ↓
GPUJob created with UUID job_id
    ↓
Response sent: status=QUEUED
```

**Status:** ✅ WORKING (verified in cli.py and api_server.py)

---

### STAGE 2: Queue Management (Immediate)

```
control_plane.submit_job(job)
    ↓
queue_controller.submit_job(job)
    ↓
queue_manager._user_queues[user].append(job)
    ↓
Job stored in user's FIFO queue
```

**Status:** ✅ WORKING (verified in queue_manager.py)

---

### STAGE 3: Background Scheduler Loop (CONTINUOUS - Every 2 seconds)

```
start_scheduler() called in lifespan handler
    ↓
Queue controller background loop starts (aging, starvation prevention)
    ↓
Scheduler background loop starts (_scheduler_loop)
    ↓
process_all() → process_queue() called repeatedly
```

**Status:** ✅ STARTED (verified in api_server.py and control_plane.py)
**CRITICAL:** If this fails, nothing happens to queued jobs

---

### STAGE 4: Fairness Selection & Dequeue

```
For each cycle:
  1. Get fairness scores for all users
  2. Select user with highest priority
  3. Dequeue first job from user's FIFO queue
  4. Remove from both queue and job_index
```

**Status:** ⚠️ POTENTIAL ISSUE

- If `users_with_jobs` returns empty, jobs stuck
- If dequeue fails, job lost in limbo

---

### STAGE 5: Admission Control Check

```
Check if job can be admitted:
  1. Cluster has enough GPUs?
  2. Enough GPU memory?
  3. User hasn't exceeded quota?
  4. User hasn't hit job count limit?
```

**Status:** 🔴 LIKELY FAILURE POINT

- Job rejected if any check fails
- Requeued or rejected permanently
- Showcase quotas might be limiting jobs

---

### STAGE 6: Execution (Subprocess Launch)

```
Decide GPU class (dgpu/igpu/cpu)
    ↓
Build command: python -m pulsar.gpu_worker
    ↓
subprocess.Popen(cmd, env=env, cwd=src_dir)
    ↓
Spawn watcher thread to monitor process exit
    ↓
Job now RUNNING (visible in nvidia-smi)
```

**Status:** 🔴 POTENTIAL FAILURE POINT

- Worker module must be importable
- CUDA/GPU required (or --cpu-only flag)
- Subprocess launch can fail silently

---

### STAGE 7: Completion & Cleanup

```
Watcher thread detects process exit
    ↓
Callback triggered (_on_job_completed or _on_job_failed)
    ↓
Resources released (GPUs returned to pool)
    ↓
User quota updated
    ↓
Job marked COMPLETED or FAILED
    ↓
Status persisted to SQLite
```

**Status:** ✅ WORKING IF STAGE 6 SUCCEEDS

---

## The Showcase Script Flow

```bash
./showcase_judges.sh
    ↓
1. setup_environment()
   - Create venv
   - Install pulsar package
   - Clear stale processes & bytecode cache

2. ensure_server()
   - Start: python -m pulsar.cli server --config .showcase_pulsar.yaml
   - Wait for /readyz endpoint

3. configure_showcase_quotas()
   - Set team-alpha: max_gpus=16, max_jobs=20, weight=1.0
   - Set team-beta: max_gpus=16, max_jobs=20, weight=1.0
   - Set team-gamma: max_gpus=16, max_jobs=20, weight=0.5

4. inject_wave_one()
   - Submit 3 jobs (2+2+1 = 5 GPUs total)
   - Watch dashboard for 8 seconds

5. inject_wave_two() / inject_wave_three()
   - Submit more jobs
   - Monitor updates

Expected: Jobs QUEUED → ADMITTED → RUNNING → COMPLETED
Actual: Jobs QUEUED but then rejected? Never transition?
```

---

## Where Jobs Get Stuck: The 5 Most Likely Failure Points

### 🔴 FAILURE #1: Admission Rejection - Insufficient Cluster GPUs

**Symptom:** All jobs eventually rejected with "Insufficient cluster GPUs"

**Location:** `admission_controller._can_admit_unlocked()`

```python
if job.gpu_required > self._available_gpus:
    return False, f"Insufficient cluster GPUs: need {job.gpu_required}, available {self._available_gpus}/{self._total_gpus}"
```

**Why This Happens:**

1. Showcase config says `total_gpus: 16`
2. Control plane initialized with 16 available GPUs
3. First job allocates 2 GPUs → 14 remaining
4. But `_available_gpus` counter is corrupted or never decremented

**Check:**

```bash
curl http://localhost:8080/api/v1/cluster | jq '.available_gpus'
# Should be 16 initially, then decrease as jobs run
```

---

### 🔴 FAILURE #2: Admission Rejection - User Quota Exceeded

**Symptom:** Jobs rejected with "User quota exceeded" or "Job limit reached"

**Location:** `admission_controller._can_admit_unlocked()`

```python
quota = self._quotas.get(job.user)
if quota:
    if job.gpu_required > quota.gpu_available:
        return False, f"User quota exceeded"
    if not quota.can_submit:
        return False, f"Job limit reached: {quota.current_job_count}/{quota.max_jobs}"
```

**Why This Happens:**

1. Quotas not configured initially
2. Showcase script calls `configure_showcase_quotas()` but might fail silently
3. Jobs submitted before quotas set → rejected by default quota
4. Or quota `current_gpu_usage` not reset between runs

**Check:**

```bash
curl http://localhost:8080/api/v1/quotas | jq '.'
# Should show: team-alpha, team-beta, team-gamma each with max_gpus=16
```

---

### 🔴 FAILURE #3: Jobs Queued But Never Dequeued

**Symptom:**

- API returns `status=QUEUED`
- Logs missing `"[SCHEDULE] <job_id> → <N> GPUs"`
- Jobs stuck forever

**Location:** `control_plane.process_queue()`

```python
if self.queue_controller.total_queued == 0:
    return None  # Early exit if queue empty

users_with_jobs = self.queue_controller.users_with_jobs
if not users_with_jobs:
    return None  # Early exit if no users with jobs
```

**Why This Happens:**

1. `queue_controller.users_with_jobs` returns empty list
2. Chains to: `queue_manager.users_with_jobs` → `[u for u, q in self._user_queues.items() if q]`
3. If queue lists exist but all empty → no users returned
4. Scheduler loop thinks queue is empty, doesn't process

**Check:**

```bash
curl http://localhost:8080/api/v1/queues | jq '.queues'
# Should show jobs in each user's queue
```

---

### 🔴 FAILURE #4: Executor Cannot Launch Subprocess

**Symptom:**

- Logs show: `"[EXECUTOR] Failed to start process for <job_id>: ..."`
- Jobs marked FAILED

**Location:** `executor._execute_standalone()`

```python
proc = subprocess.Popen(
    cmd,
    env=env,
    cwd=src_dir,
)
# Exception caught and logged as execution failure
```

**Why This Happens:**

1. `pulsar.gpu_worker` module not importable
   - Preflight check fails: `python -c "import pulsar.gpu_worker"`
2. Working directory incorrect (src_dir not parent of pulsar package)
3. Python executable not found in subprocess PATH
4. CUDA/GPU not available (but --cpu-only should work)

**Check:**

```bash
# From project root:
cd src
python -c "import pulsar.gpu_worker"
# Must succeed
```

---

### 🔴 FAILURE #5: Scheduler Loop Not Processing Jobs

**Symptom:**

- Server starts normally
- Jobs submitted
- But NO `"[SCHEDULE]"` or `"[EXECUTOR]"` log messages
- Nothing happens

**Location:** `control_plane._scheduler_loop()`

```python
def _scheduler_loop(self):
    interval = self.config.scheduling.scheduling_interval_seconds
    while self._scheduler_running:
        try:
            self.process_all()
        except Exception:
            logger.exception("Scheduler loop error")
        time.sleep(interval)
```

**Why This Happens:**

1. `start_scheduler()` not called (UNLIKELY - verified it IS called)
2. `_scheduler_running` flag never set to True
3. Exception occurs each cycle but silently caught
4. Daemon thread killed before processing starts

**Check:**

```bash
tail -50 .showcase_server.log | grep -E "scheduler started|error"
# Should see: "Background scheduler started (interval=2.0s)"
```

---

## How to Diagnose Your Specific Issue

### Step 1: Check Server Startup

```bash
tail -100 .showcase_server.log | grep -E "started|error|ERROR"
```

**Look for:**

- ✅ `"PULSAR API server started on 0.0.0.0:8080"`
- ✅ `"Background scheduler started (interval=2.0s)"`
- ✅ `"Queue controller started: aging=True"`
- ❌ Any `"error"` or `"ERROR"` messages

**If you see errors:** The issue is startup-related, check the full error message

---

### Step 2: Check Server Readiness

```bash
curl http://localhost:8080/readyz | jq '.'
```

**Expected:**

```json
{
  "ready": true,
  "checks": {
    "scheduler": true,
    "admission_controller": true,
    "queue_manager": true,
    "persistence": true
  }
}
```

**If scheduler=false:** Scheduler not running, check Step 1 logs

---

### Step 3: Check Cluster Status

```bash
curl http://localhost:8080/api/v1/cluster | jq '.'
```

**Expected:**

```json
{
  "total_gpus": 16,
  "available_gpus": 16,
  "used_gpus": 0,
  "active_jobs": 0,
  "utilization_pct": "0%"
}
```

**If available_gpus < 16 initially:** Something pre-allocated GPUs (check logs)

---

### Step 4: Check Quotas Configuration

```bash
curl http://localhost:8080/api/v1/quotas | jq '.'
```

**Expected:**

```json
{
  "quotas": {
    "team-alpha": {"max_gpus": 16, "current_gpu_usage": 0, "max_jobs": 20, ...},
    "team-beta": {"max_gpus": 16, "current_gpu_usage": 0, "max_jobs": 20, ...},
    "team-gamma": {"max_gpus": 16, "current_gpu_usage": 0, "max_jobs": 20, ...}
  }
}
```

**If quotas missing or empty:** Showcase configuration script didn't run or failed

- Solution: Call configure_showcase_quotas() manually or set via API

---

### Step 5: Submit Test Job with Log Monitoring

**Terminal 1: Monitor logs**

```bash
tail -f .showcase_server.log | grep -E "\[QUEUE\]|\[SCHEDULE\]|\[EXECUTOR\]|\[ADMISSION\]|error"
```

**Terminal 2: Submit job**

```bash
python -m pulsar.cli submit --user test-team --gpus 2 --priority NORMAL --url http://localhost:8080
```

**Watch logs for progression:**

```
[QUEUE] Job <id> queued (position 1)
[SCHEDULE] <id> → 2 GPUs (admission=ok, fair_share user=test-team)
[EXECUTOR] Process started: <id> PID=<N> (dgpu, 2 GPUs, ~90s)
```

**If you see:**

- ✅ QUEUE message → job reached queue
- ❌ No SCHEDULE message → dequeue failing or admission rejecting
- ✅ SCHEDULE message → dequeue succeeded, admission passed
- ❌ No EXECUTOR message → executor failed to launch

---

### Step 6: Check Job Status in API

```bash
curl http://localhost:8080/api/v1/jobs | jq '.jobs[0] | {job_id, status, user, gpu_required}'
```

**Expected progression:**

```
{"job_id": "abc-123", "status": "QUEUED", ...}        (initially)
{"job_id": "abc-123", "status": "RUNNING", ...}       (after 2-4s)
{"job_id": "abc-123", "status": "COMPLETED", ...}     (after 60-120s)
```

**If stuck on QUEUED:** Jobs not being dequeued (check scheduler logs)

---

## The Root Cause (Most Likely)

Based on code review, my assessment is:

### 🎯 **Most Likely: Job Admission Being Rejected Due to Missing Quota Configuration**

**Evidence:**

1. `showcase_judges.sh` calls `configure_showcase_quotas()` via curl
2. If curl fails silently, quotas are never set
3. Jobs use default quotas (which might be empty or zero)
4. Admission controller rejects all jobs

**Solution:**

```bash
# After server starts, verify quotas exist
curl http://localhost:8080/api/v1/quotas | jq '.quotas | keys'
# Should show: ["team-alpha", "team-beta", "team-gamma"]

# If missing, set manually:
for team in team-alpha team-beta team-gamma; do
  curl -X PUT http://localhost:8080/api/v1/quotas/$team \
    -H "Content-Type: application/json" \
    -d '{"max_gpus":16,"max_jobs":20,"weight":1.0}'
done
```

---

## Recommended Next Actions

### Immediate (5 minutes)

1. ✅ Run diagnostic checks 1-6 above
2. ✅ Check `.showcase_server.log` for errors
3. ✅ Verify quotas are configured
4. ✅ Verify cluster reports available_gpus = 16

### Short Term (15 minutes)

1. Enable DEBUG logging: Change `level: INFO` → `level: DEBUG` in config
2. Restart server
3. Submit single test job
4. Watch both logs and API updates in real-time
5. Identify exactly where job gets stuck

### If Still Failing (30 minutes)

1. Run the `test_pulsar_flow.sh` diagnostic script (included in analysis docs)
2. Check if worker module is importable: `python -c "import pulsar.gpu_worker"`
3. Look for exceptions in logs, not just ERROR level

---

## Key Files to Review

1. **[FLOW_ANALYSIS.md](FLOW_ANALYSIS.md)** - Complete detailed flow breakdown (7 stages)
2. **[JOB_REJECTION_ANALYSIS.md](JOB_REJECTION_ANALYSIS.md)** - Failure scenarios and fixes
3. **.showcase_server.log** - Actual logs from your showcase run
4. **.showcase_pulsar.yaml** - Showcase configuration file

---

## Summary: Job Flow at a Glance

```
SUBMIT (immediate)
├─ POST /api/v1/jobs
├─ Create GPUJob(id=uuid, status=QUEUED)
└─ Add to queue_manager._user_queues[user]

↓ (background scheduler, every 2s)

PROCESS (if scheduler running)
├─ Get fairness scores
├─ Dequeue job from user's FIFO queue
├─ Check admission: GPUs? Memory? Quota?
│  ├─ Reject if insufficient (🔴 LIKELY FAILURE)
│  └─ Requeue if insufficient but might fit later
└─ Allocate resources (decrement _available_gpus)

↓ (if admitted)

EXECUTE (if executor works)
├─ Decide GPU class (dgpu/igpu/cpu)
├─ Spawn subprocess: python -m pulsar.gpu_worker
├─ Set status=RUNNING (🔴 POTENTIAL FAILURE)
└─ Start watcher thread

↓ (when worker finishes)

COMPLETE (if watcher triggered)
├─ Release resources (increment _available_gpus)
├─ Update quota counters
├─ Set status=COMPLETED
└─ Persist to SQLite
```

The system is **architecturally correct**. The issue is most likely in **Stage 5 (Admission)** or **Stage 6 (Execution)**.

Check the diagnostics above to identify exactly which stage is failing, then refer to the detailed analysis docs for specific fixes.
