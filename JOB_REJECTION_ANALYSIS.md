# PULSAR Job Rejection Analysis & Solutions

## CRITICAL FINDINGS

### ✅ GOOD NEWS: Code Structure is Sound

The architecture is well-designed with proper:

- ✅ Thread-safe locking (RLock usage)
- ✅ Clean separation of concerns
- ✅ `start_scheduler()` IS called in API lifespan handler
- ✅ Background loops ARE started (queue_controller + scheduler)
- ✅ Job executor properly spawns subprocesses
- ✅ Watcher threads monitor process completion

---

## TOP 5 LIKELY FAILURE SCENARIOS

### SCENARIO 1: 🔴 Jobs Queued But Never Dequeued

**Symptoms:**

- API returns "status: QUEUED"
- Dashboard shows jobs in Queue section
- But jobs never transition to RUNNING
- Logs show: `"[SCHEDULE] <job_id> → <N> GPUs"` is MISSING

**Root Causes:**

#### 1A: Queue Empty Detection Bug

In `control_plane.process_queue()`:

```python
if self.queue_controller.total_queued == 0:
    return None  # Exit early if queue appears empty
```

**Potential Issue:** `queue_controller.users_with_jobs` might return empty list even though jobs exist

- The property chains to `queue_manager.users_with_jobs`
- Which filters: `[u for u, q in self._user_queues.items() if q]`

**Check:**

```python
# In process_queue():
users_with_jobs = self.queue_controller.users_with_jobs
if not users_with_jobs:
    return None  # ← If this fires, jobs are stuck!
```

**Fix:** Debug the queue state:

```bash
curl http://localhost:8080/api/v1/queues
```

Should show jobs in `queues[user][jobs]` array

---

#### 1B: Job Removal from Queue Failed

In `queue_manager.get_next_job()`:

```python
job = self._user_queues[selected_user].pop(0)  # Remove from queue
del self._job_index[job.job_id]  # Remove from index
```

**Potential Issue:** If exception occurs here, job is dequeued but not processed

- Exception caught higher up
- Job lost in limbo

**Check Logs For:**

- Any `"Exception"` or `"Error"` in logs around dequeue time

---

### SCENARIO 2: 🔴 Admission Controller Rejecting All Jobs

**Symptoms:**

- Jobs accepted (200 OK response)
- Jobs appear QUEUED
- But admission rejects them: `"Insufficient cluster GPUs"`
- Jobs are requeued infinitely

**Root Cause:**

#### 2A: Available GPUs Not Initialized Correctly

```python
def __init__(self, total_gpus: int, ...):
    self._total_gpus = total_gpus
    self._available_gpus = total_gpus  # ← Must match cluster.total_gpus in config
```

**Showcase Config:**

```yaml
cluster:
  total_gpus: 16
```

**Check:**

- Verify config file has `total_gpus: 16`
- Check logs for: `"Admission Controller initialized with X GPUs"`

#### 2B: Quota Blocking All Users

```python
quota = self._quotas.get(job.user)
if quota:
    if job.gpu_required > quota.gpu_available:
        return False, "User quota exceeded"  # ← BLOCKED
```

**Showcase Setup:**

```bash
configure_showcase_quotas()  # Sets max_gpus=16 for each team
```

**Check:**

```bash
curl http://localhost:8080/api/v1/quotas
```

Each team should have: `"current_gpu_usage": 0, "max_gpus": 16`

---

### SCENARIO 3: 🔴 Executor Cannot Launch Subprocess

**Symptoms:**

- Jobs reach executor
- Logs show: `"[EXECUTOR] Failed to start process for <job>"`
- Jobs marked FAILED

**Root Cause:**

#### 3A: gpu_worker Module Not Importable

```python
preflight = subprocess.run(
    [sys.executable, "-c", "import pulsar.gpu_worker"],
    cwd=src_dir,  # Must be parent of 'pulsar' package
    ...
)
if preflight.returncode != 0:
    return False, f"Worker import failed: {error}"  # ← FAILS HERE
```

**Check:**

```bash
# From project root:
cd src
python -c "import pulsar.gpu_worker"
```

Should succeed silently

#### 3B: Subprocess Launch Permission Error

```python
proc = subprocess.Popen(
    cmd,
    env=env,
    cwd=src_dir,
)
```

**Common Issues:**

- Python executable not found (venv not activated)
- Working directory incorrect
- Environment variables cause issues

**Check Logs For:**

```
"[EXECUTOR] Failed to start process for <job_id>: FileNotFoundError: [Errno 2] No such file or directory"
```

---

### SCENARIO 4: 🔴 Scheduler Loop Not Running

**Symptoms:**

- Server starts (HTTP endpoint responds)
- Jobs submitted successfully
- But jobs NEVER processed
- No `"[SCHEDULE]"` log messages

**Root Cause:**

#### 4A: `start_scheduler()` Not Called (UNLIKELY - we verified it IS called)

- But check if exception occurs during scheduler startup

**Check Logs For:**

```
"Background scheduler started (interval=..."
"Queue controller started: aging=..."
```

If these don't appear, check startup logs:

```bash
tail -100 .showcase_server.log
```

#### 4B: Exception in `_scheduler_loop()` Silently Caught

```python
def _scheduler_loop(self):
    while self._scheduler_running:
        try:
            self.process_all()
        except Exception:
            logger.exception("Scheduler loop error")  # ← Logs but continues
        time.sleep(interval)
```

**Check Logs For:**

```
"Scheduler loop error"
```

If present, the loop IS running but hitting exceptions each cycle

#### 4C: Daemon Thread Killed on Exit

```python
sched_thread = threading.Thread(target=self._scheduler_loop, daemon=True)
sched_thread.start()
```

**Risk:** If main thread exits before background threads finish, they're killed

- This shouldn't happen with `uvicorn.run()` but possible if error in startup

**Check:**

- Server continues running after startup
- `curl http://localhost:8080/readyz` returns 200

---

### SCENARIO 5: 🔴 Job Status Stuck in QUEUED

**Symptoms:**

- Logs show: `"[SCHEDULE] <job_id> → <N> GPUs"` (job WAS dequeued)
- But API still shows `status: QUEUED`
- Job not in `Running Jobs` on dashboard

**Root Cause:**

#### 5A: Job Status Update Lost

```python
# After successful execution:
success, exec_msg = self.executor.execute(job)
if not success:
    job.status = JobStatus.FAILED
    return None

# Status should be set to RUNNING inside executor
# But check if overwritten later
```

**Check:** Compare:

1. What executor logs: `"[EXECUTOR] Process started: ... PID=..."`
2. What API returns: `curl http://localhost:8080/api/v1/jobs/<job_id>`

Should both show status updates

#### 5B: Persistence Layer Not Saving Updates

```python
if self._store:
    self._store.save_job(job)
```

**Issue:** If persistence enabled, job might not be saved before status update

- On next load, old status restored
- Database transaction fails silently

**Check:**

```bash
# Disable persistence for testing
max_queue_depth_per_tenant: 50
persistence:
  enabled: false  # ← Try this
```

---

## DIAGNOSTIC CHECKLIST

### Step 1: Verify Server is Ready

```bash
curl -v http://localhost:8080/readyz
# Should return: {"ready": true, "checks": {...}}
```

### Step 2: Check Scheduler is Running

```bash
# Check if background threads started
tail -50 .showcase_server.log | grep -E "scheduler started|Queue controller|error"
```

Expected output:

```
"Background scheduler started (interval=2.0"
"Queue controller started: aging=True"
```

### Step 3: Submit Test Job and Monitor

```bash
# Terminal 1: Watch logs
tail -f .showcase_server.log | grep -E "QUEUE|SCHEDULE|EXECUTOR|ADMISSION"

# Terminal 2: Submit job
python -m pulsar.cli submit --user test-user --gpus 2 --url http://localhost:8080

# Watch output for:
# [QUEUE] Job ... queued (position 1)
# [SCHEDULE] ... → 2 GPUs
# [EXECUTOR] Process started: ... PID=...
```

### Step 4: Check Queue Status

```bash
curl http://localhost:8080/api/v1/queues | jq '.'
# Should show jobs in users_with_jobs
```

### Step 5: Check Admission Controller

```bash
curl http://localhost:8080/api/v1/cluster | jq '.'
# Should show: "available_gpus": 16, "used_gpus": 0
```

### Step 6: Check Job Details

```bash
curl http://localhost:8080/api/v1/jobs?status=QUEUED | jq '.'
# Should show queued jobs with correct user/gpu_required

curl http://localhost:8080/api/v1/jobs?status=RUNNING | jq '.'
# Should show running jobs after processing starts
```

---

## FIXING THE ISSUES

### FIX 1: Enable Debug Logging

Edit `showcase_judges.sh`:

```bash
prepare_showcase_config() {
  cat > "$SHOWCASE_CONFIG" <<CFG
...
logging:
  level: DEBUG  # ← Change from INFO to DEBUG
  format: text
CFG
}
```

Restart and check for detailed trace messages

### FIX 2: Verify Python Path

Ensure venv is activated:

```bash
source venv/bin/activate
which python  # Should show /path/to/venv/bin/python

# Test import
python -c "import pulsar.gpu_worker; print('OK')"
```

### FIX 3: Test Submission Directly

```bash
# Start server
python -m pulsar.cli server --config .showcase_pulsar.yaml &
sleep 3

# Submit via CLI
python -m pulsar.cli submit --user test-team --gpus 2 --url http://localhost:8080

# Check via API
curl http://localhost:8080/api/v1/jobs | jq '.jobs[0] | {job_id, status, user}'
```

Watch logs simultaneously to see flow

### FIX 4: Disable Persistence (Temporary)

Edit config to find source of latency:

```yaml
persistence:
  enabled: false  # ← Try false to eliminate DB ops
```

If jobs process faster, persistence is the bottleneck

### FIX 5: Check Quota Configuration

```bash
# After server starts, verify quotas set correctly
curl http://localhost:8080/api/v1/quotas | jq '.'

# If quotas not set, manually update:
curl -X PUT http://localhost:8080/api/v1/quotas/test-team \
  -H "Content-Type: application/json" \
  -d '{"max_gpus":16,"max_jobs":20,"weight":1.0}'
```

---

## COMPLETE DIAGNOSTIC SCRIPT

Save this as `test_pulsar_flow.sh`:

```bash
#!/bin/bash
set -e

PROJECT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
VENV_DIR="$PROJECT_DIR/venv"
PYTHON_BIN="$VENV_DIR/bin/python"
BASE_URL="http://localhost:8080"

echo "=== PULSAR FLOW DIAGNOSTIC ==="

echo ""
echo "1. Check Python setup"
if ! command -v "$PYTHON_BIN" >/dev/null 2>&1; then
    echo "ERROR: Python venv not found at $PYTHON_BIN"
    exit 1
fi
echo "✓ Python: $PYTHON_BIN"

echo ""
echo "2. Check worker module import"
if ! "$PYTHON_BIN" -c "import pulsar.gpu_worker" 2>/dev/null; then
    echo "ERROR: gpu_worker module not importable"
    exit 1
fi
echo "✓ Worker module imported"

echo ""
echo "3. Check server readiness"
if ! curl -fsS "$BASE_URL/readyz" >/dev/null 2>&1; then
    echo "ERROR: Server not ready at $BASE_URL"
    echo "Start with: python -m pulsar.cli server"
    exit 1
fi
echo "✓ Server ready"

echo ""
echo "4. Check scheduler running"
health=$(curl -fsS "$BASE_URL/healthz")
scheduler_running=$(echo "$health" | grep -o '"scheduler_running": true' || echo "MISSING")
if [[ "$scheduler_running" == "" ]]; then
    echo "WARNING: scheduler_running not true"
    echo "Response: $health"
fi
echo "✓ Health check: $health"

echo ""
echo "5. Check cluster status"
cluster=$(curl -fsS "$BASE_URL/api/v1/cluster")
echo "Cluster: $cluster" | head -3

echo ""
echo "6. Submit test job"
job=$(curl -fsS -X POST "$BASE_URL/api/v1/jobs" \
  -H "Content-Type: application/json" \
  -d '{"user":"test-team","gpu_required":2,"priority":"NORMAL"}')
job_id=$(echo "$job" | grep -o '"job_id":"[^"]*' | cut -d'"' -f4)
status=$(echo "$job" | grep -o '"status":"[^"]*' | cut -d'"' -f4)
echo "Submitted: job_id=$job_id, status=$status"

echo ""
echo "7. Check job status progression (polling for 30s)"
for i in {1..6}; do
    sleep 5
    job_data=$(curl -fsS "$BASE_URL/api/v1/jobs/$job_id")
    current_status=$(echo "$job_data" | grep -o '"status":"[^"]*' | cut -d'"' -f4)
    echo "  [$((i*5))s] Status: $current_status"
    if [[ "$current_status" == "COMPLETED" ]]; then
        echo "✓ Job completed successfully!"
        exit 0
    fi
done

echo ""
echo "⚠ Job did not complete within 30s"
echo "Last status: $current_status"
echo ""
echo "Debug info:"
echo "Queue: $(curl -fsS '$BASE_URL/api/v1/queues' | head -3)"
echo ""
```

Run it:

```bash
chmod +x test_pulsar_flow.sh
./test_pulsar_flow.sh
```

---

## EXPECTED SUCCESS OUTPUT

When everything works:

```
=== PULSAR FLOW DIAGNOSTIC ===

1. Check Python setup
✓ Python: /home/user/Pulsar/venv/bin/python

2. Check worker module import
✓ Worker module imported

3. Check server readiness
✓ Server ready

4. Check scheduler running
✓ Health check: {"status": "healthy", "scheduler_running": true, ...}

5. Check cluster status
Cluster: {"total_gpus": 16, "available_gpus": 16, "used_gpus": 0, ...}

6. Submit test job
Submitted: job_id=a1b2c3d4, status=QUEUED

7. Check job status progression (polling for 30s)
  [5s] Status: RUNNING
  [10s] Status: RUNNING
  [15s] Status: COMPLETED
✓ Job completed successfully!
```

---

## FINAL SUMMARY

The Pulsar system is architecturally sound. Jobs are likely being rejected due to ONE of:

1. **Admission rejection** - insufficient GPUs or quota exceeded
2. **Executor failure** - subprocess can't launch
3. **Scheduler not processing** - though start_scheduler() IS called, check for exceptions
4. **Queue state issue** - jobs queued but users_with_jobs returns empty

**Next Steps:**

1. Check `.showcase_server.log` for error patterns
2. Run the diagnostic script above
3. Enable DEBUG logging
4. Verify quotas are configured
5. Test worker module import manually

The complete flow works correctly when configured properly. Focus on the diagnostic checks above.
