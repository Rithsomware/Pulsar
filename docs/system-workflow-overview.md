# PULSAR System Workflow Overview

This document describes how a job moves through PULSAR from submission to completion.

## 1. End-to-End Lifecycle

### Step 1: Submission

Jobs are submitted through:

- CLI (`pulsar submit`)
- API (`POST /api/v1/jobs`)

The control plane creates a job record and stores initial metadata (user, GPU request, priority, workload details, preferred GPU class).

### Step 2: Queueing

The job enters a per-user queue managed by `QueueManager` and coordinated by `QueueController`.

- Status: `QUEUED`
- Queue metadata is updated for aging/starvation tracking.

### Step 3: Selection and Scoring

On each scheduler cycle, the control plane:

- Applies queue-controller logic (aging/starvation and preemption signaling behavior).
- Uses scheduling policy (`fair_share`, `priority`, `fifo`, `backfill`) to select candidates.
- Uses fairness usage state to prioritize users who consumed fewer resources.

### Step 4: Admission Check

`AdmissionController` validates whether the selected job can run now:

- Free GPU capacity
- GPU memory capacity model
- User quota limits (max GPUs, max concurrent jobs)

If checks pass, the job is admitted.

### Step 5: Optional Preemption Path

If admission fails for a high-priority job and preemption is enabled:

- `PreemptionEngine` searches for lower-priority preemptible victims.
- Victims are terminated/released.
- Victim jobs are requeued.
- Admission is retried for the incoming job.

### Step 6: Execution

The admitted job is launched by `JobExecutor`.

- Standalone mode: local subprocess launch.
- Kubernetes mode: Job/Pod creation with GPU resource requests.

Job state transitions to `RUNNING`, and runtime identifiers (for example PID/pod) are tracked.

### Step 7: Monitoring and Completion

`JobWatcher` monitors process/pod outcomes and reports callbacks:

- Success: mark `COMPLETED`
- Failure: mark `FAILED`
- User/system stop: mark `CANCELLED` or `PREEMPTED` as appropriate

Resources are released, fairness usage is updated, metrics are recorded, and job/event state is persisted.

## 2. Scheduler Loop Behavior

The control plane continuously runs background loops:

- Scheduler loop: calls `process_all()` at configured intervals.
- Queue controller logic: maintains queue health and starvation protections.
- Watcher loop: detects execution completion/failure and triggers cleanup.

These loops keep queue policy, execution state, and accounting synchronized.

## 3. State Transitions

Common job status flow:

`QUEUED -> RUNNING -> COMPLETED`

Alternative paths:

- `QUEUED -> RUNNING -> FAILED`
- `QUEUED -> CANCELLED`
- `RUNNING -> PREEMPTED -> QUEUED -> RUNNING`

## 4. Persistence and Recovery Workflow

When persistence is enabled:

- Job and event state is written to SQLite.
- On restart, control plane recovery reloads `QUEUED` and `RUNNING` jobs.
- Recovered running jobs are re-accounted into admission and fairness tracking.
- Recovered queued jobs are reinserted into queue flow.

This allows restart continuity instead of losing in-flight scheduling context.

## 5. Operational Visibility

Workflow progress is observable through:

- Dashboard (`/`)
- API endpoints (`/api/v1/*`)
- Metrics endpoint (`/api/v1/metrics`)
- Logs from scheduler, queue controller, and executor/watcher components

Together, these provide traceability from submission to final outcome.
