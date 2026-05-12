# System Flow (PULSAR Runtime)

## End-to-End Job Lifecycle

1. Submission
   - Source: CLI (`pulsar submit`) or API (`POST /api/v1/jobs`).
   - Job object is created with user, GPU request, priority, and execution metadata.

2. Queueing
   - Job enters per-user queue in `QueueManager`.
   - `QueueController` tracks aging/starvation metadata.

3. Selection
   - Scheduling loop computes per-user fairness scores.
   - Dequeue strategy depends on configured policy (`fair_share`, `priority`, `fifo`, `backfill`, `drf` behavior in report context).

4. Admission
   - `AdmissionController` checks:
     - cluster GPU/memory availability
     - per-user quota and job count limits

5. Optional Preemption
   - If insufficient capacity and incoming job priority is high enough, `PreemptionEngine` may select lower-priority preemptible victims.
   - Victims are released/terminated and requeued.

6. Execution
   - Standalone mode: local worker subprocess launch.
   - K8s mode: Kubernetes Job creation with GPU resource requests.
   - Assigned lane/class metadata is recorded (`dgpu`/`igpu`/`cpu` decisioning path).

7. Monitoring and Completion
   - Watcher tracks process/pod completion/failure.
   - Resources are released.
   - Metrics and fairness usage are updated.
   - Job status and events are persisted to SQLite.

## Control Plane Loops

- Scheduler loop: periodic `process_all()` based on config interval.
- Queue controller loop: aging, starvation checks, preemption signals.
- Executor watcher loop: process/pod status callbacks.

## Persistent State

- Jobs table stores lifecycle timestamps and scheduling/execution metadata.
- Events table stores operational logs suitable for dashboard/event feeds.
- Recovery path restores queued/running jobs on restart.

