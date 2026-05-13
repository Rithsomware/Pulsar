# PULSAR System Architecture Overview

This document gives a practical, code-aligned overview of how PULSAR is structured today.

## 1. System Purpose

PULSAR is a GPU queue and fairness control plane. It sits between job submission and execution, enforcing policy decisions before launching workloads.

- In standalone mode, it launches local worker processes.
- In Kubernetes mode, it creates GPU-aware Jobs/Pods.

The same queue, admission, fairness, and preemption logic applies in both modes.

## 2. Architecture Layers

### Interface Layer

- CLI entrypoint: `src/pulsar/cli.py`
- REST API and dashboard: `src/pulsar/api_server.py`, `src/pulsar/dashboard.py`

This layer receives job submissions and exposes operational visibility.

### Control Plane Orchestration Layer

- Orchestrator: `src/pulsar/control_plane.py`

This is the coordination hub. It wires all components, runs scheduler loops, restores state on startup, and drives the job lifecycle.

### Scheduling and Policy Layer

- Queueing: `src/pulsar/queue_manager.py`
- Queue behavior (aging/starvation/signals): `src/pulsar/queue_controller.py`
- Fair-share scoring: `src/pulsar/fair_scheduler.py`
- Admission and quotas: `src/pulsar/admission_controller.py`
- Preemption decisions: `src/pulsar/preemption.py`

This layer answers: "Which job should run next, and is it allowed to run now?"

### Execution Layer

- Executor and watcher: `src/pulsar/executor.py`

This layer answers: "How is the admitted job launched and monitored?"

- Standalone: launches local subprocesses.
- Kubernetes: creates Jobs with GPU resource requests and tracks completion.

### State, Metrics, and Configuration Layer

- Persistence: `src/pulsar/persistence.py` (SQLite-backed job/event state)
- Metrics: `src/pulsar/metrics.py`
- Configuration: `src/pulsar/config.py`

This layer supports recovery, observability, and runtime tuning.

## 3. Core Runtime Topology

```
CLI/API
  -> Control Plane
     -> Queue Controller + Queue Manager
     -> Fair Scheduler
     -> Admission Controller
     -> Preemption Engine (optional)
     -> Executor + Watcher
     -> Persistence + Metrics
```

## 4. Major Design Properties

- Policy-execution separation: scheduling decisions are decoupled from execution backend.
- Dual runtime path: same policy logic, different launch mechanism (local vs Kubernetes).
- Persistent recovery: queued/running jobs are restored on restart when persistence is enabled.
- Multi-tenant fairness: per-user quotas and weighted sharing prevent resource monopolization.
- Preemptive control: higher-priority jobs can evict lower-priority preemptible workloads.

## 5. Deployment Context

The repository includes both active Python control-plane components and partial Go KGWE components.

- Active day-to-day runtime path is centered in `src/pulsar/`.
- Go modules under `src/` (for example `src/scheduler/`) represent broader platform capabilities with varying integration maturity.

For operational behavior, treat `src/pulsar/` as the source of truth for current production-like workflow.
