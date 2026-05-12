# Project Brief (Current State)

## One-line Description

PULSAR is a GPU queue + fairness control plane for shared AI clusters, with local standalone execution and Kubernetes job execution paths.

## What Exists Today

The repository is a hybrid:

- A **working Python control plane** under `src/pulsar/` (FastAPI + scheduler + executor + dashboard + persistence).
- A **broader Go module set** under `src/{scheduler,discovery,sharing,api,monitoring}` representing KGWE platform capabilities.
- A **mixed naming/deployment layer** where PULSAR and KGWE terms coexist in manifests, metrics, and docs.

## Primary Runtime Path

The clearest runnable path today is PULSAR Python:

1. API/CLI accepts jobs.
2. Queue manager + fairness scheduler order jobs.
3. Admission controller enforces capacity and per-user quotas.
4. Preemption engine evicts lower-priority preemptible jobs if needed.
5. Executor launches:
   - local subprocess workers in standalone mode, or
   - Kubernetes Jobs with GPU resource requests in cluster mode.
6. Metrics and SQLite persistence provide observability and restart recovery.

Core files:

- `src/pulsar/control_plane.py`
- `src/pulsar/api_server.py`
- `src/pulsar/cli.py`
- `src/pulsar/executor.py`
- `src/pulsar/persistence.py`
- `src/pulsar/metrics.py`

## Design Intent

The design intent is to have queue/fairness policy control happen before GPU workload execution, with support for:

- fair-share behavior
- quotas
- preemption
- fallback execution lanes (`dgpu -> igpu -> cpu` in standalone decisioning)
- monitoring visibility (dashboard + `/metrics`)

## Repository Reality Check

There is ongoing migration work and pre-existing local changes in this repository. Treat the current state as active transition rather than a fully normalized, single-brand final release.

