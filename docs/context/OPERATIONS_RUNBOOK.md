# Operations Runbook

## Local Development Workflow (PULSAR-first)

1. Create/activate Python virtual environment.
2. Install package in editable mode.
3. Start the API server.
4. Submit jobs from CLI or API.
5. Observe dashboard, metrics, and persistence.

## Commands

```bash
# from repo root
python3 -m venv venv
source venv/bin/activate
pip install --upgrade pip setuptools wheel
pip install -e .

# start server
cd src
python -m pulsar.cli server
```

In another terminal:

```bash
source venv/bin/activate
cd src
python -m pulsar.cli submit -u team-a -g 2
python -m pulsar.cli jobs
python -m pulsar.cli cluster
python -m pulsar.cli fairness
```

## Key Endpoints

- Dashboard: `GET /`
- OpenAPI docs: `GET /docs`
- Jobs: `POST/GET/DELETE /api/v1/jobs...`
- Cluster summary: `GET /api/v1/cluster`
- Fairness report: `GET /api/v1/fairness`
- Prometheus metrics: `GET /api/v1/metrics`
- Dashboard JSON payload: `GET /api/v1/dashboard`
- Health checks: `GET /healthz`, `GET /readyz`

## Persistence Notes

- SQLite is enabled by default.
- Default DB path is configured via `PulsarConfig` (`pulsar.db` unless overridden).
- Active and queued jobs are recovered on startup.

## Kubernetes Notes

- `deploy/pulsar.yaml` is the direct PULSAR deployment manifest.
- Helm chart currently resides under `deploy/helm/kgwe/` and includes mixed KGWE/PULSAR nomenclature.
- Executor chooses K8s Job mode when Kubernetes client configuration is available.

## Verification Checklist

1. API server starts and `/healthz` returns healthy.
2. Job submission returns queued job ID.
3. Scheduler transitions jobs to running.
4. `/api/v1/cluster` reflects GPU allocation changes.
5. `/api/v1/fairness` updates usage and Jain's fairness index.
6. `/api/v1/metrics` exposes Prometheus-formatted metrics.
7. On restart, queued/running state restoration behaves as expected.

