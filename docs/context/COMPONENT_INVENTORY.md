# Component Inventory

Status legend:

- `active`: used in primary runtime workflows
- `partial`: implemented but not fully integrated end-to-end
- `scaffold`: present as structure/intent, not fully wired

## Python (PULSAR) Components

| Component | Path | Status | Notes |
|---|---|---|---|
| Control plane orchestration | `src/pulsar/control_plane.py` | active | Core scheduling loop, admission, fairness, preemption, execution hooks |
| REST API server | `src/pulsar/api_server.py` | active | Jobs/queues/quotas/fairness/metrics/dashboard endpoints |
| CLI | `src/pulsar/cli.py` | active | User-facing control plane CLI (`pulsar ...`) |
| Queue manager | `src/pulsar/queue_manager.py` | active | Per-user queueing and policy-aware dequeue |
| Queue controller | `src/pulsar/queue_controller.py` | active | Aging, starvation handling, queue telemetry |
| Fair scheduler | `src/pulsar/fair_scheduler.py` | active | Weighted fairness and DRF-style share tracking |
| Admission controller | `src/pulsar/admission_controller.py` | active | Capacity + quota enforcement |
| Preemption engine | `src/pulsar/preemption.py` | active | Priority-based victim selection |
| Executor + watcher | `src/pulsar/executor.py` | active | Standalone process mode and K8s Job mode |
| Dashboard renderer | `src/pulsar/dashboard.py` | active | HTML dashboard + JS refresh |
| Persistence | `src/pulsar/persistence.py` | active | SQLite job/event storage |
| Config | `src/pulsar/config.py` | active | YAML-backed config model |
| Kubernetes integration helpers | `src/pulsar/k8s_integration.py` | partial | CRD export/helper utilities; not fully first-class in API wiring |

## Go (KGWE-Named) Components

| Component | Path | Status | Notes |
|---|---|---|---|
| Topology-aware scheduler library | `src/scheduler/scheduler.go` | partial | Substantial logic, but repo currently lacks expected `cmd/*` entrypoints |
| Scheduler extender handlers | `src/scheduler/extender.go` | partial | Extender-specific code present; integration maturity depends on deployment wiring |
| Discovery service library | `src/discovery/discovery.go` | partial | NVML/K8s interfaces and topology model |
| MIG controller library | `src/sharing/mig_controller.go` | partial | Strategy and lifecycle primitives |
| Cost engine library | `src/api/cost_engine.go` | partial | Chargeback and budget logic framework |
| Prometheus exporter | `src/monitoring/prometheus_exporter.go` | partial | Exports KGWE-prefixed metrics |

## Deployment and Packaging

| Area | Path | Status | Notes |
|---|---|---|---|
| PULSAR deployment manifest | `deploy/pulsar.yaml` | active | Direct deployment path for control plane |
| Helm chart (KGWE naming) | `deploy/helm/kgwe/` | partial | Rich chart, still KGWE-branded and mixed naming |
| Dockerfiles | `docker/` | partial | PULSAR + KGWE component images coexist |
| Python package metadata | `pyproject.toml` | active | Project name `pulsar-gpu-control-plane`, CLI entrypoint present |
| Go module metadata | `go.mod` | partial | Module path still `github.com/nvidia/kgwe` |

## Tests

| Test Area | Path | Status | Notes |
|---|---|---|---|
| PULSAR unit tests | `tests/test_pulsar.py` | active | Covers core Python scheduler components |
| Go integration/e2e targets | `Makefile` references | scaffold | Make targets assume directories not present in this snapshot |

