# Known Gaps and Risks

## 1. Mixed Branding and Naming

Symptoms:

- Python package and runtime are PULSAR-branded.
- Go module path and many deployment/metrics assets remain KGWE-branded.
- Helm chart path/name uses `deploy/helm/kgwe`.

Risk:

- Contributor confusion and accidental divergence in naming conventions.

## 2. Build Target Drift in `Makefile`

Symptoms:

- `Makefile` references `./cmd/*` build targets.
- Current repo snapshot does not include a `cmd/` directory.

Risk:

- `make build` for Go binaries can fail or mislead maintainers about supported workflows.

## 3. API Scheduler-Extender Integration Inconsistency

Symptoms:

- In `src/pulsar/api_server.py`, scheduler bind handler references `cp.k8s`.
- `PulsarControlPlane` is centered around `executor` and does not expose `k8s` in the same shape.

Risk:

- Runtime errors in extender code path if called as-is.

## 4. Queue Starvation Promotion Side Effect

Symptoms:

- Starvation force-promotion path uses queue cancellation behavior in a way that may set unexpected job status before re-scheduling.

Risk:

- Incorrect lifecycle state transitions or telemetry artifacts.

## 5. Dual/Residual Artifact Noise

Symptoms:

- Multiple local DB/coverage artifacts (`pulsar.db`, `coverage.out`, etc.) can appear at root and under `src`.

Risk:

- Accidental commits of generated state and noisy diffs.

## 6. Deployment Surface Area Split

Symptoms:

- Both direct PULSAR manifest and broad KGWE Helm resources coexist.
- CRD groups and labels may still use KGWE domain conventions.

Risk:

- Operational ambiguity and increased onboarding friction.

## 7. Documentation Drift

Symptoms:

- README and docs include both “PULSAR on top of KGWE” and takeover-era phrasing.

Risk:

- New contributors may not know which runtime path is authoritative.

## Recommended Near-Term Priorities

1. Declare one canonical runtime path in docs and CI (PULSAR first).
2. Normalize deployment naming and chart structure.
3. Fix extender endpoint wiring issues.
4. Reconcile `Makefile` with actual source tree.
5. Add/update guardrails for generated artifacts in `.gitignore`.

