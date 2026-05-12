# PULSAR Takeover Roadmap

Goal: fully transition this repository into a PULSAR-native project while preserving functionality and minimizing migration risk.

## Phase 0: Freeze and Baseline

1. Tag current state before rename-heavy changes.
2. Capture baseline test results for Python workflows.
3. Record current deployability status for direct manifest and Helm chart.

## Phase 1: Canonical Identity

1. Decide canonical repository name and module namespace.
2. Update README intro and top-level docs to PULSAR-first positioning.
3. Add compatibility notes for any retained KGWE references.

Deliverables:

- Updated project narrative
- Versioned migration note for old naming

## Phase 2: Source and Module Renames

1. Replace KGWE-named constants/labels/annotations where safe.
2. Decide go module path strategy:
   - immediate rename, or
   - staged compatibility period with migration aliases.
3. Normalize metric prefixes if required by observability consumers.

Deliverables:

- Consistent naming in source code
- Clear migration guide for downstream users

## Phase 3: Deployment Normalization

1. Rename Helm chart path and metadata to PULSAR conventions.
2. Align namespaces, service names, labels, and chart helper names.
3. Reconcile CRD API group strategy (preserve compatibility if needed).

Deliverables:

- Single authoritative deployment route
- Backward-compatibility matrix (if old chart/CRDs supported)

## Phase 4: Build and CI Alignment

1. Reconcile `Makefile` targets with actual repository layout.
2. Remove dead targets or restore missing entrypoints intentionally.
3. Add CI checks for:
   - Python tests
   - lint/format
   - packaging sanity

Deliverables:

- Green CI with truthful build targets
- Contributor confidence in automation

## Phase 5: Runtime Hardening

1. Fix scheduler extender code path inconsistencies.
2. Validate starvation/preemption lifecycle transitions.
3. Expand tests for mixed execution modes (standalone + K8s-aware paths).

Deliverables:

- Lower operational risk
- Better lifecycle correctness

## Definition of Done

The takeover is complete when:

1. Repo naming, docs, and deployment assets all reflect PULSAR as primary identity.
2. Core runbook commands are valid and reproducible.
3. No critical path depends on legacy KGWE naming assumptions.
4. Compatibility decisions are explicitly documented.

