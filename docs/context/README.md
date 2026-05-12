# PULSAR Context Pack

Last updated: 2026-05-12

This folder captures high-signal context for this repository so contributors can quickly understand current behavior, maturity, and direction without reverse-engineering the entire codebase.

## Read Order

1. `PROJECT_BRIEF.md`  
   Big-picture explanation of what the system is today.
2. `COMPONENT_INVENTORY.md`  
   Component-by-component status and ownership map.
3. `SYSTEM_FLOW.md`  
   End-to-end lifecycle and control-loop flow.
4. `OPERATIONS_RUNBOOK.md`  
   Practical run/test/deploy workflows.
5. `KNOWN_GAPS_AND_RISKS.md`  
   Current inconsistencies, sharp edges, and risk items.
6. `PULSAR_TAKEOVER_ROADMAP.md`  
   Plan to fully transition the mixed KGWE/PULSAR repo into a PULSAR-first project.
7. `context.json`  
   Machine-readable summary for automation and AI tooling.

## Intended Use

- Before coding: read `PROJECT_BRIEF.md` and `KNOWN_GAPS_AND_RISKS.md`.
- Before operations/deploy: read `OPERATIONS_RUNBOOK.md`.
- Before branding/refactor work: read `PULSAR_TAKEOVER_ROADMAP.md`.
