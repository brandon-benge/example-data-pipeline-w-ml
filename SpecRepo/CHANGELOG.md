# SpecRepo Changelog

> Purpose: Track meaningful changes to the specification over time
> Goal: Preserve decision history as the system, constraints, and understanding evolve
> Example policy: Any block labeled `Example Only` is illustrative only.

---

## Change Entry Template

### YYYY-MM-DD
- Changed:
- Why:
- Files affected:
- Risk / impact:
- Follow-up required:

### Example Only

### 2026-03-24
- Changed: Clarified that assignment runtime may serve from stale config for up to 60 seconds.
- Why: The architecture review accepted bounded staleness to meet latency goals.
- Files affected: `PROBLEM.md`, `CONSISTENCY.md`, `ARCHITECTURE.md`, `REQUIREMENTS.md`.
- Risk / impact: Medium; reduces latency risk but changes correctness envelope.
- Follow-up required: Add cache-age alert and stale-read acceptance tests.

---

## Entries

### 2026-03-25
- Changed: Replaced the single shared Kubernetes namespace assumption with workload-aligned namespaces for infrastructure, ingestion, processing, serving, and governance workloads.
- Why: The deployment contract now needs namespace boundaries that reflect operational responsibility instead of a flat `data-platform` grouping.
- Files affected: `REQUIREMENTS.md`, `ARCHITECTURE.md`.
- Risk / impact: Medium; the spec now intentionally diverges from current manifests and runbooks until implementation catches up.
- Follow-up required: Update `k8s/`, `README.md`, and runbooks to adopt the namespace mapping and new service DNS names.

### 2026-03-19
- Changed: Added the initial day-one and optional `SpecRepo/` document templates.
- Why: Establish the full spec repository structure for iterative system design.
- Files affected: `README.md`, `PROBLEM.md`, `INVARIANTS.md`, `REQUIREMENTS.md`, `DATA_MODEL.md`, `CONSISTENCY.md`, `ARCHITECTURE.md`, `FAILURE_MODES.md`, `SCALING.md`, `OBSERVABILITY.md`, `SECURITY.md`, `TEST_PLAN.md`, `API_CONTRACTS.yaml`, `CHANGELOG.md`.
- Risk / impact: Low; these are templates and do not yet define system-specific behavior.
- Follow-up required: Replace placeholders with concrete system decisions during design.
