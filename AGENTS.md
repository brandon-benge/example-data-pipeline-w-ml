# AGENTS.md

Copy this file to the repository root as `AGENTS.md` when you want Codex or other agent systems that support `AGENTS.md` to understand your specification layout.

Replace the placeholders before use.

## Start Here

When a task involves the governed system, read `SpecRepo/` in this order:

1. `SpecRepo/README.md`
2. `SpecRepo/PROBLEM.md`
3. `SpecRepo/INVARIANTS.md`
4. `SpecRepo/REQUIREMENTS.md`
5. `SpecRepo/DATA_MODEL.md`
6. `SpecRepo/CONSISTENCY.md`
7. `SpecRepo/ARCHITECTURE.md`

If present and relevant to the task, then read:

8. `SpecRepo/SECURITY.md`
9. `SpecRepo/OBSERVABILITY.md`
10. `SpecRepo/TEST_PLAN.md`
11. `SpecRepo/FAILURE_MODES.md`
12. `SpecRepo/SCALING.md`
13. `SpecRepo/API_CONTRACTS.yaml`
14. `SpecRepo/CHANGELOG.md`

## Spec Interpretation Rules

- `SpecRepo/PROBLEM.md` defines mission, scope, constraints, and success.
- `SpecRepo/INVARIANTS.md` defines hard constraints and non-negotiable guarantees.
- `SpecRepo/REQUIREMENTS.md` defines expected system behavior.
- `SpecRepo/DATA_MODEL.md` defines entities, ownership, keys, and lifecycle.
- `SpecRepo/CONSISTENCY.md` defines read/write semantics, ordering, and concurrency behavior.
- `SpecRepo/ARCHITECTURE.md` defines boundaries, components, and design tradeoffs.

## Conflict Rules

- If code conflicts with the written spec, treat the spec as authoritative unless the task clearly updates the spec.
- If two spec files conflict, prefer the more specific file and call out the conflict.
- If a required decision is missing from `SpecRepo/`, do not silently invent it.
- Do not silently invent missing requirements.

## Working Rule

The governed source of truth for system behavior lives under `SpecRepo/`.
