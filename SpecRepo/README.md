# SpecRepo

> Note
> The repository boundary is being narrowed to the data platform. Training, model registry, inference, experimentation, and Redis-backed online features are moving to a sibling ML platform repository. Until the spec text is fully rewritten, read these files together with [../docs/ml_platform_split.md](../docs/ml_platform_split.md).

This directory is the canonical replacement for the root `ARCHITECTURE.md`. The intent is that all system-defining content lives here so the root architecture file can be removed without losing platform requirements, contracts, or implementation expectations.

## Purpose

The spec makes design, implementation, governance, and validation rules explicit for the laptop-scale retail advertising and commerce data platform. It covers source generation, CDC, streaming, Iceberg storage, BI, governance, ML training, online features, and local deployment expectations.

## How To Read It

Read the files in this order:

1. [PROBLEM.md](./PROBLEM.md)
2. [INVARIANTS.md](./INVARIANTS.md)
3. [REQUIREMENTS.md](./REQUIREMENTS.md)
4. [DATA_MODEL.md](./DATA_MODEL.md)
5. [CONSISTENCY.md](./CONSISTENCY.md)
6. [ARCHITECTURE.md](./ARCHITECTURE.md)
7. [API_CONTRACTS.yaml](./API_CONTRACTS.yaml)
8. Supporting docs in `FAILURE_MODES.md`, `SCALING.md`, `OBSERVABILITY.md`, `SECURITY.md`, and `TEST_PLAN.md`

## File Map

- [PROBLEM.md](./PROBLEM.md): mission, actors, scope, success criteria, constraints, and assumptions.
- [INVARIANTS.md](./INVARIANTS.md): non-negotiable correctness, governance, and ML rules.
- [REQUIREMENTS.md](./REQUIREMENTS.md): functional and non-functional obligations plus launch acceptance criteria.
- [DATA_MODEL.md](./DATA_MODEL.md): entities, source schemas, table inventories, key structure, and lifecycle.
- [CONSISTENCY.md](./CONSISTENCY.md): ordering, replay, deduplication, commit semantics, and rebuild guarantees.
- [ARCHITECTURE.md](./ARCHITECTURE.md): component model, deployment model, execution flow, sequence diagrams, and tradeoffs.
- [API_CONTRACTS.yaml](./API_CONTRACTS.yaml): machine-readable HTTP, Kafka, dataset-record, and online-feature contracts.

## Rationale

High-level rationale and narrative explanation may still live in `docs/architecture_rationale.md`, but the build contract belongs under `SpecRepo`.
