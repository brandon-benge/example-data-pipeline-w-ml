This directory stores dataset certification seed metadata for the main Bronze, Silver, Gold, and ML-facing datasets in the repo.

What is here
- `datasets.yaml`: curated certification records with owner, intended use, review cadence, and consumer-readiness guidance.

Why it is useful
- It answers a question the raw tables do not answer on their own: "Should a person use this dataset directly?"
- It makes the intended maturity difference between layers explicit:
  - Bronze datasets are operational and not consumer-ready.
  - Silver datasets are curated but still closer to pipeline semantics.
  - Gold marts and dimensions are the primary analytical interfaces.
  - Feature tables are for ML workflows, not general BI.

Current implementation
- The file is hand-authored seed metadata, not generated metadata.
- No pipeline writes here automatically.
- Validation now checks that the certification file exists, so this directory is no longer a placeholder.

Current limitations
- The certification tiers are still lightweight governance labels. They are not enforced by access control or policy automation.
- There is no approval workflow, reviewer history, or freshness SLA attached yet.
- The file covers the important datasets, not every single table in the repo.

Practical interpretation
- Use `datasets.yaml` when deciding which datasets should be shown to consumers as preferred interfaces.
- Treat it as lightweight governance guidance that can evolve with the pipeline, not as a formal enterprise certification workflow.
