This directory stores business glossary content for the main entities, metrics, and governance terms used in the demo.

What is here
- `terms.yaml`: curated glossary entries with definitions, synonyms, and related datasets.

Why it is useful
- It gives analysts and reviewers a shared vocabulary for the most important domain concepts.
- It helps explain terms that are otherwise only implied by table names, such as:
  - `customer_token`
  - `attributed_revenue`
  - `feature parity`
  - `conversion rate`
- It links business terms to the concrete Iceberg datasets where those concepts appear.

Current implementation
- The file is hand-authored seed metadata, not generated metadata.
- No pipeline writes here automatically.
- Validation now checks that the glossary file exists so this directory is no longer just an empty placeholder.

Current limitations
- The glossary is intentionally small. It covers the most important concepts in the repo, not every column or model.
- There is no UI, search layer, or ownership workflow around it.
- The terms are not yet connected to dbt docs or column-level metadata.

Practical interpretation
- Use `terms.yaml` when you want the business meaning of the datasets, not the pipeline mechanics.
- Expand this file when a concept becomes important enough that a new person could misunderstand it from the code alone.
