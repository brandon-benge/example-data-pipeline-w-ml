# Activation Spec

This folder holds a spec-first design for the layer that sits on top of the lakehouse in this repo: identity, profile, segmentation, activation, machine learning (ML) scoring, customer relationship management (CRM) and closed-loop measurement.

It is written as a reference architecture with two instances:

- **Enterprise instance.** A regulated insurer that runs Adobe Experience Platform (AEP), Marketo Engage and Salesforce Marketing Cloud (SFMC), and is moving its warehouse to Databricks. The person-to-advisor relationship is the core of its identity and CRM model.
- **Benge Software instance.** The same logical design applied to Benge Software LLC and its first product, MakeItOurs, at a startup budget. Every entity carries a `product_id` so later products plug in without a redesign.

This is a design, not a build. The examples under [examples/](./examples) show the shape of the contracts, but nothing in this folder runs yet.

## Reading Order

| # | File | Depth | What it decides |
|---|------|-------|-----------------|
| 0 | [00_FRAMING.md](./00_FRAMING.md) | core | Problem, actors, invariants, and the enterprise-to-Benge concept map |
| 1 | [01_TARGET_ARCHITECTURE.md](./01_TARGET_ARCHITECTURE.md) | deep | End-to-end layers, what lives in Adobe Experience Platform (AEP) vs. Databricks, and what flows back |
| 2 | [02_IDENTITY.md](./02_IDENTITY.md) | deep | Offline identity graph build, serving path, survivorship, build vs. buy |
| 3 | [03_SEGMENTATION.md](./03_SEGMENTATION.md) | deep | Segment types, segments as code, where each audience is computed |
| 4 | [04_ACTIVATION.md](./04_ACTIVATION.md) | deep | Destinations, Marketo vs. Salesforce Marketing Cloud (SFMC) routing, consent gating, workflows |
| 5 | [05_ML.md](./05_ML.md) | deep | Propensity and next-best-action models, feature reuse, score write-back |
| 6 | [06_CRM.md](./06_CRM.md) | deep | Advisor-led CRM data model, lead routing, offline outcome capture |
| 7 | [07_GOVERNANCE_PRIVACY_MEASUREMENT.md](./07_GOVERNANCE_PRIVACY_MEASUREMENT.md) | light | Labels and purpose limitation, consent, taxonomy, closed loop |
| 8 | [08_BENGE_SOFTWARE_INSTANCE.md](./08_BENGE_SOFTWARE_INSTANCE.md) | deep | The same spec for MakeItOurs and future products |
| 9 | [09_ROADMAP.md](./09_ROADMAP.md) | light | Phased delivery, first 90 days, proof-of-concept plan |
| – | [GLOSSARY.md](./GLOSSARY.md) | reference | Acronyms and terms |

## Relationship to the Rest of SpecRepo

The existing files under `SpecRepo/` define the lakehouse: CDC, Bronze, Silver, Gold, governance metadata and offline feature datasets. This folder treats that lakehouse as its source layer and does not change any guarantee made there. Where this design needs a new guarantee from the lakehouse, it says so explicitly in the "Upstream asks" section of the relevant file.

Mapping from this repo to the target platform:

| This repo (laptop scale) | Enterprise target | Benge Software target |
|---|---|---|
| Postgres + Debezium CDC | Policy admin, CRM and agency systems via Lakeflow Connect or CDC | Product Postgres via CDC |
| Kafka `events.session_event` | AEP Web/Mobile SDK (Edge Network) plus server events | App and site events into Kafka or a hosted collector |
| Iceberg on MinIO + REST catalog | Delta/Iceberg tables in Unity Catalog | Iceberg or Delta, same layering |
| Spark Silver jobs | Lakeflow Declarative Pipelines / Spark jobs | Spark or dbt-on-DuckDB until volume demands Spark |
| dbt Gold marts and `features/` | dbt or Databricks SQL on Unity Catalog, feature tables | dbt Gold and feature tables |
| `config/governance/*.yaml` | Unity Catalog tags and attribute-based access control (ABAC) plus AEP data usage labels | Same YAML, enforced in dbt and the activation worker |
| Trino + Superset | Databricks SQL, AEP dashboards, BI tool | Trino or DuckDB plus Superset |

## Conventions

- Diagrams are Mermaid so they render on GitHub and can be edited in review.
- Product names and capabilities were checked against vendor docs in October 2026. Anything marked **(verify)** should be confirmed against the tenant's actual license before it is relied on.
- Numbers called "targets" are proposed service level objectives (SLOs), not measured results.
