# dbt Models Glossary

This directory contains the dbt layer for the warehouse portion of the project. In this repo, dbt primarily reads curated `silver` tables and produces `gold` analytics models, plus versioned ML feature tables.

## How this repo organizes dbt models

The top-level model groups are defined in [dbt_project.yml](./dbt_project.yml):

- `models/staging`
- `models/features`
- `models/marts/dimensions`
- `models/marts/facts`
- `models/marts/marts`
- `models/semantic`

By convention in this project:

- `staging` models standardize curated upstream Silver tables for downstream dbt use.
- `features` models build ML-ready feature tables and labels.
- `dimensions`, `facts`, and `marts` build broad-access Gold analytics datasets.
- `semantic` models provide thin business-facing views over stable marts.

## Industry-standard terms used here

### Source

A `source` is a table dbt does not build itself, but reads from as an upstream dependency. Sources usually represent data produced by another system or pipeline layer.

In this repo, sources are declared in [models/staging/sources.yml](./models/staging/sources.yml) and point at curated `silver` tables such as `silver_customer_current` and `silver_order_header`.

### Staging model

A `staging` model is a light normalization layer that makes upstream data easier and safer to reuse. Industry-standard staging models usually:

- rename columns into a cleaner convention
- cast data types
- standardize grain and null handling
- expose only the fields downstream models should depend on

In this repo, staging models live in [models/staging](./models/staging) and act as the first dbt-owned layer on top of Spark-produced `silver` tables.

### Dimension

A `dimension` is a descriptive entity table used for filtering, grouping, and joining facts. Dimensions answer questions like "who", "what", "where", and "which".

Common examples:

- customer
- product
- advertiser
- campaign
- date

In this repo, dimensions live in [models/marts/dimensions](./models/marts/dimensions). Examples include:

- `dim_customer`
- `dim_product`
- `dim_advertiser`
- `dim_campaign`
- `dim_sales_rep`
- `dim_date`

`dim_customer` is also an example of governance-aware modeling: it exposes a tokenized customer key instead of a raw customer identifier.

### Fact

A `fact` table stores measurable business events at a defined grain. Facts usually contain:

- foreign keys to dimensions
- timestamps or dates
- numeric measures
- one row per event or per periodic snapshot grain

Facts answer questions like:

- what happened
- how many
- how much
- when

In this repo, fact models live in [models/marts/facts](./models/marts/facts). Examples include:

- `fct_orders`
- `fct_order_items`
- `fct_session_events`
- `fct_campaign_daily`
- `fct_advertiser_daily`
- `fct_sales_activity`

### Mart

A `mart` is a business-consumption table built for a specific analytical use case. Industry-standard marts usually combine multiple facts, dimensions, and business rules into a stable shape that analysts and dashboards can use directly.

Compared with raw facts and dimensions, a mart is more opinionated:

- metrics are often precomputed
- business logic is more explicit
- columns are chosen for a particular workflow or team

In this repo, marts live in [models/marts/marts](./models/marts/marts). Examples include:

- `mart_campaign_performance`
- `mart_advertiser_engagement`
- `mart_customer_conversion`

### Feature table

A `feature` table is an ML-oriented dataset used for training, batch scoring, or parity checks between offline and online features. Industry-standard feature tables usually contain:

- an entity key
- an `as_of_date` or feature timestamp
- input features derived only from information available up to that time
- a label for supervised training, when applicable
- a feature definition or version marker

In this repo, feature models live in [models/features](./models/features). Examples include:

- `customer_purchase_features_v1`
- `customer_purchase_realtime_features_v1`
- `campaign_success_features_v1`
- `advertiser_budget_features_v1`

These are materialized to the `silver` schema in this project because they support the ML workflow rather than broad-access Gold analytics.

### Semantic model

A `semantic` model is a thin, business-facing layer over already-stable analytical tables. In industry practice, semantic layers provide consistent names and reusable business meaning for BI tools, metrics layers, or downstream consumption.

In this repo, semantic models live in [models/semantic](./models/semantic) and sit on top of marts:

- `semantic_campaign_performance`
- `semantic_advertiser_engagement`
- `semantic_customer_conversion`

These are intentionally thin and are closer to presentation-ready views than raw transformation steps.

### Macro

A `macro` is reusable SQL or Jinja logic inside dbt. Macros are commonly used to:

- avoid repeating SQL expressions
- define custom tests
- enforce project conventions
- inject environment-specific behavior

In this repo, macros live in [macros](./macros). For example, [macros/tokenize_identifier.sql](./macros/tokenize_identifier.sql) implements deterministic identifier tokenization for safe joins without exposing raw IDs.

### Test

A dbt `test` is an assertion about model quality or integrity. Common examples include:

- `not_null`
- `unique`
- accepted values
- referential integrity between tables

This repo uses schema tests in `schema.yml` files and custom tests/macros such as non-negative and ratio bounds checks.

## Repo-specific modeling conventions

### Silver vs Gold

This project uses both `silver` and `gold` schemas:

- `silver` holds curated operational and feature-building tables
- `gold` holds business-facing analytics tables for BI and broad consumption

dbt reads from Spark-produced Silver data, then produces:

- `gold` staging, facts, dimensions, marts, and semantic models
- `silver` feature tables for ML workflows

### Tokenization and masking

This repo follows a governance pattern where:

- readable PII is masked before broad-access publication
- identifiers that still need joinability are deterministically tokenized

That means analytics models can join on safe keys such as `customer_token` without exposing raw customer identifiers.

Implementation details live outside and inside dbt:

- Spark masking helpers: [../spark/utils/masking.py](../spark/utils/masking.py)
- dbt tokenization macro: [macros/tokenize_identifier.sql](./macros/tokenize_identifier.sql)
- governance documentation: [../docs/governance.md](../docs/governance.md)

## Quick mental model

You can think about the dbt flow in this repo as:

1. `source`: registered upstream Silver tables
2. `staging`: normalized dbt entry layer
3. `dimensions` and `facts`: core warehouse models
4. `marts`: business-ready analytical datasets
5. `semantic`: thin presentation-oriented views
6. `features`: ML-ready datasets built for training and scoring workflows
