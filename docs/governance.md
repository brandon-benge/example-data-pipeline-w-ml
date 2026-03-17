# Governance

This repository models governance as file-backed metadata and code-driven enforcement suitable for a local demo.

## Scope

Governance covers:

- classification
- ownership
- masking
- tokenization
- access policy metadata
- certification metadata
- lineage metadata
- DQ rule metadata

## Source files

Repository-managed governance configuration lives under [config/governance](../config/governance):

- [classification.yaml](../config/governance/classification.yaml)
- [ownership.yaml](../config/governance/ownership.yaml)
- [masking.yaml](../config/governance/masking.yaml)
- [tokenization.yaml](../config/governance/tokenization.yaml)
- [access_policies.yaml](../config/governance/access_policies.yaml)
- [certification.yaml](../config/governance/certification.yaml)
- [dq_rules.yaml](../config/governance/dq_rules.yaml)

File-backed metadata outputs live under [metadata/](../metadata).

## Classification model

Supported dataset classifications:

- `public`
- `internal`
- `confidential`
- `restricted`

Local defaults:

- Bronze defaults to `internal`
- Silver defaults to `confidential`
- Gold defaults to `internal`

Restricted paths currently include customer and sales-activity Silver outputs where masking and handling are most sensitive.

## Ownership model

Suggested domain ownership follows the architecture:

- `commerce_platform` for customer, order, and session-oriented assets
- `ad_platform` for advertiser, campaign, and sales-oriented assets
- `analytics_engineering` for curated Gold dimensions, facts, marts, and semantic BI models
- `ml_platform` for ML feature datasets and parity outputs

Spark ownership enrichment is implemented in [spark/utils/ownership.py](../spark/utils/ownership.py).

## Masking and tokenization

Restricted fields from the architecture include:

- customer first name
- customer last name
- customer email
- customer phone
- customer zip code
- sales activity notes

Implementation behavior:

- broad-access Silver and Gold customer-facing outputs use masked fields or omit restricted free text
- deterministic tokenization is used where joinability is required without exposing raw identifiers
- customer-facing Gold and ML feature datasets use tokenized customer keys for broad access

Spark masking/tokenization helpers:

- [spark/utils/masking.py](../spark/utils/masking.py)
- [dbt/macros/tokenize_identifier.sql](../dbt/macros/tokenize_identifier.sql)

## Access policy model

This local demo stores policy metadata but does not implement a full enforcement engine.

Intended control model:

- RBAC for coarse-grained roles such as `platform_admin`, `data_engineer`, `analyst`, and `ml_engineer`
- ABAC using dataset classification, environment, team, and approved purpose-of-use

In practice for the local demo:

- Trino and Superset are positioned on curated Gold datasets by default
- restricted and confidential handling is represented through masking, tokenization, ownership, and file-backed metadata

## Certification and discoverability

Certification metadata is stored in [config/governance/certification.yaml](../config/governance/certification.yaml) and mirrored conceptually in the BI layer where curated Gold marts are treated as promoted datasets.

The architecture expectation is:

- Bronze is hidden or engineering-oriented
- Silver is discoverable to engineering and producer roles
- certified Gold marts are promoted for BI consumption

## Lineage

Lineage is file-backed and append-oriented in this repo.

Current lineage writers:

- Spark lineage logging in [spark/utils/lineage.py](../spark/utils/lineage.py)

Lineage output location:

- [metadata/lineage](../metadata/lineage)

## Practical local interpretation

This repository does not try to implement a full external governance platform. Instead it keeps governance:

- repository-managed
- transparent
- inspectable as code and metadata files
- aligned to the architecture’s masking, tokenization, ownership, DQ, and lineage rules

## Enforcement status

The table below reflects the current implementation, not the intended future state.

| Governance area | Status | Current enforcement path | Notes |
| --- | --- | --- | --- |
| Classification defaults | Partially enforced | [spark/utils/ownership.py](../spark/utils/ownership.py) and [spark/jobs/_pipelines.py](../spark/jobs/_pipelines.py) | Used to stamp `sensitivity_class` metadata on Silver outputs. Not used to allow or deny access. |
| Ownership | Partially enforced | [spark/utils/ownership.py](../spark/utils/ownership.py) and [spark/jobs/_pipelines.py](../spark/jobs/_pipelines.py) | Used to stamp `data_owner` metadata. No approval workflow or access enforcement. |
| Masking | Enforced | [spark/utils/masking.py](../spark/utils/masking.py) and [spark/jobs/_pipelines.py](../spark/jobs/_pipelines.py) | Customer PII is masked in curated Silver outputs. |
| Tokenization | Enforced | [spark/utils/masking.py](../spark/utils/masking.py) and [dbt/macros/tokenize_identifier.sql](../dbt/macros/tokenize_identifier.sql) | Deterministic tokenization is used for customer-safe joins and curated outputs. |
| DQ rules | Enforced | [spark/utils/common.py](../spark/utils/common.py) and [spark/jobs/_pipelines.py](../spark/jobs/_pipelines.py) | Rules are evaluated and written to [metadata/table_contracts/dq_results.jsonl](../metadata/table_contracts/dq_results.jsonl). |
| Lineage | Enforced | [spark/utils/lineage.py](../spark/utils/lineage.py) | Lineage is recorded as file-backed metadata under [metadata/lineage](../metadata/lineage). |
| Access policies | Metadata only | [config/governance/access_policies.yaml](../config/governance/access_policies.yaml) | No runtime RBAC or ABAC engine was found in Trino, Superset, Spark, or app code. |
| Certification | Metadata only | [config/governance/certification.yaml](../config/governance/certification.yaml) | Certification tiers are documented but do not gate promotion or access. |

## Interpretation guidance

Use the governance files in this repo as:

- implemented controls for masking, tokenization, DQ, and lineage
- metadata contracts for ownership, classification, access policy, and certification

Do not interpret the current repo as providing:

- full RBAC or ABAC enforcement
- policy-driven dynamic masking
- certification-based publishing gates
