# Data Model

> Note
> Online feature serving and model-registry ownership are being moved to a sibling ML platform repository. Use [../docs/ml_platform_split.md](../docs/ml_platform_split.md) as the active boundary reference during the transition.

## 1. Source System Design

### CDC-managed Postgres tables

#### `sales_rep`
- Primary key: `sales_rep_id`
- Fields: `rep_name`, `team_name`, `region`, `manager_name`, `status`, `created_at`, `updated_at`

#### `customer`
- Primary key: `customer_id`
- Fields: `first_name`, `last_name`, `email`, `phone`, `city`, `state`, `zip_code`, `status`, `created_at`, `updated_at`

#### `advertiser`
- Primary key: `advertiser_id`
- Foreign keys: `owner_sales_rep_id -> sales_rep.sales_rep_id`
- Fields: `advertiser_name`, `industry`, `account_tier`, `region`, `status`, `created_at`, `updated_at`

#### `product`
- Primary key: `product_id`
- Natural key: `sku`
- Fields: `product_name`, `brand`, `category`, `subcategory`, `list_price`, `cost`, `active_flag`, `created_at`, `updated_at`

#### `campaign`
- Primary key: `campaign_id`
- Foreign keys: `advertiser_id -> advertiser.advertiser_id`
- Fields: `campaign_name`, `campaign_type`, `objective`, `budget_amount`, `start_date`, `end_date`, `status`, `created_at`, `updated_at`

#### `campaign_product`
- Primary key: `campaign_product_id`
- Foreign keys: `campaign_id -> campaign.campaign_id`, `product_id -> product.product_id`
- Fields: `bid_amount`, `priority`, `created_at`, `updated_at`

#### `customer_session`
- Primary key: `session_id`
- Foreign keys: `customer_id -> customer.customer_id`
- Fields: `session_start_ts`, `session_end_ts`, `device_type`, `channel`, `referrer_type`, `created_at`, `updated_at`

#### `order_header`
- Primary key: `order_id`
- Foreign keys: `customer_id -> customer.customer_id`
- Fields: `order_ts`, `order_status`, `subtotal_amount`, `discount_amount`, `tax_amount`, `total_amount`, `payment_type`, `created_at`, `updated_at`

#### `order_item`
- Primary key: `order_item_id`
- Foreign keys: `order_id -> order_header.order_id`, `product_id -> product.product_id`
- Fields: `quantity`, `unit_price`, `line_amount`, `attributed_campaign_id`, `created_at`, `updated_at`

#### `sales_activity`
- Primary key: `sales_activity_id`
- Foreign keys: `advertiser_id -> advertiser.advertiser_id`, `sales_rep_id -> sales_rep.sales_rep_id`
- Fields: `activity_ts`, `activity_type`, `activity_outcome`, `notes`, `created_at`, `updated_at`

### Direct event stream

#### `session_event`
- Key options: `event_uuid` or `session_id`
- Canonical dedupe key: `event_uuid`
- Fields: `event_uuid`, `event_id`, `session_id`, `customer_id`, `event_ts`, `event_type`, `product_id`, `campaign_id`, `page_type`, `search_term`, `position_in_list`, `ingest_ts`, `producer_version`, `schema_version`
- Allowed `event_type`: `product_view`, `ad_impression`, `ad_click`, `add_to_cart`, `checkout_start`
- Explicit exclusion: `purchase` is not emitted as a direct event and is derived from `order_header` plus `order_item`

## 2. Entities

### Entity: `Source OLTP tables`
- Purpose: authoritative operational records for advertisers, campaigns, products, customers, orders, order items, sales reps, and sales activity.
- Authoritative source: Postgres.
- Mutable or immutable: mutable through OLTP updates and deletes captured via CDC.
- Sensitive fields: customer and advertiser identifiers, contact and profile attributes, and restricted joinable identifiers.
- Retention expectation: retained long enough to generate CDC history and validation baselines.

### Entity: `Behavioral events`
- Purpose: append-only customer session and interaction events written directly to Kafka.
- Authoritative source: event generator.
- Mutable or immutable: immutable after publication.
- Sensitive fields: customer/session identifiers and behavioral context fields when classified as restricted.
- Retention expectation: Kafka retention sufficient for Bronze replay and late-event handling.

### Entity: `Bronze tables`
- Purpose: raw append-preserving history for CDC and direct events with source semantics and ingestion metadata intact.
- Authoritative source: Kafka source streams materialized into Iceberg.
- Mutable or immutable: append-only history.
- Sensitive fields: may include restricted fields prior to Silver governance handling.
- Retention expectation: retained as canonical replay source for Silver rebuilds.

### Entity: `Silver tables`
- Purpose: deterministic current-state dimensions, clean facts, aggregates, governed feature snapshots, and parity-reference datasets.
- Authoritative source: Spark transformations from Bronze.
- Mutable or immutable: rewritten by deterministic batch jobs and backfills.
- Sensitive fields: masked or tokenized in broad-access paths.
- Retention expectation: retained long enough to rebuild Gold and validate online feature parity.

### Entity: `Gold tables`
- Purpose: curated dimensions, facts, semantic marts, BI-ready datasets, and reusable feature tables.
- Authoritative source: dbt transformations from Silver only.
- Mutable or immutable: derived and rebuildable.
- Sensitive fields: restricted to governed access paths only.
- Retention expectation: retained as published analytical serving layer.

### Entity: `Online features`
- Purpose: approved low-latency feature state for inference-facing access.
- Authoritative source: Flink streaming computations derived from shared feature definitions.
- Mutable or immutable: mutable by streaming upserts; rebuildable from retained sources.
- Sensitive fields: restricted attributes must remain governed and only approved features are exposed.
- Retention expectation: operational online state, not the sole system of record.
- Example online feature groups: customer features such as `views_7d`, `ad_clicks_7d`, `add_to_cart_7d`; campaign features such as `impressions_7d`, `clicks_7d`, `ctr_7d`; advertiser features such as `active_campaigns_30d`, `sales_contacts_14d`, and `budget_delta_30d`.
- Historical request-time context may be hydrated separately from Iceberg-backed feature tables, including `purchases_30d`, `avg_order_value_90d`, and `days_since_last_purchase`.

### Entity: `ML model registry metadata`
- Purpose: metadata for trained model artifacts, versions, and publication history.
- Authoritative source: ML training pipeline writes into `iceberg.silver.ml_model_registry`.
- Mutable or immutable: append-oriented version metadata.
- Sensitive fields: model lineage and training metadata, not raw PII.
- Retention expectation: retained across retraining cycles.
- Example model objectives include customer purchase propensity, campaign success propensity, and advertiser budget expansion propensity.

### Entity: `Governance metadata`
- Purpose: ownership, lineage, classification, masking, tokenization, access policy, certification, and discoverability metadata for published datasets.
- Authoritative source: file-backed metadata catalog and governance configuration.
- Mutable or immutable: versioned by repository changes.
- Sensitive fields: access intent and steward metadata.
- Retention expectation: retained for the lifetime of governed datasets.

## 3. Lakehouse Table Inventory

### Bronze tables
- `bronze_sales_rep_cdc`
- `bronze_customer_cdc`
- `bronze_advertiser_cdc`
- `bronze_product_cdc`
- `bronze_campaign_cdc`
- `bronze_campaign_product_cdc`
- `bronze_customer_session_cdc`
- `bronze_order_header_cdc`
- `bronze_order_item_cdc`
- `bronze_sales_activity_cdc`
- `bronze_session_event_raw`

### Silver current-state tables
- `silver_sales_rep_current`
- `silver_customer_current`
- `silver_advertiser_current`
- `silver_product_current`
- `silver_campaign_current`
- `silver_campaign_product_current`

### Silver clean operational facts
- `silver_customer_session`
- `silver_session_event_clean`
- `silver_order_header`
- `silver_order_item`
- `silver_sales_activity`

### Silver aggregates and feature-friendly tables
- `silver_customer_daily_metrics`
- `silver_product_daily_metrics`
- `silver_campaign_daily_metrics`
- `silver_advertiser_daily_metrics`
- `customer_purchase_features_v1`
- `customer_realtime_features_v1_parity`
- `customer_purchase_realtime_features_v1`
- `campaign_success_features_v1`
- `advertiser_budget_features_v1`
- `ml_model_registry`

### Gold dimensions
- `dim_customer`
- `dim_product`
- `dim_advertiser`
- `dim_sales_rep`
- `dim_campaign`
- `dim_date`

### Gold facts and marts
- `fct_session_events`
- `fct_orders`
- `fct_order_items`
- `fct_sales_activity`
- `fct_campaign_daily`
- `fct_advertiser_daily`
- `mart_campaign_performance`
- `mart_advertiser_engagement`
- `mart_customer_conversion`

## 4. Relationships

### Relationship: `Source OLTP tables -> Bronze CDC tables`
- Cardinality: one source table to one raw Bronze history table, with one Kafka Connect Iceberg sink per Postgres source table.
- Ownership direction: source table defines business-key semantics; Bronze preserves source history.
- Delete behavior: deletes are carried explicitly as CDC semantics.
- Consistency expectation: Bronze reflects append-preserving CDC history with source time and ingest metadata.

### Relationship: `Behavioral events -> Bronze event tables`
- Cardinality: one event topic to one raw Bronze event history table.
- Ownership direction: event schema and topic define the record contract.
- Delete behavior: none; direct events are append-only.
- Consistency expectation: schema-valid events land in Bronze, unresolved schema events go to DLQ.

### Relationship: `Bronze -> Silver`
- Cardinality: one-or-more raw sources to one or more governed tables.
- Ownership direction: Silver derives from Bronze but does not mutate Bronze.
- Delete behavior: source deletes affect latest-state outputs according to CDC rules.
- Consistency expectation: deterministic deduplication, validation, governance application, and partition/window rebuildability.

### Relationship: `Silver -> Gold`
- Cardinality: one-or-more Silver models to one or more curated Gold models.
- Ownership direction: Gold is wholly derived from Silver.
- Delete behavior: Gold rebuilds from current retained Silver state.
- Consistency expectation: no direct Bronze or topic reads are allowed.

### Relationship: `Shared feature definitions -> Silver features and Online features`
- Cardinality: one versioned definition set to both offline and online implementations.
- Ownership direction: feature specification owns both computation paths.
- Delete behavior: deprecated feature versions remain traceable until retired intentionally.
- Consistency expectation: offline recomputation is canonical for parity checks against Redis.

### Relationship: `Datasets -> Governance metadata`
- Cardinality: every published Iceberg dataset maps to ownership, lineage, classification, and access metadata.
- Ownership direction: governance metadata describes but does not own dataset contents.
- Delete behavior: metadata must not disappear while a dataset remains published.
- Consistency expectation: published assets are not considered complete without governance metadata.

## 5. Keys and Contracts

### Primary Keys
- Source entities use stable business keys per operational table.
- Bronze records preserve source keys plus source-time and ingest-time metadata.
- Silver current-state tables use defined business keys that must be unique and non-null.
- `ml_model_registry` uses model/version identity recorded by the training pipeline.

### Foreign Keys
- Facts reference required dimensions or parent entities through business-key or surrogate-key mappings defined in Silver and Gold models.
- Governance metadata references datasets by catalog, schema, and table identity.

### Idempotency / Dedup Keys
- CDC records use stable source keys plus source ordering fields.
- Direct events use `event_uuid` as the canonical dedupe key.
- Model publication uses model version metadata to avoid duplicate registry entries for the same release intent.

### Partitioning
- Bronze CDC tables partition by `ingest_date`.
- Bronze direct events partition by `event_date` or `ingest_date`.
- Silver and Gold partition using production-shaped date or update columns where practical.

### Representative Contracts
- `bronze_customer_cdc` includes Debezium envelope metadata plus flattened customer payload fields.
- `silver_customer_current` includes masked customer fields, ownership metadata, and sensitivity metadata.
- `silver_session_event_clean` includes deduped direct-event payload plus producer and schema version metadata.
- `mart_campaign_performance` includes daily campaign metrics, revenue, budget, and campaign status.
- Redis customer serving records use key pattern `features:customer:{customer_id}:v1` and store `views_1h`, `views_24h`, `ad_clicks_24h`, `add_to_cart_24h`, `feature_version`, `last_event_ts`, `updated_at`, and `ttl_seconds`.
- Redis key expiration is enforced by the Redis writer using `EXPIRE`; `ttl_seconds` documents expected TTL rather than being the enforcement mechanism itself.

## 6. Lifecycle

### Entity: `Bronze tables`
- States: created, append-active, replayed, retained.
- Creation path: Kafka Connect and Flink land records from Kafka into Iceberg.
- Update path: append only.
- Terminal states: decommissioned only by explicit retention or redesign policy.
- Invalid transitions: in-place mutation that erases raw source semantics.

### Entity: `Silver tables`
- States: built, validated, published, backfilled.
- Creation path: Spark reads Bronze and emits deterministic outputs.
- Update path: rerun or backfill from Bronze under the same rule set.
- Terminal states: replaced by newer derived state while retaining rebuildability.
- Invalid transitions: manual mutation outside governed transformation logic.

### Entity: `Gold tables`
- States: modeled, tested, published, certified.
- Creation path: dbt builds from Silver.
- Update path: dbt reruns after Silver changes or model revisions.
- Terminal states: deprecated or recertified.
- Invalid transitions: direct raw-source refresh outside Silver.

### Entity: `Online features`
- States: computed, served, replayed, validated.
- Creation path: Flink computes approved features from streaming inputs.
- Update path: streaming upserts or replay-based rebuilds.
- Terminal states: retired when feature definition version is withdrawn.
- Invalid transitions: serving features not backed by approved shared definitions.

## 7. Schema Evolution

- Backward compatibility policy: additive-first changes for sources, Bronze, and published datasets where practical.
- Versioning strategy: explicit schemas, versioned feature definitions, versioned model artifacts, and governed metadata revisions.
- Migration strategy: update explicit schemas, land new fields, validate downstream compatibility, then cut over dependent transforms.
- Rollback strategy: rebuild derived layers from retained upstream sources using the prior accepted logic.
- Deprecation window: depends on dataset role, but published Gold and feature contracts require documented transition periods before breaking changes.

## 8. Ownership

### System Ownership
- Postgres source records are owned by the synthetic source generation workflow.
- Bronze ingestion state is owned by the ingestion and streaming layer.
- Silver tables are owned by Spark transformation logic.
- Gold tables are owned by dbt models.
- Online feature state is owned by streaming feature jobs.
- Governance metadata is owned by the repository-backed metadata and governance layer.
- Model registry metadata is owned by the ML training pipeline.
- Suggested default ownership mappings: customer, order, and session domain to `commerce_platform`; advertiser, campaign, and sales domain to `ad_platform`; Gold marts to `analytics_engineering`; ML features to `ml_platform`.

### Write Authority
- Source generator may create and update synthetic source records and direct events.
- Debezium, Kafka Connect, and Flink may materialize Bronze records.
- Spark jobs may create and update Silver datasets.
- dbt may create and update Gold datasets.
- Flink may update Redis online feature state.
- ML training jobs may create model artifacts and registry metadata.

### Derived vs Authoritative Data
- Authoritative records: source OLTP tables, direct behavioral events, governance metadata definitions, and model publication metadata.
- Derived / cached / replicated records: Bronze materializations, Silver and Gold models, Redis feature state, dashboards, and BI assets.

## 9. Data Risks

1. Some restricted identifiers require tokenization rather than simple masking to preserve approved joins safely.
2. Late-arriving or duplicated behavioral events need a clearly enforced watermark and dedup policy to keep Silver deterministic.
3. Certification, discoverability, and access metadata can drift from published tables if repository-backed metadata is not kept in lockstep with transformations.
