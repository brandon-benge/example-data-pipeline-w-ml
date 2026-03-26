# Architecture Rationale and Supporting Context

This document captures explanatory material that is useful for human readers but is not required as the primary implementation specification for building the repository. The build-oriented source of truth remains `ARCHITECTURE.md`.

## Purpose

This repository contains a single, laptop-scale platform that simulates an end-to-end modern data and ML stack:
- Python creates synthetic relational business data and direct event streams.
- Postgres stores mutable operational tables.
- Kafka Connect runs a Debezium source connector to capture CDC from Postgres into Kafka.
- Kafka also carries direct behavioral event streams.
- Kafka Connect lands CDC Bronze tables into Iceberg on MinIO, while Flink ingests the direct event stream and updates Redis with approved low-latency online features.
- Spark batch jobs perform quality validation, deduplication, masking, governance enrichment, and backfills to produce clean Silver data.
- dbt executes Spark SQL transformations to produce Gold curated marts and Iceberg-backed ML feature tables.
- BI queries run against the Gold layer.
- ML training jobs consume dbt-built Iceberg feature tables, publish model artifacts to MinIO, and register model metadata in Iceberg.
- The Iceberg REST catalog uses the existing Postgres service for JDBC metadata in local mode; this keeps the stack small, but a production-style design would separate catalog metadata from the OLTP source database.

The design is intentionally small enough to run on a desktop through Docker Compose while still demonstrating the architecture patterns expected in production-grade streaming, data platform, and MLOps environments.

## Goals

### Primary goals
- Demonstrate CDC + event streaming in one coherent architecture.
- Show raw-to-curated lakehouse layering with Iceberg.
- Support replay, dedupe, backfills, and schema evolution.
- Enforce governance concerns: classification, masking, tokenization, ownership, lineage, and policy-based access.
- Produce ML-ready Silver datasets and BI-ready Gold marts.
- Support both offline feature generation and real-time online feature serving for low-latency ML use cases.
- Deliver a containerized open-source BI dashboard experience directly on Gold tables.
- Keep the stack light enough for a desktop environment.

### Secondary goals
- Make each stage independently testable.
- Allow selective service startup and shutdown.
- Keep synthetic data narrow, relational, and behaviorally rich.
- Create a repo that is interview-demo ready.

## BI Rationale

### Why Superset + Trino

- Apache Superset is open-source, container-friendly, and well suited for dashboard demos.
- Trino has strong Iceberg support and can query Gold tables directly via the Iceberg REST catalog and MinIO-backed storage.
- Trino remains the shared SQL access layer for analytics and operational checks, but it must also allow controlled write paths used by compose-managed catalog bootstrap and ML model-registry metadata writes.

### BI question examples

The Gold layer is intended to support questions such as:
- which campaigns are driving impressions, clicks, orders, and revenue over time
- which advertisers are expanding spend or engaging with sales most effectively
- how customers move through browse, click, cart, checkout, and purchase funnels
- which categories, products, and channels contribute most to conversion and revenue

## ML Rationale

Training jobs use dbt-built Iceberg feature tables in `iceberg.silver`. The current repo trains custom logistic-regression classifiers in Python, writes local training artifacts under `ml/artifacts/`, and publishes canonical copies to MinIO.

The platform also supports real-time online feature serving using Redis as the low-latency feature store for approved serving features.

Offline and online feature consistency is managed through shared versioned feature definitions. Spark and Flink both consume the same feature-definition contract, while Silver remains the canonical offline reference for reconciliation and training.

The current repo boundary is:

- dbt builds offline feature tables
- ML code trains models from those tables
- training emits local artifact files under `ml/artifacts/`, while canonical model binaries are stored in MinIO object storage
- model-version metadata is stored in an Iceberg registry table
- the compose-managed `ml-inference` container is the runtime serving path, queries `iceberg.silver.ml_model_registry` for the latest manifest, downloads artifacts from MinIO in memory, and serves the scoring endpoints
- `tools/demo_realtime_scoring.py` is the repo's CLI helper for exercising the same scoring logic used by the inference service

### Current algorithm set

- custom logistic regression for `customer_purchase_next_7d`
- custom logistic regression for `campaign_success_flag`
- custom logistic regression for `advertiser_budget_increase_next_30d`

### Example modeling scope

- customer behavior features: views_7d, ad_clicks_7d, add_to_cart_7d, purchases_30d, avg_order_value_90d, and days_since_last_purchase
- campaign performance features: impressions_7d, clicks_7d, ctr_7d, attributed_orders_30d, and attributed_revenue_30d
- advertiser engagement features: active_campaigns_30d, sales_contacts_14d, and budget_delta_30d

These features are built from governed Silver session, order, campaign, advertiser, and sales activity tables with point-in-time joins and leakage-safe lookback windows.

Selected serving features such as recent customer views, clicks, and carts are maintained incrementally by Flink from Kafka streams and materialized into Redis for online inference and application-side retrieval. Historical purchase and value context is hydrated from offline Iceberg feature tables rather than maintained as live Redis aggregates in the current repo.

### Initial training targets

- customer_purchase_next_7d
- campaign_success_flag
- advertiser_budget_increase_next_30d

## Final Notes

This architecture intentionally chooses a clear separation of concerns:
- Postgres + Kafka Connect / Debezium source connector for mutable source entities
- Kafka direct events for behavioral streaming
- Flink for ingestion-oriented Bronze landing
- Spark for correctness-heavy Silver processing
- dbt for Gold analytics modeling
- Trino as the shared SQL serving layer for Iceberg tables, with controlled write use for catalog bootstrap and ML metadata registration
- Apache Superset for dashboarding on approved Gold marts
- ML training from dbt-built Iceberg feature tables for point-in-time safety

It is not the smallest possible stack, but it is the smallest stack that still tells a credible modern platform story with explicit CDC ingestion, governed medallion processing, BI serving, and containerized ML inference.
> Note
> This document still contains pre-split ML-platform discussion. Training, model registry, inference, experimentation, and Redis-backed online features are moving to a sibling ML platform repository. Use [ML Platform Split](./ml_platform_split.md) for the current repository boundary.
