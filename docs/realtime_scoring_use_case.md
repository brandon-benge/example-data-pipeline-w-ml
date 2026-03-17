# Real-Time Scoring Use Case

## Purpose

This document explains the current runtime scoring pattern in the repo: one governed feature platform supporting multiple model artifacts, with customer scoring using Redis-backed request-time features plus offline context hydrated from Iceberg through Trino.

## Primary Use Case: Customer Purchase Propensity

The core question is:

`How likely is this customer to purchase soon while they are active right now?`

Possible actions:
- show a promotion
- prioritize recommendations
- trigger a support or sales interaction
- avoid unnecessary discount spend

## Why This Uses Both Redis and Iceberg

### Redis

Redis serves the hot event features used at request time:
- `views_1h`
- `views_24h`
- `ad_clicks_24h`
- `add_to_cart_24h`

These are incrementally maintained by Flink from the session-event stream.

### Iceberg

Iceberg-backed offline feature tables provide the slower-moving customer context:
- `purchases_30d`
- `avg_order_value_90d`
- `days_since_last_purchase`

In this repo, that context is hydrated through Trino at sign-in or session start.

## End-to-End Flow

1. Spark produces governed Silver tables from Bronze.
2. dbt builds offline ML feature tables in `iceberg.silver`.
3. The compose-managed `ml-training` container runs `ml/train.py` for the configured feature groups.
4. Training publishes model artifacts to MinIO and records model-version metadata in `iceberg.silver.ml_model_registry`.
5. Flink updates Redis with live customer event features.
6. The compose-managed `ml-inference` service loads the latest artifact for the requested use case.
7. It fetches live Redis features for customer scoring and hydrates offline campaign or advertiser context through Trino where needed.
8. It returns the score and can write it back to Redis.

## Example Decision

Assume Redis returns:
- `views_1h = 8`
- `add_to_cart_24h = 2`

And offline context provides:
- `purchases_30d = 0`
- `days_since_last_purchase = 120`

The model may score the customer as high intent, which can drive a recommendation or promotion decision during the session.

## Other Model Artifacts On The Same Platform

The same feature-platform pattern also supports:

- `campaign`
  - predicts `campaign_success_flag`
  - uses offline campaign performance features
- `advertiser`
  - predicts `advertiser_budget_increase_next_30d`
  - uses offline advertiser engagement and budget features

Important boundary:
- customer scoring is Redis-backed and request-time oriented
- campaign scoring is a separate request and is hydrated from offline Iceberg feature data
- advertiser scoring is a separate request and is hydrated from offline Iceberg feature data

## Demo In This Repo

```bash
docker compose exec dbt dbt build --select features
docker compose up ml-training
curl http://localhost:8010/health
curl -X POST http://localhost:8010/score/customer_purchase \
  -H 'Content-Type: application/json' \
  -d '{"customer_id": 123, "write_redis": true}'
curl -X POST http://localhost:8010/score/campaign_success \
  -H 'Content-Type: application/json' \
  -d '{"campaign_id": 456, "write_redis": true}'
curl -X POST http://localhost:8010/score/advertiser_budget_expansion \
  -H 'Content-Type: application/json' \
  -d '{"advertiser_id": 789, "write_redis": true}'
```

This demo:
- materializes offline ML feature tables in Iceberg through dbt
- trains model artifacts from those tables in a container and publishes them to MinIO
- serves separate containerized inference endpoints for each use case
- fetches live customer features from Redis where low-latency request-time features matter
- hydrates offline context from Iceberg through Trino for campaign and advertiser scoring
- returns three independent scores:
  - `customer_purchase_propensity`
  - `campaign_success_propensity`
  - `advertiser_budget_expansion_propensity`

## Simple Mental Model

- Iceberg answers: `What do we know from governed historical data?`
- Redis answers: `What is happening right now?`
- The model answers: `What is likely to happen next?`
