# Demo Workflow

## Purpose

This runbook is the shortest reliable path through the local demo.

The preferred entrypoint is the stage wrapper:

```bash
bash tools/run_stack_workflow.sh --stop-at <stage>
```

VS Code tasks in [.vscode/tasks.json](../../.vscode/tasks.json) call that wrapper directly.

## Recommended flow

Use the wrapper unless you are debugging one stage in isolation.

```bash
bash tools/run_stack_workflow.sh --stop-at ingestion
bash tools/run_stack_workflow.sh --stop-at stream-processing
bash tools/run_stack_workflow.sh --stop-at batch
bash tools/run_stack_workflow.sh --stop-at analytics
bash tools/run_stack_workflow.sh --stop-at ml
```

What the wrapper does:

1. Starts the services required for the requested stage.
2. Runs the synthetic generator at the right point in the flow.
3. Waits for the stage to settle.
4. Runs [tools/validate_pipeline.py](../../tools/validate_pipeline.py) for that stage.
5. Preserves the shared lakehouse backbone between stages so Iceberg catalog state and object storage stay aligned.

## Stage map

- `ingestion`
  - Postgres, Kafka, Schema Registry, Kafka Connect source, generator inputs
- `stream-processing`
  - Kafka Connect sinks, Flink, Bronze Iceberg tables, Redis online features
- `batch`
  - Spark, dbt scheduler, Silver tables, Gold tables, offline feature tables, metadata artifacts
- `analytics`
  - Trino, Superset, Gold-serving surface
- `ml`
  - ML training, model registry records, MinIO artifacts, inference API

## Manual flow

Use this only if you want explicit control over every step.

```bash
python3 tools/manage_stack.py up ingestion
python3 generator/app.py --config params.yaml --mode both
python3 tools/validate_pipeline.py --stack ingestion

python3 tools/manage_stack.py stop ingestion
python3 tools/manage_stack.py up stream-processing
python3 tools/validate_pipeline.py --stack stream-processing

python3 tools/manage_stack.py stop stream-processing
python3 tools/manage_stack.py up batch
python3 tools/validate_pipeline.py --stack batch

python3 tools/manage_stack.py stop batch
python3 tools/manage_stack.py up analytics
python3 tools/validate_pipeline.py --stack analytics

python3 tools/manage_stack.py stop analytics
python3 tools/manage_stack.py up ml
python3 tools/validate_pipeline.py --stack ml
```

Inference checks:

```bash
curl http://localhost:8010/health
curl http://localhost:8010/models/latest

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

Annotated sample `customer_purchase` response:

```jsonc
{
  "customer_id": 123, // the customer id passed in the request body
  "score": 0.068521, // the raw model output: probability of customer_purchase_next_7d
  "artifact_manifest": "s3://ml-artifacts/manifests/customer_realtime_customer_purchase_next_7d_20260318T200611Z.json", // exact model manifest used for scoring
  "hydrated_offline_features": { // offline context fetched from Iceberg through Trino
    "customer_id": "123", // customer id as returned by Trino
    "as_of_date": "2026-03-13", // latest offline feature date used for this customer
    "views_1h": "0", // offline parity/reference value for the short-window feature
    "views_24h": "0", // offline parity/reference value for the short-window feature
    "ad_clicks_24h": "0", // offline parity/reference value for the short-window feature
    "add_to_cart_24h": "0", // offline parity/reference value for the short-window feature
    "purchases_30d": "0", // longer-horizon offline feature
    "avg_order_value_90d": "0.0", // longer-horizon offline feature
    "days_since_last_purchase": "9999", // recency feature; effectively no prior purchase
    "feature_version": "customer_realtime_features_v1", // feature contract version
    "last_event_ts": "2026-03-13 06:58:22.000000", // last event timestamp in the offline parity row
    "updated_at": "2026-03-18 20:05:25.879440 UTC", // when the offline parity row was last refreshed
    "ttl_seconds": "86400" // expected TTL for the related online feature record
  },
  "online_features": { // low-latency feature record fetched from Redis
    "customer_id": "123", // Redis entity id
    "feature_version": "customer_realtime_features_v1", // online feature contract version
    "last_event_ts": "2026-03-13T06:58:22+00:00", // last event timestamp stored in Redis
    "updated_at": "2026-03-18T19:26:37.587512+00:00", // when Redis last updated this record
    "ttl_seconds": "86400", // TTL policy stored with the Redis record
    "views_1h": "1", // online short-window feature from Redis
    "views_24h": "1", // online short-window feature from Redis
    "ad_clicks_24h": "1", // online short-window feature from Redis
    "add_to_cart_24h": "0" // online short-window feature from Redis
  },
  "scoring_features": { // final merged payload actually passed into the model
    "customer_id": 123, // request entity id
    "views_1h": 1, // came from Redis
    "views_24h": 1, // came from Redis
    "ad_clicks_24h": 1, // came from Redis
    "add_to_cart_24h": 0, // came from Redis
    "purchases_30d": 0, // came from offline Iceberg context
    "avg_order_value_90d": 0.0, // came from offline Iceberg context
    "days_since_last_purchase": 9999 // came from offline Iceberg context
  },
  "score_output_key": "scores:customer:123:purchase_propensity:v1" // Redis key written because write_redis=true
}
```

## Important operating notes

- `python3 tools/manage_stack.py stop <stack>` stops the long-running services in that stack and removes one-shot bootstrap containers for that stack.
- `stream-processing`, `batch`, `analytics`, and `ml` preserve `postgres`, `minio`, `iceberg-rest`, and `trino` on stop. That is intentional and prevents Iceberg catalog/object-store drift during stage transitions.
- `iceberg` is the standard Trino catalog name in this repo. Use `iceberg.<schema>.<table>` for manual checks.
- The synthetic generator is the only repo component intended to be run manually from the command line.
- Kafka Connect source and sink run on separate workers in local mode.
- Spark owns Bronze-to-Silver. dbt owns Silver-to-Gold and SQL-managed feature tables.
- Training writes local artifacts under `ml/artifacts/`, publishes canonical artifacts to MinIO, and records model metadata in `iceberg.silver.ml_model_registry`.
- Runtime inference does not read `ml/artifacts/`. It queries `iceberg.silver.ml_model_registry` for the latest manifest and downloads artifacts from MinIO at runtime.

## Local demo contract

- Keep generated Postgres source tables at `>= 50,000` rows in [params.yaml](../../params.yaml).
- This is not just a tuning preference. In local runs, lower-volume CDC topics repeatedly stalled in the Iceberg Kafka Connect sink control and commit path while connectors still appeared `RUNNING`.
- The failure pattern looked like:
  - Bronze rows stayed at `0`
  - source offsets did not commit
  - DLQs stayed empty
  - sink logs repeated `committed to 0 table(s)`
- Raising source-table volumes resolved that local sink behavior consistently.

## Demo surfaces

- Superset: `http://localhost:8088`
- Trino: `http://localhost:8080`
- Schema Registry: `http://localhost:8081`
- Flink UI: `http://localhost:8082`
- Spark UI: `http://localhost:4040`
- metadata HTTP server: `http://localhost:9002`
- ML inference API: `http://localhost:8010`

## Reset

To start fresh:

```bash
docker compose down -v --remove-orphans
docker volume prune -f
bash tools/reset_local_state.sh
```

The reset script also clears generated local state such as:

- metadata history snapshots
- `dbt/target`
- local ML training artifacts
- `state/flink/checkpoints`
- `state/flink/savepoints`
