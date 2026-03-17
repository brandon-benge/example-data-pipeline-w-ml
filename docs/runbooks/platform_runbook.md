# Platform Runbook

## Purpose

This runbook defines the operator commands referenced by the architecture for bringing up the local platform, loading data, running jobs, validating outputs, and troubleshooting common issues.

## Startup and shutdown

Start the full local stack:

```bash
docker compose up -d
```

Stop the full local stack:

```bash
docker compose down
```

Reset local state:

```bash
docker compose down -v
```

## Start one logical stack at a time

If you want to conserve laptop CPU and memory, use the stack wrapper instead of starting the whole platform. The wrapper intentionally starts only one logical slice plus its required dependencies.

List the available stacks:

```bash
python3 tools/manage_stack.py list
```

Start one stack:

```bash
python3 tools/manage_stack.py up ingestion
python3 tools/manage_stack.py up stream-processing
python3 tools/manage_stack.py up streaming
python3 tools/manage_stack.py up batch
python3 tools/manage_stack.py up analytics
python3 tools/manage_stack.py up ml
```

Stop one stack:

```bash
python3 tools/manage_stack.py stop ingestion
python3 tools/manage_stack.py stop stream-processing
python3 tools/manage_stack.py stop streaming
python3 tools/manage_stack.py stop batch
python3 tools/manage_stack.py stop analytics
python3 tools/manage_stack.py stop ml
```

`stop` halts the long-running services in the logical stack and removes the stack's bootstrap-style orchestration containers, including one-shot `*-bootstrap` services and the Flink job-submission services.

Inspect one stack:

```bash
python3 tools/manage_stack.py ps streaming
python3 tools/manage_stack.py ps --all streaming
python3 tools/manage_stack.py logs -f batch
```

`ps` shows only running containers by default. Use `--all` to include exited bootstrap/orchestration containers.

Recommended usage on a laptop:

- `ingestion`: Postgres, Kafka, Schema Registry, and the Kafka Connect source worker for CDC and event intake
- `stream-processing`: Postgres, Kafka, Schema Registry, the Kafka Connect sink worker, Flink, Redis, Trino, MinIO, and Iceberg REST
- `streaming`: Postgres, Kafka, Schema Registry, Kafka Connect source worker, Kafka Connect sink worker, Flink, Redis, Trino, MinIO, Iceberg REST
- `batch`: Postgres, Spark, dbt, Trino, metadata, MinIO, Iceberg REST
- `analytics`: Postgres, Trino, Superset, metadata, MinIO, Iceberg REST
- `ml`: Postgres, Trino, Redis, MinIO, metadata, the one-shot `ml-training` container, and the `ml-inference` API service

The stack commands are designed for one-at-a-time operation. Shared services like MinIO and Iceberg REST appear in multiple stacks, so stopping one stack may also stop services another stack was using.

`streaming` remains the combined convenience stack. Use `ingestion` when you only need source-system intake into Kafka. Use `stream-processing` when you only need Kafka-to-Bronze/Redis processing. `stream-processing` no longer pulls up the Debezium source worker transitively.

The current local catalog design also shares the existing Postgres instance and database with the Iceberg REST catalog metadata backend. That is intentional for laptop simplicity, but it means `stream-processing` also depends on Postgres even when it is not reading OLTP tables directly. In a more realistic setup, the catalog should use a separate database from the OLTP source tables.

## Kafka topic creation

Kafka topics are created automatically during `docker compose up` from `config/kafka/topics/*.env` by the compose-managed bootstrap service.

Check the bootstrap service logs if needed:

```bash
docker compose logs kafka-bootstrap
```

## Kafka Connect connector registration

Kafka Connect registration is managed during `docker compose up` by the source and sink bootstrap services from:

- `config/debezium/connector-postgres.json`
- `config/debezium/register-connector.sh`

Before connector registration, the compose-managed `iceberg-cdc-bootstrap` service creates the required REST-catalog namespaces and Bronze CDC tables independently of Spark and Flink.

Check connector status:

```bash
curl http://localhost:8083/connectors/postgres-cdc-connector/status
curl http://localhost:8084/connectors/postgres-cdc-customer-iceberg-sink/status
curl http://localhost:8084/connectors/postgres-cdc-order-header-iceberg-sink/status
```

The CDC Bronze landing path now uses one Kafka Connect Iceberg sink connector per Postgres source table on a dedicated sink worker. The Debezium source connector runs on a separate Kafka Connect source worker so the initial Postgres snapshot does not compete for memory with the Iceberg sink connectors.

## Seed source data

The synthetic generator is the only component intended to be run manually:

Install generator dependencies first:

```bash
python3 -m venv .venv
source .venv/bin/activate
python3 -m pip install -r generator/requirements.txt
```

Then run the generator:

```bash
python generator/app.py --config params.yaml --mode both
```

The generator runs on your host, so it should use Kafka's external listener on `localhost:19092`.
That is now the default in the generator config. If you override it manually, keep it aligned:

```bash
KAFKA_BOOTSTRAP_SERVERS=localhost:19092 python generator/app.py --config params.yaml --mode both
```

For generator behavior, rerun semantics, and how to create Kafka events without rewriting Postgres entities, see [../generator.md](../generator.md).

## Run Flink jobs

Flink infrastructure and job submission both start through Docker Compose. Flink is now used for the direct event stream and online features; CDC Bronze landing is handled by Kafka Connect Iceberg sink connectors, one per Postgres source table.

Check the Flink submitter logs if needed:

```bash
docker compose logs flink-bootstrap-bronze-events
docker compose logs flink-bootstrap-online-features
```

## Run Spark Silver jobs

Spark infrastructure and batch execution both start through Docker Compose. The `spark-bootstrap` service runs the Silver, aggregate, and offline feature build jobs on a recurring interval through the long-lived Spark scheduler.

Check Spark batch logs if needed:

```bash
docker compose logs spark-bootstrap
```

Follow the live Spark scheduler logs:

```bash
docker compose logs -f spark-bootstrap
```

Filter Spark logs by scheduled pipeline name:

```bash
docker compose logs spark-bootstrap | rg "bronze_to_silver_dimensions"
docker compose logs spark-bootstrap | rg "bronze_to_silver_facts"
docker compose logs spark-bootstrap | rg "silver_aggregates"
docker compose logs spark-bootstrap | rg "build_ml_features"
```

The scheduler emits `Running pipeline <name>` and `Pipeline <name> failed` log lines for each pipeline, so these filtered commands are the per-job view for Spark in this repo.

Check the Spark UI while the scheduler is running:

```bash
open http://localhost:4040
```

## Run dbt

The `dbt` container starts through Docker Compose for manual commands, and the `dbt-scheduler` container runs chunked dbt builds automatically every 2 minutes. The scheduler runs the chunks below with a `0.1` second pause between them:

```bash
dbt build --select staging
sleep 0.1
dbt build --select dimensions
sleep 0.1
dbt build --select facts
sleep 0.1
dbt build --select marts
sleep 0.1
dbt build --select features
sleep 0.1
dbt build --select semantic
```

Inspect the scheduler logs if needed:

```bash
docker compose logs dbt-scheduler
docker compose logs -f dbt-scheduler
```

Run dbt commands inside the container:

```bash
docker compose exec dbt dbt debug
docker compose exec dbt dbt run
docker compose exec dbt dbt test
docker compose exec dbt dbt build
```

Use `dbt run` if you only want to materialize models. Use `dbt build` if you want the normal dbt workflow of models plus tests.

The repository dbt project under `dbt/` primarily targets the `iceberg.gold` schema, with the `features` selector materialized into `iceberg.silver`. It reads from Silver-only inputs through the Spark session adapter configured in `config/dbt/profiles.yml`.

If you want to run the same chunked flow manually instead of waiting for the scheduler:

```bash
docker compose exec dbt dbt build --select staging
docker compose exec dbt dbt build --select dimensions
docker compose exec dbt dbt build --select facts
docker compose exec dbt dbt build --select marts
docker compose exec dbt dbt build --select features
docker compose exec dbt dbt build --select semantic
```

If you want to run only part of the project ad hoc:

```bash
docker compose exec dbt dbt run --select marts
docker compose exec dbt dbt test --select staging
```

## Run ML training

ML code is implemented under `ml/`. dbt now builds the offline ML feature tables in Iceberg, and the training flow reads those tables directly. Local copies of artifacts are still written under `ml/artifacts/`, and the canonical artifact copies are published to the MinIO `ml-artifacts` bucket with version metadata written to `iceberg.silver.ml_model_registry`.

Build the dbt-managed ML feature tables from the batch stack:

```bash
docker compose exec dbt dbt build --select features
```

The `features` selector materializes:

- `iceberg.silver.customer_purchase_features_v1`
- `iceberg.silver.customer_purchase_realtime_features_v1`
- `iceberg.silver.campaign_success_features_v1`
- `iceberg.silver.advertiser_budget_features_v1`
- `iceberg.silver.ml_model_registry` is populated by the training workflow, not dbt

Run the compose-managed ML training container:

```bash
docker compose up ml-training
```

Follow the training logs if needed:

```bash
docker compose logs -f ml-training
```

The training container runs all three training steps:

```bash
curl http://localhost:8010/health
```

The ML runtime now follows a more production-shaped split:

- dbt builds offline feature tables
- `ml-training` trains all configured models
- model artifacts are published to MinIO and registered in Iceberg
- `ml-inference` serves model-specific REST endpoints

Concretely, `ml-inference` now:

- loads the latest model artifacts from the Iceberg registry and MinIO
- fetches live customer features from Redis for request-time customer scoring
- hydrates offline campaign and advertiser context from Iceberg through Trino
- serves separate endpoints for each scoring use case

- `POST /score/customer_purchase`
- `POST /score/campaign_success`
- `POST /score/advertiser_budget_expansion`

Example inference calls:

```bash
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

## Superset bootstrap

Superset metadata initialization, admin-user creation, Trino connection setup, and dashboard import are all handled during `docker compose up` by the compose-managed bootstrap service.

Check the bootstrap logs if needed:

```bash
docker compose logs superset-bootstrap
```

## Validation commands

Run the end-to-end validator:

```bash
python3 tools/validate_pipeline.py
```

Run validation for only one logical stack:

```bash
python3 tools/validate_pipeline.py --stack streaming
python3 tools/validate_pipeline.py --stack batch
python3 tools/validate_pipeline.py --stack analytics
python3 tools/validate_pipeline.py --stack ml
```

Stack validation scope:

- `streaming`: streaming services plus Bronze tables
- `batch`: batch services plus Silver, ML feature, and Gold tables
- `analytics`: analytics services plus Gold tables
- `ml`: ML assets, registry metadata, Iceberg-backed ML feature tables, the training container, and the inference service

Skip a section if you only want part of the validation surface:

```bash
python3 tools/validate_pipeline.py --skip redis --skip metadata
```

Check Kafka topics:

```bash
kafka-topics --bootstrap-server localhost:19092 --list
```

Check Schema Registry subjects:

```bash
curl http://localhost:8081/subjects
```

Check Trino:

```bash
curl http://localhost:8080/v1/info
```

Check MinIO:

```bash
curl http://localhost:9000/minio/health/live
```

Check Superset:

```bash
curl http://localhost:8088/health
```

## Redis validation

Inspect a customer feature record:

```bash
docker compose exec redis redis-cli HGETALL features:customer:123:v1
```

Check TTL:

```bash
docker compose exec redis redis-cli TTL features:customer:123:v1
```

`ttl_seconds` inside the record documents the intended TTL. Redis key expiry is enforced separately with `EXPIRE`.

## Troubleshooting

If topics are missing:
- rerun `docker compose up kafka-bootstrap`
- verify `config/kafka/topics/*.env` files are present and valid

If CDC is not flowing:
- check the connector status endpoint
- inspect Postgres replication/WAL settings
- confirm Kafka Connect can reach `postgres:5432`

If direct events fail schema validation:
- inspect `dlq.events.session_event_schema`
- verify the subject exists in Schema Registry

If Spark output is missing:
- inspect the Spark container logs
- verify the expected Silver and feature tables exist in the Iceberg catalog

If Redis features are missing:
- check Flink job health
- verify the entity key exists in source events
- confirm the key has not expired

If dashboards are missing:
- inspect `docker compose logs superset-bootstrap`

If ML artifacts are missing:
- inspect `ml/artifacts/` for dataset snapshots, model files, metrics, and manifests
- confirm the expected Iceberg-backed ML feature tables were materialized upstream
