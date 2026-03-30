# Platform Runbook

This runbook targets the current repository boundary and Kubernetes deployment surface.

- Canonical manifest: [k8s/platform.yaml](../../k8s/platform.yaml)
- Preferred operator entrypoint: [tools/run_stack_workflow.sh](../../tools/run_stack_workflow.sh)
- Low-level stack helper: [tools/manage_stack.py](../../tools/manage_stack.py)
- Validator: [tools/validate_pipeline.py](../../tools/validate_pipeline.py)

For the current repository split, this repo owns ingestion, Bronze, Silver, Gold, metadata, and BI. ML training, model storage and registry behavior, hot feature serving, and model hosting now live in [`example-model-routing`](https://github.com/brandon-benge/example-model-routing).

## Purpose

Use this runbook to:

- start the platform in the supported order
- seed source data
- inspect Kafka, Flink, Spark, dbt, Trino, metadata, and Superset
- validate Bronze, Silver, Gold, and published offline feature datasets
- troubleshoot the current data-platform deployment

## Preferred Workflow

The supported operator flow is the staged wrapper:

```bash
bash tools/run_stack_workflow.sh --stop-at ingestion
bash tools/run_stack_workflow.sh --stop-at stream-processing
bash tools/run_stack_workflow.sh --stop-at batch
bash tools/run_stack_workflow.sh --stop-at analytics
```

Before `stream-processing`, the workflow validates the local Iceberg Kafka Connect
runtime JARs, builds them locally from upstream source if needed, ensures
`pvc/kafka-connect-plugin-cache` exists, and uploads the JARs before the full
platform manifest is applied.

```bash
bash tools/preload_connect_plugin_cache.sh
```

The wrapper:

1. Applies the Kubernetes platform through `tools/manage_stack.py up <stage>`.
2. Waits for runtime readiness with `tools/validate_pipeline.py --stack <stage> --stability-seconds 0 ...`.
3. Opens only the workstation port-forwards needed for the current stage.
4. Runs the synthetic generator during `ingestion`.
5. Runs full validation for the stage after the stage is ready.
6. Reuses the same deployed platform between stages.

It is staged by readiness and validation, not by tearing services down between phases.

## Stage Map

- `ingestion`
  - healthy services: Postgres, Kafka, Schema Registry, Kafka Connect source
  - host access: Postgres `5432`, Kafka `19092`, Schema Registry `8081`
  - extra action: runs the synthetic generator from the host
- `stream-processing`
  - healthy services: Kafka Connect sinks, MinIO, Iceberg REST, Flink, Trino
  - host access: Flink UI `8082`, Trino `8080`
  - prerequisite: `pvc/kafka-connect-plugin-cache` already contains the Iceberg Kafka Connect plugin jars
- `batch`
  - healthy services: Spark, dbt, metadata, Trino, MinIO, Iceberg REST
  - host access: metadata `9002`
- `analytics`
  - healthy services: Superset, Trino, metadata, MinIO, Iceberg REST
  - host access: Superset `8088`

## Startup And Shutdown

Preferred startup:

```bash
bash tools/run_stack_workflow.sh --stop-at analytics
```

Preferred teardown:

```bash
bash tools/run_stack_workflow.sh destroy
```

Low-level teardown:

```bash
kubectl delete -f k8s/platform.yaml --ignore-not-found=true
```

## Stack Commands

List stacks:

```bash
python3 tools/manage_stack.py list
```

Start a stack directly:

```bash
python3 tools/manage_stack.py up ingestion
python3 tools/manage_stack.py up stream-processing
python3 tools/manage_stack.py up streaming
python3 tools/manage_stack.py up batch
python3 tools/manage_stack.py up analytics
```

Stop a stack directly:

```bash
python3 tools/manage_stack.py stop ingestion
python3 tools/manage_stack.py stop stream-processing
python3 tools/manage_stack.py stop streaming
python3 tools/manage_stack.py stop batch
python3 tools/manage_stack.py stop analytics
```

Inspect one stack:

```bash
python3 tools/manage_stack.py ps streaming
python3 tools/manage_stack.py ps --all streaming
python3 tools/manage_stack.py logs batch
python3 tools/manage_stack.py logs --follow analytics
```

Recommended stack usage:

- `ingestion`: source-system intake into Kafka
- `stream-processing`: Kafka-to-Bronze processing
- `streaming`: combined ingestion plus stream-processing
- `batch`: Spark, dbt, metadata, and published Iceberg datasets
- `analytics`: Superset over curated Gold data

## Manual Access

Primary interfaces:

```bash
kubectl -n data-platform-serve port-forward svc/trino 8080:8080
kubectl -n data-platform-process port-forward svc/flink-jobmanager 8082:8081
kubectl -n data-platform-serve port-forward svc/superset 8088:8088
```

Optional debugging endpoints:

```bash
kubectl -n data-platform-infra port-forward svc/schema-registry 8081:8081
kubectl -n data-platform-govern port-forward svc/metadata 9002:9002
```

Generator access from the host:

```bash
kubectl -n data-platform-infra port-forward svc/postgres 5432:5432
kubectl -n data-platform-infra port-forward svc/kafka 19092:19092
kubectl -n data-platform-infra port-forward svc/schema-registry 8081:8081
```

## Platform Commands

```bash
kubectl apply -f k8s/platform.yaml
kubectl delete -f k8s/platform.yaml --ignore-not-found=true
kubectl get pods -A
kubectl -n data-platform-infra logs job/kafka-bootstrap
kubectl -n data-platform-serve logs deploy/superset -c superset-bootstrap
```

If `kubectl apply -f k8s/platform.yaml` fails on immutable `StorageClass` fields for `do-block-storage-retain`, delete that `StorageClass` and apply again. The staged workflow already recreates it through `tools/manage_stack.py up <stage>`.

## Seed Source Data

The synthetic generator is the only component intended to run manually from the host.

Install generator dependencies:

```bash
python3 -m venv .venv
source .venv/bin/activate
python3 -m pip install -r generator/requirements.txt
```

Run the generator:

```bash
python3 generator/app.py --config params.yaml --mode both
```

If you run it manually, keep Kafka aligned with the external listener:

```bash
KAFKA_BOOTSTRAP_SERVERS=localhost:19092 python3 generator/app.py --config params.yaml --mode both
```

## Kafka And Connect

Kafka topics are initialized by the `kafka-bootstrap` job during platform startup.

Check topic bootstrap logs:

```bash
kubectl -n data-platform-infra logs job/kafka-bootstrap
```

Kafka Connect source and sink registration are managed by bootstrap jobs from:

- `config/debezium/connector-postgres.json`
- `config/debezium/register-connector.sh`

The sink deployment no longer builds the Iceberg plugin at startup. The helper
below validates the required JARs, builds them locally from upstream Iceberg if
they are missing, ensures the repo-managed `do-block-storage-retain`
`StorageClass` and `pvc/kafka-connect-plugin-cache` exist, waits for the PVC to
bind, and uploads the resulting JAR set:

```bash
bash tools/preload_connect_plugin_cache.sh
```

If you already have an extracted runtime `lib/` directory, pass it explicitly:

```bash
bash tools/preload_connect_plugin_cache.sh /path/to/iceberg-kafka-connect-runtime/lib
```

Before sink registration, the `iceberg-cdc-bootstrap` job creates REST catalog namespaces and Bronze CDC tables.

Check connector status:

```bash
curl http://localhost:8083/connectors/postgres-cdc-connector/status
curl http://localhost:8084/connectors/postgres-cdc-customer-iceberg-sink/status
curl http://localhost:8084/connectors/postgres-cdc-order-header-iceberg-sink/status
```

## Flink

Flink handles direct-event Bronze landing in this repo.

Check Flink bootstrap logs:

```bash
kubectl -n data-platform-process logs job/flink-bootstrap-bronze-events
```

Check the job overview:

```bash
curl http://localhost:8082/jobs/overview
```

## Spark

Spark infrastructure and recurring batch execution run through the `spark` and `spark-bootstrap` deployments.

Check Spark scheduler logs:

```bash
kubectl -n data-platform-process logs deployment/spark-bootstrap
kubectl -n data-platform-process logs -f deployment/spark-bootstrap
```

Filter logs by pipeline name:

```bash
kubectl -n data-platform-process logs deployment/spark-bootstrap | rg "bronze_to_silver_dimensions"
kubectl -n data-platform-process logs deployment/spark-bootstrap | rg "bronze_to_silver_facts"
kubectl -n data-platform-process logs deployment/spark-bootstrap | rg "silver_aggregates"
kubectl -n data-platform-process logs deployment/spark-bootstrap | rg "build_ml_features"
```

The `build_ml_features` Spark output still belongs to this repo because it publishes offline feature datasets that downstream ML systems consume.

## dbt

The `dbt` deployment is available for manual commands, and `dbt-scheduler` runs recurring chunked builds.

Inspect scheduler logs:

```bash
kubectl -n data-platform-process logs deployment/dbt-scheduler
kubectl -n data-platform-process logs -f deployment/dbt-scheduler
```

Run dbt commands manually:

```bash
kubectl -n data-platform-process exec deploy/dbt -- dbt debug
kubectl -n data-platform-process exec deploy/dbt -- dbt run
kubectl -n data-platform-process exec deploy/dbt -- dbt test
kubectl -n data-platform-process exec deploy/dbt -- dbt build
```

Run selected parts of the project:

```bash
kubectl -n data-platform-process exec deploy/dbt -- dbt build --select staging
kubectl -n data-platform-process exec deploy/dbt -- dbt build --select dimensions
kubectl -n data-platform-process exec deploy/dbt -- dbt build --select facts
kubectl -n data-platform-process exec deploy/dbt -- dbt build --select marts
kubectl -n data-platform-process exec deploy/dbt -- dbt build --select features
kubectl -n data-platform-process exec deploy/dbt -- dbt build --select semantic
```

In this repo, `features` still materializes offline feature tables into `iceberg.silver` for downstream consumers.

## Validation

Run the end-to-end validator:

```bash
python3 tools/validate_pipeline.py
```

Run validation for one logical stack:

```bash
python3 tools/validate_pipeline.py --stack streaming
python3 tools/validate_pipeline.py --stack batch
python3 tools/validate_pipeline.py --stack analytics
```

Stack validation scope:

- `streaming`: streaming services plus Bronze tables
- `batch`: batch services plus Silver, offline feature, and Gold tables
- `analytics`: analytics services plus Gold tables

Skip sections if needed:

```bash
python3 tools/validate_pipeline.py --skip metadata
python3 tools/validate_pipeline.py --skip dbt
```

Basic service checks:

```bash
curl http://localhost:8081/subjects
curl http://localhost:8080/v1/info
curl http://localhost:8088/health
curl http://localhost:9000/minio/health/live
```

Check Kafka topics:

```bash
kubectl -n data-platform-infra exec statefulset/kafka -c kafka -- /opt/kafka/bin/kafka-topics.sh --bootstrap-server kafka:9092 --list
```

## Troubleshooting

If topics are missing:

- rerun `kubectl delete job -n data-platform-infra kafka-bootstrap && kubectl apply -f k8s/platform.yaml`
- verify `config/kafka/topics/*.env` files are present and valid

If CDC is not flowing:

- check connector status endpoints
- inspect Postgres replication and WAL settings
- confirm Kafka Connect can reach `postgres:5432`

If Bronze data is missing:

- inspect `kafka-connect-sinks` and `flink-bootstrap-bronze-events`
- verify the expected Bronze Iceberg tables exist through Trino

If Spark outputs are missing:

- inspect `spark-bootstrap` logs
- verify Silver tables exist in `iceberg.silver`

If dbt outputs are missing:

- inspect `dbt-scheduler` logs
- run `dbt build --select ...` manually inside the dbt deployment

If dashboards are missing:

- inspect `kubectl -n data-platform-serve logs deploy/superset -c superset-bootstrap`

If offline feature tables are missing:

- inspect Spark logs for `build_ml_features`
- run `kubectl -n data-platform-process exec deploy/dbt -- dbt build --select features`
- verify the downstream ML platform is consuming the right Iceberg tables rather than expecting this repo to host inference or Redis-serving paths
