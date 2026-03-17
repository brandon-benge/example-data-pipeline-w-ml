# Demo Workflow

## Purpose

This runbook documents the architecture-aligned local demo flow.

## Workflow

1. Start the local platform with `docker compose up -d`.
2. Run the synthetic generator manually to seed Postgres CDC tables and the `events.session_event` stream with `python3 generator/app.py --config params.yaml --mode both`.
3. Let the compose-managed `iceberg-cdc-bootstrap` service create the REST-catalog namespaces and Bronze CDC table DDL, then let the dedicated Kafka Connect source worker register Debezium CDC while the dedicated Kafka Connect sink worker registers one CDC Iceberg sink connector per Postgres source table. Flink handles the direct event stream and Redis online features.
4. Let the compose-managed `dbt-scheduler` container build the dbt project every 2 minutes in broader stage-level chunks with a `0.1` second pause between each chunk.
5. Validate the platform, including Gold tables, with `python3 tools/validate_pipeline.py`.

## Stack-first workflow

If you do not want the whole platform running at once, use this end-to-end stack sequence:

1. `python3 tools/manage_stack.py up ingestion`
2. `python3 generator/app.py --config params.yaml --mode both`
3. `python3 tools/validate_pipeline.py --stack ingestion`
4. `python3 tools/manage_stack.py stop ingestion`
5. `python3 tools/manage_stack.py up stream-processing`
6. `python3 tools/validate_pipeline.py --stack stream-processing`
7. `python3 tools/manage_stack.py stop stream-processing`
8. `python3 tools/manage_stack.py up batch`
9. Wait for `dbt-scheduler` or run:
   ```bash
   docker compose exec dbt dbt build --select features
   ```
10. `python3 tools/validate_pipeline.py --stack batch`
11. `python3 tools/manage_stack.py stop batch`
12. `python3 tools/manage_stack.py up analytics`
13. `python3 tools/validate_pipeline.py --stack analytics`
14. `python3 tools/manage_stack.py stop analytics`
15. `python3 tools/manage_stack.py up ml`
16. Wait for the compose-managed `ml-training` container to complete:
   ```bash
   docker compose logs -f ml-training
   ```
17. `python3 tools/validate_pipeline.py --stack ml`
18. Validate the inference service:
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
19. `python3 tools/manage_stack.py stop ml`

Notes:
- `python3 tools/manage_stack.py stop <stack>` stops the long-running services in that stack and removes the stack's bootstrap-style orchestration containers, including one-shot `*-bootstrap` services and the Flink job-submission services.
- `python3 tools/manage_stack.py ps <stack>` shows only running containers by default. Use `python3 tools/manage_stack.py ps --all <stack>` if you want to include exited bootstrap containers too.
- In local mode, `stream-processing` also needs `postgres` because the Iceberg REST catalog stores its metadata in the shared Postgres instance.
- `stream-processing` is independent of the Debezium source worker. If `kafka-connect-source` comes up, that should only be because you explicitly started `ingestion` or `streaming`.
- The sink bootstrap supports a diagnostic allowlist. In the current CDC isolation mode, only the `order_header` sink connector is registered so the CDC write path can be debugged one failing connector at a time.

The compose-managed `ml-inference` container serves separate real-world scoring endpoints:

- `POST /score/customer_purchase`
- `POST /score/campaign_success`
- `POST /score/advertiser_budget_expansion`

## Demo surfaces

- Superset dashboards on `http://localhost:8088`
- Trino coordinator on `http://localhost:8080`
- Schema Registry on `http://localhost:8081`
- metadata HTTP server on `http://localhost:9002`
- flink UI on `http://localhost:8082`
- spark UI on `http://localhost:4040`
- ML inference API on `http://localhost:8010`

## Notes

- Startup remains compose-first.
- The synthetic generator remains the only repository component intended to be run manually from the command line.
- The compose-managed `iceberg-cdc-bootstrap` service owns REST-catalog namespace creation and Bronze CDC table DDL bootstrap; Spark and Flink do not create the CDC Bronze tables.
- Kafka Connect lands Postgres CDC topics into Bronze Iceberg tables through one sink connector per source table on a dedicated sink worker; the Debezium source connector runs on its own Kafka Connect source worker. Flink is reserved for direct event ingestion and online features.
- Apache Spark jobs perform the Bronze-to-Silver transformations, applying governance logic, data quality checks, masking/tokenization rules, and lineage writes before producing Silver tables.
- The Iceberg REST catalog uses the existing Postgres service for JDBC metadata in this local demo. That shared database is a convenience choice; a more realistic deployment would isolate catalog metadata in a separate database.
- dbt Gold model builds are scheduled by the compose-managed `dbt-scheduler` service.
- Stack validation follows stage boundaries:
- `--stack ingestion` checks source-system intake, Kafka, Schema Registry, the Debezium source worker, Postgres, and generator config
- `--stack stream-processing` checks Kafka-to-Bronze/Redis processing services plus Bronze tables
- `--stack streaming` checks streaming services plus Bronze tables
- `--stack batch` checks batch services plus Silver, ML feature, and Gold tables
- `--stack analytics` checks analytics services plus Gold tables
- `--stack ml` checks ML assets plus Iceberg-backed ML feature tables
- ML artifacts are cached locally under `ml/artifacts/`, published to the MinIO `ml-artifacts` bucket, and registered in `iceberg.silver.ml_model_registry`.
