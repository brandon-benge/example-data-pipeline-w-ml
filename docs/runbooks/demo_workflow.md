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
```

What the wrapper does:

1. Applies the Kubernetes platform once if it is not already present.
2. Uses [tools/validate_pipeline.py](../../tools/validate_pipeline.py) to wait for the current stage's runtime services to be ready.
3. Opens only the workstation port-forwards that are valid for that stage.
4. Runs the synthetic generator during `ingestion`.
5. Runs the full stage validation after the stage work completes.
6. Reuses the same cluster deployment between stages instead of deleting workloads between phases.

Important clarification:

- The wrapper is phased in terms of readiness, validation, and port-forward availability.
- The wrapper is not phased by deleting services after each step.
- Services are deleted only if you explicitly run `bash tools/run_stack_workflow.sh stop <stack>` or `bash tools/run_stack_workflow.sh destroy`.

## Stage map

- `ingestion`
  - runtime focus: Postgres, Kafka, Schema Registry, Kafka Connect source
  - host access added after readiness: Postgres `5432`, Kafka `19092`, Schema Registry `8081`
  - action: generator seed run
- `stream-processing`
  - runtime focus: Kafka Connect sinks, MinIO, Iceberg REST, Flink, Trino
  - host access added after readiness: Flink UI `8082`, Trino `8080`
- `batch`
  - runtime focus: Spark, dbt scheduler, metadata, Trino, MinIO, Iceberg REST
  - host access added after readiness: metadata `9002`
- `analytics`
  - runtime focus: Superset, Trino, metadata, MinIO, Iceberg REST
  - host access added after readiness: Superset `8088`

## Manual flow

Use this only if you want explicit control over every step. The staged wrapper above is the preferred operator path.

```bash
python3 tools/manage_stack.py up ingestion
kubectl -n data-platform-infra port-forward svc/postgres 5432:5432
kubectl -n data-platform-infra port-forward svc/kafka 19092:19092
kubectl -n data-platform-infra port-forward svc/schema-registry 8081:8081
python3 generator/app.py --config params.yaml --mode both
python3 tools/validate_pipeline.py --stack ingestion

python3 tools/manage_stack.py up stream-processing
python3 tools/validate_pipeline.py --stack stream-processing

python3 tools/manage_stack.py up batch
python3 tools/validate_pipeline.py --stack batch

python3 tools/manage_stack.py up analytics
python3 tools/validate_pipeline.py --stack analytics
```

Run the `kubectl port-forward` commands in separate terminals if you follow the manual flow. The staged wrapper handles port-forwards automatically, but only after the relevant stage validates as ready.

## Important operating notes

- `python3 tools/manage_stack.py stop <stack>` deletes the Kubernetes resources mapped to that stack while preserving shared dependencies defined for the next stages.
- `stream-processing`, `batch`, and `analytics` preserve `postgres`, `minio`, `iceberg-rest`, and `trino` on stop. That is intentional and prevents Iceberg catalog/object-store drift during stage transitions.
- The preferred `run_stack_workflow.sh` path does not call `stop` between phases. It reuses the same deployed platform as it advances through the stages.
- `iceberg` is the standard Trino catalog name in this repo. Use `iceberg.<schema>.<table>` for manual checks.
- The synthetic generator is the only repo component intended to be run manually from the command line.
- Kafka Connect source and sink run on separate Kubernetes workers in this repo.
- Spark owns Bronze-to-Silver. dbt owns Silver-to-Gold and SQL-managed offline feature tables.
- Training, registry, inference, experimentation, and Redis-backed online features are moving to a sibling ML platform repository. See [ML Platform Split](../ml_platform_split.md).

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

## Reset

To start fresh:

```bash
kubectl delete -f k8s/platform.yaml --ignore-not-found=true
```
