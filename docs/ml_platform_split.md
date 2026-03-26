# ML Platform Split

This repo is being narrowed to the data platform boundary:

- source generation
- CDC, Kafka, Bronze, Silver, and Gold
- metadata, governance, lineage, and BI access
- published offline feature datasets for downstream consumers

The following ML-platform responsibilities should move to a sibling repo such as `../example-ml-platform`:

- model training
- model registry writes and registry queries
- inference APIs and serving containers
- experimentation and rollout logic
- Redis-backed online feature serving
- offline/online feature parity and reconciliation owned by the serving path

## Assets To Move

- [ml/](/Users/brandonbenge/Desktop/GitProjects/example-data-pipeline-w-ml/ml)
- [requirements-ml.txt](/Users/brandonbenge/Desktop/GitProjects/example-data-pipeline-w-ml/requirements-ml.txt)
- [config/ml-training/](/Users/brandonbenge/Desktop/GitProjects/example-data-pipeline-w-ml/config/ml-training)
- [config/features/online_feature_defs.yaml](/Users/brandonbenge/Desktop/GitProjects/example-data-pipeline-w-ml/config/features/online_feature_defs.yaml)
- [config/redis/](/Users/brandonbenge/Desktop/GitProjects/example-data-pipeline-w-ml/config/redis)
- [flink/jobs/online_features_to_redis.py](/Users/brandonbenge/Desktop/GitProjects/example-data-pipeline-w-ml/flink/jobs/online_features_to_redis.py)
- [tests/unit/test_evaluate.py](/Users/brandonbenge/Desktop/GitProjects/example-data-pipeline-w-ml/tests/unit/test_evaluate.py)
- [tests/unit/test_features.py](/Users/brandonbenge/Desktop/GitProjects/example-data-pipeline-w-ml/tests/unit/test_features.py)
- [tests/unit/test_labels.py](/Users/brandonbenge/Desktop/GitProjects/example-data-pipeline-w-ml/tests/unit/test_labels.py)
- [tests/integration/test_online_store_reconciliation.py](/Users/brandonbenge/Desktop/GitProjects/example-data-pipeline-w-ml/tests/integration/test_online_store_reconciliation.py)
- [tests/integration/test_training_pipeline.py](/Users/brandonbenge/Desktop/GitProjects/example-data-pipeline-w-ml/tests/integration/test_training_pipeline.py)
- [docs/realtime_scoring_use_case.md](/Users/brandonbenge/Desktop/GitProjects/example-data-pipeline-w-ml/docs/realtime_scoring_use_case.md)
- ML-specific sections in [docs/runbooks/platform_runbook.md](/Users/brandonbenge/Desktop/GitProjects/example-data-pipeline-w-ml/docs/runbooks/platform_runbook.md)

## Assets To Keep Here

- [dbt/models/features/](/Users/brandonbenge/Desktop/GitProjects/example-data-pipeline-w-ml/dbt/models/features)
- [config/features/offline_feature_defs.yaml](/Users/brandonbenge/Desktop/GitProjects/example-data-pipeline-w-ml/config/features/offline_feature_defs.yaml)
- [metadata/](/Users/brandonbenge/Desktop/GitProjects/example-data-pipeline-w-ml/metadata)
- [spark/](/Users/brandonbenge/Desktop/GitProjects/example-data-pipeline-w-ml/spark)
- [flink/jobs/bronze_events_to_iceberg.py](/Users/brandonbenge/Desktop/GitProjects/example-data-pipeline-w-ml/flink/jobs/bronze_events_to_iceberg.py)

## Migration Order

1. Create the sibling ML repo and copy the move set above.
2. Point the ML repo at published offline feature tables in this repo's Iceberg environment.
3. Recreate the ML repo's Kubernetes manifests for training, inference, and Redis.
4. Remove remaining ML code and stale ML docs from this repo after the sibling repo is green.

## Current Repo Changes

The current repo already stopped owning these services in its operator surface:

- `ml` workflow stage removed from [tools/run_stack_workflow.sh](/Users/brandonbenge/Desktop/GitProjects/example-data-pipeline-w-ml/tools/run_stack_workflow.sh)
- ML and Redis services removed from [tools/platform_stacks.py](/Users/brandonbenge/Desktop/GitProjects/example-data-pipeline-w-ml/tools/platform_stacks.py)
- ML and Redis validation removed from [tools/validate_pipeline.py](/Users/brandonbenge/Desktop/GitProjects/example-data-pipeline-w-ml/tools/validate_pipeline.py)
- Redis, online-feature Flink bootstrap, ML training, and ML inference removed from [k8s/platform.yaml](/Users/brandonbenge/Desktop/GitProjects/example-data-pipeline-w-ml/k8s/platform.yaml)
