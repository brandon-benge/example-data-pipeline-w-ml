# ML

## Purpose

The `ml/` package owns the local training, artifact, and inference logic for the demo models.

It does not build the feature tables itself. Offline feature tables are produced upstream in Iceberg and then consumed here for training and scoring.

## What gets trained

The repo currently trains three logistic-regression classifiers:

- `customer_realtime`
  - predicts `customer_purchase_next_7d`
- `campaign`
  - predicts `campaign_success_flag`
- `advertiser`
  - predicts `advertiser_budget_increase_next_30d`

The training table mapping is defined in [train.py](./train.py):

- `iceberg.silver.customer_purchase_realtime_features_v1`
- `iceberg.silver.campaign_success_features_v1`
- `iceberg.silver.advertiser_budget_features_v1`

## Training flow

The main entrypoint is [train.py](./train.py).

At a high level it does this:

1. Read feature rows from Iceberg through Trino.
2. Split rows chronologically into train and test sets.
3. Fit a custom logistic-regression model.
4. Write local artifacts under `ml/artifacts/`.
5. Upload canonical artifacts to MinIO.
6. Register the model version in `iceberg.silver.ml_model_registry`.

Important functions:

- `build_feature_rows(...)`
  - loads feature rows from the configured Iceberg feature table
- `train_from_rows(...)`
  - trains the model and writes artifacts
- `_register_model_version(...)`
  - writes model metadata into `iceberg.silver.ml_model_registry`

### How rows are selected

`build_feature_rows(...)` normally reads directly from the configured Iceberg feature table for the requested feature group:

- `customer` -> `iceberg.silver.customer_purchase_features_v1`
- `customer_realtime` -> `iceberg.silver.customer_purchase_realtime_features_v1`
- `campaign` -> `iceberg.silver.campaign_success_features_v1`
- `advertiser` -> `iceberg.silver.advertiser_budget_features_v1`

The runtime path is table-driven. The older file-path input mode still exists only as a compatibility path for tests.

### How rows are split

`train_from_rows(...)` calls `_chronological_split(...)`.

That split is intentionally time-ordered instead of random:

- rows are sorted by `as_of_date`
- customer rows use `customer_token` as the tie-breaker
- campaign and advertiser rows use `entity_id` as the tie-breaker
- the default split is `80%` train and `20%` test

This keeps the local demo closer to a realistic time-based evaluation and avoids obvious leakage from shuffled future rows.

### Which columns become features

The trainer does not use every column from the table.

`train_from_rows(...)` excludes metadata and identifier fields such as:

- `as_of_date`
- `entity_id`
- `customer_id`
- `customer_token`
- `feature_group`
- `feature_definition_version`
- `label_name`
- `label_value`
- `advertiser_id`
- `generated_ts`
- `last_event_ts`
- `updated_at`
- `ttl_seconds`

Everything else on the row is treated as a numeric feature.

So the actual model input vector is inferred from the table shape rather than hard-coded separately in the training loop.

### How fitting works

The fitting path is:

1. `_prepare_matrix(...)`
   - converts each feature row into a numeric matrix in feature-name order
2. `_column_stats(...)`
   - computes one mean and one standard deviation per feature column
3. `_scale_matrix(...)`
   - standardizes each feature using those means and standard deviations
4. `fit_logistic_regression(...)`
   - trains a binary logistic-regression model with batch gradient descent

Important implementation details:

- learning rate: `0.1`
- epochs: `500`
- weights are initialized to `0.0`
- bias is initialized to `0.0`
- every epoch computes the full gradient across the training rows
- probabilities use the sigmoid function

This is a simple baseline trainer, not scikit-learn.

That is intentional in this repo:

- the model format is easy to inspect
- the learned parameters are portable
- the prediction code in inference is straightforward and self-contained

### How evaluation works

After fitting, the trainer scores the test set and writes summary metrics using [evaluate.py](./evaluate.py).

The metric payload includes:

- `accuracy`
- `precision`
- `recall`
- `roc_auc`
- `train_rows`
- `test_rows`
- `feature_names`
- `feature_definition_version`
- `trained_at`

Those metrics are written both locally and into the registry metadata record.

### Artifact and registry behavior

`train_from_rows(...)` has two important controls:

- `publish_artifacts`
  - when `true`, dataset/model/metrics/manifest files are uploaded to MinIO
- `register_model`
  - when `true`, the model version is inserted into `iceberg.silver.ml_model_registry`

That split exists so tests can validate local training behavior without requiring a running object store or Trino registry write path.

## Model structure

The model class is [LogisticRegressionModel](./train.py).

It stores:

- `feature_names`
- `means`
- `stds`
- `weights`
- `bias`
- `label_name`
- `feature_definition_version`

This is a simple hand-rolled logistic regression:

- inputs are scaled using stored column means and standard deviations
- prediction is `sigmoid(bias + dot(weights, scaled_features))`
- `predict_proba(...)` returns the positive-class probability

The model is serialized with `pickle`.

## Artifacts

Training writes four local artifacts per model version under [artifacts/](./artifacts):

- `datasets/<stem>.jsonl`
- `models/<stem>.pkl`
- `metrics/<stem>.json`
- `manifests/<stem>.json`

The manifest ties the model version together and records both local artifact paths and object-store artifact URIs.

Canonical copies are uploaded to MinIO by [artifact_store.py](./artifact_store.py).

## Model registry

The model registry table is:

- `iceberg.silver.ml_model_registry`

It records:

- feature group
- label name
- feature-definition version
- training timestamp
- artifact URIs
- local artifact paths
- train/test row counts
- evaluation metrics
- model status

Training currently inserts new rows into that table from [train.py](./train.py).

## Inference flow

Runtime model loading is handled by [inference.py](./inference.py).

The runtime path is:

1. Query `iceberg.silver.ml_model_registry` for the latest manifest for a feature group.
2. Download the manifest from MinIO in memory.
3. Download the model from MinIO in memory.
4. Unpickle the model.
5. Build the scoring payload.
6. Run `predict_one(...)`.

Important detail:

- the inference path does not use a local artifact cache
- it does not rely on `ml/artifacts/`
- it reads the latest manifest from Iceberg and the model bytes from MinIO at runtime

## Scoring logic

Scoring orchestration lives in [scoring.py](./scoring.py).

There are three main scoring entrypoints:

- `score_customer(...)`
- `score_campaign(...)`
- `score_advertiser(...)`

### Customer scoring

Customer scoring combines:

- offline context from Iceberg
- online event features from Redis
- the latest `customer_realtime` model

Offline features are fetched from:

- `iceberg.silver.silver_customer_daily_metrics`
- `iceberg.silver.silver_order_header`
- `iceberg.silver.customer_realtime_features_v1_parity`

Online features are fetched from Redis through [online_store.py](./online_store.py).

The customer model input row is the merged feature payload built by `build_customer_realtime_payload(...)` in [scoring.py](./scoring.py).

In practice that payload is:

- `views_1h`
- `views_24h`
- `ad_clicks_24h`
- `add_to_cart_24h`
- `purchases_30d`
- `avg_order_value_90d`
- `days_since_last_purchase`

Redis provides the short-window online signals and Iceberg provides the longer-horizon offline context. If Redis does not have a value for a field, scoring falls back to the offline row.

### Campaign and advertiser scoring

Campaign and advertiser scoring use offline Iceberg features only in the current repo.

Those features are fetched through Trino from:

- `iceberg.silver.silver_campaign_daily_metrics`
- `iceberg.silver.silver_advertiser_daily_metrics`

## What the model returns

The model itself returns one value:

- the positive-class probability as a float

That comes from [predict_one](./inference.py), which calls `LogisticRegressionModel.predict_proba(...)` and returns the first probability from the result set.

Examples:

- customer model -> probability of `customer_purchase_next_7d`
- campaign model -> probability of `campaign_success_flag`
- advertiser model -> probability of `advertiser_budget_increase_next_30d`

So the raw model output is a single number such as:

```python
0.742381
```

That is not the full API response. The scoring layer wraps that probability into a richer payload that also includes the context used to produce it.

For example, `score_customer(...)` in [scoring.py](./scoring.py) returns:

- `customer_id`
- `score`
- `artifact_manifest`
- `hydrated_offline_features`
- `online_features`
- `scoring_features`
- optionally `score_output_key` when `write_redis=true`

So the distinction is:

- model output = one probability
- inference response = probability plus supporting feature context and artifact metadata

## API surface

The FastAPI service lives in [inference_api.py](./inference_api.py).

Endpoints:

- `GET /health`
- `GET /models/latest`
- `POST /score/customer_purchase`
- `POST /score/campaign_success`
- `POST /score/advertiser_budget_expansion`

Example requests:

```bash
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

## CLI helper

For local CLI scoring outside the API, use:

- [../tools/demo_realtime_scoring.py](../tools/demo_realtime_scoring.py)

Example:

```bash
python3 tools/demo_realtime_scoring.py \
  --customer-id 123 \
  --campaign-id 456 \
  --advertiser-id 789 \
  --write-redis
```

## Notes

- Feature engineering logic lives upstream in Iceberg-backed feature tables, not in this package.
- This package is responsible for training, artifact handling, runtime model loading, and score generation.
- The current implementation is intentionally lightweight and local-demo friendly, not a full production MLOps stack.
