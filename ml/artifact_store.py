from __future__ import annotations

import os
from pathlib import Path

from ml.features import PROJECT_ROOT


MODEL_BUCKET = os.getenv("MODEL_ARTIFACT_BUCKET", "ml-artifacts")
MODEL_ENDPOINT = os.getenv("MODEL_ARTIFACT_S3_ENDPOINT", "http://localhost:9000")
MODEL_REGION = os.getenv("MODEL_ARTIFACT_REGION", "us-east-1")
MODEL_ACCESS_KEY = os.getenv("AWS_ACCESS_KEY_ID", "minio")
MODEL_SECRET_KEY = os.getenv("AWS_SECRET_ACCESS_KEY", "minio123")


def _s3_client():
    try:
        import boto3  # type: ignore
    except Exception as exc:  # pragma: no cover - dependency installation path
        raise RuntimeError("boto3 is required for MinIO-backed model artifact storage") from exc

    return boto3.client(
        "s3",
        endpoint_url=MODEL_ENDPOINT,
        aws_access_key_id=MODEL_ACCESS_KEY,
        aws_secret_access_key=MODEL_SECRET_KEY,
        region_name=MODEL_REGION,
    )


def ensure_bucket() -> None:
    client = _s3_client()
    buckets = {bucket["Name"] for bucket in client.list_buckets().get("Buckets", [])}
    if MODEL_BUCKET not in buckets:
        client.create_bucket(Bucket=MODEL_BUCKET)


def upload_file(local_path: str | Path, object_key: str) -> str:
    path = Path(local_path)
    client = _s3_client()
    ensure_bucket()
    client.upload_file(str(path), MODEL_BUCKET, object_key)
    return f"s3://{MODEL_BUCKET}/{object_key}"


def download_file(uri: str, destination: str | Path) -> Path:
    bucket, key = parse_s3_uri(uri)
    destination_path = Path(destination)
    destination_path.parent.mkdir(parents=True, exist_ok=True)
    client = _s3_client()
    client.download_file(bucket, key, str(destination_path))
    return destination_path


def parse_s3_uri(uri: str) -> tuple[str, str]:
    if not uri.startswith("s3://"):
        raise ValueError(f"Unsupported artifact uri: {uri}")
    bucket_and_key = uri[5:]
    bucket, key = bucket_and_key.split("/", 1)
    return bucket, key


def cache_path(uri: str) -> Path:
    _, key = parse_s3_uri(uri)
    return PROJECT_ROOT / ".cache" / "model_artifacts" / key
