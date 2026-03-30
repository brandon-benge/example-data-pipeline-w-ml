from __future__ import annotations

import base64
import json
import os
import uuid
from dataclasses import dataclass
from datetime import datetime, timedelta, timezone
from pathlib import Path
from typing import Any

import requests
import yaml
from fastavro import parse_schema, schemaless_reader
from pyflink.datastream import StreamExecutionEnvironment
from pyflink.table import EnvironmentSettings, StreamTableEnvironment, TableEnvironment


def _first_existing_path(*candidates: Path) -> Path:
    for candidate in candidates:
        if candidate.exists():
            return candidate
    return candidates[0]


PROJECT_ROOT = _first_existing_path(
    Path("/opt/flink/usrlib/repo"),
    Path(__file__).resolve().parents[2],
)
ICEBERG_DDL_PATH = _first_existing_path(
    PROJECT_ROOT / "flink" / "sql" / "iceberg_ddl.sql",
    Path(__file__).resolve().parents[1] / "sql" / "iceberg_ddl.sql",
)
ONLINE_FEATURE_DEFS_PATH = _first_existing_path(
    PROJECT_ROOT / "config" / "features" / "online_feature_defs.yaml",
    Path("/opt/flink/usrlib/repo/config/features/online_feature_defs.yaml"),
)
SESSION_EVENT_SCHEMA_PATH = _first_existing_path(
    PROJECT_ROOT / "generator" / "schemas" / "session_event.json",
    Path("/opt/flink/usrlib/repo/generator/schemas/session_event.json"),
)

ALLOWED_EVENT_TYPES = {
    "product_view",
    "ad_impression",
    "ad_click",
    "add_to_cart",
    "checkout_start",
}

_DECODER_CACHE: dict[str, "SessionEventDecoder"] = {}


@dataclass(frozen=True)
class PayloadField:
    name: str
    sql_type: str
    logical_type: str


@dataclass(frozen=True)
class CdcTableSpec:
    source_table: str
    topic: str
    bronze_table: str
    primary_key_field: str
    payload_fields: tuple[PayloadField, ...]


CDC_TABLE_SPECS: tuple[CdcTableSpec, ...] = (
    CdcTableSpec(
        source_table="sales_rep",
        topic="cdc.sales_rep",
        bronze_table="bronze_sales_rep_cdc",
        primary_key_field="sales_rep_id",
        payload_fields=(
            PayloadField("sales_rep_id", "BIGINT", "bigint"),
            PayloadField("rep_name", "STRING", "string"),
            PayloadField("team_name", "STRING", "string"),
            PayloadField("region", "STRING", "string"),
            PayloadField("manager_name", "STRING", "string"),
            PayloadField("status", "STRING", "string"),
            PayloadField("created_at", "TIMESTAMP(3)", "timestamp"),
            PayloadField("updated_at", "TIMESTAMP(3)", "timestamp"),
        ),
    ),
    CdcTableSpec(
        source_table="customer",
        topic="cdc.customer",
        bronze_table="bronze_customer_cdc",
        primary_key_field="customer_id",
        payload_fields=(
            PayloadField("customer_id", "BIGINT", "bigint"),
            PayloadField("first_name", "STRING", "string"),
            PayloadField("last_name", "STRING", "string"),
            PayloadField("email", "STRING", "string"),
            PayloadField("phone", "STRING", "string"),
            PayloadField("city", "STRING", "string"),
            PayloadField("state", "STRING", "string"),
            PayloadField("zip_code", "STRING", "string"),
            PayloadField("status", "STRING", "string"),
            PayloadField("created_at", "TIMESTAMP(3)", "timestamp"),
            PayloadField("updated_at", "TIMESTAMP(3)", "timestamp"),
        ),
    ),
    CdcTableSpec(
        source_table="advertiser",
        topic="cdc.advertiser",
        bronze_table="bronze_advertiser_cdc",
        primary_key_field="advertiser_id",
        payload_fields=(
            PayloadField("advertiser_id", "BIGINT", "bigint"),
            PayloadField("advertiser_name", "STRING", "string"),
            PayloadField("industry", "STRING", "string"),
            PayloadField("account_tier", "STRING", "string"),
            PayloadField("region", "STRING", "string"),
            PayloadField("owner_sales_rep_id", "BIGINT", "bigint"),
            PayloadField("status", "STRING", "string"),
            PayloadField("created_at", "TIMESTAMP(3)", "timestamp"),
            PayloadField("updated_at", "TIMESTAMP(3)", "timestamp"),
        ),
    ),
    CdcTableSpec(
        source_table="product",
        topic="cdc.product",
        bronze_table="bronze_product_cdc",
        primary_key_field="product_id",
        payload_fields=(
            PayloadField("product_id", "BIGINT", "bigint"),
            PayloadField("sku", "STRING", "string"),
            PayloadField("product_name", "STRING", "string"),
            PayloadField("brand", "STRING", "string"),
            PayloadField("category", "STRING", "string"),
            PayloadField("subcategory", "STRING", "string"),
            PayloadField("list_price", "DECIMAL(12,2)", "decimal"),
            PayloadField("cost", "DECIMAL(12,2)", "decimal"),
            PayloadField("active_flag", "BOOLEAN", "boolean"),
            PayloadField("created_at", "TIMESTAMP(3)", "timestamp"),
            PayloadField("updated_at", "TIMESTAMP(3)", "timestamp"),
        ),
    ),
    CdcTableSpec(
        source_table="campaign",
        topic="cdc.campaign",
        bronze_table="bronze_campaign_cdc",
        primary_key_field="campaign_id",
        payload_fields=(
            PayloadField("campaign_id", "BIGINT", "bigint"),
            PayloadField("advertiser_id", "BIGINT", "bigint"),
            PayloadField("campaign_name", "STRING", "string"),
            PayloadField("campaign_type", "STRING", "string"),
            PayloadField("objective", "STRING", "string"),
            PayloadField("budget_amount", "DECIMAL(14,2)", "decimal"),
            PayloadField("start_date", "DATE", "date"),
            PayloadField("end_date", "DATE", "date"),
            PayloadField("status", "STRING", "string"),
            PayloadField("created_at", "TIMESTAMP(3)", "timestamp"),
            PayloadField("updated_at", "TIMESTAMP(3)", "timestamp"),
        ),
    ),
    CdcTableSpec(
        source_table="campaign_product",
        topic="cdc.campaign_product",
        bronze_table="bronze_campaign_product_cdc",
        primary_key_field="campaign_product_id",
        payload_fields=(
            PayloadField("campaign_product_id", "BIGINT", "bigint"),
            PayloadField("campaign_id", "BIGINT", "bigint"),
            PayloadField("product_id", "BIGINT", "bigint"),
            PayloadField("bid_amount", "DECIMAL(10,2)", "decimal"),
            PayloadField("priority", "INT", "int"),
            PayloadField("created_at", "TIMESTAMP(3)", "timestamp"),
            PayloadField("updated_at", "TIMESTAMP(3)", "timestamp"),
        ),
    ),
    CdcTableSpec(
        source_table="customer_session",
        topic="cdc.customer_session",
        bronze_table="bronze_customer_session_cdc",
        primary_key_field="session_id",
        payload_fields=(
            PayloadField("session_id", "BIGINT", "bigint"),
            PayloadField("customer_id", "BIGINT", "bigint"),
            PayloadField("session_start_ts", "TIMESTAMP(3)", "timestamp"),
            PayloadField("session_end_ts", "TIMESTAMP(3)", "timestamp"),
            PayloadField("device_type", "STRING", "string"),
            PayloadField("channel", "STRING", "string"),
            PayloadField("referrer_type", "STRING", "string"),
            PayloadField("created_at", "TIMESTAMP(3)", "timestamp"),
            PayloadField("updated_at", "TIMESTAMP(3)", "timestamp"),
        ),
    ),
    CdcTableSpec(
        source_table="order_header",
        topic="cdc.order_header",
        bronze_table="bronze_order_header_cdc",
        primary_key_field="order_id",
        payload_fields=(
            PayloadField("order_id", "BIGINT", "bigint"),
            PayloadField("customer_id", "BIGINT", "bigint"),
            PayloadField("order_ts", "TIMESTAMP(3)", "timestamp"),
            PayloadField("order_status", "STRING", "string"),
            PayloadField("subtotal_amount", "DECIMAL(12,2)", "decimal"),
            PayloadField("discount_amount", "DECIMAL(12,2)", "decimal"),
            PayloadField("tax_amount", "DECIMAL(12,2)", "decimal"),
            PayloadField("total_amount", "DECIMAL(12,2)", "decimal"),
            PayloadField("payment_type", "STRING", "string"),
            PayloadField("created_at", "TIMESTAMP(3)", "timestamp"),
            PayloadField("updated_at", "TIMESTAMP(3)", "timestamp"),
        ),
    ),
    CdcTableSpec(
        source_table="order_item",
        topic="cdc.order_item",
        bronze_table="bronze_order_item_cdc",
        primary_key_field="order_item_id",
        payload_fields=(
            PayloadField("order_item_id", "BIGINT", "bigint"),
            PayloadField("order_id", "BIGINT", "bigint"),
            PayloadField("product_id", "BIGINT", "bigint"),
            PayloadField("quantity", "INT", "int"),
            PayloadField("unit_price", "DECIMAL(12,2)", "decimal"),
            PayloadField("line_amount", "DECIMAL(12,2)", "decimal"),
            PayloadField("attributed_campaign_id", "BIGINT", "bigint"),
            PayloadField("created_at", "TIMESTAMP(3)", "timestamp"),
            PayloadField("updated_at", "TIMESTAMP(3)", "timestamp"),
        ),
    ),
    CdcTableSpec(
        source_table="sales_activity",
        topic="cdc.sales_activity",
        bronze_table="bronze_sales_activity_cdc",
        primary_key_field="sales_activity_id",
        payload_fields=(
            PayloadField("sales_activity_id", "BIGINT", "bigint"),
            PayloadField("advertiser_id", "BIGINT", "bigint"),
            PayloadField("sales_rep_id", "BIGINT", "bigint"),
            PayloadField("activity_ts", "TIMESTAMP(3)", "timestamp"),
            PayloadField("activity_type", "STRING", "string"),
            PayloadField("activity_outcome", "STRING", "string"),
            PayloadField("notes", "STRING", "string"),
            PayloadField("created_at", "TIMESTAMP(3)", "timestamp"),
            PayloadField("updated_at", "TIMESTAMP(3)", "timestamp"),
        ),
    ),
)


def env(name: str, default: str) -> str:
    return os.getenv(name, default)


def build_stream_table_environment(job_name: str) -> tuple[StreamExecutionEnvironment, StreamTableEnvironment]:
    stream_env = StreamExecutionEnvironment.get_execution_environment()
    settings = EnvironmentSettings.in_streaming_mode()
    table_env = StreamTableEnvironment.create(stream_env, environment_settings=settings)
    configure_table_environment(table_env, job_name)
    return stream_env, table_env


def build_table_environment(job_name: str) -> TableEnvironment:
    table_env = TableEnvironment.create(EnvironmentSettings.in_streaming_mode())
    configure_table_environment(table_env, job_name)
    return table_env


def configure_table_environment(table_env: TableEnvironment, job_name: str) -> None:
    config = table_env.get_config().get_configuration()
    config.set_string("pipeline.name", job_name)
    config.set_string("table.local-time-zone", "UTC")
    config.set_string("parallelism.default", env("FLINK_PARALLELISM", "4"))
    config.set_string("execution.checkpointing.interval", env("FLINK_CHECKPOINT_INTERVAL", "60 s"))
    config.set_string("execution.checkpointing.mode", "EXACTLY_ONCE")
    config.set_string("execution.checkpointing.min-pause", env("FLINK_CHECKPOINT_MIN_PAUSE", "30 s"))
    config.set_string("execution.checkpointing.timeout", env("FLINK_CHECKPOINT_TIMEOUT", "5 min"))
    config.set_string(
        "execution.checkpointing.externalized-checkpoint-retention",
        "RETAIN_ON_CANCELLATION",
    )
    config.set_string(
        "execution.checkpointing.tolerable-failed-checkpoints",
        env("FLINK_TOLERABLE_FAILED_CHECKPOINTS", "3"),
    )
    config.set_string("restart-strategy.type", "failure-rate")
    config.set_string(
        "restart-strategy.failure-rate.max-failures-per-interval",
        env("FLINK_MAX_FAILURES_PER_INTERVAL", "5"),
    )
    config.set_string(
        "restart-strategy.failure-rate.failure-rate-interval",
        env("FLINK_FAILURE_RATE_INTERVAL", "10 min"),
    )
    config.set_string(
        "restart-strategy.failure-rate.delay",
        env("FLINK_RESTART_DELAY", "15 s"),
    )
    pipeline_jars = env("FLINK_PIPELINE_JARS", "")
    if pipeline_jars:
        config.set_string("pipeline.jars", pipeline_jars)


def register_iceberg_catalog(table_env: TableEnvironment) -> None:
    table_env.execute_sql(
        f"""
        CREATE CATALOG iceberg WITH (
            'type' = 'iceberg',
            'catalog-type' = 'rest',
            'uri' = '{env("ICEBERG_REST_URI", "http://iceberg-rest.data-platform-infra:8181")}',
            'warehouse' = '{env("ICEBERG_WAREHOUSE", "s3://warehouse/")}',
            'property-version' = '1',
            'io-impl' = 'org.apache.iceberg.aws.s3.S3FileIO',
            's3.endpoint' = '{env("S3_ENDPOINT", "http://minio.data-platform-infra:9000")}',
            's3.region' = '{env("AWS_REGION", "us-east-1")}',
            's3.path-style-access' = 'true',
            's3.access-key-id' = '{env("AWS_ACCESS_KEY_ID", "minio")}',
            's3.secret-access-key' = '{env("AWS_SECRET_ACCESS_KEY", "minio123")}'
        )
        """
    )
    table_env.execute_sql("USE CATALOG iceberg")

    # The REST catalog can surface duplicate namespace creation as a 500 when
    # multiple jobs start at the same time. Prefer using the namespace first,
    # and if creation races, retry the USE after the failed CREATE.
    try:
        table_env.execute_sql("USE bronze")
        return
    except Exception:
        pass

    try:
        table_env.execute_sql("CREATE DATABASE bronze")
    except Exception:
        table_env.execute_sql("USE bronze")
        return

    table_env.execute_sql("USE bronze")


def execute_sql_file(table_env: TableEnvironment, sql_path: Path) -> None:
    sql_text = sql_path.read_text(encoding="utf-8")
    for statement in split_sql_statements(sql_text):
        if statement:
            table_env.execute_sql(statement)


def split_sql_statements(sql_text: str) -> list[str]:
    statements: list[str] = []
    current: list[str] = []
    for line in sql_text.splitlines():
        stripped = line.strip()
        if stripped.startswith("--"):
            continue
        current.append(line)
        if stripped.endswith(";"):
            statements.append("\n".join(current).rstrip().rstrip(";"))
            current = []
    tail = "\n".join(current).strip()
    if tail:
        statements.append(tail.rstrip(";"))
    return statements


def sql_literal(value: str) -> str:
    return value.replace("'", "''")


def load_online_feature_definitions() -> list[dict[str, Any]]:
    with ONLINE_FEATURE_DEFS_PATH.open("r", encoding="utf-8") as handle:
        document = yaml.safe_load(handle)
    return document["features"]


def load_session_event_schema() -> dict[str, Any]:
    with SESSION_EVENT_SCHEMA_PATH.open("r", encoding="utf-8") as handle:
        return json.load(handle)


def create_raw_kafka_source_table(
    table_env: TableEnvironment,
    table_name: str,
    topic: str,
    group_id: str,
) -> None:
    table_env.execute_sql(
        f"""
        CREATE TEMPORARY TABLE {table_name} (
            record_key BYTES,
            payload BYTES,
            source_partition INT METADATA FROM 'partition' VIRTUAL,
            source_offset BIGINT METADATA FROM 'offset' VIRTUAL,
            kafka_timestamp TIMESTAMP_LTZ(3) METADATA FROM 'timestamp' VIRTUAL
        ) WITH (
            'connector' = 'kafka',
            'topic' = '{topic}',
            'properties.bootstrap.servers' = '{env("KAFKA_BOOTSTRAP_SERVERS", "kafka.data-platform-infra:9092")}',
            'properties.group.id' = '{group_id}',
            'scan.startup.mode' = 'earliest-offset',
            'key.format' = 'raw',
            'key.fields' = 'record_key',
            'value.format' = 'raw',
            'value.fields-include' = 'EXCEPT_KEY'
        )
        """
    )


def create_json_kafka_sink(table_env: TableEnvironment, table_name: str, topic: str) -> None:
    table_env.execute_sql(
        f"""
        CREATE TEMPORARY TABLE {table_name} (
            dlq_key STRING,
            raw_payload STRING,
            original_topic STRING,
            source_partition INT,
            source_offset BIGINT,
            schema_subject STRING,
            schema_id INT,
            failure_reason STRING,
            kafka_timestamp TIMESTAMP_LTZ(3),
            dlq_emitted_ts TIMESTAMP_LTZ(3)
        ) WITH (
            'connector' = 'kafka',
            'topic' = '{topic}',
            'properties.bootstrap.servers' = '{env("KAFKA_BOOTSTRAP_SERVERS", "kafka.data-platform-infra:9092")}',
            'key.format' = 'raw',
            'key.fields' = 'dlq_key',
            'value.format' = 'json',
            'value.fields-include' = 'EXCEPT_KEY'
        )
        """
    )


def format_timestamp(value: datetime | None) -> str | None:
    if value is None:
        return None
    if value.tzinfo is not None:
        value = value.astimezone(timezone.utc).replace(tzinfo=None)
    return value.isoformat(sep=" ", timespec="milliseconds")


def parse_kafka_timestamp(value: Any) -> datetime:
    if isinstance(value, datetime):
        return value.replace(tzinfo=None)
    if value is None:
        return datetime.utcnow().replace(microsecond=0)
    return datetime.fromisoformat(str(value).replace("Z", "+00:00")).astimezone(timezone.utc).replace(tzinfo=None)


def decode_key(record_key: bytes | None) -> str | None:
    if record_key is None:
        return None
    try:
        text = record_key.decode("utf-8")
    except UnicodeDecodeError:
        return base64.b64encode(record_key).decode("ascii")
    try:
        parsed = json.loads(text)
    except json.JSONDecodeError:
        return text
    payload = parsed.get("payload", parsed)
    if isinstance(payload, dict) and len(payload) == 1:
        return str(next(iter(payload.values())))
    return json.dumps(payload, separators=(",", ":"), sort_keys=True)


def deterministic_event_uuid(source_name: str, source_partition: int, source_offset: int) -> str:
    return str(uuid.uuid5(uuid.NAMESPACE_URL, f"{source_name}:{source_partition}:{source_offset}"))


def unwrap_connect_payload(payload: bytes | None) -> tuple[dict[str, Any] | None, int]:
    if payload is None:
        return None, 1
    text = payload.decode("utf-8")
    parsed = json.loads(text)
    if "payload" in parsed:
        schema_version = int(parsed.get("schema", {}).get("version", 1))
        return parsed["payload"], schema_version
    return parsed, 1


def coerce_connect_value(value: Any, logical_type: str) -> Any:
    if value is None:
        return None
    if logical_type == "timestamp":
        if isinstance(value, str):
            return value.replace("T", " ")
        if isinstance(value, (int, float)):
            return format_timestamp(datetime.utcfromtimestamp(float(value) / 1000.0))
    if logical_type == "date":
        if isinstance(value, str):
            return value
        if isinstance(value, (int, float)):
            return (datetime(1970, 1, 1) + timedelta(days=int(value))).date().isoformat()
    if logical_type == "decimal":
        return str(value)
    if logical_type == "boolean":
        return bool(value)
    if logical_type in {"bigint", "int"}:
        return int(value)
    return value


def normalize_cdc_payload(
    spec: CdcTableSpec,
    record_key: bytes | None,
    payload: bytes | None,
    source_partition: int,
    source_offset: int,
    kafka_timestamp: Any,
) -> str:
    ingest_ts = parse_kafka_timestamp(kafka_timestamp)
    record_key_value = decode_key(record_key)
    event_uuid = deterministic_event_uuid(spec.source_table, source_partition, source_offset)
    if payload is None:
        result: dict[str, Any] = {
            "record_key": record_key_value,
            "source_table": spec.source_table,
            "op": "t",
            "source_ts_ms": None,
            "source_ts": None,
            "ingest_ts": format_timestamp(ingest_ts),
            "ingest_date": ingest_ts.date().isoformat(),
            "event_uuid": event_uuid,
            "schema_version": 1,
            "source_partition": source_partition,
            "source_offset": source_offset,
            "transaction_id": None,
            "is_tombstone": True,
        }
        for field in spec.payload_fields:
            result[f"payload_{field.name}"] = None
        return json.dumps(result, separators=(",", ":"), sort_keys=True)

    envelope, schema_version = unwrap_connect_payload(payload)
    envelope = envelope or {}
    source_block = envelope.get("source") or {}
    source_ts_ms = source_block.get("ts_ms") or envelope.get("ts_ms")
    source_ts = None
    if source_ts_ms is not None:
        source_ts = format_timestamp(datetime.utcfromtimestamp(float(source_ts_ms) / 1000.0))

    payload_row = envelope.get("after") or envelope.get("before") or {}
    if record_key_value is None and isinstance(payload_row, dict):
        primary_value = payload_row.get(spec.primary_key_field)
        record_key_value = str(primary_value) if primary_value is not None else None

    result = {
        "record_key": record_key_value,
        "source_table": spec.source_table,
        "op": envelope.get("op"),
        "source_ts_ms": source_ts_ms,
        "source_ts": source_ts,
        "ingest_ts": format_timestamp(ingest_ts),
        "ingest_date": ingest_ts.date().isoformat(),
        "event_uuid": event_uuid,
        "schema_version": schema_version,
        "source_partition": source_partition,
        "source_offset": source_offset,
        "transaction_id": (envelope.get("transaction") or {}).get("id") or source_block.get("txId"),
        "is_tombstone": False,
    }
    for field in spec.payload_fields:
        result[f"payload_{field.name}"] = coerce_connect_value(payload_row.get(field.name), field.logical_type)
    return json.dumps(result, separators=(",", ":"), sort_keys=True)


class SessionEventDecoder:
    def __init__(self, schema_registry_url: str) -> None:
        self.schema_registry_url = schema_registry_url.rstrip("/")
        self.expected_schema = load_session_event_schema()
        self.expected_schema_text = json.dumps(self.expected_schema, separators=(",", ":"), sort_keys=True)
        self.schema_cache: dict[int, Any] = {}
        self.parsed_schema_cache: dict[int, Any] = {}
        self.subject_versions_cache: dict[int, list[dict[str, Any]]] = {}

    def decode(self, payload: bytes | None) -> dict[str, Any]:
        if payload is None:
            return {"error": "null payload", "schema_subject": "events.session_event-value", "schema_id": None, "raw_payload": None}
        if len(payload) < 5:
            return {
                "error": "payload too short for Confluent wire format",
                "schema_subject": "events.session_event-value",
                "schema_id": None,
                "raw_payload": base64.b64encode(payload).decode("ascii"),
            }
        if payload[0] != 0:
            return {
                "error": "invalid Confluent wire-format magic byte",
                "schema_subject": "events.session_event-value",
                "schema_id": None,
                "raw_payload": base64.b64encode(payload).decode("ascii"),
            }
        schema_id = int.from_bytes(payload[1:5], byteorder="big", signed=False)
        schema = self._schema_for_id(schema_id)
        if schema is None:
            return {
                "error": f"schema id {schema_id} not found",
                "schema_subject": "events.session_event-value",
                "schema_id": schema_id,
                "raw_payload": base64.b64encode(payload).decode("ascii"),
            }
        if self._schema_text(schema) != self.expected_schema_text:
            return {
                "error": f"schema id {schema_id} does not match repository session_event contract",
                "schema_subject": "events.session_event-value",
                "schema_id": schema_id,
                "raw_payload": base64.b64encode(payload).decode("ascii"),
            }
        try:
            parsed_schema = self.parsed_schema_cache.setdefault(schema_id, parse_schema(schema))
            decoded = schemaless_reader(io_bytes(payload[5:]), parsed_schema)
        except Exception as exc:  # pragma: no cover - connector/runtime dependent
            return {
                "error": f"deserialization failed: {exc}",
                "schema_subject": "events.session_event-value",
                "schema_id": schema_id,
                "raw_payload": base64.b64encode(payload).decode("ascii"),
            }
        event_type = decoded.get("event_type")
        if event_type not in ALLOWED_EVENT_TYPES:
            return {
                "error": f"invalid event_type: {event_type}",
                "schema_subject": "events.session_event-value",
                "schema_id": schema_id,
                "raw_payload": base64.b64encode(payload).decode("ascii"),
            }
        decoded["schema_subject"] = "events.session_event-value"
        decoded["schema_id"] = schema_id
        decoded["raw_payload"] = base64.b64encode(payload).decode("ascii")
        return decoded

    def _schema_for_id(self, schema_id: int) -> dict[str, Any] | None:
        if schema_id in self.schema_cache:
            return self.schema_cache[schema_id]
        response = requests.get(f"{self.schema_registry_url}/schemas/ids/{schema_id}", timeout=10)
        if response.status_code != 200:
            return None
        schema_text = response.json()["schema"]
        schema = json.loads(schema_text)
        self.schema_cache[schema_id] = schema
        return schema

    def _schema_text(self, schema: dict[str, Any]) -> str:
        return json.dumps(schema, separators=(",", ":"), sort_keys=True)


def io_bytes(raw: bytes):
    import io

    return io.BytesIO(raw)


def session_event_extract(payload: bytes | None, schema_registry_url: str) -> str:
    decoder = _DECODER_CACHE.setdefault(schema_registry_url, SessionEventDecoder(schema_registry_url))
    decoded = decoder.decode(payload)
    if decoded.get("error"):
        return json.dumps(decoded, separators=(",", ":"), sort_keys=True)

    def as_naive_utc(value: Any) -> datetime:
        if isinstance(value, datetime):
            if value.tzinfo is None:
                return value
            return value.astimezone(timezone.utc).replace(tzinfo=None)
        return datetime.fromtimestamp(int(value) / 1000.0, tz=timezone.utc).replace(tzinfo=None)

    event_ts = as_naive_utc(decoded["event_ts"])
    ingest_ts = as_naive_utc(decoded["ingest_ts"])
    normalized = {
        "event_uuid": decoded["event_uuid"],
        "event_id": decoded["event_id"],
        "session_id": decoded["session_id"],
        "customer_id": decoded["customer_id"],
        "event_ts_ms": int(decoded["event_ts"].timestamp() * 1000) if isinstance(decoded["event_ts"], datetime) else decoded["event_ts"],
        "event_ts": format_timestamp(event_ts),
        "event_date": event_ts.date().isoformat(),
        "event_type": decoded["event_type"],
        "product_id": decoded.get("product_id"),
        "campaign_id": decoded.get("campaign_id"),
        "page_type": decoded.get("page_type"),
        "search_term": decoded.get("search_term"),
        "position_in_list": decoded.get("position_in_list"),
        "ingest_ts": format_timestamp(ingest_ts),
        "ingest_date": ingest_ts.date().isoformat(),
        "producer_version": decoded["producer_version"],
        "schema_version": decoded["schema_version"],
        "schema_subject": decoded["schema_subject"],
        "schema_id": decoded["schema_id"],
        "raw_payload": decoded["raw_payload"],
    }
    return json.dumps(normalized, separators=(",", ":"), sort_keys=True)
