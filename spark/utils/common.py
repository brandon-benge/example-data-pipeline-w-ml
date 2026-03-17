from __future__ import annotations

import json
import os
from datetime import datetime, timezone
from pathlib import Path
from typing import Any

from pyspark.sql.column import Column
from pyspark.sql import DataFrame, SparkSession
from pyspark.sql import functions as F
from pyspark.sql.types import StructType
from pyspark.sql.window import Window


PROJECT_ROOT = Path(__file__).resolve().parents[2]
METADATA_ROOT = PROJECT_ROOT / "metadata"
SPARK_SQL_ROOT = PROJECT_ROOT / "spark" / "sql"


def utc_now() -> datetime:
    return datetime.now(timezone.utc).replace(microsecond=0)


def build_spark(app_name: str) -> SparkSession:
    spark = (
        SparkSession.builder.appName(app_name)
        .config("spark.sql.session.timeZone", "UTC")
        .config("spark.sql.adaptive.enabled", "true")
        .getOrCreate()
    )
    spark.sparkContext.setLogLevel(os.getenv("SPARK_LOG_LEVEL", "WARN"))
    return spark


def execute_sql_file(spark: SparkSession, path: str | Path) -> None:
    sql_text = Path(path).read_text(encoding="utf-8")
    for statement in split_sql_statements(sql_text):
        if statement:
            spark.sql(statement)


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


def ensure_namespaces(spark: SparkSession) -> None:
    spark.sql("CREATE DATABASE IF NOT EXISTS iceberg.bronze")
    spark.sql("CREATE DATABASE IF NOT EXISTS iceberg.silver")
    spark.sql("CREATE DATABASE IF NOT EXISTS iceberg.gold")


def load_yaml(path: str | Path) -> Any:
    try:
        import yaml  # type: ignore

        with Path(path).open("r", encoding="utf-8") as handle:
            return yaml.safe_load(handle)
    except Exception:
        return _fallback_yaml_parse(Path(path).read_text(encoding="utf-8"))


def _fallback_yaml_parse(text: str) -> Any:
    lines: list[tuple[int, str]] = []
    for raw in text.splitlines():
        if not raw.strip() or raw.lstrip().startswith("#"):
            continue
        indent = len(raw) - len(raw.lstrip(" "))
        lines.append((indent, raw.strip()))

    def parse_block(index: int, indent: int) -> tuple[Any, int]:
        if lines[index][1].startswith("- "):
            return parse_list(index, indent)
        return parse_dict(index, indent)

    def parse_list(index: int, indent: int) -> tuple[list[Any], int]:
        result: list[Any] = []
        while index < len(lines):
            line_indent, content = lines[index]
            if line_indent != indent or not content.startswith("- "):
                break
            item_content = content[2:].strip()
            index += 1
            if not item_content:
                child, index = parse_block(index, indent + 2)
                result.append(child)
                continue
            if ":" in item_content:
                key, raw_value = item_content.split(":", 1)
                item: dict[str, Any] = {}
                raw_value = raw_value.strip()
                if raw_value:
                    item[key.strip()] = parse_scalar(raw_value)
                else:
                    child, index = parse_block(index, indent + 2)
                    item[key.strip()] = child
                while index < len(lines) and lines[index][0] > indent:
                    child_indent, child_content = lines[index]
                    if child_indent != indent + 2 or child_content.startswith("- "):
                        break
                    child_key, child_value = child_content.split(":", 1)
                    child_key = child_key.strip()
                    child_value = child_value.strip()
                    index += 1
                    if child_value:
                        item[child_key] = parse_scalar(child_value)
                    else:
                        nested, index = parse_block(index, child_indent + 2)
                        item[child_key] = nested
                result.append(item)
            else:
                result.append(parse_scalar(item_content))
        return result, index

    def parse_dict(index: int, indent: int) -> tuple[dict[str, Any], int]:
        result: dict[str, Any] = {}
        while index < len(lines):
            line_indent, content = lines[index]
            if line_indent != indent or content.startswith("- "):
                break
            key, raw_value = content.split(":", 1)
            key = key.strip()
            raw_value = raw_value.strip()
            index += 1
            if raw_value:
                result[key] = parse_scalar(raw_value)
            else:
                child, index = parse_block(index, indent + 2)
                result[key] = child
        return result, index

    def parse_scalar(raw: str) -> Any:
        value = raw.strip().strip("'").strip('"')
        if value.lower() == "true":
            return True
        if value.lower() == "false":
            return False
        if value.lower() == "null":
            return None
        if value.isdigit() or (value.startswith("-") and value[1:].isdigit()):
            return int(value)
        try:
            return float(value)
        except ValueError:
            return value

    if not lines:
        return {}
    parsed, _ = parse_block(0, lines[0][0])
    return parsed


def append_jsonl(path: str | Path, record: dict[str, Any]) -> None:
    output_path = Path(path)
    output_path.parent.mkdir(parents=True, exist_ok=True)
    with output_path.open("a", encoding="utf-8") as handle:
        handle.write(json.dumps(record, separators=(",", ":"), default=str))
        handle.write("\n")


def append_dq_result(rule_name: str, severity: str, dataset: str, passed: bool, details: dict[str, Any]) -> None:
    append_jsonl(
        METADATA_ROOT / "table_contracts" / "dq_results.jsonl",
        {
            "rule_name": rule_name,
            "severity": severity,
            "dataset": dataset,
            "passed": passed,
            "details": details,
            "recorded_at": utc_now().isoformat(),
        },
    )


def write_table(df: DataFrame, table_name: str, mode: str = "overwrite") -> None:
    writer = df.writeTo(table_name)
    if mode == "append":
        writer.append()
    else:
        writer.replace()


def _nested_field_exists(schema: StructType, path: list[str]) -> bool:
    current = schema
    for part in path:
        if not isinstance(current, StructType):
            return False
        field = next((candidate for candidate in current.fields if candidate.name == part), None)
        if field is None:
            return False
        current = field.dataType
    return True


def _existing_col(df: DataFrame, *candidates: str) -> Column | None:
    for candidate in candidates:
        if candidate in df.columns:
            return F.col(f"`{candidate}`")
        parts = candidate.split(".")
        if _nested_field_exists(df.schema, parts):
            return F.col(".".join(f"`{part}`" for part in parts))
    return None


def _coerce_timestamp(column: Column | None) -> Column | None:
    if column is None:
        return None
    return F.coalesce(
        column.cast("timestamp"),
        F.to_timestamp(F.from_unixtime(column.cast("double") / F.lit(1000.0))),
    )


def _coerce_millis(column: Column | None) -> Column | None:
    if column is None:
        return None
    return F.coalesce(
        column.cast("long"),
        F.unix_millis(column.cast("timestamp")),
    )


def normalize_cdc_df(df: DataFrame, business_key: str) -> DataFrame:
    if "record_key" in df.columns and any(column.startswith("payload_") for column in df.columns):
        raise ValueError(
            "Legacy Flink-shaped CDC Bronze rows are no longer supported. "
            "Rebuild Bronze CDC tables through Kafka Connect Iceberg sink before running Spark batch jobs."
        )

    normalized = df
    cdc_key = _existing_col(normalized, "_cdc.key", "_cdc_key")
    cdc_op = _existing_col(normalized, "_cdc.op", "_cdc_op")
    cdc_ts = _existing_col(normalized, "_cdc.ts", "_cdc_ts")
    cdc_offset = _existing_col(normalized, "_cdc.offset", "_cdc_offset", "_kafka_metadata_offset")
    kafka_timestamp = _existing_col(normalized, "_kafka_metadata_timestamp")

    if cdc_op is None or cdc_ts is None or cdc_offset is None:
        raise ValueError(
            "Unsupported CDC Bronze schema. Expected Kafka Connect Iceberg sink metadata fields "
            "('_cdc.op', '_cdc.ts', '_cdc.offset') to be present."
        )

    if "record_key" not in normalized.columns:
        key_source = cdc_key if cdc_key is not None else _existing_col(normalized, business_key)
        if key_source is not None:
            normalized = normalized.withColumn("record_key", key_source.cast("string"))
        else:
            raise ValueError(
                f"Unsupported CDC Bronze schema for business key '{business_key}'. "
                "Expected Kafka Connect key metadata or a top-level business key column."
            )

    if "op" not in normalized.columns and cdc_op is not None:
        normalized = normalized.withColumn("op", cdc_op.cast("string"))

    if "source_ts_ms" not in normalized.columns:
        source_ts_ms = _coerce_millis(cdc_ts)
        if source_ts_ms is not None:
            normalized = normalized.withColumn("source_ts_ms", source_ts_ms)

    if "source_ts" not in normalized.columns:
        source_ts = _coerce_timestamp(cdc_ts)
        if source_ts is not None:
            normalized = normalized.withColumn("source_ts", source_ts)

    if "source_offset" not in normalized.columns and cdc_offset is not None:
        normalized = normalized.withColumn("source_offset", cdc_offset.cast("long"))

    if "ingest_ts" not in normalized.columns:
        ingest_ts = _coerce_timestamp(kafka_timestamp)
        if ingest_ts is not None:
            normalized = normalized.withColumn("ingest_ts", ingest_ts)
        else:
            raise ValueError(
                "Unsupported CDC Bronze schema. Expected Kafka metadata timestamp from Kafka Connect."
            )

    if "is_tombstone" not in normalized.columns:
        if cdc_op is not None:
            normalized = normalized.withColumn("is_tombstone", cdc_op.cast("string").isin("d"))
        else:
            normalized = normalized.withColumn("is_tombstone", F.lit(False))

    metadata_columns = {
        "record_key",
        "source_table",
        "op",
        "source_ts_ms",
        "source_ts",
        "ingest_ts",
        "ingest_date",
        "event_uuid",
        "schema_version",
        "source_partition",
        "source_offset",
        "transaction_id",
        "is_tombstone",
    }
    payload_columns = [
        column
        for column in normalized.columns
        if not column.startswith("_") and not column.startswith("payload_") and column not in metadata_columns
    ]
    for column in payload_columns:
        normalized = normalized.withColumn(f"payload_{column}", F.col(f"`{column}`"))

    return normalized


def latest_cdc(df: DataFrame, business_key: str) -> DataFrame:
    df = normalize_cdc_df(df, business_key)
    key_col = F.coalesce(F.col("record_key"), F.col(f"payload_{business_key}").cast("string"))
    window = Window.partitionBy(key_col).orderBy(
        F.col("source_ts_ms").desc_nulls_last(),
        F.col("ingest_ts").desc_nulls_last(),
        F.col("source_offset").desc_nulls_last(),
    )
    return (
        df.withColumn("_business_key", key_col)
        .withColumn("_rn", F.row_number().over(window))
        .filter(F.col("_rn") == 1)
        .filter(~F.col("is_tombstone"))
        .filter(~F.col("op").isin("d"))
        .drop("_business_key", "_rn")
    )


def latest_by_event_uuid(df: DataFrame) -> DataFrame:
    window = Window.partitionBy("event_uuid").orderBy(
        F.col("ingest_ts").desc_nulls_last(),
        F.col("source_offset").desc_nulls_last(),
    )
    return df.withColumn("_rn", F.row_number().over(window)).filter(F.col("_rn") == 1).drop("_rn")


def optional_date_filter(df: DataFrame, column_name: str, start_date: str | None, end_date: str | None) -> DataFrame:
    if start_date:
        df = df.filter(F.col(column_name) >= F.to_date(F.lit(start_date)))
    if end_date:
        df = df.filter(F.col(column_name) <= F.to_date(F.lit(end_date)))
    return df


def maybe_table(spark: SparkSession, table_name: str) -> DataFrame | None:
    if spark.catalog.tableExists(table_name):
        return spark.table(table_name)
    return None
