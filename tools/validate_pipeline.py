#!/usr/bin/env python3
from __future__ import annotations

import argparse
import json
import os
import subprocess
import sys
import urllib.error
import urllib.request
from dataclasses import dataclass
from pathlib import Path

from platform_stacks import (
    DEFAULT_VALIDATION_HTTP_ENDPOINTS,
    DEFAULT_VALIDATION_SECTIONS,
    DEFAULT_VALIDATION_SERVICES,
    STACKS,
)


ROOT = Path(__file__).resolve().parents[1]

CDC_SINK_CONNECTORS: tuple[str, ...] = (
    "postgres-cdc-sales-rep-iceberg-sink",
    "postgres-cdc-customer-iceberg-sink",
    "postgres-cdc-advertiser-iceberg-sink",
    "postgres-cdc-product-iceberg-sink",
    "postgres-cdc-campaign-iceberg-sink",
    "postgres-cdc-campaign-product-iceberg-sink",
    "postgres-cdc-customer-session-iceberg-sink",
    "postgres-cdc-order-header-iceberg-sink",
    "postgres-cdc-order-item-iceberg-sink",
    "postgres-cdc-sales-activity-iceberg-sink",
)

CDC_SINK_CONNECTOR_TO_TRINO_CHECK: dict[str, str] = {
    "postgres-cdc-sales-rep-iceberg-sink": "bronze_sales_rep_cdc",
    "postgres-cdc-customer-iceberg-sink": "bronze_customer_cdc",
    "postgres-cdc-advertiser-iceberg-sink": "bronze_advertiser_cdc",
    "postgres-cdc-product-iceberg-sink": "bronze_product_cdc",
    "postgres-cdc-campaign-iceberg-sink": "bronze_campaign_cdc",
    "postgres-cdc-campaign-product-iceberg-sink": "bronze_campaign_product_cdc",
    "postgres-cdc-customer-session-iceberg-sink": "bronze_customer_session_cdc",
    "postgres-cdc-order-header-iceberg-sink": "bronze_order_header_cdc",
    "postgres-cdc-order-item-iceberg-sink": "bronze_order_item_cdc",
    "postgres-cdc-sales-activity-iceberg-sink": "bronze_sales_activity_cdc",
}

SINK_ALLOWLIST_TO_CONNECTOR: dict[str, str] = {
    "sales_rep": "postgres-cdc-sales-rep-iceberg-sink",
    "customer": "postgres-cdc-customer-iceberg-sink",
    "advertiser": "postgres-cdc-advertiser-iceberg-sink",
    "product": "postgres-cdc-product-iceberg-sink",
    "campaign": "postgres-cdc-campaign-iceberg-sink",
    "campaign_product": "postgres-cdc-campaign-product-iceberg-sink",
    "customer_session": "postgres-cdc-customer-session-iceberg-sink",
    "order_header": "postgres-cdc-order-header-iceberg-sink",
    "order_item": "postgres-cdc-order-item-iceberg-sink",
    "sales_activity": "postgres-cdc-sales-activity-iceberg-sink",
}

INGESTION_DATA_TOPICS: tuple[str, ...] = (
    "cdc.sales_rep",
    "cdc.customer",
    "cdc.advertiser",
    "cdc.product",
    "cdc.campaign",
    "cdc.campaign_product",
    "cdc.customer_session",
    "cdc.order_header",
    "cdc.order_item",
    "cdc.sales_activity",
    "cdc.transaction",
    "events.session_event",
)


@dataclass
class CheckResult:
    name: str
    ok: bool
    detail: str


@dataclass(frozen=True)
class ValidationContext:
    services: tuple[str, ...]
    http_endpoints: tuple[str, ...]
    trino_checks: tuple[str, ...] = ()


def run_command(args: list[str], *, cwd: Path = ROOT) -> str:
    completed = subprocess.run(
        args,
        cwd=cwd,
        check=True,
        capture_output=True,
        text=True,
    )
    return completed.stdout.strip()


def fetch_json(url: str) -> object:
    with urllib.request.urlopen(url, timeout=5) as response:
        return json.loads(response.read().decode("utf-8"))


def fetch_text(url: str) -> str:
    with urllib.request.urlopen(url, timeout=5) as response:
        return response.read().decode("utf-8")


def configured_cdc_sink_connectors() -> tuple[str, ...]:
    allowlist = os.getenv("CONNECT_SINK_ALLOWLIST", "").strip()
    if not allowlist:
        return CDC_SINK_CONNECTORS

    selected: list[str] = []
    for slug in (item.strip() for item in allowlist.split(",")):
        connector = SINK_ALLOWLIST_TO_CONNECTOR.get(slug)
        if connector:
            selected.append(connector)
    return tuple(selected)


def docker_compose(*args: str) -> str:
    return run_command(["docker", "compose", *args])


def docker_exec(service: str, *args: str) -> str:
    return docker_compose("exec", "-T", service, *args)


def parse_service_status() -> dict[str, dict[str, str]]:
    raw = docker_compose("ps", "--format=json")
    services: dict[str, dict[str, str]] = {}
    for line in raw.splitlines():
        if not line.strip():
            continue
        payload = json.loads(line)
        services[payload["Service"]] = payload
    return services


def load_expected_topics() -> list[str]:
    topics: list[str] = []
    for topic_file in sorted((ROOT / "config" / "kafka" / "topics").glob("*.env")):
        for line in topic_file.read_text(encoding="utf-8").splitlines():
            if line.startswith("TOPIC_NAME="):
                topics.append(line.split("=", 1)[1].strip())
                break
    topics.extend(
        [
            "debezium_source_connect_configs",
            "debezium_source_connect_offsets",
            "debezium_source_connect_statuses",
            "debezium_sink_connect_configs",
            "debezium_sink_connect_offsets",
            "debezium_sink_connect_statuses",
        ]
    )
    return topics


def query_postgres_counts() -> dict[str, int]:
    sql = (
        "SELECT 'customer', count(*) FROM customer "
        "UNION ALL SELECT 'customer_session', count(*) FROM customer_session "
        "UNION ALL SELECT 'order_header', count(*) FROM order_header "
        "UNION ALL SELECT 'sales_activity', count(*) FROM sales_activity;"
    )
    raw = docker_exec(
        "postgres",
        "psql",
        "-U",
        "app_user",
        "-d",
        "app_db",
        "-t",
        "-A",
        "-F",
        "\t",
        "-c",
        sql,
    )
    counts: dict[str, int] = {}
    for line in raw.splitlines():
        if not line.strip():
            continue
        name, value = line.split("\t", 1)
        counts[name] = int(value)
    return counts


def query_kafka_topic_offsets(topics: tuple[str, ...]) -> dict[str, int]:
    counts: dict[str, int] = {}
    for topic in topics:
        raw = docker_exec(
            "kafka",
            "/opt/kafka/bin/kafka-get-offsets.sh",
            "--bootstrap-server",
            "kafka:9092",
            "--topic",
            topic,
            "--time",
            "-1",
        )
        total = 0
        for line in raw.splitlines():
            if not line.strip():
                continue
            _, _, offset = line.rsplit(":", 2)
            total += int(offset)
        counts[topic] = total
    return counts


TRINO_COUNT_TABLES: dict[str, str] = {
    "bronze_sales_rep_cdc": "iceberg.bronze.bronze_sales_rep_cdc",
    "bronze_customer_cdc": "iceberg.bronze.bronze_customer_cdc",
    "bronze_advertiser_cdc": "iceberg.bronze.bronze_advertiser_cdc",
    "bronze_product_cdc": "iceberg.bronze.bronze_product_cdc",
    "bronze_campaign_cdc": "iceberg.bronze.bronze_campaign_cdc",
    "bronze_campaign_product_cdc": "iceberg.bronze.bronze_campaign_product_cdc",
    "bronze_customer_session_cdc": "iceberg.bronze.bronze_customer_session_cdc",
    "bronze_order_header_cdc": "iceberg.bronze.bronze_order_header_cdc",
    "bronze_order_item_cdc": "iceberg.bronze.bronze_order_item_cdc",
    "bronze_sales_activity_cdc": "iceberg.bronze.bronze_sales_activity_cdc",
    "bronze_session_event_raw": "iceberg.bronze.bronze_session_event_raw",
    "silver_customer_current": "iceberg.silver.silver_customer_current",
    "silver_session_event_clean": "iceberg.silver.silver_session_event_clean",
    "silver_order_header": "iceberg.silver.silver_order_header",
    "silver_customer_daily_metrics": "iceberg.silver.silver_customer_daily_metrics",
    "gold_dim_customer": "iceberg.gold.dim_customer",
    "gold_fct_session_events": "iceberg.gold.fct_session_events",
    "gold_fct_orders": "iceberg.gold.fct_orders",
    "gold_mart_customer_conversion": "iceberg.gold.mart_customer_conversion",
    "gold_mart_campaign_performance": "iceberg.gold.mart_campaign_performance",
    "ml_customer_purchase_features_v1": "iceberg.silver.customer_purchase_features_v1",
    "ml_customer_purchase_realtime_features_v1": "iceberg.silver.customer_purchase_realtime_features_v1",
    "ml_campaign_success_features_v1": "iceberg.silver.campaign_success_features_v1",
    "ml_advertiser_budget_features_v1": "iceberg.silver.advertiser_budget_features_v1",
    "ml_model_registry": "iceberg.silver.ml_model_registry",
}


def query_trino_counts(selected_checks: tuple[str, ...]) -> dict[str, int]:
    counts: dict[str, int] = {}
    if not selected_checks:
        return counts
    for name in selected_checks:
        sql = f"SELECT count(*) FROM {TRINO_COUNT_TABLES[name]}"
        raw = docker_exec(
            "trino",
            "trino",
            "--server",
            "http://localhost:8080",
            "--output-format",
            "TSV_HEADER",
            "--execute",
            sql,
        )
        lines = [line for line in raw.splitlines() if line.strip()]
        counts[name] = int(lines[1])
    return counts


def query_redis_feature_keys() -> list[str]:
    raw = docker_exec("redis", "redis-cli", "--scan", "--pattern", "features:customer:*:v1")
    return [line.strip() for line in raw.splitlines() if line.strip()]


def query_ml_feature_table_counts() -> dict[str, int]:
    sql = """
    SELECT 'customer_purchase_features_v1', count(*) FROM iceberg.silver.customer_purchase_features_v1
    UNION ALL
    SELECT 'customer_purchase_realtime_features_v1', count(*) FROM iceberg.silver.customer_purchase_realtime_features_v1
    UNION ALL
    SELECT 'campaign_success_features_v1', count(*) FROM iceberg.silver.campaign_success_features_v1
    UNION ALL
    SELECT 'advertiser_budget_features_v1', count(*) FROM iceberg.silver.advertiser_budget_features_v1
    UNION ALL
    SELECT 'ml_model_registry', count(*) FROM iceberg.silver.ml_model_registry
    """
    raw = docker_exec(
        "trino",
        "trino",
        "--server",
        "http://localhost:8080",
        "--output-format",
        "TSV_HEADER",
        "--execute",
        sql,
    )
    counts: dict[str, int] = {}
    lines = [line for line in raw.splitlines() if line.strip()]
    for line in lines[1:]:
        name, value = line.split("\t", 1)
        counts[name] = int(value)
    return counts


def metadata_file_nonempty(path: Path) -> bool:
    return path.exists() and path.stat().st_size > 0


def check_services(results: list[CheckResult], context: ValidationContext) -> None:
    statuses = parse_service_status()
    for service in context.services:
        payload = statuses.get(service)
        ok = payload is not None and payload.get("State") == "running"
        detail = payload.get("Status", "missing") if payload else "missing"
        results.append(CheckResult(f"service:{service}", ok, detail))


def check_http(results: list[CheckResult], context: ValidationContext) -> None:
    endpoints = {
        "schema_registry": "http://localhost:8081/subjects",
        "kafka_connect_source": "http://localhost:8083/connectors/postgres-cdc-connector/status",
        "kafka_connect_sinks": "http://localhost:8084/connectors",
        "minio": "http://localhost:9000/minio/health/live",
        "iceberg_rest": "http://localhost:8181/v1/config",
        "trino": "http://localhost:8080/v1/info",
        "superset": "http://localhost:8088/health",
        "flink": "http://localhost:8082/overview",
        "metadata": "http://localhost:9002/",
        "spark_ui": "http://localhost:4040",
        "ml_inference": "http://localhost:8010/health",
    }
    for name in context.http_endpoints:
        url = endpoints[name]
        try:
            fetch_text(url)
            results.append(CheckResult(f"http:{name}", True, url))
        except Exception as exc:  # pragma: no cover - operational path
            results.append(CheckResult(f"http:{name}", False, f"{url} -> {exc}"))


def check_connect(results: list[CheckResult], context: ValidationContext) -> None:
    connector_endpoints: dict[str, str] = {}
    if "kafka-connect-source" in context.services:
        connector_endpoints["postgres-cdc-connector"] = "http://localhost:8083"
    if "kafka-connect-sinks" in context.services:
        connector_endpoints.update({name: "http://localhost:8084" for name in configured_cdc_sink_connectors()})
    for connector_name, base_url in connector_endpoints.items():
        try:
            payload = fetch_json(f"{base_url}/connectors/{connector_name}/status")
            connector_state = payload["connector"]["state"]
            task_states = [task["state"] for task in payload.get("tasks", [])]
            ok = connector_state == "RUNNING" and task_states and all(state == "RUNNING" for state in task_states)
            detail = f"connector={connector_state} tasks={task_states}"
            results.append(CheckResult(f"connect:{connector_name}", ok, detail))
        except Exception as exc:  # pragma: no cover - operational path
            results.append(CheckResult(f"connect:{connector_name}", False, str(exc)))


def check_kafka(results: list[CheckResult], context: ValidationContext) -> None:
    try:
        topics = set(
            line.strip()
            for line in docker_exec("kafka", "/opt/kafka/bin/kafka-topics.sh", "--bootstrap-server", "kafka:9092", "--list").splitlines()
            if line.strip()
        )
        expected = load_expected_topics()
        missing = [topic for topic in expected if topic not in topics]
        results.append(
            CheckResult(
                "kafka:topics",
                not missing,
                "all expected topics present" if not missing else f"missing={missing}",
            )
        )
        if "kafka-connect-source" in context.services:
            offsets = query_kafka_topic_offsets(INGESTION_DATA_TOPICS)
            for topic, value in offsets.items():
                results.append(CheckResult(f"kafka:data:{topic}", value > 0, f"offset={value}"))
    except subprocess.CalledProcessError as exc:
        results.append(CheckResult("kafka:topics", False, exc.stderr.strip() or exc.stdout.strip()))


def check_flink(results: list[CheckResult]) -> None:
    expected = {
        "bronze-events-to-iceberg",
        "online-features-to-redis",
    }
    try:
        payload = fetch_json("http://localhost:8082/jobs/overview")
        jobs_by_name: dict[str, list[dict[str, object]]] = {}
        for job in payload.get("jobs", []):
            jobs_by_name.setdefault(job["name"], []).append(job)

        for job_name in sorted(expected):
            jobs = jobs_by_name.get(job_name, [])
            running_job = next((job for job in jobs if job.get("state") == "RUNNING"), None)
            job = running_job or (jobs[0] if jobs else None)
            ok = running_job is not None
            if job is None:
                detail = "missing"
            else:
                detail = f"state={job.get('state')} tasks={json.dumps(job['tasks'], sort_keys=True)} seen={len(jobs)}"
            results.append(CheckResult(f"flink:{job_name}", ok, detail))
    except Exception as exc:  # pragma: no cover - operational path
        for job_name in sorted(expected):
            results.append(CheckResult(f"flink:{job_name}", False, str(exc)))


def check_postgres(results: list[CheckResult]) -> None:
    try:
        counts = query_postgres_counts()
        for name, value in counts.items():
            results.append(CheckResult(f"postgres:{name}", value > 0, f"rows={value}"))
    except subprocess.CalledProcessError as exc:
        results.append(CheckResult("postgres:counts", False, exc.stderr.strip() or exc.stdout.strip()))


def check_trino(results: list[CheckResult], context: ValidationContext) -> None:
    selected_checks = context.trino_checks or tuple(TRINO_COUNT_TABLES)
    for name in selected_checks:
        try:
            counts = query_trino_counts((name,))
            value = counts[name]
            results.append(CheckResult(f"trino:{name}", value > 0, f"rows={value}"))
        except subprocess.CalledProcessError as exc:
            results.append(CheckResult(f"trino:{name}", False, exc.stderr.strip() or exc.stdout.strip()))


def check_redis(results: list[CheckResult]) -> None:
    try:
        keys = query_redis_feature_keys()
        results.append(CheckResult("redis:feature_keys", len(keys) > 0, f"keys={len(keys)}"))
    except subprocess.CalledProcessError as exc:
        results.append(CheckResult("redis:feature_keys", False, exc.stderr.strip() or exc.stdout.strip()))


def check_metadata(results: list[CheckResult]) -> None:
    paths = [
        ROOT / "metadata" / "lineage" / "bronze_to_silver_dimensions.jsonl",
        ROOT / "metadata" / "lineage" / "bronze_to_silver_facts.jsonl",
        ROOT / "metadata" / "lineage" / "silver_aggregates.jsonl",
        ROOT / "metadata" / "lineage" / "build_ml_features.jsonl",
        ROOT / "metadata" / "table_contracts" / "dq_results.jsonl",
    ]
    for path in paths:
        results.append(CheckResult(f"metadata:{path.name}", metadata_file_nonempty(path), str(path.relative_to(ROOT))))


def check_dbt(results: list[CheckResult]) -> None:
    try:
        raw = docker_exec("dbt", "python3", "-c", "import dbt.adapters.spark; print('ok')")
        results.append(CheckResult("dbt:adapter", raw.strip() == "ok", raw.strip()))
    except subprocess.CalledProcessError as exc:
        results.append(CheckResult("dbt:adapter", False, exc.stderr.strip() or exc.stdout.strip()))


def check_generator(results: list[CheckResult]) -> None:
    schema_path = ROOT / "generator" / "schemas" / "session_event.json"
    params_path = ROOT / "params.yaml"
    requirements_path = ROOT / "generator" / "requirements.txt"
    results.append(CheckResult("generator:schema", schema_path.exists(), str(schema_path.relative_to(ROOT))))
    results.append(CheckResult("generator:params", params_path.exists(), str(params_path.relative_to(ROOT))))
    results.append(CheckResult("generator:requirements", requirements_path.exists(), str(requirements_path.relative_to(ROOT))))
    try:
        raw = run_command(
            [
                sys.executable,
                "-c",
                (
                    "from generator.config import load_settings;"
                    "s=load_settings('params.yaml');"
                    "print(f'customers={s.customers} events={s.events_per_minute} orders={s.orders_per_hour}')"
                ),
            ]
        )
        results.append(CheckResult("generator:load_settings", True, raw))
    except subprocess.CalledProcessError as exc:
        results.append(CheckResult("generator:load_settings", False, exc.stderr.strip() or exc.stdout.strip()))


def check_ml(results: list[CheckResult]) -> None:
    paths = [
        ROOT / "ml" / "train.py",
        ROOT / "ml" / "features.py",
        ROOT / "ml" / "labels.py",
        ROOT / "ml" / "online_store.py",
        ROOT / "scripts" / "demo_realtime_scoring.py",
        ROOT / "ml" / "inference_api.py",
        ROOT / "requirements-ml.txt",
        ROOT / "ml" / "artifacts",
    ]
    for path in paths:
        ok = path.exists()
        detail = str(path.relative_to(ROOT))
        if path.is_dir():
            detail = f"{detail} ({'present' if ok else 'missing'})"
        results.append(CheckResult(f"ml:{path.name}", ok, detail))
    try:
        counts = query_ml_feature_table_counts()
        for name, value in counts.items():
            results.append(CheckResult(f"ml:{name}", value > 0, f"rows={value}"))
    except subprocess.CalledProcessError as exc:
        results.append(CheckResult("ml:feature_tables", False, exc.stderr.strip() or exc.stdout.strip()))


def render(results: list[CheckResult]) -> int:
    failures = [result for result in results if not result.ok]
    for result in results:
        status = "PASS" if result.ok else "FAIL"
        print(f"[{status}] {result.name}: {result.detail}")
    print()
    print(f"Summary: {len(results) - len(failures)} passed, {len(failures)} failed")
    return 1 if failures else 0


def parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser(description="Validate the local end-to-end pipeline.")
    parser.add_argument(
        "--stack",
        choices=["full", *sorted(STACKS)],
        default="full",
        help="Validate only the services and checks relevant to one logical stack.",
    )
    parser.add_argument(
        "--skip",
        action="append",
        default=[],
        choices=list(DEFAULT_VALIDATION_SECTIONS),
        help="Skip one validation section. Can be passed multiple times.",
    )
    return parser.parse_args()


def main() -> int:
    args = parse_args()
    results: list[CheckResult] = []
    if args.stack == "full":
        enabled_sections = set(DEFAULT_VALIDATION_SECTIONS)
        context = ValidationContext(
            services=DEFAULT_VALIDATION_SERVICES,
            http_endpoints=DEFAULT_VALIDATION_HTTP_ENDPOINTS,
            trino_checks=tuple(TRINO_COUNT_TABLES),
        )
    else:
        stack = STACKS[args.stack]
        enabled_sections = set(stack.validation_sections)
        trino_checks = stack.validation_trino_checks
        if args.stack == "stream-processing":
            selected_cdc_checks = {
                CDC_SINK_CONNECTOR_TO_TRINO_CHECK[name] for name in configured_cdc_sink_connectors()
            }
            trino_checks = tuple(
                check for check in trino_checks if check == "bronze_session_event_raw" or check in selected_cdc_checks
            )
        context = ValidationContext(
            services=stack.validation_services,
            http_endpoints=stack.validation_http_endpoints,
            trino_checks=trino_checks,
        )
    sections = {
        "services": lambda current_results: check_services(current_results, context),
        "http": lambda current_results: check_http(current_results, context),
        "connect": lambda results: check_connect(results, context),
        "kafka": lambda results: check_kafka(results, context),
        "flink": check_flink,
        "postgres": check_postgres,
        "trino": lambda current_results: check_trino(current_results, context),
        "redis": check_redis,
        "metadata": check_metadata,
        "dbt": check_dbt,
        "generator": check_generator,
        "ml": check_ml,
    }
    for name, fn in sections.items():
        if name in enabled_sections and name not in args.skip:
            try:
                fn(results)
            except subprocess.CalledProcessError as exc:
                detail = exc.stderr.strip() or exc.stdout.strip() or str(exc)
                results.append(CheckResult(f"{name}:section", False, detail))
            except urllib.error.URLError as exc:
                results.append(CheckResult(f"{name}:section", False, str(exc)))
            except Exception as exc:  # pragma: no cover - defensive operational path
                results.append(CheckResult(f"{name}:section", False, repr(exc)))
    return render(results)


if __name__ == "__main__":
    sys.exit(main())
