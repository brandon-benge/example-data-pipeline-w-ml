#!/usr/bin/env python3
from __future__ import annotations

import argparse
import json
import os
import socket
import subprocess
import sys
import time
import urllib.error
import urllib.parse
import urllib.request
from dataclasses import dataclass
from pathlib import Path

ROOT = Path(__file__).resolve().parents[1]
if str(ROOT) not in sys.path:
    sys.path.insert(0, str(ROOT))

from generator.config import load_settings
from platform_stacks import (
    DEFAULT_VALIDATION_HTTP_ENDPOINTS,
    DEFAULT_VALIDATION_SECTIONS,
    DEFAULT_VALIDATION_SERVICES,
    SERVICE_RESOURCE_KIND,
    STACKS,
    canonical_namespace_for_service,
    namespace_candidates_for_service,
)

STABILITY_CHECK_STACKS: tuple[str, ...] = ("stream-processing", "streaming", "batch")
DEFAULT_STABILITY_WINDOW_SECONDS = 60
STABILITY_POLL_SECONDS = 5

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


def kubectl(*args: str) -> str:
    return run_command(["kubectl", *args])


def kubectl_success(*args: str) -> bool:
    completed = subprocess.run(
        ["kubectl", *args],
        cwd=ROOT,
        capture_output=True,
        text=True,
    )
    return completed.returncode == 0


def fetch_json(url: str) -> object:
    with urllib.request.urlopen(url, timeout=5) as response:
        return json.loads(response.read().decode("utf-8"))


def fetch_text(url: str) -> str:
    with urllib.request.urlopen(url, timeout=5) as response:
        return response.read().decode("utf-8")


def tcp_connect(host: str, port: int) -> None:
    with socket.create_connection((host, port), timeout=5):
        return


def configured_cdc_sink_connectors() -> tuple[str, ...]:
    allowlist = os.getenv("CONNECT_SINK_ALLOWLIST", "").strip()
    if not allowlist:
        allowlist = configured_cdc_sink_allowlist()
    if not allowlist:
        return CDC_SINK_CONNECTORS

    selected: list[str] = []
    for slug in (item.strip() for item in allowlist.split(",")):
        connector = SINK_ALLOWLIST_TO_CONNECTOR.get(slug)
        if connector:
            selected.append(connector)
    return tuple(selected)


def configured_cdc_sink_allowlist() -> str:
    return ""


def resolve_service_namespace(service: str) -> str:
    kind = SERVICE_RESOURCE_KIND.get(service, "service")
    for namespace in namespace_candidates_for_service(service):
        if kubectl_success("get", kind, service, "-n", namespace):
            return namespace
    return canonical_namespace_for_service(service)


def resolve_pod_name(service: str) -> str:
    namespace = resolve_service_namespace(service)
    raw = kubectl(
        "get",
        "pods",
        "-n",
        namespace,
        "-l",
        f"app.kubernetes.io/name={service}",
        "-o",
        "json",
    )
    items = json.loads(raw).get("items", [])
    if not items:
        raise subprocess.CalledProcessError(
            returncode=1,
            cmd=["kubectl", "get", "pods", "-n", namespace, "-l", f"app.kubernetes.io/name={service}", "-o", "json"],
            stderr=f"no pods found for service {service} in namespace {namespace}",
        )

    def pod_rank(item: dict[str, object]) -> tuple[int, str]:
        status = item.get("status", {}) if isinstance(item, dict) else {}
        phase = str(status.get("phase", ""))
        container_statuses = status.get("containerStatuses", [])
        ready = any(
            isinstance(container_status, dict) and container_status.get("ready")
            for container_status in container_statuses
        )
        if ready and phase == "Running":
            return (0, str(item["metadata"]["name"]))
        if phase == "Running":
            return (1, str(item["metadata"]["name"]))
        return (2, str(item["metadata"]["name"]))

    return min(items, key=pod_rank)["metadata"]["name"]


def kubectl_exec(service: str, *args: str) -> str:
    pod = resolve_pod_name(service)
    return kubectl("exec", "-n", resolve_service_namespace(service), pod, "--", *args)


def kubectl_exec_in_container(service: str, container: str, *args: str) -> str:
    pod = resolve_pod_name(service)
    return kubectl("exec", "-n", resolve_service_namespace(service), pod, "-c", container, "--", *args)


def get_first_pod_creation_timestamp(service: str) -> str:
    namespace = resolve_service_namespace(service)
    return kubectl(
        "get",
        "pods",
        "-n",
        namespace,
        "-l",
        f"app.kubernetes.io/name={service}",
        "-o",
        "jsonpath={.items[0].metadata.creationTimestamp}",
    )


def get_job_completion_timestamp(job_name: str) -> str:
    namespace = resolve_service_namespace(job_name)
    return kubectl(
        "get",
        "job",
        job_name,
        "-n",
        namespace,
        "-o",
        "jsonpath={.status.completionTime}",
    )


def list_kafka_topics(
    *,
    attempts: int = 5,
    delay_seconds: int = 2,
    expected_topics: set[str] | None = None,
) -> set[str]:
    last_error: subprocess.CalledProcessError | None = None
    last_topics: set[str] = set()
    for attempt in range(1, attempts + 1):
        try:
            raw = kubectl_exec_in_container(
                "kafka",
                "kafka",
                "/opt/kafka/bin/kafka-topics.sh",
                "--bootstrap-server",
                "kafka:9092",
                "--list",
            )
            topics = {line.strip() for line in raw.splitlines() if line.strip()}
            last_topics = topics
            if expected_topics is None:
                return topics
            missing = sorted(expected_topics - topics)
            if not missing:
                return topics
            if attempt == attempts:
                return topics
            print(
                "[validate:kafka] topic list incomplete on attempt "
                f"{attempt}/{attempts}: found={len(topics)} missing={len(missing)}",
                flush=True,
            )
            time.sleep(delay_seconds)
            continue
        except subprocess.CalledProcessError as exc:
            stderr = (exc.stderr or "").strip()
            stdout = (exc.stdout or "").strip()
            detail = stderr or stdout
            last_error = exc
            transient = (
                'container not found ("kafka")' in detail
                or "unable to upgrade connection" in detail
                or "PodInitializing" in detail
            )
            if not transient or attempt == attempts:
                raise
            print(
                f"[validate:kafka] transient topic-list failure on attempt {attempt}/{attempts}: {detail}",
                flush=True,
            )
            time.sleep(delay_seconds)
    if last_error is not None:
        raise last_error
    return last_topics


def parse_service_status() -> dict[str, dict[str, str]]:
    services: dict[str, dict[str, str]] = {}
    for name, kind in SERVICE_RESOURCE_KIND.items():
        namespace = resolve_service_namespace(name)
        try:
            raw = kubectl("get", kind, name, "-n", namespace, "-o", "json")
        except subprocess.CalledProcessError:
            continue
        item = json.loads(raw)
        kube_kind = item["kind"]
        status = item.get("status", {})
        if kube_kind in {"Deployment", "StatefulSet"}:
            desired = item["spec"].get("replicas", 1)
            ready = status.get("readyReplicas", 0)
            state = "running" if ready >= desired else "pending"
            detail = f"ready={ready}/{desired}"
        elif kube_kind == "CronJob":
            schedule = item["spec"].get("schedule", "unknown")
            state = "running"
            detail = f"schedule={schedule}"
        else:
            succeeded = status.get("succeeded", 0)
            failed = status.get("failed", 0)
            active = status.get("active", 0)
            state = "running" if active else ("completed" if succeeded else "failed" if failed else "pending")
            detail = f"active={active} succeeded={succeeded} failed={failed}"
        services[name] = {"State": state, "Status": f"{detail} namespace={namespace}"}
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
            "_schemas",
            "debezium_source_connect_configs",
            "debezium_source_connect_offsets",
            "debezium_source_connect_statuses",
            "debezium_sink_connect_configs",
            "debezium_sink_connect_offsets",
            "debezium_sink_connect_statuses",
        ]
    )
    return topics


def load_expected_schema_subjects() -> list[str]:
    return sorted(path.stem for path in (ROOT / "config" / "schema-registry" / "subjects").glob("*.json"))


def query_postgres_counts() -> dict[str, int]:
    sql = (
        "SELECT 'customer', count(*) FROM customer "
        "UNION ALL SELECT 'customer_session', count(*) FROM customer_session "
        "UNION ALL SELECT 'order_header', count(*) FROM order_header "
        "UNION ALL SELECT 'sales_activity', count(*) FROM sales_activity;"
    )
    raw = kubectl_exec(
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
    total_topics = len(topics)
    for index, topic in enumerate(topics, start=1):
        print(f"[validate:kafka] Reading topic offset {index}/{total_topics}: {topic}", flush=True)
        raw = kubectl_exec(
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
}


def query_trino_counts(selected_checks: tuple[str, ...]) -> dict[str, int]:
    counts: dict[str, int] = {}
    if not selected_checks:
        return counts
    for name in selected_checks:
        sql = f"SELECT count(*) FROM {TRINO_COUNT_TABLES[name]}"
        raw = kubectl_exec(
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


def metadata_file_nonempty(path: Path) -> bool:
    return path.exists() and path.stat().st_size > 0


def check_services(results: list[CheckResult], context: ValidationContext) -> None:
    statuses = parse_service_status()
    for service in context.services:
        payload = statuses.get(service)
        ok = payload is not None and payload.get("State") in {"running", "completed"}
        detail = payload.get("Status", "missing") if payload else "missing"
        results.append(CheckResult(f"service:{service}", ok, detail))


def check_http(results: list[CheckResult], context: ValidationContext) -> None:
    endpoints = {
        "schema_registry": "schema-registry",
        "kafka_connect_source": "kafka-connect-source",
        "kafka_connect_sinks": "kafka-connect-sinks",
        "minio": "minio",
        "iceberg_rest": "iceberg-rest",
        "trino": "trino",
        "superset": "superset",
        "flink": "flink-jobmanager",
        "metadata": "metadata",
        "spark_ui": "spark-bootstrap",
    }
    for name in context.http_endpoints:
        try:
            service = endpoints[name]
            if name == "schema_registry":
                raw = kubectl_exec(service, "sh", "-lc", "curl -fsS http://localhost:8081/subjects")
                subjects = json.loads(raw)
                expected_subjects = load_expected_schema_subjects()
                missing_subjects = [subject for subject in expected_subjects if subject not in subjects]
                ok = not missing_subjects
                detail = (
                    f"subjects={len(subjects)}"
                    if ok
                    else f"subjects={len(subjects)} missing={missing_subjects}"
                )
                results.append(CheckResult(f"http:{name}", ok, detail))
                continue

            namespace = resolve_service_namespace(service)
            raw = kubectl("get", "svc", service, "-n", namespace, "-o", "name")
            results.append(CheckResult(f"http:{name}", True, raw))
        except Exception as exc:  # pragma: no cover - operational path
            results.append(CheckResult(f"http:{name}", False, str(exc)))


def check_connect(results: list[CheckResult], context: ValidationContext) -> None:
    connector_endpoints: dict[str, str] = {}
    if "kafka-connect-source" in context.services:
        connector_endpoints["postgres-cdc-connector"] = "kafka-connect-source"
    if "kafka-connect-sinks" in context.services:
        connector_endpoints.update({name: "kafka-connect-sinks" for name in configured_cdc_sink_connectors()})
    for connector_name, service in connector_endpoints.items():
        try:
            raw = kubectl_exec(service, "sh", "-lc", f"curl -fsS http://localhost:8083/connectors/{connector_name}/status")
            payload = json.loads(raw)
            connector_state = payload["connector"]["state"]
            task_states = [task["state"] for task in payload.get("tasks", [])]
            ok = connector_state == "RUNNING" and task_states and all(state == "RUNNING" for state in task_states)
            detail = f"connector={connector_state} tasks={task_states}"
            results.append(CheckResult(f"connect:{connector_name}", ok, detail))
        except Exception as exc:  # pragma: no cover - operational path
            results.append(CheckResult(f"connect:{connector_name}", False, str(exc)))


def check_kafka(results: list[CheckResult], context: ValidationContext) -> None:
    try:
        kafka_created_at = get_first_pod_creation_timestamp("kafka").strip()
        bootstrap_completed_at = get_job_completion_timestamp("kafka-bootstrap").strip()
        bootstrap_fresh = (
            bool(kafka_created_at)
            and bool(bootstrap_completed_at)
            and bootstrap_completed_at >= kafka_created_at
        )
        detail = f"kafka_created_at={kafka_created_at or 'missing'} bootstrap_completed_at={bootstrap_completed_at or 'missing'}"
        results.append(CheckResult("kafka:bootstrap_fresh", bootstrap_fresh, detail))
        if not bootstrap_fresh:
            return
    except subprocess.CalledProcessError as exc:
        results.append(CheckResult("kafka:bootstrap_fresh", False, exc.stderr.strip() or exc.stdout.strip()))
        return

    try:
        print("[validate:kafka] Listing broker topics", flush=True)
        expected = load_expected_topics()
        topics = list_kafka_topics(expected_topics=set(expected))
        missing = [topic for topic in expected if topic not in topics]
        print(
            f"[validate:kafka] Broker topic listing complete. expected={len(expected)} found={len(topics)} missing={len(missing)}",
            flush=True,
        )
        results.append(
            CheckResult(
                "kafka:topics",
                not missing,
                "all expected topics present" if not missing else f"missing={missing}",
            )
        )
        if missing:
            return
        if "kafka-connect-source" in context.services:
            offsets = query_kafka_topic_offsets(INGESTION_DATA_TOPICS)
            for topic, value in offsets.items():
                results.append(CheckResult(f"kafka:data:{topic}", value > 0, f"offset={value}"))
    except subprocess.CalledProcessError as exc:
        results.append(CheckResult("kafka:topics", False, exc.stderr.strip() or exc.stdout.strip()))


def check_flink(results: list[CheckResult]) -> None:
    expected = {
        "bronze-events-to-iceberg",
    }
    try:
        payload = json.loads(kubectl_exec("flink-jobmanager", "sh", "-lc", "curl -fsS http://localhost:8081/jobs/overview"))
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


def check_metadata(results: list[CheckResult]) -> None:
    paths = [
        ROOT / "metadata" / "catalog_seed.yaml",
        ROOT / "metadata" / "glossary" / "terms.yaml",
        ROOT / "metadata" / "certification" / "datasets.yaml",
        ROOT / "metadata" / "lineage" / "bronze_to_silver_dimensions.jsonl",
        ROOT / "metadata" / "lineage" / "bronze_to_silver_facts.jsonl",
        ROOT / "metadata" / "lineage" / "silver_aggregates.jsonl",
        ROOT / "metadata" / "lineage" / "build_ml_features.jsonl",
        ROOT / "metadata" / "lineage" / "dbt_runs.jsonl",
        ROOT / "metadata" / "lineage" / "latest_runs.json",
        ROOT / "metadata" / "table_contracts" / "dq_results.jsonl",
        ROOT / "metadata" / "table_contracts" / "dbt_test_results.jsonl",
        ROOT / "metadata" / "table_contracts" / "latest_results.json",
    ]
    for path in paths:
        results.append(CheckResult(f"metadata:{path.name}", metadata_file_nonempty(path), str(path.relative_to(ROOT))))


def check_dbt(results: list[CheckResult]) -> None:
    try:
        raw = kubectl_exec("dbt", "python3", "-c", "import dbt.adapters.spark; print('ok')")
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
    settings = None
    if schema_path.exists():
        try:
            schema = json.loads(schema_path.read_text(encoding="utf-8"))
            schema_name = schema.get("name", "unknown")
            field_count = len(schema.get("fields", []))
            results.append(CheckResult("generator:schema_json", True, f"name={schema_name} fields={field_count}"))
        except Exception as exc:
            results.append(CheckResult("generator:schema_json", False, str(exc)))
    try:
        settings = load_settings("params.yaml")
        raw = f"customers={settings.customers} events={settings.events_per_minute} orders={settings.orders_per_hour}"
        results.append(CheckResult("generator:load_settings", True, raw))
    except Exception as exc:
        results.append(CheckResult("generator:load_settings", False, str(exc)))
        return

    try:
        parsed = urllib.parse.urlparse(settings.schema_registry_url)
        host = parsed.hostname or "localhost"
        port = parsed.port or (443 if parsed.scheme == "https" else 80)
        tcp_connect(host, port)
        results.append(CheckResult("generator:schema_registry_access", True, f"{host}:{port}"))
    except Exception as exc:
        results.append(CheckResult("generator:schema_registry_access", False, str(exc)))

    try:
        subjects = fetch_json(f"{settings.schema_registry_url.rstrip('/')}/subjects")
        subject_count = len(subjects) if isinstance(subjects, list) else 0
        results.append(CheckResult("generator:schema_registry_subjects", True, f"subjects={subject_count}"))
    except Exception as exc:
        results.append(CheckResult("generator:schema_registry_subjects", False, str(exc)))

    try:
        tcp_connect(settings.postgres_host, settings.postgres_port)
        results.append(
            CheckResult(
                "generator:postgres_access",
                True,
                f"{settings.postgres_host}:{settings.postgres_port}/{settings.postgres_db}",
            )
        )
    except Exception as exc:
        results.append(CheckResult("generator:postgres_access", False, str(exc)))

    kafka_targets = [item.strip() for item in settings.kafka_bootstrap_servers.split(",") if item.strip()]
    kafka_access_ok = False
    kafka_detail = "no bootstrap servers configured"
    for target in kafka_targets:
        host, separator, port_str = target.rpartition(":")
        if not separator:
            host = target
            port_str = "9092"
        try:
            tcp_connect(host, int(port_str))
            kafka_access_ok = True
            kafka_detail = f"{host}:{port_str}"
            break
        except Exception as exc:
            kafka_detail = f"{target} ({exc})"
    results.append(CheckResult("generator:kafka_access", kafka_access_ok, kafka_detail))

    try:
        expected = load_expected_topics()
        topics = list_kafka_topics(expected_topics=set(expected))
        missing = [topic for topic in expected if topic not in topics]
        results.append(
            CheckResult(
                "generator:kafka_topics",
                not missing,
                "all expected topics present" if not missing else f"missing={missing}",
            )
        )
    except subprocess.CalledProcessError as exc:
        results.append(CheckResult("generator:kafka_topics", False, exc.stderr.strip() or exc.stdout.strip()))


def capture_stability_metrics(
    context: ValidationContext,
    enabled_sections: set[str],
    *,
    include_postgres: bool,
) -> dict[str, int]:
    metrics: dict[str, int] = {}

    if include_postgres or "postgres" in enabled_sections:
        for name, value in query_postgres_counts().items():
            metrics[f"postgres:{name}"] = value

    if "trino" in enabled_sections:
        for name, value in query_trino_counts(context.trino_checks).items():
            metrics[f"trino:{name}"] = value

    return metrics


def check_data_stability(
    results: list[CheckResult],
    *,
    stack_name: str,
    context: ValidationContext,
    enabled_sections: set[str],
    stability_window_seconds: int,
) -> None:
    if stack_name not in STABILITY_CHECK_STACKS:
        return

    if stability_window_seconds <= 0:
        results.append(CheckResult(f"stability:{stack_name}", True, "stability wait disabled"))
        return

    metric_count = 0
    try:
        include_postgres = stack_name in {"stream-processing", "streaming", "batch"}
        baseline = capture_stability_metrics(
            context,
            enabled_sections,
            include_postgres=include_postgres,
        )
        metric_count = len(baseline)
        if not baseline:
            results.append(CheckResult(f"stability:{stack_name}", True, "no count-based metrics selected"))
            return

        samples = max(1, stability_window_seconds // STABILITY_POLL_SECONDS)
        print(f"[stability:{stack_name}] Monitoring {metric_count} metrics for {stability_window_seconds}s.")
        for sample in range(1, samples + 1):
            print(f"[stability:{stack_name}] Sample {sample}/{samples}")
            time.sleep(STABILITY_POLL_SECONDS)
            current = capture_stability_metrics(
                context,
                enabled_sections,
                include_postgres=include_postgres,
            )
            increases = [
                f"{name} {baseline[name]}->{value}"
                for name, value in current.items()
                if name in baseline and value > baseline[name]
            ]
            if increases:
                results.append(
                    CheckResult(
                        f"stability:{stack_name}",
                        False,
                        "data increased during stability window: " + "; ".join(increases[:5]),
                    )
                )
                return
            baseline = current

        results.append(
            CheckResult(
                f"stability:{stack_name}",
                True,
                f"no increases across {metric_count} metrics for {stability_window_seconds}s",
            )
        )
    except subprocess.CalledProcessError as exc:
        results.append(CheckResult(f"stability:{stack_name}", False, exc.stderr.strip() or exc.stdout.strip()))
    except urllib.error.URLError as exc:
        results.append(CheckResult(f"stability:{stack_name}", False, str(exc)))
    except Exception as exc:  # pragma: no cover - defensive operational path
        results.append(CheckResult(f"stability:{stack_name}", False, repr(exc)))


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
    parser.add_argument(
        "--stability-seconds",
        type=int,
        default=DEFAULT_STABILITY_WINDOW_SECONDS,
        help="How long streaming and batch validations must observe stable counts after passing all checks. Use 0 to disable.",
    )
    return parser.parse_args()


def main() -> int:
    args = parse_args()
    results: list[CheckResult] = []
    stack_name = args.stack
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
        "metadata": check_metadata,
        "dbt": check_dbt,
        "generator": check_generator,
    }
    for name, fn in sections.items():
        if name in enabled_sections and name not in args.skip:
            print(f"[validate] Running section: {name}", flush=True)
            try:
                fn(results)
            except subprocess.CalledProcessError as exc:
                detail = exc.stderr.strip() or exc.stdout.strip() or str(exc)
                results.append(CheckResult(f"{name}:section", False, detail))
            except urllib.error.URLError as exc:
                results.append(CheckResult(f"{name}:section", False, str(exc)))
            except Exception as exc:  # pragma: no cover - defensive operational path
                results.append(CheckResult(f"{name}:section", False, repr(exc)))
            print(f"[validate] Finished section: {name}", flush=True)

    if stack_name != "full" and not any(not result.ok for result in results):
        check_data_stability(
            results,
            stack_name=stack_name,
            context=context,
            enabled_sections=enabled_sections,
            stability_window_seconds=args.stability_seconds,
        )

    return render(results)


if __name__ == "__main__":
    sys.exit(main())
