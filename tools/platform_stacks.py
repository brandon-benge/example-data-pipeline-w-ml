from __future__ import annotations

from dataclasses import dataclass


LEGACY_K8S_NAMESPACE = "data-platform"
WORKLOAD_NAMESPACES: dict[str, str] = {
    "infra": "data-platform-infra",
    "ingest": "data-platform-ingest",
    "process": "data-platform-process",
    "serve": "data-platform-serve",
    "govern": "data-platform-govern",
}

SERVICE_NAMESPACE_GROUP: dict[str, str] = {
    "postgres": "infra",
    "kafka": "infra",
    "kafka-bootstrap": "infra",
    "schema-registry": "infra",
    "schema-registry-bootstrap": "infra",
    "minio": "infra",
    "minio-bootstrap": "infra",
    "iceberg-rest": "infra",
    "iceberg-cdc-bootstrap": "infra",
    "generator": "ingest",
    "kafka-connect-source": "ingest",
    "kafka-connect-source-bootstrap": "ingest",
    "kafka-connect-sinks": "ingest",
    "kafka-connect-sinks-bootstrap": "ingest",
    "flink-jobmanager": "process",
    "flink-taskmanager": "process",
    "flink-bootstrap-bronze-events": "process",
    "spark": "process",
    "spark-bootstrap": "process",
    "dbt": "process",
    "dbt-scheduler": "process",
    "trino": "serve",
    "superset": "serve",
    "metadata": "govern",
}

SERVICE_RESOURCE_KIND: dict[str, str] = {
    "postgres": "statefulset",
    "kafka": "statefulset",
    "kafka-bootstrap": "job",
    "schema-registry": "deployment",
    "schema-registry-bootstrap": "job",
    "kafka-connect-source": "deployment",
    "kafka-connect-source-bootstrap": "job",
    "minio": "statefulset",
    "minio-bootstrap": "job",
    "iceberg-rest": "deployment",
    "trino": "deployment",
    "iceberg-cdc-bootstrap": "job",
    "kafka-connect-sinks": "deployment",
    "kafka-connect-sinks-bootstrap": "job",
    "flink-jobmanager": "deployment",
    "flink-taskmanager": "deployment",
    "flink-bootstrap-bronze-events": "job",
    "spark": "deployment",
    "spark-bootstrap": "deployment",
    "dbt": "deployment",
    "dbt-scheduler": "deployment",
    "metadata": "deployment",
    "superset": "deployment",
}


@dataclass(frozen=True)
class StackDefinition:
    name: str
    description: str
    services: tuple[str, ...]
    validation_sections: tuple[str, ...]
    validation_services: tuple[str, ...]
    validation_http_endpoints: tuple[str, ...]
    validation_trino_checks: tuple[str, ...] = ()
    stop_preserve_services: tuple[str, ...] = ()


STACKS: dict[str, StackDefinition] = {
    "ingestion": StackDefinition(
        name="ingestion",
        description="Postgres CDC and event intake into Kafka plus schema management.",
        services=(
            "postgres",
            "kafka",
            "kafka-bootstrap",
            "schema-registry",
            "schema-registry-bootstrap",
            "kafka-connect-source",
            "kafka-connect-source-bootstrap",
        ),
        validation_sections=("services", "http", "connect", "kafka", "postgres", "generator"),
        validation_services=(
            "postgres",
            "kafka",
            "schema-registry",
            "kafka-connect-source",
        ),
        validation_http_endpoints=("schema_registry", "kafka_connect_source"),
        stop_preserve_services=(
            "postgres",
            "kafka",
            "kafka-bootstrap",
            "schema-registry",
            "schema-registry-bootstrap",
        ),
    ),
    "stream-processing": StackDefinition(
        name="stream-processing",
        description="Kafka-to-Bronze processing with sink connectors, Flink, and Iceberg.",
        services=(
            "postgres",
            "kafka",
            "kafka-bootstrap",
            "schema-registry",
            "schema-registry-bootstrap",
            "kafka-connect-sinks",
            "kafka-connect-sinks-bootstrap",
            "minio",
            "minio-bootstrap",
            "iceberg-rest",
            "iceberg-cdc-bootstrap",
            "flink-jobmanager",
            "flink-taskmanager",
            "flink-bootstrap-bronze-events",
            "trino",
        ),
        validation_sections=("services", "http", "connect", "kafka", "flink", "generator", "trino"),
        validation_services=(
            "postgres",
            "kafka",
            "schema-registry",
            "kafka-connect-sinks",
            "minio",
            "iceberg-rest",
            "flink-jobmanager",
            "flink-taskmanager",
            "trino",
        ),
        validation_http_endpoints=("schema_registry", "kafka_connect_sinks", "minio", "iceberg_rest", "flink", "trino"),
        validation_trino_checks=(
            "bronze_sales_rep_cdc",
            "bronze_customer_cdc",
            "bronze_advertiser_cdc",
            "bronze_product_cdc",
            "bronze_campaign_cdc",
            "bronze_campaign_product_cdc",
            "bronze_customer_session_cdc",
            "bronze_order_header_cdc",
            "bronze_order_item_cdc",
            "bronze_sales_activity_cdc",
            "bronze_session_event_raw",
        ),
        stop_preserve_services=(
            "postgres",
            "minio",
            "minio-bootstrap",
            "iceberg-rest",
            "trino",
            "metadata",
        ),
    ),
    "streaming": StackDefinition(
        name="streaming",
        description="Combined ingestion plus stream-processing flow from source systems into Bronze.",
        services=(
            "postgres",
            "kafka",
            "kafka-bootstrap",
            "schema-registry",
            "schema-registry-bootstrap",
            "kafka-connect-source",
            "kafka-connect-source-bootstrap",
            "kafka-connect-sinks",
            "kafka-connect-sinks-bootstrap",
            "minio",
            "minio-bootstrap",
            "iceberg-rest",
            "iceberg-cdc-bootstrap",
            "flink-jobmanager",
            "flink-taskmanager",
            "flink-bootstrap-bronze-events",
            "trino",
        ),
        validation_sections=("services", "http", "connect", "kafka", "flink", "postgres", "generator", "trino"),
        validation_services=(
            "postgres",
            "kafka",
            "schema-registry",
            "kafka-connect-source",
            "kafka-connect-sinks",
            "minio",
            "iceberg-rest",
            "flink-jobmanager",
            "flink-taskmanager",
            "trino",
        ),
        validation_http_endpoints=("schema_registry", "kafka_connect_source", "kafka_connect_sinks", "minio", "iceberg_rest", "flink", "trino"),
        validation_trino_checks=(
            "bronze_sales_rep_cdc",
            "bronze_customer_cdc",
            "bronze_advertiser_cdc",
            "bronze_product_cdc",
            "bronze_campaign_cdc",
            "bronze_campaign_product_cdc",
            "bronze_customer_session_cdc",
            "bronze_order_header_cdc",
            "bronze_order_item_cdc",
            "bronze_sales_activity_cdc",
            "bronze_session_event_raw",
        ),
        stop_preserve_services=(
            "postgres",
            "kafka",
            "kafka-bootstrap",
            "schema-registry",
            "schema-registry-bootstrap",
            "minio",
            "iceberg-rest",
            "trino",
        ),
    ),
    "batch": StackDefinition(
        name="batch",
        description="Spark batch processing, dbt Gold modeling, and metadata publishing.",
        services=(
            "postgres",
            "minio",
            "minio-bootstrap",
            "iceberg-rest",
            "spark",
            "spark-bootstrap",
            "dbt",
            "dbt-scheduler",
            "metadata",
            "trino",
        ),
        validation_sections=("services", "http", "metadata", "dbt", "trino"),
        validation_services=("postgres", "minio", "iceberg-rest", "spark", "spark-bootstrap", "dbt", "dbt-scheduler", "metadata", "trino"),
        validation_http_endpoints=("minio", "iceberg_rest", "spark_ui", "metadata", "trino"),
        validation_trino_checks=(
            "silver_customer_current",
            "silver_session_event_clean",
            "silver_order_header",
            "silver_customer_daily_metrics",
            "gold_dim_customer",
            "gold_fct_session_events",
            "gold_fct_orders",
            "gold_mart_customer_conversion",
            "gold_mart_campaign_performance",
            "ml_customer_purchase_features_v1",
            "ml_customer_purchase_realtime_features_v1",
            "ml_campaign_success_features_v1",
            "ml_advertiser_budget_features_v1",
        ),
        stop_preserve_services=(
            "postgres",
            "minio",
            "minio-bootstrap",
            "iceberg-rest",
            "trino",
            "metadata",
        ),
    ),
    "analytics": StackDefinition(
        name="analytics",
        description="Trino and Superset analytics layer over Iceberg and metadata.",
        services=(
            "postgres",
            "minio",
            "minio-bootstrap",
            "iceberg-rest",
            "trino",
            "superset",
            "metadata",
        ),
        validation_sections=("services", "http", "trino", "metadata"),
        validation_services=("postgres", "minio", "iceberg-rest", "trino", "superset", "metadata"),
        validation_http_endpoints=("minio", "iceberg_rest", "trino", "superset", "metadata"),
        validation_trino_checks=(
            "gold_dim_customer",
            "gold_fct_session_events",
            "gold_fct_orders",
            "gold_mart_customer_conversion",
            "gold_mart_campaign_performance",
        ),
        stop_preserve_services=(
            "postgres",
            "minio",
            "minio-bootstrap",
            "iceberg-rest",
            "trino",
            "metadata",
        ),
    ),
}


DEFAULT_VALIDATION_SECTIONS: tuple[str, ...] = (
    "services",
    "http",
    "connect",
    "kafka",
    "flink",
    "postgres",
    "trino",
    "metadata",
    "dbt",
    "generator",
)


DEFAULT_VALIDATION_SERVICES: tuple[str, ...] = (
    "postgres",
    "kafka",
    "schema-registry",
    "kafka-connect-source",
    "kafka-connect-sinks",
    "minio",
    "iceberg-rest",
    "flink-jobmanager",
    "flink-taskmanager",
    "spark",
    "spark-bootstrap",
    "trino",
    "dbt",
    "metadata",
    "superset",
)


DEFAULT_VALIDATION_HTTP_ENDPOINTS: tuple[str, ...] = (
    "schema_registry",
    "kafka_connect_source",
    "kafka_connect_sinks",
    "minio",
    "iceberg_rest",
    "trino",
    "superset",
    "flink",
    "metadata",
    "spark_ui",
)


def canonical_namespace_for_service(service: str) -> str:
    group = SERVICE_NAMESPACE_GROUP.get(service)
    if group is None:
        return LEGACY_K8S_NAMESPACE
    return WORKLOAD_NAMESPACES[group]


def namespace_candidates_for_service(service: str) -> tuple[str, ...]:
    canonical = canonical_namespace_for_service(service)
    if canonical == LEGACY_K8S_NAMESPACE:
        return (LEGACY_K8S_NAMESPACE,)
    return (canonical, LEGACY_K8S_NAMESPACE)
