from __future__ import annotations

from pyflink.table import DataTypes
from pyflink.table.udf import udf

from flink.jobs.common import (
    PROJECT_ROOT,
    build_table_environment,
    create_json_kafka_sink,
    create_raw_kafka_source_table,
    env,
    execute_sql_file,
    register_iceberg_catalog,
    session_event_extract,
)

EVENT_ICEBERG_DDL_PATH = PROJECT_ROOT / "flink" / "sql" / "event_iceberg_ddl.sql"


def main() -> None:
    table_env = build_table_environment("bronze-events-to-iceberg")
    register_iceberg_catalog(table_env)
    execute_sql_file(table_env, EVENT_ICEBERG_DDL_PATH)

    create_raw_kafka_source_table(
        table_env,
        table_name="raw_session_event",
        topic="events.session_event",
        group_id="bronze-session-event",
    )
    create_json_kafka_sink(
        table_env,
        table_name="session_event_schema_dlq",
        topic="dlq.events.session_event_schema",
    )

    decoder = udf(
        lambda payload: session_event_extract(payload, env("SCHEMA_REGISTRY_URL", "http://schema-registry:8081")),
        result_type=DataTypes.STRING(),
    )
    table_env.create_temporary_system_function("decode_session_event", decoder)

    table_env.execute_sql(
        """
        CREATE TEMPORARY VIEW decoded_session_event AS
        SELECT
            source_partition,
            source_offset,
            kafka_timestamp,
            decode_session_event(payload) AS decoded_json
        FROM raw_session_event
        """
    )

    statement_set = table_env.create_statement_set()
    statement_set.add_insert_sql(
        """
        INSERT INTO bronze_session_event_raw
        SELECT
            CAST(JSON_VALUE(decoded_json, '$.event_uuid') AS STRING) AS event_uuid,
            CAST(JSON_VALUE(decoded_json, '$.event_id') AS BIGINT) AS event_id,
            CAST(JSON_VALUE(decoded_json, '$.session_id') AS BIGINT) AS session_id,
            CAST(JSON_VALUE(decoded_json, '$.customer_id') AS BIGINT) AS customer_id,
            CAST(JSON_VALUE(decoded_json, '$.event_ts') AS TIMESTAMP(3)) AS event_ts,
            CAST(JSON_VALUE(decoded_json, '$.event_date') AS DATE) AS event_date,
            CAST(JSON_VALUE(decoded_json, '$.event_type') AS STRING) AS event_type,
            CAST(JSON_VALUE(decoded_json, '$.product_id') AS BIGINT) AS product_id,
            CAST(JSON_VALUE(decoded_json, '$.campaign_id') AS BIGINT) AS campaign_id,
            CAST(JSON_VALUE(decoded_json, '$.page_type') AS STRING) AS page_type,
            CAST(JSON_VALUE(decoded_json, '$.search_term') AS STRING) AS search_term,
            CAST(JSON_VALUE(decoded_json, '$.position_in_list') AS INT) AS position_in_list,
            CAST(JSON_VALUE(decoded_json, '$.ingest_ts') AS TIMESTAMP(3)) AS ingest_ts,
            CAST(JSON_VALUE(decoded_json, '$.ingest_date') AS DATE) AS ingest_date,
            CAST(JSON_VALUE(decoded_json, '$.producer_version') AS STRING) AS producer_version,
            CAST(JSON_VALUE(decoded_json, '$.schema_version') AS INT) AS schema_version,
            source_partition,
            source_offset
        FROM decoded_session_event
        WHERE JSON_VALUE(decoded_json, '$.error') IS NULL
        """
    )
    statement_set.add_insert_sql(
        """
        INSERT INTO session_event_schema_dlq
        SELECT
            CONCAT('events.session_event-', CAST(source_partition AS STRING), '-', CAST(source_offset AS STRING)) AS dlq_key,
            CAST(JSON_VALUE(decoded_json, '$.raw_payload') AS STRING) AS raw_payload,
            'events.session_event' AS original_topic,
            source_partition,
            source_offset,
            CAST(JSON_VALUE(decoded_json, '$.schema_subject') AS STRING) AS schema_subject,
            CAST(JSON_VALUE(decoded_json, '$.schema_id') AS INT) AS schema_id,
            CAST(JSON_VALUE(decoded_json, '$.error') AS STRING) AS failure_reason,
            kafka_timestamp,
            CURRENT_TIMESTAMP AS dlq_emitted_ts
        FROM decoded_session_event
        WHERE JSON_VALUE(decoded_json, '$.error') IS NOT NULL
        """
    )
    statement_set.execute().wait()


if __name__ == "__main__":
    main()
