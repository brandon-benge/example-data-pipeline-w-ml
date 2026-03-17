from __future__ import annotations

import json
from collections import defaultdict
from datetime import datetime, timezone
from typing import Any

import redis
from pyflink.common import Types
from pyflink.datastream.functions import KeyedProcessFunction, RuntimeContext
from pyflink.datastream.state import ValueStateDescriptor
from pyflink.table import DataTypes
from pyflink.table.udf import udf

from flink.jobs.common import (
    build_stream_table_environment,
    create_raw_kafka_source_table,
    env,
    load_online_feature_definitions,
    session_event_extract,
)


class CustomerFeatureAggregator(KeyedProcessFunction):
    def __init__(self, feature_definition: dict[str, Any], redis_host: str, redis_port: int) -> None:
        self.feature_definition = feature_definition
        self.redis_host = redis_host
        self.redis_port = redis_port
        self.state = None
        self.redis_client = None
        self.aggregations = feature_definition["aggregations"]
        self.ttl_seconds = int(feature_definition["ttl_seconds"])
        self.redis_key_pattern = feature_definition["redis_key_pattern"]

    def open(self, runtime_context: RuntimeContext) -> None:
        self.state = runtime_context.get_state(ValueStateDescriptor("customer_feature_state", Types.STRING()))
        self.redis_client = redis.Redis(host=self.redis_host, port=self.redis_port, decode_responses=True)

    def process_element(self, value, ctx: "KeyedProcessFunction.Context"):
        current_state = json.loads(self.state.value()) if self.state.value() else {"events": []}
        event = {
            "event_uuid": value["event_uuid"],
            "event_type": value["event_type"],
            "event_ts_ms": int(value["event_ts_ms"]),
        }
        current_state["events"].append(event)
        latest_event_ts_ms = max(item["event_ts_ms"] for item in current_state["events"])
        max_window_seconds = max(int(item.get("window_seconds", 0)) for item in self.aggregations)
        lower_bound_ms = latest_event_ts_ms - (max_window_seconds * 1000)
        current_state["events"] = [item for item in current_state["events"] if item["event_ts_ms"] >= lower_bound_ms]
        self.state.update(json.dumps(current_state, separators=(",", ":")))

        features = self._compute_features(current_state["events"], latest_event_ts_ms)
        customer_id = value["customer_id"]
        redis_key = self.redis_key_pattern.format(customer_id=customer_id)
        updated_at = datetime.now(timezone.utc).isoformat()
        last_event_ts = datetime.fromtimestamp(latest_event_ts_ms / 1000.0, tz=timezone.utc).isoformat()

        redis_record = {
            "customer_id": str(customer_id),
            "feature_version": self.feature_definition["name"],
            "last_event_ts": last_event_ts,
            "updated_at": updated_at,
            "ttl_seconds": str(self.ttl_seconds),
        }
        for key, value_ in features.items():
            redis_record[key] = str(value_)

        with self.redis_client.pipeline(transaction=False) as pipeline:
            pipeline.hset(redis_key, mapping=redis_record)
            pipeline.expire(redis_key, self.ttl_seconds)
            pipeline.execute()
        yield redis_record

    def _compute_features(self, events: list[dict[str, Any]], latest_event_ts_ms: int) -> dict[str, int]:
        features = defaultdict(int)
        for aggregation in self.aggregations:
            window_seconds = int(aggregation["window_seconds"])
            threshold_ms = latest_event_ts_ms - (window_seconds * 1000)
            when_type = aggregation.get("when", {}).get("event_type")
            eligible = [
                event
                for event in events
                if event["event_ts_ms"] >= threshold_ms and (when_type is None or event["event_type"] == when_type)
            ]
            if aggregation["function"] == "count_distinct":
                features[aggregation["as"]] = len({event["event_uuid"] for event in eligible})
        return dict(features)


def main() -> None:
    stream_env, table_env = build_stream_table_environment("online-features-to-redis")
    create_raw_kafka_source_table(
        table_env,
        table_name="raw_session_event_features",
        topic="events.session_event",
        group_id="online-features-session-event",
    )

    decoder = udf(
        lambda payload: session_event_extract(payload, env("SCHEMA_REGISTRY_URL", "http://schema-registry:8081")),
        result_type=DataTypes.STRING(),
    )
    table_env.create_temporary_system_function("decode_session_event", decoder)
    table_env.execute_sql(
        """
        CREATE TEMPORARY VIEW valid_session_events AS
        SELECT
            CAST(JSON_VALUE(decoded_json, '$.event_uuid') AS STRING) AS event_uuid,
            CAST(JSON_VALUE(decoded_json, '$.customer_id') AS BIGINT) AS customer_id,
            CAST(JSON_VALUE(decoded_json, '$.event_type') AS STRING) AS event_type,
            CAST(JSON_VALUE(decoded_json, '$.event_ts_ms') AS BIGINT) AS event_ts_ms
        FROM (
            SELECT decode_session_event(payload) AS decoded_json
            FROM raw_session_event_features
        )
        WHERE JSON_VALUE(decoded_json, '$.error') IS NULL
        """
    )

    feature_definition = load_online_feature_definitions()[0]
    allowed_event_types = tuple(feature_definition["sources"][0]["filters"]["event_type"])
    valid_events_table = table_env.sql_query(
        f"""
        SELECT *
        FROM valid_session_events
        WHERE event_type IN ({', '.join(repr(item) for item in allowed_event_types)})
        """
    )
    valid_events_stream = table_env.to_data_stream(valid_events_table)
    keyed_stream = valid_events_stream.key_by(lambda row: row["customer_id"], key_type=Types.LONG())
    sink_stream = keyed_stream.process(
        CustomerFeatureAggregator(
            feature_definition=feature_definition,
            redis_host=env("REDIS_HOST", "redis"),
            redis_port=int(env("REDIS_PORT", "6379")),
        ),
        output_type=Types.MAP(Types.STRING(), Types.STRING()),
    )
    if env("FLINK_DEBUG_PRINT", "false").lower() == "true":
        sink_stream.print()
    stream_env.execute("online-features-to-redis")


if __name__ == "__main__":
    main()
