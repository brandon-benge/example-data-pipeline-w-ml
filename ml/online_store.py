from __future__ import annotations

from datetime import datetime, timezone
from typing import Any

from ml.features import as_float, as_int, load_online_feature_definitions


def customer_redis_key(customer_id: int) -> str:
    feature_def = load_online_feature_definitions()[0]
    return feature_def["redis_key_pattern"].format(customer_id=customer_id)


def expected_customer_online_record(
    customer_id: int,
    realtime_row: dict[str, Any],
    customer_feature_row: dict[str, Any],
) -> dict[str, Any]:
    feature_def = load_online_feature_definitions()[0]
    return {
        "customer_id": customer_id,
        "views_1h": as_int(realtime_row.get("views_1h", 0)),
        "views_24h": as_int(realtime_row.get("views_24h", 0)),
        "ad_clicks_24h": as_int(realtime_row.get("ad_clicks_24h", 0)),
        "add_to_cart_24h": as_int(realtime_row.get("add_to_cart_24h", 0)),
        "purchases_30d": as_int(customer_feature_row.get("purchases_30d", 0)),
        "avg_order_value_90d": round(as_float(customer_feature_row.get("avg_order_value_90d", 0.0)), 4),
        "days_since_last_purchase": as_int(customer_feature_row.get("days_since_last_purchase", 9999)),
        "feature_version": feature_def["name"],
        "last_event_ts": realtime_row.get("last_event_ts"),
        "updated_at": realtime_row.get("updated_at", datetime.now(timezone.utc).isoformat()),
        "ttl_seconds": as_int(feature_def["ttl_seconds"]),
    }


def normalize_online_record(payload: dict[str, Any]) -> dict[str, Any]:
    return {
        "customer_id": as_int(payload["customer_id"]),
        "views_1h": as_int(payload.get("views_1h", 0)),
        "views_24h": as_int(payload.get("views_24h", 0)),
        "ad_clicks_24h": as_int(payload.get("ad_clicks_24h", 0)),
        "add_to_cart_24h": as_int(payload.get("add_to_cart_24h", 0)),
        "purchases_30d": as_int(payload.get("purchases_30d", 0)),
        "avg_order_value_90d": round(as_float(payload.get("avg_order_value_90d", 0.0)), 4),
        "days_since_last_purchase": as_int(payload.get("days_since_last_purchase", 9999)),
        "feature_version": str(payload.get("feature_version", "")),
        "last_event_ts": payload.get("last_event_ts"),
        "updated_at": payload.get("updated_at"),
        "ttl_seconds": as_int(payload.get("ttl_seconds", 0)),
    }


def compare_customer_online_record(expected: dict[str, Any], actual: dict[str, Any], tolerance: float = 1e-6) -> dict[str, dict[str, Any]]:
    normalized_actual = normalize_online_record(actual)
    mismatches: dict[str, dict[str, Any]] = {}
    for key, expected_value in expected.items():
        actual_value = normalized_actual.get(key)
        if isinstance(expected_value, float):
            if abs(expected_value - as_float(actual_value)) > tolerance:
                mismatches[key] = {"expected": expected_value, "actual": actual_value}
        elif expected_value != actual_value:
            mismatches[key] = {"expected": expected_value, "actual": actual_value}
    return mismatches


def fetch_customer_record(customer_id: int, host: str = "localhost", port: int = 6379) -> dict[str, Any]:
    try:
        import redis  # type: ignore
    except Exception as exc:  # pragma: no cover - optional dependency for local demo only
        raise RuntimeError("redis package is required to fetch online feature records") from exc

    client = redis.Redis(host=host, port=port, decode_responses=True)
    return client.hgetall(customer_redis_key(customer_id))
