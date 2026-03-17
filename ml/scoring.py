from __future__ import annotations

from datetime import datetime, timezone
from typing import Any

from ml.features import as_int
from ml.inference import latest_manifest, load_model_from_manifest, predict_one
from ml.online_store import fetch_customer_record
from ml.trino_utils import run_trino_query, sql_literal


def fetch_customer_offline_features(customer_id: int) -> dict[str, Any]:
    rows = run_trino_query(
        f"""
        WITH as_of_row AS (
            SELECT customer_id, CAST(MAX(metric_date) AS DATE) AS as_of_date
            FROM iceberg.silver.silver_customer_daily_metrics
            WHERE customer_id = {sql_literal(customer_id)}
            GROUP BY customer_id
        ),
        order_stats AS (
            SELECT
                a.customer_id,
                a.as_of_date,
                SUM(CASE WHEN DATE_DIFF('day', CAST(o.order_ts AS DATE), a.as_of_date) BETWEEN 0 AND 29 THEN 1 ELSE 0 END) AS purchases_30d,
                AVG(CASE WHEN DATE_DIFF('day', CAST(o.order_ts AS DATE), a.as_of_date) BETWEEN 0 AND 89 THEN o.total_amount END) AS avg_order_value_90d,
                COALESCE(
                    DATE_DIFF(
                        'day',
                        MAX(CASE WHEN CAST(o.order_ts AS DATE) <= a.as_of_date THEN CAST(o.order_ts AS DATE) END),
                        a.as_of_date
                    ),
                    9999
                ) AS days_since_last_purchase
            FROM as_of_row a
            LEFT JOIN iceberg.silver.silver_order_header o
                ON a.customer_id = o.customer_id
            GROUP BY a.customer_id, a.as_of_date
        ),
        parity AS (
            SELECT
                customer_id,
                views_1h,
                views_24h,
                ad_clicks_24h,
                add_to_cart_24h,
                feature_version,
                last_event_ts,
                updated_at,
                ttl_seconds,
                ROW_NUMBER() OVER (PARTITION BY customer_id ORDER BY as_of_ts DESC) AS row_num
            FROM iceberg.silver.customer_realtime_features_v1_parity
            WHERE customer_id = {sql_literal(customer_id)}
        )
        SELECT
            CAST(s.customer_id AS BIGINT) AS customer_id,
            CAST(s.as_of_date AS VARCHAR) AS as_of_date,
            COALESCE(CAST(p.views_1h AS BIGINT), 0) AS views_1h,
            COALESCE(CAST(p.views_24h AS BIGINT), 0) AS views_24h,
            COALESCE(CAST(p.ad_clicks_24h AS BIGINT), 0) AS ad_clicks_24h,
            COALESCE(CAST(p.add_to_cart_24h AS BIGINT), 0) AS add_to_cart_24h,
            COALESCE(CAST(s.purchases_30d AS BIGINT), 0) AS purchases_30d,
            COALESCE(CAST(s.avg_order_value_90d AS DOUBLE), 0.0) AS avg_order_value_90d,
            COALESCE(CAST(s.days_since_last_purchase AS INTEGER), 9999) AS days_since_last_purchase,
            p.feature_version,
            CAST(p.last_event_ts AS VARCHAR) AS last_event_ts,
            CAST(p.updated_at AS VARCHAR) AS updated_at,
            COALESCE(CAST(p.ttl_seconds AS INTEGER), 0) AS ttl_seconds
        FROM order_stats s
        LEFT JOIN parity p
            ON s.customer_id = p.customer_id AND p.row_num = 1
        """
    )
    if not rows:
        raise ValueError(f"No customer feature rows found for customer_id={customer_id}")
    return rows[0]


def fetch_campaign_offline_features(campaign_id: int) -> dict[str, Any]:
    rows = run_trino_query(
        f"""
        WITH as_of_row AS (
            SELECT campaign_id, advertiser_id, CAST(MAX(metric_date) AS DATE) AS as_of_date
            FROM iceberg.silver.silver_campaign_daily_metrics
            WHERE campaign_id = {sql_literal(campaign_id)}
            GROUP BY campaign_id, advertiser_id
        )
        SELECT
            a.campaign_id AS entity_id,
            a.advertiser_id,
            CAST(a.as_of_date AS VARCHAR) AS as_of_date,
            COALESCE(SUM(CASE WHEN DATE_DIFF('day', d.metric_date, a.as_of_date) BETWEEN 0 AND 6 THEN d.impressions ELSE 0 END), 0) AS impressions_7d,
            COALESCE(SUM(CASE WHEN DATE_DIFF('day', d.metric_date, a.as_of_date) BETWEEN 0 AND 6 THEN d.clicks ELSE 0 END), 0) AS clicks_7d,
            COALESCE(
                CAST(SUM(CASE WHEN DATE_DIFF('day', d.metric_date, a.as_of_date) BETWEEN 0 AND 6 THEN d.clicks ELSE 0 END) AS DOUBLE) /
                NULLIF(CAST(SUM(CASE WHEN DATE_DIFF('day', d.metric_date, a.as_of_date) BETWEEN 0 AND 6 THEN d.impressions ELSE 0 END) AS DOUBLE), 0.0),
                0.0
            ) AS ctr_7d,
            COALESCE(SUM(CASE WHEN DATE_DIFF('day', d.metric_date, a.as_of_date) BETWEEN 0 AND 29 THEN d.attributed_orders ELSE 0 END), 0) AS attributed_orders_30d,
            COALESCE(SUM(CASE WHEN DATE_DIFF('day', d.metric_date, a.as_of_date) BETWEEN 0 AND 29 THEN d.attributed_revenue ELSE 0.0 END), 0.0) AS attributed_revenue_30d
        FROM as_of_row a
        LEFT JOIN iceberg.silver.silver_campaign_daily_metrics d
            ON a.campaign_id = d.campaign_id
        GROUP BY a.campaign_id, a.advertiser_id, a.as_of_date
        """
    )
    if not rows:
        raise ValueError(f"No campaign feature rows found for campaign_id={campaign_id}")
    return rows[0]


def fetch_advertiser_offline_features(advertiser_id: int) -> dict[str, Any]:
    rows = run_trino_query(
        f"""
        WITH as_of_row AS (
            SELECT advertiser_id, CAST(MAX(metric_date) AS DATE) AS as_of_date
            FROM iceberg.silver.silver_advertiser_daily_metrics
            WHERE advertiser_id = {sql_literal(advertiser_id)}
            GROUP BY advertiser_id
        ),
        budget_history AS (
            SELECT
                metric_date,
                advertiser_id,
                CAST(active_campaigns * 1000 AS DOUBLE) AS total_budget_amount
            FROM iceberg.silver.silver_advertiser_daily_metrics
            WHERE advertiser_id = {sql_literal(advertiser_id)}
        )
        SELECT
            a.advertiser_id AS entity_id,
            CAST(a.as_of_date AS VARCHAR) AS as_of_date,
            COALESCE(MAX(CASE WHEN DATE_DIFF('day', d.metric_date, a.as_of_date) BETWEEN 0 AND 29 THEN d.active_campaigns ELSE 0 END), 0) AS active_campaigns_30d,
            COALESCE(SUM(CASE WHEN DATE_DIFF('day', d.metric_date, a.as_of_date) BETWEEN 0 AND 13 THEN d.sales_contacts ELSE 0 END), 0) AS sales_contacts_14d,
            COALESCE(
                MAX(CASE WHEN b.metric_date > a.as_of_date AND b.metric_date <= DATE_ADD('day', 30, a.as_of_date) THEN b.total_budget_amount END),
                0.0
            ) -
            COALESCE(MAX(CASE WHEN b.metric_date <= a.as_of_date THEN b.total_budget_amount END), 0.0) AS budget_delta_30d,
            COALESCE(SUM(CASE WHEN DATE_DIFF('day', d.metric_date, a.as_of_date) BETWEEN 0 AND 6 THEN d.impressions ELSE 0 END), 0) AS impressions_7d,
            COALESCE(SUM(CASE WHEN DATE_DIFF('day', d.metric_date, a.as_of_date) BETWEEN 0 AND 6 THEN d.clicks ELSE 0 END), 0) AS clicks_7d,
            COALESCE(SUM(CASE WHEN DATE_DIFF('day', d.metric_date, a.as_of_date) BETWEEN 0 AND 29 THEN d.attributed_revenue ELSE 0.0 END), 0.0) AS attributed_revenue_30d
        FROM as_of_row a
        LEFT JOIN iceberg.silver.silver_advertiser_daily_metrics d
            ON a.advertiser_id = d.advertiser_id
        LEFT JOIN budget_history b
            ON a.advertiser_id = b.advertiser_id
        GROUP BY a.advertiser_id, a.as_of_date
        """
    )
    if not rows:
        raise ValueError(f"No advertiser feature rows found for advertiser_id={advertiser_id}")
    return rows[0]


def build_customer_realtime_payload(customer_id: int, offline_row: dict[str, Any], redis_row: dict[str, Any]) -> dict[str, Any]:
    return {
        "customer_id": customer_id,
        "views_1h": as_int(redis_row.get("views_1h", offline_row.get("views_1h", 0))),
        "views_24h": as_int(redis_row.get("views_24h", offline_row.get("views_24h", 0))),
        "ad_clicks_24h": as_int(redis_row.get("ad_clicks_24h", offline_row.get("ad_clicks_24h", 0))),
        "add_to_cart_24h": as_int(redis_row.get("add_to_cart_24h", offline_row.get("add_to_cart_24h", 0))),
        "purchases_30d": as_int(redis_row.get("purchases_30d", offline_row.get("purchases_30d", 0))),
        "avg_order_value_90d": float(redis_row.get("avg_order_value_90d", offline_row.get("avg_order_value_90d", 0.0))),
        "days_since_last_purchase": as_int(redis_row.get("days_since_last_purchase", offline_row.get("days_since_last_purchase", 9999))),
    }


def _redis_client(host: str, port: int):
    try:
        import redis  # type: ignore
    except Exception as exc:  # pragma: no cover
        raise RuntimeError("redis package is required to write scoring outputs") from exc
    return redis.Redis(host=host, port=port, decode_responses=True)


def write_customer_score_output(
    *,
    host: str,
    port: int,
    customer_id: int,
    customer_score: float,
 ) -> str:
    client = _redis_client(host, port)
    scored_at = datetime.now(timezone.utc).isoformat()
    customer_key = f"scores:customer:{customer_id}:purchase_propensity:v1"
    customer_payload = {
        "customer_id": customer_id,
        "score_name": "customer_purchase_propensity",
        "score": round(customer_score, 6),
        "scored_at": scored_at,
        "feature_source": "redis+offline",
    }
    client.hset(customer_key, mapping={key: str(value) for key, value in customer_payload.items()})
    client.expire(customer_key, 3600)
    return customer_key


def write_campaign_score_output(*, host: str, port: int, campaign_id: int, campaign_score: float) -> str:
    client = _redis_client(host, port)
    scored_at = datetime.now(timezone.utc).isoformat()
    campaign_key = f"scores:campaign:{campaign_id}:success_propensity:v1"
    campaign_payload = {
        "campaign_id": campaign_id,
        "score_name": "campaign_success_propensity",
        "score": round(campaign_score, 6),
        "scored_at": scored_at,
        "feature_source": "offline",
    }
    client.hset(campaign_key, mapping={key: str(value) for key, value in campaign_payload.items()})
    client.expire(campaign_key, 3600)
    return campaign_key


def write_advertiser_score_output(*, host: str, port: int, advertiser_id: int, advertiser_score: float) -> str:
    client = _redis_client(host, port)
    scored_at = datetime.now(timezone.utc).isoformat()
    advertiser_key = f"scores:advertiser:{advertiser_id}:budget_expansion_propensity:v1"
    advertiser_payload = {
        "advertiser_id": advertiser_id,
        "score_name": "advertiser_budget_expansion_propensity",
        "score": round(advertiser_score, 6),
        "scored_at": scored_at,
        "feature_source": "offline",
    }
    client.hset(advertiser_key, mapping={key: str(value) for key, value in advertiser_payload.items()})
    client.expire(advertiser_key, 3600)
    return advertiser_key


def score_customer(
    *,
    customer_id: int,
    redis_host: str = "localhost",
    redis_port: int = 6379,
    customer_manifest: str | None = None,
    write_redis: bool = False,
) -> dict[str, Any]:
    customer_manifest_path = str(customer_manifest) if customer_manifest else str(latest_manifest("customer_realtime"))
    _, customer_model = load_model_from_manifest(customer_manifest_path)

    customer_offline_row = fetch_customer_offline_features(customer_id)
    customer_online_row = fetch_customer_record(customer_id, host=redis_host, port=redis_port)
    customer_payload = build_customer_realtime_payload(customer_id, customer_offline_row, customer_online_row)

    customer_score = predict_one(customer_model, customer_payload)

    result: dict[str, Any] = {
        "customer_id": customer_id,
        "score": round(customer_score, 6),
        "artifact_manifest": customer_manifest_path,
        "hydrated_offline_features": customer_offline_row,
        "online_features": customer_online_row,
        "scoring_features": customer_payload,
    }
    if write_redis:
        result["score_output_key"] = write_customer_score_output(
            host=redis_host,
            port=redis_port,
            customer_id=customer_id,
            customer_score=customer_score,
        )
    return result


def score_campaign(
    *,
    campaign_id: int,
    redis_host: str = "localhost",
    redis_port: int = 6379,
    campaign_manifest: str | None = None,
    write_redis: bool = False,
) -> dict[str, Any]:
    campaign_manifest_path = str(campaign_manifest) if campaign_manifest else str(latest_manifest("campaign"))
    _, campaign_model = load_model_from_manifest(campaign_manifest_path)
    campaign_offline_row = fetch_campaign_offline_features(campaign_id)
    campaign_score = predict_one(campaign_model, campaign_offline_row)
    result: dict[str, Any] = {
        "campaign_id": campaign_id,
        "score": round(campaign_score, 6),
        "artifact_manifest": campaign_manifest_path,
        "hydrated_offline_features": campaign_offline_row,
        "scoring_features": {
            key: value
            for key, value in campaign_offline_row.items()
            if key not in {"label_name", "label_value"}
        },
    }
    if write_redis:
        result["score_output_key"] = write_campaign_score_output(
            host=redis_host,
            port=redis_port,
            campaign_id=campaign_id,
            campaign_score=campaign_score,
        )
    return result


def score_advertiser(
    *,
    advertiser_id: int,
    redis_host: str = "localhost",
    redis_port: int = 6379,
    advertiser_manifest: str | None = None,
    write_redis: bool = False,
) -> dict[str, Any]:
    advertiser_manifest_path = str(advertiser_manifest) if advertiser_manifest else str(latest_manifest("advertiser"))
    _, advertiser_model = load_model_from_manifest(advertiser_manifest_path)
    advertiser_offline_row = fetch_advertiser_offline_features(advertiser_id)
    advertiser_score = predict_one(advertiser_model, advertiser_offline_row)
    result: dict[str, Any] = {
        "advertiser_id": advertiser_id,
        "score": round(advertiser_score, 6),
        "artifact_manifest": advertiser_manifest_path,
        "hydrated_offline_features": advertiser_offline_row,
        "scoring_features": {
            key: value
            for key, value in advertiser_offline_row.items()
            if key not in {"label_name", "label_value"}
        },
    }
    if write_redis:
        result["score_output_key"] = write_advertiser_score_output(
            host=redis_host,
            port=redis_port,
            advertiser_id=advertiser_id,
            advertiser_score=advertiser_score,
        )
    return result
