from __future__ import annotations

from datetime import datetime, timedelta, timezone
from random import Random
from uuid import uuid4


DEVICE_TYPES = ["mobile", "desktop", "tablet"]
CHANNEL_WEIGHTS = {
    "paid_search": 0.24,
    "organic_search": 0.26,
    "email": 0.16,
    "social": 0.18,
    "direct": 0.16,
}
CONVERSION_STAGE_THRESHOLDS = (
    ("purchase", 0.20),
    ("checkout", 0.36),
    ("cart", 0.56),
    ("click", 0.78),
    ("view", 1.00),
)


def _weighted_choice(rng: Random, weighted_values: dict[str, float]) -> str:
    selected = rng.random()
    running = 0.0
    for value, weight in weighted_values.items():
        running += weight
        if selected <= running:
            return value
    return next(reversed(weighted_values))


def _pick_conversion_stage(rng: Random) -> str:
    selected = rng.random()
    for stage, threshold in CONVERSION_STAGE_THRESHOLDS:
        if selected <= threshold:
            return stage
    return "view"


def _event_ts_to_epoch_ms(value: datetime) -> int:
    return int(value.replace(tzinfo=timezone.utc).timestamp() * 1000)


def generate_customer_sessions(
    rng: Random,
    customers: list[dict[str, object]],
    count: int,
    now: datetime,
) -> list[dict[str, object]]:
    rows: list[dict[str, object]] = []
    historical_days = 28
    for session_id in range(1, count + 1):
        customer = customers[rng.randrange(len(customers))]
        day_offset = (session_id - 1) % historical_days
        minutes_into_day = rng.randint(30, (60 * 24) - 90)
        start_ts = now.replace(hour=0, minute=0, second=0, microsecond=0) - timedelta(days=day_offset)
        start_ts = start_ts + timedelta(minutes=minutes_into_day)
        end_ts = start_ts + timedelta(minutes=rng.randint(3, 45))
        session_day_end = start_ts.replace(hour=23, minute=59, second=0, microsecond=0)
        if end_ts > session_day_end:
            end_ts = session_day_end
        channel = _weighted_choice(rng, CHANNEL_WEIGHTS)
        rows.append(
            {
                "session_id": session_id,
                "customer_id": customer["customer_id"],
                "session_start_ts": start_ts,
                "session_end_ts": end_ts,
                "device_type": DEVICE_TYPES[rng.randrange(len(DEVICE_TYPES))],
                "channel": channel,
                "referrer_type": channel.replace("_search", "") if channel != "direct" else "direct",
                "created_at": start_ts,
                "updated_at": end_ts,
            }
        )
    return rows


def build_session_plans(
    rng: Random,
    sessions: list[dict[str, object]],
    campaigns: list[dict[str, object]],
    products: list[dict[str, object]],
    campaign_products: list[dict[str, object]],
) -> dict[int, dict[str, object]]:
    products_by_id = {int(product["product_id"]): product for product in products}
    campaign_products_by_campaign: dict[int, list[int]] = {}
    for bridge in campaign_products:
        campaign_products_by_campaign.setdefault(int(bridge["campaign_id"]), []).append(int(bridge["product_id"]))

    session_plans: dict[int, dict[str, object]] = {}
    for session in sessions:
        channel = str(session["channel"])
        campaign_id = None
        if channel in {"paid_search", "email", "social"}:
            campaign_id = int(campaigns[rng.randrange(len(campaigns))]["campaign_id"])

        campaign_product_ids = campaign_products_by_campaign.get(campaign_id or -1, [])
        if campaign_product_ids:
            primary_product_id = campaign_product_ids[rng.randrange(len(campaign_product_ids))]
        else:
            primary_product_id = int(products[rng.randrange(len(products))]["product_id"])

        category = str(products_by_id[primary_product_id]["category"])
        sibling_product_ids = [
            int(product["product_id"])
            for product in products
            if str(product["category"]) == category and int(product["product_id"]) != primary_product_id
        ]

        session_plans[int(session["session_id"])] = {
            "session_id": int(session["session_id"]),
            "customer_id": int(session["customer_id"]),
            "session_start_ts": session["session_start_ts"],
            "session_end_ts": session["session_end_ts"],
            "channel": channel,
            "campaign_id": campaign_id,
            "primary_product_id": primary_product_id,
            "secondary_product_ids": rng.sample(
                sibling_product_ids,
                k=min(len(sibling_product_ids), rng.randint(0, 2)),
            ),
            "conversion_stage": _pick_conversion_stage(rng),
            "planned_orders": 0,
        }

    return session_plans


def generate_session_events(
    rng: Random,
    session_plans: dict[int, dict[str, object]],
    event_count: int,
    schema_version: int,
    producer_version: str,
    now: datetime,
    duplicate_rate: float,
    late_event_rate: float,
    late_event_max_minutes: int,
) -> list[dict[str, object]]:
    def build_event(
        event_id: int,
        session_plan: dict[str, object],
        event_ts: datetime,
        event_type: str,
        product_id: int | None,
        campaign_id: int | None,
        page_type: str,
        position_in_list: int | None,
        search_term: str | None,
    ) -> dict[str, object]:
        actual_event_ts = event_ts
        if rng.random() < late_event_rate:
            late_candidate = actual_event_ts - timedelta(minutes=rng.randint(1, late_event_max_minutes))
            actual_event_ts = max(late_candidate, session_plan["session_start_ts"])
        return {
            "event_uuid": str(uuid4()),
            "event_id": event_id,
            "session_id": session_plan["session_id"],
            "customer_id": session_plan["customer_id"],
            "event_ts": _event_ts_to_epoch_ms(actual_event_ts),
            "event_type": event_type,
            "product_id": product_id,
            "campaign_id": campaign_id,
            "page_type": page_type,
            "search_term": search_term,
            "position_in_list": position_in_list,
            "ingest_ts": _event_ts_to_epoch_ms(now),
            "producer_version": producer_version,
            "schema_version": schema_version,
        }

    selected_plans = list(session_plans.values())
    rng.shuffle(selected_plans)
    events: list[dict[str, object]] = []
    event_id = 1

    for session_plan in selected_plans:
        base_ts = session_plan["session_start_ts"] + timedelta(seconds=rng.randint(15, 90))
        session_day_end = session_plan["session_start_ts"].replace(hour=23, minute=59, second=0, microsecond=0)
        event_specs: list[tuple[str, int | None, int | None, str, int | None, str | None, int]] = []
        planned_orders = int(session_plan.get("planned_orders", 0))
        effective_stage = "purchase" if planned_orders > 0 else str(session_plan["conversion_stage"])
        required_funnel_events = max(planned_orders, 1)

        if session_plan["campaign_id"] is not None:
            impression_count = max(1, required_funnel_events if effective_stage == "purchase" else 1)
            for impression_index in range(impression_count):
                event_specs.append(
                    (
                        "ad_impression",
                        None,
                        int(session_plan["campaign_id"]),
                        "campaign_landing",
                        rng.randint(1, 8),
                        None,
                        20 + (impression_index * 15),
                    )
                )
        if effective_stage in {"click", "cart", "checkout", "purchase"} or rng.random() < 0.45:
            click_count = max(1, required_funnel_events if planned_orders > 0 else 1)
            for click_index in range(click_count):
                event_specs.append(
                    (
                        "ad_click",
                        int(session_plan["primary_product_id"]),
                        int(session_plan["campaign_id"]) if session_plan["campaign_id"] is not None else None,
                        "campaign_landing" if session_plan["campaign_id"] is not None else "product",
                        None,
                        None,
                        45 + (click_index * 20),
                    )
                )

        product_view_count = max(required_funnel_events, 1) if planned_orders > 0 else (1 if effective_stage == "view" else rng.randint(1, 3))
        for _ in range(product_view_count):
            product_id = int(session_plan["primary_product_id"])
            if session_plan["secondary_product_ids"] and rng.random() < 0.35:
                product_id = int(
                    session_plan["secondary_product_ids"][rng.randrange(len(session_plan["secondary_product_ids"]))]
                )
            event_specs.append(
                (
                    "product_view",
                    product_id,
                    int(session_plan["campaign_id"]) if session_plan["campaign_id"] is not None and rng.random() < 0.65 else None,
                    "product",
                    rng.randint(1, 20),
                    "search-product",
                    70 + rng.randint(0, 50),
                )
            )

        if effective_stage in {"cart", "checkout", "purchase"}:
            cart_count = max(required_funnel_events, 1) if planned_orders > 0 else 1
            for cart_index in range(cart_count):
                event_specs.append(
                    ("add_to_cart", int(session_plan["primary_product_id"]), None, "cart", None, None, 140 + (cart_index * 25))
                )
        if effective_stage in {"checkout", "purchase"}:
            checkout_count = max(required_funnel_events, 1) if planned_orders > 0 else 1
            for checkout_index in range(checkout_count):
                event_specs.append(
                    ("checkout_start", int(session_plan["primary_product_id"]), None, "cart", None, None, 220 + (checkout_index * 25))
                )

        for event_type, product_id, campaign_id, page_type, position_in_list, search_term, seconds_after in event_specs:
            event_ts = base_ts + timedelta(seconds=seconds_after)
            if event_ts > session_day_end:
                event_ts = session_day_end
            if event_ts > now:
                event_ts = now - timedelta(seconds=rng.randint(10, 90))
            events.append(
                build_event(
                    event_id,
                    session_plan,
                    event_ts,
                    event_type,
                    product_id,
                    campaign_id,
                    page_type,
                    position_in_list,
                    search_term,
                )
            )
            event_id += 1

    while len(events) < event_count:
        session_plan = selected_plans[rng.randrange(len(selected_plans))]
        event_type = "ad_impression" if session_plan["campaign_id"] is not None and rng.random() < 0.3 else "product_view"
        product_id = None if event_type == "ad_impression" else int(session_plan["primary_product_id"])
        campaign_id = (
            int(session_plan["campaign_id"])
            if session_plan["campaign_id"] is not None and event_type in {"ad_impression", "product_view"}
            else None
        )
        event_ts = session_plan["session_start_ts"] + timedelta(seconds=rng.randint(60, 1500))
        if event_ts > now:
            event_ts = now - timedelta(seconds=rng.randint(5, 60))
        events.append(
            build_event(
                event_id,
                session_plan,
                event_ts,
                event_type,
                product_id,
                campaign_id,
                "campaign_landing" if event_type == "ad_impression" else "product",
                rng.randint(1, 20) if event_type in {"ad_impression", "product_view"} else None,
                "search-product" if event_type == "product_view" else None,
            )
        )
        event_id += 1

    duplicate_count = min(len(events), int(len(events) * duplicate_rate))
    duplicate_rows = [dict(events[index]) for index in rng.sample(range(len(events)), k=duplicate_count)] if duplicate_count else []
    events.extend(duplicate_rows)
    return sorted(events, key=lambda row: (row["event_ts"], row["event_id"], row["event_uuid"]))
