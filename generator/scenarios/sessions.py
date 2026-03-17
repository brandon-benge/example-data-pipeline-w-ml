from __future__ import annotations

from datetime import datetime, timedelta, timezone
from random import Random
from uuid import uuid4

from generator.config import ALLOWED_EVENT_TYPES


DEVICE_TYPES = ["mobile", "desktop", "tablet"]
CHANNELS = ["paid_search", "organic_search", "email", "social", "direct"]
REFERRERS = ["search", "social", "email", "affiliate", "direct"]
PAGE_TYPES = ["home", "search", "product", "cart", "campaign_landing"]


def generate_customer_sessions(
    rng: Random,
    customers: list[dict[str, object]],
    count: int,
    now: datetime,
) -> list[dict[str, object]]:
    rows = []
    for session_id in range(1, count + 1):
        customer = customers[rng.randrange(len(customers))]
        start_ts = now - timedelta(minutes=rng.randint(1, 60 * 72))
        end_ts = start_ts + timedelta(minutes=rng.randint(3, 45))
        rows.append(
            {
                "session_id": session_id,
                "customer_id": customer["customer_id"],
                "session_start_ts": start_ts,
                "session_end_ts": end_ts,
                "device_type": DEVICE_TYPES[rng.randrange(len(DEVICE_TYPES))],
                "channel": CHANNELS[rng.randrange(len(CHANNELS))],
                "referrer_type": REFERRERS[rng.randrange(len(REFERRERS))],
                "created_at": start_ts,
                "updated_at": end_ts,
            }
        )
    return rows


def generate_session_events(
    rng: Random,
    sessions: list[dict[str, object]],
    campaigns: list[dict[str, object]],
    products: list[dict[str, object]],
    event_count: int,
    schema_version: int,
    producer_version: str,
    now: datetime,
    duplicate_rate: float,
    late_event_rate: float,
    late_event_max_minutes: int,
) -> list[dict[str, object]]:
    def to_epoch_ms(value: datetime) -> int:
        return int(value.replace(tzinfo=timezone.utc).timestamp() * 1000)

    events: list[dict[str, object]] = []
    for event_id in range(1, event_count + 1):
        session = sessions[rng.randrange(len(sessions))]
        event_type = ALLOWED_EVENT_TYPES[rng.randrange(len(ALLOWED_EVENT_TYPES))]
        base_event_ts = session["session_start_ts"] + timedelta(seconds=rng.randint(5, 1800))
        if base_event_ts > now:
            base_event_ts = now - timedelta(seconds=rng.randint(5, 120))
        if rng.random() < late_event_rate:
            late_candidate = base_event_ts - timedelta(minutes=rng.randint(1, late_event_max_minutes))
            event_ts = max(late_candidate, session["session_start_ts"])
        else:
            event_ts = base_event_ts
        product = products[rng.randrange(len(products))]
        campaign = campaigns[rng.randrange(len(campaigns))]
        event = {
            "event_uuid": str(uuid4()),
            "event_id": event_id,
            "session_id": session["session_id"],
            "customer_id": session["customer_id"],
            "event_ts": to_epoch_ms(event_ts),
            "event_type": event_type,
            "product_id": product["product_id"] if event_type != "ad_impression" else None,
            "campaign_id": campaign["campaign_id"] if event_type in {"ad_impression", "ad_click", "product_view"} else None,
            "page_type": PAGE_TYPES[rng.randrange(len(PAGE_TYPES))],
            "search_term": f"search-{product['category']}".lower() if event_type == "product_view" else None,
            "position_in_list": rng.randint(1, 20) if event_type in {"product_view", "ad_impression"} else None,
            "ingest_ts": to_epoch_ms(now),
            "producer_version": producer_version,
            "schema_version": schema_version,
        }
        events.append(event)

    duplicate_count = min(len(events), int(len(events) * duplicate_rate))
    duplicate_rows = [dict(events[index]) for index in rng.sample(range(len(events)), k=duplicate_count)] if duplicate_count else []
    events.extend(duplicate_rows)
    return sorted(events, key=lambda row: (row["event_ts"], row["event_id"], row["event_uuid"]))
