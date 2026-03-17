from __future__ import annotations

import os
from dataclasses import dataclass
from datetime import datetime
from pathlib import Path
from typing import Any


ALLOWED_EVENT_TYPES = (
    "product_view",
    "ad_impression",
    "ad_click",
    "add_to_cart",
    "checkout_start",
)


@dataclass(frozen=True)
class GeneratorSettings:
    customers: int
    events_per_minute: int
    orders_per_hour: int
    seed: int
    duplicate_rate: float
    late_event_rate: float
    late_event_max_minutes: int
    sales_reps: int
    advertisers: int
    products: int
    campaigns: int
    sessions: int
    sales_activities: int
    postgres_host: str
    postgres_port: int
    postgres_db: str
    postgres_user: str
    postgres_password: str
    kafka_bootstrap_servers: str
    kafka_topic: str
    schema_registry_url: str
    producer_version: str
    schema_version: int
    generated_at: datetime

    @property
    def postgres_dsn(self) -> str:
        return (
            f"host={self.postgres_host} port={self.postgres_port} dbname={self.postgres_db} "
            f"user={self.postgres_user} password={self.postgres_password}"
        )


def _read_yaml(path: str | Path) -> dict[str, Any]:
    config_path = Path(path)
    if not config_path.exists():
        return {}
    loaded: dict[str, Any] = {}
    with config_path.open("r", encoding="utf-8") as handle:
        for raw_line in handle:
            line = raw_line.strip()
            if not line or line.startswith("#"):
                continue
            if ":" not in line:
                raise ValueError(f"Expected key: value format in {config_path}: {raw_line.rstrip()}")
            key, value = line.split(":", 1)
            loaded[key.strip()] = value.strip().strip("'\"")
    return loaded


def _int_value(source: dict[str, Any], key: str, default: int) -> int:
    value = source.get(key, default)
    return int(value)


def _float_value(source: dict[str, Any], key: str, default: float) -> float:
    value = source.get(key, default)
    return float(value)


def load_settings(config_path: str | Path, overrides: dict[str, Any] | None = None) -> GeneratorSettings:
    raw = _read_yaml(config_path)
    overrides = overrides or {}

    customers = int(overrides.get("customers") or _int_value(raw, "customers", 10000))
    events_per_minute = int(overrides.get("events_per_minute") or _int_value(raw, "events_per_minute", 200))
    orders_per_hour = int(overrides.get("orders_per_hour") or _int_value(raw, "orders_per_hour", 100))

    sales_reps = max(4, min(12, max(1, customers // 2000)))
    advertisers = max(12, min(80, max(1, customers // 250)))
    products = max(30, min(250, max(1, customers // 120)))
    campaigns = max(advertisers, min(200, advertisers * 2))
    sessions = max(customers // 2, events_per_minute * 3)
    sales_activities = max(advertisers // 2, orders_per_hour // 4)

    return GeneratorSettings(
        customers=customers,
        events_per_minute=events_per_minute,
        orders_per_hour=orders_per_hour,
        seed=int(overrides.get("seed") or os.getenv("GENERATOR_SEED", "42")),
        duplicate_rate=_float_value(raw, "duplicate_rate", 0.05),
        late_event_rate=_float_value(raw, "late_event_rate", 0.08),
        late_event_max_minutes=_int_value(raw, "late_event_max_minutes", 90),
        sales_reps=int(overrides.get("sales_reps") or sales_reps),
        advertisers=int(overrides.get("advertisers") or advertisers),
        products=int(overrides.get("products") or products),
        campaigns=int(overrides.get("campaigns") or campaigns),
        sessions=int(overrides.get("sessions") or sessions),
        sales_activities=int(overrides.get("sales_activities") or sales_activities),
        postgres_host=os.getenv("POSTGRES_HOST", "localhost"),
        postgres_port=int(os.getenv("POSTGRES_PORT", "5432")),
        postgres_db=os.getenv("POSTGRES_DB", "app_db"),
        postgres_user=os.getenv("POSTGRES_USER", "app_user"),
        postgres_password=os.getenv("POSTGRES_PASSWORD", "app_password"),
        kafka_bootstrap_servers=os.getenv("KAFKA_BOOTSTRAP_SERVERS", "localhost:19092"),
        kafka_topic=os.getenv("KAFKA_TOPIC", "events.session_event"),
        schema_registry_url=os.getenv("SCHEMA_REGISTRY_URL", "http://localhost:8081"),
        producer_version=os.getenv("GENERATOR_PRODUCER_VERSION", "generator-v1"),
        schema_version=int(os.getenv("GENERATOR_SCHEMA_VERSION", "1")),
        generated_at=datetime.utcnow().replace(microsecond=0),
    )
