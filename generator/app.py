from __future__ import annotations

import argparse
import sys
from pathlib import Path
from random import Random
from typing import Any

if __package__ in (None, ""):
    sys.path.insert(0, str(Path(__file__).resolve().parent.parent))

from generator.config import GeneratorSettings, load_settings
from generator.scenarios.advertisers import generate_advertisers
from generator.scenarios.campaigns import generate_campaign_products, generate_campaigns
from generator.scenarios.customers import generate_customers, generate_sales_reps
from generator.scenarios.orders import generate_orders_and_items
from generator.scenarios.sales_activity import generate_sales_activities
from generator.scenarios.sessions import generate_customer_sessions, generate_session_events
from generator.scenarios.products import generate_products


def build_parser() -> argparse.ArgumentParser:
    parser = argparse.ArgumentParser(description="Synthetic source data generator")
    parser.add_argument("--config", default="params.yaml", help="Path to YAML config file.")
    parser.add_argument("--mode", choices=["postgres", "kafka", "both"], default="both")
    parser.add_argument("--seed", type=int, help="Override RNG seed.")
    parser.add_argument("--customers", type=int, help="Override customer count.")
    parser.add_argument("--events-per-minute", type=int, help="Override session_event count.")
    parser.add_argument("--orders-per-hour", type=int, help="Override order count.")
    return parser


def build_source_bundle(settings: GeneratorSettings) -> tuple[dict[str, list[dict[str, Any]]], list[dict[str, Any]]]:
    rng = Random(settings.seed)
    sales_reps = generate_sales_reps(rng, settings.sales_reps, settings.generated_at)
    customers = generate_customers(rng, settings.customers, settings.generated_at)
    advertisers = generate_advertisers(rng, settings.advertisers, sales_reps, settings.generated_at)
    products = generate_products(rng, settings.products, settings.generated_at)
    campaigns = generate_campaigns(rng, settings.campaigns, advertisers, settings.generated_at)
    campaign_products = generate_campaign_products(rng, campaigns, products, settings.generated_at)
    sessions = generate_customer_sessions(rng, customers, settings.sessions, settings.generated_at)
    order_headers, order_items = generate_orders_and_items(
        rng,
        customers,
        products,
        campaigns,
        settings.orders_per_hour,
        settings.generated_at,
    )
    sales_activities = generate_sales_activities(
        rng,
        advertisers,
        sales_reps,
        settings.sales_activities,
        settings.generated_at,
    )
    events = generate_session_events(
        rng,
        sessions,
        campaigns,
        products,
        settings.events_per_minute,
        settings.schema_version,
        settings.producer_version,
        settings.generated_at,
        settings.duplicate_rate,
        settings.late_event_rate,
        settings.late_event_max_minutes,
    )

    bundle = {
        "sales_rep": sales_reps,
        "customer": customers,
        "advertiser": advertisers,
        "product": products,
        "campaign": campaigns,
        "campaign_product": campaign_products,
        "customer_session": sessions,
        "order_header": order_headers,
        "order_item": order_items,
        "sales_activity": sales_activities,
    }
    return bundle, events


def parse_args() -> argparse.Namespace:
    return build_parser().parse_args()


def main() -> None:
    args = parse_args()
    overrides = {
        key: value
        for key, value in {
            "seed": args.seed,
            "customers": args.customers,
            "events_per_minute": args.events_per_minute,
            "orders_per_hour": args.orders_per_hour,
        }.items()
        if value is not None
    }
    settings = load_settings(args.config, overrides=overrides)
    bundle, events = build_source_bundle(settings)

    if args.mode in {"postgres", "both"}:
        from generator.postgres_writer import PostgresWriter

        writer = PostgresWriter(settings.postgres_dsn)
        counts = writer.write_bundle(bundle)
        print("Postgres rows written:")
        for table_name, row_count in counts.items():
            print(f"  {table_name}: {row_count}")

    if args.mode in {"kafka", "both"}:
        from generator.kafka_event_producer import SessionEventProducer

        producer = SessionEventProducer(
            bootstrap_servers=settings.kafka_bootstrap_servers,
            schema_registry_url=settings.schema_registry_url,
            topic=settings.kafka_topic,
            schema_path=Path(__file__).parent / "schemas" / "session_event.json",
        )
        published = producer.publish(events)
        print(f"Kafka events published: {published}")

    print(
        "Generation complete:",
        f"customers={settings.customers}",
        f"orders={settings.orders_per_hour}",
        f"session_events={len(events)}",
        f"seed={settings.seed}",
    )


if __name__ == "__main__":
    main()
