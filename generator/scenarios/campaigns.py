from __future__ import annotations

from datetime import datetime, timedelta
from decimal import Decimal
from random import Random


CAMPAIGN_TYPES = ["sponsored_product", "display", "social"]
OBJECTIVES = ["awareness", "traffic", "conversion"]
STATUSES = ["draft", "active", "paused", "completed"]


def generate_campaigns(
    rng: Random,
    count: int,
    advertisers: list[dict[str, object]],
    now: datetime,
) -> list[dict[str, object]]:
    rows = []
    for campaign_id in range(1, count + 1):
        advertiser = advertisers[(campaign_id - 1) % len(advertisers)]
        start_date = (now - timedelta(days=rng.randint(0, 90))).date()
        end_date = start_date + timedelta(days=rng.randint(14, 120))
        created_at = datetime.combine(start_date, datetime.min.time(), tzinfo=now.tzinfo)
        rows.append(
            {
                "campaign_id": campaign_id,
                "advertiser_id": advertiser["advertiser_id"],
                "campaign_name": f"{advertiser['advertiser_name']} Campaign {campaign_id}",
                "campaign_type": CAMPAIGN_TYPES[rng.randrange(len(CAMPAIGN_TYPES))],
                "objective": OBJECTIVES[rng.randrange(len(OBJECTIVES))],
                "budget_amount": Decimal(str(round(rng.uniform(1000, 25000), 2))),
                "start_date": start_date,
                "end_date": end_date,
                "status": STATUSES[rng.randrange(len(STATUSES))],
                "created_at": created_at,
                "updated_at": created_at + timedelta(days=rng.randint(0, 40)),
            }
        )
    return rows


def generate_campaign_products(
    rng: Random,
    campaigns: list[dict[str, object]],
    products: list[dict[str, object]],
    now: datetime,
) -> list[dict[str, object]]:
    rows = []
    bridge_id = 1
    for campaign in campaigns:
        for product in rng.sample(products, k=min(len(products), rng.randint(1, 3))):
            created_at = now - timedelta(days=rng.randint(0, 60))
            rows.append(
                {
                    "campaign_product_id": bridge_id,
                    "campaign_id": campaign["campaign_id"],
                    "product_id": product["product_id"],
                    "bid_amount": Decimal(str(round(rng.uniform(0.25, 8.50), 2))),
                    "priority": rng.randint(1, 5),
                    "created_at": created_at,
                    "updated_at": created_at + timedelta(days=rng.randint(0, 10)),
                }
            )
            bridge_id += 1
    return rows
