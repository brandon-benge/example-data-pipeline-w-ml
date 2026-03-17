from __future__ import annotations

from datetime import datetime, timedelta
from random import Random


INDUSTRIES = ["retail", "electronics", "home", "beauty", "sports", "grocery"]
ACCOUNT_TIERS = ["emerging", "growth", "enterprise"]
REGIONS = ["east", "west", "central", "south"]


def generate_advertisers(
    rng: Random,
    count: int,
    sales_reps: list[dict[str, object]],
    now: datetime,
) -> list[dict[str, object]]:
    rows = []
    for advertiser_id in range(1, count + 1):
        created_at = now - timedelta(days=rng.randint(60, 1000))
        owner = sales_reps[(advertiser_id - 1) % len(sales_reps)]
        industry = INDUSTRIES[rng.randrange(len(INDUSTRIES))]
        rows.append(
            {
                "advertiser_id": advertiser_id,
                "advertiser_name": f"{industry.title()} Media {advertiser_id}",
                "industry": industry,
                "account_tier": ACCOUNT_TIERS[rng.randrange(len(ACCOUNT_TIERS))],
                "region": REGIONS[rng.randrange(len(REGIONS))],
                "owner_sales_rep_id": owner["sales_rep_id"],
                "status": "active" if rng.random() > 0.05 else "paused",
                "created_at": created_at,
                "updated_at": created_at + timedelta(days=rng.randint(0, 250)),
            }
        )
    return rows
