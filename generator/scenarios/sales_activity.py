from __future__ import annotations

from datetime import datetime, timedelta
from random import Random


ACTIVITY_TYPES = ["email", "call", "meeting", "demo"]
ACTIVITY_OUTCOMES = ["scheduled_follow_up", "budget_review", "renewal_risk", "won", "no_response"]


def generate_sales_activities(
    rng: Random,
    advertisers: list[dict[str, object]],
    sales_reps: list[dict[str, object]],
    count: int,
    now: datetime,
) -> list[dict[str, object]]:
    rows = []
    for activity_id in range(1, count + 1):
        advertiser = advertisers[rng.randrange(len(advertisers))]
        sales_rep = sales_reps[rng.randrange(len(sales_reps))]
        activity_ts = now - timedelta(hours=rng.randint(1, 24 * 30))
        activity_type = ACTIVITY_TYPES[rng.randrange(len(ACTIVITY_TYPES))]
        rows.append(
            {
                "sales_activity_id": activity_id,
                "advertiser_id": advertiser["advertiser_id"],
                "sales_rep_id": sales_rep["sales_rep_id"],
                "activity_ts": activity_ts,
                "activity_type": activity_type,
                "activity_outcome": ACTIVITY_OUTCOMES[rng.randrange(len(ACTIVITY_OUTCOMES))],
                "notes": f"{activity_type} touchpoint for advertiser {advertiser['advertiser_id']}",
                "created_at": activity_ts,
                "updated_at": activity_ts + timedelta(minutes=rng.randint(5, 90)),
            }
        )
    return rows
