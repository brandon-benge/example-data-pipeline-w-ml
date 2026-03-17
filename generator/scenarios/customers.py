from __future__ import annotations

from datetime import datetime, timedelta
from random import Random


FIRST_NAMES = [
    "Ava",
    "Liam",
    "Noah",
    "Emma",
    "Sophia",
    "Mia",
    "Olivia",
    "Mason",
    "Lucas",
    "Charlotte",
]

LAST_NAMES = [
    "Parker",
    "Nguyen",
    "Bennett",
    "Brooks",
    "Foster",
    "Diaz",
    "Reed",
    "Murphy",
    "Morgan",
    "Kelly",
]

CITIES = [
    ("New York", "NY", "10001"),
    ("Boston", "MA", "02108"),
    ("Chicago", "IL", "60601"),
    ("Atlanta", "GA", "30303"),
    ("Austin", "TX", "78701"),
    ("Seattle", "WA", "98101"),
    ("Denver", "CO", "80202"),
]


def generate_sales_reps(rng: Random, count: int, now: datetime) -> list[dict[str, object]]:
    teams = ["east_enterprise", "west_growth", "midmarket", "strategic"]
    regions = ["east", "west", "central", "south"]
    rows = []
    for rep_id in range(1, count + 1):
        created_at = now - timedelta(days=rng.randint(200, 900))
        rows.append(
            {
                "sales_rep_id": rep_id,
                "rep_name": f"{FIRST_NAMES[rep_id % len(FIRST_NAMES)]} {LAST_NAMES[(rep_id + 2) % len(LAST_NAMES)]}",
                "team_name": teams[rep_id % len(teams)],
                "region": regions[rep_id % len(regions)],
                "manager_name": f"{FIRST_NAMES[(rep_id + 3) % len(FIRST_NAMES)]} {LAST_NAMES[(rep_id + 5) % len(LAST_NAMES)]}",
                "status": "active",
                "created_at": created_at,
                "updated_at": created_at + timedelta(days=rng.randint(1, 120)),
            }
        )
    return rows


def generate_customers(rng: Random, count: int, now: datetime) -> list[dict[str, object]]:
    rows = []
    for customer_id in range(1, count + 1):
        first_name = FIRST_NAMES[rng.randrange(len(FIRST_NAMES))]
        last_name = LAST_NAMES[rng.randrange(len(LAST_NAMES))]
        city, state, zip_code = CITIES[rng.randrange(len(CITIES))]
        created_at = now - timedelta(days=rng.randint(30, 720))
        email_name = f"{first_name}.{last_name}.{customer_id}".lower()
        rows.append(
            {
                "customer_id": customer_id,
                "first_name": first_name,
                "last_name": last_name,
                "email": f"{email_name}@example.com",
                "phone": f"555-{customer_id % 900 + 100:03d}-{customer_id % 10000:04d}",
                "city": city,
                "state": state,
                "zip_code": zip_code,
                "status": "active" if rng.random() > 0.03 else "inactive",
                "created_at": created_at,
                "updated_at": created_at + timedelta(days=rng.randint(0, 180)),
            }
        )
    return rows
