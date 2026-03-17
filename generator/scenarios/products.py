from __future__ import annotations

from datetime import datetime, timedelta
from decimal import Decimal
from random import Random


CATEGORIES = {
    "Electronics": ["Audio", "TV", "Mobile"],
    "Home": ["Kitchen", "Furniture", "Decor"],
    "Beauty": ["Skincare", "Makeup", "Hair"],
    "Sports": ["Fitness", "Outdoor", "Cycling"],
}

BRANDS = ["Northstar", "BluePeak", "Golden Hour", "Luma", "Summit", "Cedar"]


def generate_products(rng: Random, count: int, now: datetime) -> list[dict[str, object]]:
    rows = []
    for product_id in range(1, count + 1):
        category = list(CATEGORIES.keys())[product_id % len(CATEGORIES)]
        subcategory = CATEGORIES[category][product_id % len(CATEGORIES[category])]
        list_price = Decimal(str(round(rng.uniform(9.99, 499.99), 2)))
        cost = (list_price * Decimal("0.62")).quantize(Decimal("0.01"))
        created_at = now - timedelta(days=rng.randint(20, 1000))
        rows.append(
            {
                "product_id": product_id,
                "sku": f"SKU-{product_id:06d}",
                "product_name": f"{BRANDS[product_id % len(BRANDS)]} {subcategory} {product_id}",
                "brand": BRANDS[rng.randrange(len(BRANDS))],
                "category": category,
                "subcategory": subcategory,
                "list_price": list_price,
                "cost": cost,
                "active_flag": rng.random() > 0.04,
                "created_at": created_at,
                "updated_at": created_at + timedelta(days=rng.randint(0, 180)),
            }
        )
    return rows
