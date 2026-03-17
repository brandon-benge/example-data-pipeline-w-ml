from __future__ import annotations

from datetime import datetime, timedelta
from decimal import Decimal
from random import Random


ORDER_STATUSES = ["placed", "paid", "shipped", "completed"]
PAYMENT_TYPES = ["card", "paypal", "wallet"]


def generate_orders_and_items(
    rng: Random,
    customers: list[dict[str, object]],
    products: list[dict[str, object]],
    campaigns: list[dict[str, object]],
    order_count: int,
    now: datetime,
) -> tuple[list[dict[str, object]], list[dict[str, object]]]:
    headers: list[dict[str, object]] = []
    items: list[dict[str, object]] = []
    item_id = 1

    for order_id in range(1, order_count + 1):
        customer = customers[rng.randrange(len(customers))]
        order_ts = now - timedelta(minutes=rng.randint(1, 60 * 24 * 14))
        item_count = rng.randint(1, 4)
        subtotal = Decimal("0.00")
        item_rows = []
        for _ in range(item_count):
            product = products[rng.randrange(len(products))]
            quantity = rng.randint(1, 3)
            unit_price = Decimal(str(product["list_price"]))
            line_amount = (unit_price * quantity).quantize(Decimal("0.01"))
            subtotal += line_amount
            item_rows.append(
                {
                    "order_item_id": item_id,
                    "order_id": order_id,
                    "product_id": product["product_id"],
                    "quantity": quantity,
                    "unit_price": unit_price,
                    "line_amount": line_amount,
                    "attributed_campaign_id": campaigns[rng.randrange(len(campaigns))]["campaign_id"] if rng.random() < 0.55 else None,
                    "created_at": order_ts,
                    "updated_at": order_ts + timedelta(minutes=rng.randint(1, 120)),
                }
            )
            item_id += 1
        discount = Decimal(str(round(float(subtotal) * rng.uniform(0.0, 0.12), 2)))
        tax = Decimal(str(round(float(max(subtotal - discount, Decimal("0.00"))) * 0.08, 2)))
        total = (subtotal - discount + tax).quantize(Decimal("0.01"))
        headers.append(
            {
                "order_id": order_id,
                "customer_id": customer["customer_id"],
                "order_ts": order_ts,
                "order_status": ORDER_STATUSES[rng.randrange(len(ORDER_STATUSES))],
                "subtotal_amount": subtotal.quantize(Decimal("0.01")),
                "discount_amount": discount,
                "tax_amount": tax,
                "total_amount": total,
                "payment_type": PAYMENT_TYPES[rng.randrange(len(PAYMENT_TYPES))],
                "created_at": order_ts,
                "updated_at": order_ts + timedelta(minutes=rng.randint(1, 180)),
            }
        )
        items.extend(item_rows)

    return headers, items
