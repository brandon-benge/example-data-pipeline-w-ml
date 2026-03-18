from __future__ import annotations

from datetime import datetime, timedelta
from decimal import Decimal
from random import Random


ORDER_STATUSES = ["placed", "paid", "shipped", "completed"]
PAYMENT_TYPES = ["card", "paypal", "wallet"]
HIGH_INTENT_STAGES = {"cart", "checkout", "purchase"}


def generate_orders_and_items(
    rng: Random,
    customers: list[dict[str, object]],
    products: list[dict[str, object]],
    campaigns: list[dict[str, object]],
    session_plans: dict[int, dict[str, object]],
    order_count: int,
    now: datetime,
) -> tuple[list[dict[str, object]], list[dict[str, object]]]:
    products_by_id = {int(product["product_id"]): product for product in products}
    session_plan_rows = list(session_plans.values())
    high_intent_plans = [plan for plan in session_plan_rows if str(plan["conversion_stage"]) in HIGH_INTENT_STAGES]
    attributed_campaign_ids = {int(campaign["campaign_id"]) for campaign in campaigns}

    headers: list[dict[str, object]] = []
    items: list[dict[str, object]] = []
    item_id = 1

    for order_id in range(1, order_count + 1):
        if not high_intent_plans:
            raise ValueError("Expected at least one high-intent session plan for order generation.")

        session_plan = high_intent_plans[rng.randrange(len(high_intent_plans))]
        customer_id = int(session_plan["customer_id"])

        # Keep order timestamps on the same session day so Silver daily metrics
        # do not show purchases with no matching funnel activity for that day.
        order_ts = session_plan["session_end_ts"] + timedelta(minutes=rng.randint(5, 180))
        session_day_end = session_plan["session_start_ts"].replace(hour=23, minute=55, second=0, microsecond=0)
        if order_ts > session_day_end:
            order_ts = session_day_end - timedelta(minutes=rng.randint(0, 45))
        if order_ts <= session_plan["session_start_ts"]:
            order_ts = min(
                session_day_end,
                session_plan["session_start_ts"] + timedelta(seconds=rng.randint(30, 240)),
            )
        if order_ts > now:
            order_ts = now - timedelta(minutes=rng.randint(1, 60))

        primary_product_id = int(session_plan["primary_product_id"])
        candidate_product_ids = [primary_product_id] + [
            int(product_id) for product_id in session_plan["secondary_product_ids"]
        ]
        campaign_id = int(session_plan["campaign_id"]) if session_plan["campaign_id"] is not None else None
        session_plan["planned_orders"] = int(session_plan.get("planned_orders", 0)) + 1

        item_count = rng.randint(1, 4)
        subtotal = Decimal("0.00")
        item_rows: list[dict[str, object]] = []
        for item_offset in range(item_count):
            if item_offset < len(candidate_product_ids):
                product_id = int(candidate_product_ids[item_offset])
            else:
                product_id = int(products[rng.randrange(len(products))]["product_id"])
            product = products_by_id[product_id]
            quantity = rng.randint(1, 3)
            unit_price = Decimal(str(product["list_price"]))
            line_amount = (unit_price * quantity).quantize(Decimal("0.01"))
            subtotal += line_amount
            item_rows.append(
                {
                    "order_item_id": item_id,
                    "order_id": order_id,
                    "product_id": product_id,
                    "quantity": quantity,
                    "unit_price": unit_price,
                    "line_amount": line_amount,
                    "attributed_campaign_id": campaign_id if campaign_id in attributed_campaign_ids and rng.random() < 0.88 else None,
                    "created_at": order_ts,
                    "updated_at": order_ts + timedelta(minutes=rng.randint(5, 150)),
                }
            )
            item_id += 1

        discount = Decimal(str(round(float(subtotal) * rng.uniform(0.0, 0.10), 2)))
        taxable_subtotal = max(subtotal - discount, Decimal("0.00"))
        tax = Decimal(str(round(float(taxable_subtotal) * 0.08, 2)))
        total = (subtotal - discount + tax).quantize(Decimal("0.01"))
        headers.append(
            {
                "order_id": order_id,
                "customer_id": customer_id,
                "order_ts": order_ts,
                "order_status": ORDER_STATUSES[rng.randrange(len(ORDER_STATUSES))],
                "subtotal_amount": subtotal.quantize(Decimal("0.01")),
                "discount_amount": discount,
                "tax_amount": tax,
                "total_amount": total,
                "payment_type": PAYMENT_TYPES[rng.randrange(len(PAYMENT_TYPES))],
                "created_at": order_ts,
                "updated_at": order_ts + timedelta(minutes=rng.randint(15, 240)),
            }
        )
        items.extend(item_rows)

    return headers, items
