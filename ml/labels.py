from __future__ import annotations

from datetime import date
from typing import Any

from ml.features import as_float, as_int, parse_date, parse_timestamp


def customer_purchase_next_7d(as_of_date: date, future_order_rows: list[dict[str, Any]]) -> int:
    for order in future_order_rows:
        order_date = parse_timestamp(order["order_ts"]).date()
        if 0 < (order_date - as_of_date).days <= 7:
            return 1
    return 0


def campaign_success_flag(as_of_date: date, campaign_rows: list[dict[str, Any]], ctr_threshold: float = 0.02) -> int:
    for row in campaign_rows:
        metric_date = parse_date(row["metric_date"])
        lookahead_days = (metric_date - as_of_date).days
        if 0 < lookahead_days <= 7:
            impressions = as_int(row.get("impressions", 0))
            clicks = as_int(row.get("clicks", 0))
            ctr = (clicks / impressions) if impressions else 0.0
            if as_int(row.get("attributed_orders", 0)) > 0 or ctr >= ctr_threshold:
                return 1
    return 0


def advertiser_budget_increase_next_30d(
    as_of_date: date,
    advertiser_id: int,
    budget_history_rows: list[dict[str, Any]],
    return_delta: bool = False,
) -> int | float:
    if not budget_history_rows:
        return 0.0 if return_delta else 0

    current_budget = 0.0
    future_budget = 0.0
    for row in budget_history_rows:
        row_date = parse_date(row.get("snapshot_date", row.get("metric_date")))
        if as_int(row["advertiser_id"]) != advertiser_id:
            continue
        total_budget = as_float(row.get("budget_amount", row.get("total_budget_amount", 0.0)))
        if row_date <= as_of_date:
            current_budget = max(current_budget, total_budget)
        elif 0 < (row_date - as_of_date).days <= 30:
            future_budget = max(future_budget, total_budget)
    delta = future_budget - current_budget
    if return_delta:
        return delta
    return 1 if delta > 0 else 0
