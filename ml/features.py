from __future__ import annotations

import csv
import json
from datetime import date, datetime
from pathlib import Path
from typing import Any, Iterable


PROJECT_ROOT = Path(__file__).resolve().parents[1]
CONFIG_ROOT = PROJECT_ROOT / "config" / "features"


def load_records(path: str | Path) -> list[dict[str, Any]]:
    input_path = Path(path)
    if input_path.suffix == ".json":
        payload = json.loads(input_path.read_text(encoding="utf-8"))
        return payload if isinstance(payload, list) else payload["rows"]
    if input_path.suffix == ".jsonl":
        rows = []
        for line in input_path.read_text(encoding="utf-8").splitlines():
            if line.strip():
                rows.append(json.loads(line))
        return rows
    if input_path.suffix == ".csv":
        with input_path.open("r", encoding="utf-8", newline="") as handle:
            return list(csv.DictReader(handle))
    raise ValueError(f"Unsupported record format: {input_path}")


def write_json(path: str | Path, payload: dict[str, Any]) -> None:
    output_path = Path(path)
    output_path.parent.mkdir(parents=True, exist_ok=True)
    output_path.write_text(json.dumps(payload, indent=2, sort_keys=True), encoding="utf-8")


def write_jsonl(path: str | Path, rows: Iterable[dict[str, Any]]) -> None:
    output_path = Path(path)
    output_path.parent.mkdir(parents=True, exist_ok=True)
    with output_path.open("w", encoding="utf-8") as handle:
        for row in rows:
            handle.write(json.dumps(row, sort_keys=True))
            handle.write("\n")


def load_yaml(path: str | Path) -> Any:
    try:
        import yaml  # type: ignore

        with Path(path).open("r", encoding="utf-8") as handle:
            return yaml.safe_load(handle)
    except Exception:
        return _fallback_yaml_parse(Path(path).read_text(encoding="utf-8"))


def _fallback_yaml_parse(text: str) -> Any:
    lines: list[tuple[int, str]] = []
    for raw in text.splitlines():
        if not raw.strip() or raw.lstrip().startswith("#"):
            continue
        indent = len(raw) - len(raw.lstrip(" "))
        lines.append((indent, raw.strip()))

    def parse_block(index: int, indent: int) -> tuple[Any, int]:
        if lines[index][1].startswith("- "):
            return parse_list(index, indent)
        return parse_dict(index, indent)

    def parse_list(index: int, indent: int) -> tuple[list[Any], int]:
        result: list[Any] = []
        while index < len(lines):
            line_indent, content = lines[index]
            if line_indent != indent or not content.startswith("- "):
                break
            item_content = content[2:].strip()
            index += 1
            if not item_content:
                child, index = parse_block(index, indent + 2)
                result.append(child)
                continue
            if ":" in item_content:
                key, raw_value = item_content.split(":", 1)
                item: dict[str, Any] = {}
                raw_value = raw_value.strip()
                if raw_value:
                    item[key.strip()] = parse_scalar(raw_value)
                else:
                    child, index = parse_block(index, indent + 2)
                    item[key.strip()] = child
                while index < len(lines) and lines[index][0] > indent:
                    child_indent, child_content = lines[index]
                    if child_indent != indent + 2 or child_content.startswith("- "):
                        break
                    child_key, child_value = child_content.split(":", 1)
                    child_key = child_key.strip()
                    child_value = child_value.strip()
                    index += 1
                    if child_value:
                        item[child_key] = parse_scalar(child_value)
                    else:
                        nested, index = parse_block(index, child_indent + 2)
                        item[child_key] = nested
                result.append(item)
            else:
                result.append(parse_scalar(item_content))
        return result, index

    def parse_dict(index: int, indent: int) -> tuple[dict[str, Any], int]:
        result: dict[str, Any] = {}
        while index < len(lines):
            line_indent, content = lines[index]
            if line_indent != indent or content.startswith("- "):
                break
            key, raw_value = content.split(":", 1)
            key = key.strip()
            raw_value = raw_value.strip()
            index += 1
            if raw_value:
                result[key] = parse_scalar(raw_value)
            else:
                child, index = parse_block(index, indent + 2)
                result[key] = child
        return result, index

    def parse_scalar(raw: str) -> Any:
        value = raw.strip().strip("'").strip('"')
        if value.lower() == "true":
            return True
        if value.lower() == "false":
            return False
        if value.lower() == "null":
            return None
        if value.isdigit() or (value.startswith("-") and value[1:].isdigit()):
            return int(value)
        try:
            return float(value)
        except ValueError:
            return value

    if not lines:
        return {}
    parsed, _ = parse_block(0, lines[0][0])
    return parsed


def load_offline_feature_definitions() -> list[dict[str, Any]]:
    return load_yaml(CONFIG_ROOT / "offline_feature_defs.yaml")["features"]


def load_online_feature_definitions() -> list[dict[str, Any]]:
    return load_yaml(CONFIG_ROOT / "online_feature_defs.yaml")["features"]


def offline_feature_definition(name: str) -> dict[str, Any]:
    return next(feature for feature in load_offline_feature_definitions() if feature["name"] == name)


def parse_date(value: Any) -> date:
    if isinstance(value, date) and not isinstance(value, datetime):
        return value
    if isinstance(value, datetime):
        return value.date()
    text = str(value)
    if "T" in text:
        return datetime.fromisoformat(text.replace("Z", "+00:00")).date()
    return date.fromisoformat(text[:10])


def parse_timestamp(value: Any) -> datetime:
    if isinstance(value, datetime):
        return value
    text = str(value)
    if text.endswith("Z"):
        text = text.replace("Z", "+00:00")
    if "T" in text:
        return datetime.fromisoformat(text)
    return datetime.fromisoformat(f"{text}T00:00:00")


def as_float(value: Any, default: float = 0.0) -> float:
    if value in (None, ""):
        return default
    return float(value)


def as_int(value: Any, default: int = 0) -> int:
    if value in (None, ""):
        return default
    return int(float(value))


def tokenize_identifier(value: Any, salt: str = "local-demo-tokenization-salt") -> str:
    import hashlib

    raw = f"{salt}::{value}".encode("utf-8")
    return hashlib.sha256(raw).hexdigest()


def _window_rows(rows: list[dict[str, Any]], as_of_date: date, lookback_days: int, date_field: str) -> list[dict[str, Any]]:
    lower_bound = as_of_date.toordinal() - (lookback_days - 1)
    return [row for row in rows if lower_bound <= parse_date(row[date_field]).toordinal() <= as_of_date.toordinal()]


def build_customer_training_rows(customer_daily_rows: list[dict[str, Any]], order_rows: list[dict[str, Any]]) -> list[dict[str, Any]]:
    from ml.labels import customer_purchase_next_7d

    feature_def = offline_feature_definition("customer_purchase_features_v1")
    customer_daily_by_customer: dict[int, list[dict[str, Any]]] = {}
    orders_by_customer: dict[int, list[dict[str, Any]]] = {}

    for row in customer_daily_rows:
        customer_id = as_int(row["customer_id"])
        customer_daily_by_customer.setdefault(customer_id, []).append(row)
    for row in order_rows:
        customer_id = as_int(row["customer_id"])
        orders_by_customer.setdefault(customer_id, []).append(row)

    assembled: list[dict[str, Any]] = []
    for customer_id, daily_rows in customer_daily_by_customer.items():
        ordered_daily = sorted(daily_rows, key=lambda item: parse_date(item["metric_date"]))
        customer_orders = sorted(orders_by_customer.get(customer_id, []), key=lambda item: parse_timestamp(item["order_ts"]))
        for row in ordered_daily:
            as_of_date = parse_date(row["metric_date"])
            past_7d = _window_rows(ordered_daily, as_of_date, 7, "metric_date")
            past_30d = _window_rows(ordered_daily, as_of_date, 30, "metric_date")
            past_90d_orders = [
                order for order in customer_orders
                if 0 <= (as_of_date - parse_timestamp(order["order_ts"]).date()).days < 90
            ]
            prior_orders = [order for order in customer_orders if parse_timestamp(order["order_ts"]).date() <= as_of_date]
            last_purchase_date = parse_timestamp(prior_orders[-1]["order_ts"]).date() if prior_orders else None
            future_orders = [order for order in customer_orders if parse_timestamp(order["order_ts"]).date() > as_of_date]

            purchases_30d = sum(as_int(item.get("purchases", 0)) for item in past_30d)
            avg_order_value_90d = (
                sum(as_float(order.get("total_amount", 0.0)) for order in past_90d_orders) / len(past_90d_orders)
                if past_90d_orders
                else 0.0
            )
            days_since_last_purchase = (as_of_date - last_purchase_date).days if last_purchase_date else 9999

            assembled.append(
                {
                    "as_of_date": as_of_date.isoformat(),
                    "customer_token": tokenize_identifier(customer_id),
                    "feature_group": "customer",
                    "feature_definition_version": feature_def["name"],
                    "views_7d": sum(as_int(item.get("views", 0)) for item in past_7d),
                    "ad_clicks_7d": sum(as_int(item.get("ad_clicks", 0)) for item in past_7d),
                    "add_to_cart_7d": sum(as_int(item.get("add_to_cart", 0)) for item in past_7d),
                    "purchases_30d": purchases_30d,
                    "avg_order_value_90d": round(avg_order_value_90d, 4),
                    "days_since_last_purchase": days_since_last_purchase,
                    "label_name": feature_def["label"]["name"],
                    "label_value": customer_purchase_next_7d(as_of_date, future_orders),
                }
            )
    return sorted(assembled, key=lambda item: (item["as_of_date"], item["customer_token"]))


def build_customer_realtime_training_rows(customer_daily_rows: list[dict[str, Any]], order_rows: list[dict[str, Any]]) -> list[dict[str, Any]]:
    from ml.labels import customer_purchase_next_7d

    feature_def = offline_feature_definition("customer_purchase_realtime_features_v1")
    customer_daily_by_customer: dict[int, list[dict[str, Any]]] = {}
    orders_by_customer: dict[int, list[dict[str, Any]]] = {}

    for row in customer_daily_rows:
        customer_id = as_int(row["customer_id"])
        customer_daily_by_customer.setdefault(customer_id, []).append(row)
    for row in order_rows:
        customer_id = as_int(row["customer_id"])
        orders_by_customer.setdefault(customer_id, []).append(row)

    assembled: list[dict[str, Any]] = []
    for customer_id, daily_rows in customer_daily_by_customer.items():
        ordered_daily = sorted(daily_rows, key=lambda item: parse_date(item["metric_date"]))
        customer_orders = sorted(orders_by_customer.get(customer_id, []), key=lambda item: parse_timestamp(item["order_ts"]))
        for row in ordered_daily:
            as_of_date = parse_date(row["metric_date"])
            past_30d = _window_rows(ordered_daily, as_of_date, 30, "metric_date")
            past_90d_orders = [
                order for order in customer_orders
                if 0 <= (as_of_date - parse_timestamp(order["order_ts"]).date()).days < 90
            ]
            prior_orders = [order for order in customer_orders if parse_timestamp(order["order_ts"]).date() <= as_of_date]
            last_purchase_date = parse_timestamp(prior_orders[-1]["order_ts"]).date() if prior_orders else None
            future_orders = [order for order in customer_orders if parse_timestamp(order["order_ts"]).date() > as_of_date]

            avg_order_value_90d = (
                sum(as_float(order.get("total_amount", 0.0)) for order in past_90d_orders) / len(past_90d_orders)
                if past_90d_orders
                else 0.0
            )
            days_since_last_purchase = (as_of_date - last_purchase_date).days if last_purchase_date else 9999

            assembled.append(
                {
                    "as_of_date": as_of_date.isoformat(),
                    "customer_id": customer_id,
                    "customer_token": tokenize_identifier(customer_id),
                    "feature_group": "customer_realtime",
                    "feature_definition_version": feature_def["name"],
                    "views_1h": min(as_int(row.get("views", 0)), 1),
                    "views_24h": as_int(row.get("views", 0)),
                    "ad_clicks_24h": as_int(row.get("ad_clicks", 0)),
                    "add_to_cart_24h": as_int(row.get("add_to_cart", 0)),
                    "purchases_30d": sum(as_int(item.get("purchases", 0)) for item in past_30d),
                    "avg_order_value_90d": round(avg_order_value_90d, 4),
                    "days_since_last_purchase": days_since_last_purchase,
                    "label_name": feature_def["label"]["name"],
                    "label_value": customer_purchase_next_7d(as_of_date, future_orders),
                }
            )
    return sorted(assembled, key=lambda item: (item["as_of_date"], item["customer_token"]))


def build_campaign_feature_rows(campaign_daily_rows: list[dict[str, Any]]) -> list[dict[str, Any]]:
    from ml.labels import campaign_success_flag
    feature_def = offline_feature_definition("campaign_success_features_v1")

    campaign_by_id: dict[int, list[dict[str, Any]]] = {}
    for row in campaign_daily_rows:
        campaign_by_id.setdefault(as_int(row["campaign_id"]), []).append(row)

    assembled: list[dict[str, Any]] = []
    for campaign_id, rows in campaign_by_id.items():
        ordered = sorted(rows, key=lambda item: parse_date(item["metric_date"]))
        for row in ordered:
            as_of_date = parse_date(row["metric_date"])
            past_7d = _window_rows(ordered, as_of_date, 7, "metric_date")
            past_30d = _window_rows(ordered, as_of_date, 30, "metric_date")
            clicks_7d = sum(as_int(item.get("clicks", 0)) for item in past_7d)
            impressions_7d = sum(as_int(item.get("impressions", 0)) for item in past_7d)
            assembled.append(
                {
                    "as_of_date": as_of_date.isoformat(),
                    "entity_id": campaign_id,
                    "advertiser_id": as_int(row["advertiser_id"]),
                    "feature_group": "campaign",
                    "feature_definition_version": feature_def["name"],
                    "impressions_7d": impressions_7d,
                    "clicks_7d": clicks_7d,
                    "ctr_7d": (clicks_7d / impressions_7d) if impressions_7d else 0.0,
                    "attributed_orders_30d": sum(as_int(item.get("attributed_orders", 0)) for item in past_30d),
                    "attributed_revenue_30d": round(sum(as_float(item.get("attributed_revenue", 0.0)) for item in past_30d), 2),
                    "label_name": feature_def["label"]["name"],
                    "label_value": campaign_success_flag(as_of_date, ordered),
                }
            )
    return sorted(assembled, key=lambda item: (item["as_of_date"], item["entity_id"]))


def build_advertiser_feature_rows(
    advertiser_daily_rows: list[dict[str, Any]],
    campaign_budget_rows: list[dict[str, Any]] | None = None,
) -> list[dict[str, Any]]:
    from ml.labels import advertiser_budget_increase_next_30d
    feature_def = offline_feature_definition("advertiser_budget_features_v1")

    advertiser_by_id: dict[int, list[dict[str, Any]]] = {}
    for row in advertiser_daily_rows:
        advertiser_by_id.setdefault(as_int(row["advertiser_id"]), []).append(row)

    budget_rows_by_advertiser: dict[int, list[dict[str, Any]]] = {}
    for row in campaign_budget_rows or []:
        budget_rows_by_advertiser.setdefault(as_int(row["advertiser_id"]), []).append(row)

    assembled: list[dict[str, Any]] = []
    for advertiser_id, rows in advertiser_by_id.items():
        ordered = sorted(rows, key=lambda item: parse_date(item["metric_date"]))
        budget_rows = sorted(
            budget_rows_by_advertiser.get(advertiser_id, []),
            key=lambda item: parse_date(item.get("snapshot_date", item.get("metric_date"))),
        )
        for row in ordered:
            as_of_date = parse_date(row["metric_date"])
            past_14d = _window_rows(ordered, as_of_date, 14, "metric_date")
            past_30d = _window_rows(ordered, as_of_date, 30, "metric_date")
            assembled.append(
                {
                    "as_of_date": as_of_date.isoformat(),
                    "entity_id": advertiser_id,
                    "feature_group": "advertiser",
                    "feature_definition_version": feature_def["name"],
                    "active_campaigns_30d": max((as_int(item.get("active_campaigns", 0)) for item in past_30d), default=0),
                    "sales_contacts_14d": sum(as_int(item.get("sales_contacts", 0)) for item in past_14d),
                    "budget_delta_30d": round(
                        advertiser_budget_increase_next_30d(as_of_date, advertiser_id, budget_rows, return_delta=True),
                        2,
                    ),
                    "impressions_7d": sum(as_int(item.get("impressions", 0)) for item in _window_rows(ordered, as_of_date, 7, "metric_date")),
                    "clicks_7d": sum(as_int(item.get("clicks", 0)) for item in _window_rows(ordered, as_of_date, 7, "metric_date")),
                    "attributed_revenue_30d": round(sum(as_float(item.get("attributed_revenue", 0.0)) for item in past_30d), 2),
                    "label_name": feature_def["label"]["name"],
                    "label_value": advertiser_budget_increase_next_30d(as_of_date, advertiser_id, budget_rows),
                }
            )
    return sorted(assembled, key=lambda item: (item["as_of_date"], item["entity_id"]))
