from __future__ import annotations

from typing import Any

try:
    import psycopg
except ImportError:  # pragma: no cover - dependency guard
    psycopg = None


UPSERT_STATEMENTS = {
    "sales_rep": """
        INSERT INTO sales_rep (
            sales_rep_id, rep_name, team_name, region, manager_name, status, created_at, updated_at
        ) VALUES (
            %(sales_rep_id)s, %(rep_name)s, %(team_name)s, %(region)s, %(manager_name)s, %(status)s, %(created_at)s, %(updated_at)s
        )
        ON CONFLICT (sales_rep_id) DO UPDATE SET
            rep_name = EXCLUDED.rep_name,
            team_name = EXCLUDED.team_name,
            region = EXCLUDED.region,
            manager_name = EXCLUDED.manager_name,
            status = EXCLUDED.status,
            created_at = EXCLUDED.created_at,
            updated_at = EXCLUDED.updated_at
    """,
    "customer": """
        INSERT INTO customer (
            customer_id, first_name, last_name, email, phone, city, state, zip_code, status, created_at, updated_at
        ) VALUES (
            %(customer_id)s, %(first_name)s, %(last_name)s, %(email)s, %(phone)s, %(city)s, %(state)s, %(zip_code)s, %(status)s, %(created_at)s, %(updated_at)s
        )
        ON CONFLICT (customer_id) DO UPDATE SET
            first_name = EXCLUDED.first_name,
            last_name = EXCLUDED.last_name,
            email = EXCLUDED.email,
            phone = EXCLUDED.phone,
            city = EXCLUDED.city,
            state = EXCLUDED.state,
            zip_code = EXCLUDED.zip_code,
            status = EXCLUDED.status,
            created_at = EXCLUDED.created_at,
            updated_at = EXCLUDED.updated_at
    """,
    "advertiser": """
        INSERT INTO advertiser (
            advertiser_id, advertiser_name, industry, account_tier, region, owner_sales_rep_id, status, created_at, updated_at
        ) VALUES (
            %(advertiser_id)s, %(advertiser_name)s, %(industry)s, %(account_tier)s, %(region)s, %(owner_sales_rep_id)s, %(status)s, %(created_at)s, %(updated_at)s
        )
        ON CONFLICT (advertiser_id) DO UPDATE SET
            advertiser_name = EXCLUDED.advertiser_name,
            industry = EXCLUDED.industry,
            account_tier = EXCLUDED.account_tier,
            region = EXCLUDED.region,
            owner_sales_rep_id = EXCLUDED.owner_sales_rep_id,
            status = EXCLUDED.status,
            created_at = EXCLUDED.created_at,
            updated_at = EXCLUDED.updated_at
    """,
    "product": """
        INSERT INTO product (
            product_id, sku, product_name, brand, category, subcategory, list_price, cost, active_flag, created_at, updated_at
        ) VALUES (
            %(product_id)s, %(sku)s, %(product_name)s, %(brand)s, %(category)s, %(subcategory)s, %(list_price)s, %(cost)s, %(active_flag)s, %(created_at)s, %(updated_at)s
        )
        ON CONFLICT (product_id) DO UPDATE SET
            sku = EXCLUDED.sku,
            product_name = EXCLUDED.product_name,
            brand = EXCLUDED.brand,
            category = EXCLUDED.category,
            subcategory = EXCLUDED.subcategory,
            list_price = EXCLUDED.list_price,
            cost = EXCLUDED.cost,
            active_flag = EXCLUDED.active_flag,
            created_at = EXCLUDED.created_at,
            updated_at = EXCLUDED.updated_at
    """,
    "campaign": """
        INSERT INTO campaign (
            campaign_id, advertiser_id, campaign_name, campaign_type, objective, budget_amount, start_date, end_date, status, created_at, updated_at
        ) VALUES (
            %(campaign_id)s, %(advertiser_id)s, %(campaign_name)s, %(campaign_type)s, %(objective)s, %(budget_amount)s, %(start_date)s, %(end_date)s, %(status)s, %(created_at)s, %(updated_at)s
        )
        ON CONFLICT (campaign_id) DO UPDATE SET
            advertiser_id = EXCLUDED.advertiser_id,
            campaign_name = EXCLUDED.campaign_name,
            campaign_type = EXCLUDED.campaign_type,
            objective = EXCLUDED.objective,
            budget_amount = EXCLUDED.budget_amount,
            start_date = EXCLUDED.start_date,
            end_date = EXCLUDED.end_date,
            status = EXCLUDED.status,
            created_at = EXCLUDED.created_at,
            updated_at = EXCLUDED.updated_at
    """,
    "campaign_product": """
        INSERT INTO campaign_product (
            campaign_product_id, campaign_id, product_id, bid_amount, priority, created_at, updated_at
        ) VALUES (
            %(campaign_product_id)s, %(campaign_id)s, %(product_id)s, %(bid_amount)s, %(priority)s, %(created_at)s, %(updated_at)s
        )
        ON CONFLICT (campaign_product_id) DO UPDATE SET
            campaign_id = EXCLUDED.campaign_id,
            product_id = EXCLUDED.product_id,
            bid_amount = EXCLUDED.bid_amount,
            priority = EXCLUDED.priority,
            created_at = EXCLUDED.created_at,
            updated_at = EXCLUDED.updated_at
    """,
    "customer_session": """
        INSERT INTO customer_session (
            session_id, customer_id, session_start_ts, session_end_ts, device_type, channel, referrer_type, created_at, updated_at
        ) VALUES (
            %(session_id)s, %(customer_id)s, %(session_start_ts)s, %(session_end_ts)s, %(device_type)s, %(channel)s, %(referrer_type)s, %(created_at)s, %(updated_at)s
        )
        ON CONFLICT (session_id) DO UPDATE SET
            customer_id = EXCLUDED.customer_id,
            session_start_ts = EXCLUDED.session_start_ts,
            session_end_ts = EXCLUDED.session_end_ts,
            device_type = EXCLUDED.device_type,
            channel = EXCLUDED.channel,
            referrer_type = EXCLUDED.referrer_type,
            created_at = EXCLUDED.created_at,
            updated_at = EXCLUDED.updated_at
    """,
    "order_header": """
        INSERT INTO order_header (
            order_id, customer_id, order_ts, order_status, subtotal_amount, discount_amount, tax_amount, total_amount, payment_type, created_at, updated_at
        ) VALUES (
            %(order_id)s, %(customer_id)s, %(order_ts)s, %(order_status)s, %(subtotal_amount)s, %(discount_amount)s, %(tax_amount)s, %(total_amount)s, %(payment_type)s, %(created_at)s, %(updated_at)s
        )
        ON CONFLICT (order_id) DO UPDATE SET
            customer_id = EXCLUDED.customer_id,
            order_ts = EXCLUDED.order_ts,
            order_status = EXCLUDED.order_status,
            subtotal_amount = EXCLUDED.subtotal_amount,
            discount_amount = EXCLUDED.discount_amount,
            tax_amount = EXCLUDED.tax_amount,
            total_amount = EXCLUDED.total_amount,
            payment_type = EXCLUDED.payment_type,
            created_at = EXCLUDED.created_at,
            updated_at = EXCLUDED.updated_at
    """,
    "order_item": """
        INSERT INTO order_item (
            order_item_id, order_id, product_id, quantity, unit_price, line_amount, attributed_campaign_id, created_at, updated_at
        ) VALUES (
            %(order_item_id)s, %(order_id)s, %(product_id)s, %(quantity)s, %(unit_price)s, %(line_amount)s, %(attributed_campaign_id)s, %(created_at)s, %(updated_at)s
        )
        ON CONFLICT (order_item_id) DO UPDATE SET
            order_id = EXCLUDED.order_id,
            product_id = EXCLUDED.product_id,
            quantity = EXCLUDED.quantity,
            unit_price = EXCLUDED.unit_price,
            line_amount = EXCLUDED.line_amount,
            attributed_campaign_id = EXCLUDED.attributed_campaign_id,
            created_at = EXCLUDED.created_at,
            updated_at = EXCLUDED.updated_at
    """,
    "sales_activity": """
        INSERT INTO sales_activity (
            sales_activity_id, advertiser_id, sales_rep_id, activity_ts, activity_type, activity_outcome, notes, created_at, updated_at
        ) VALUES (
            %(sales_activity_id)s, %(advertiser_id)s, %(sales_rep_id)s, %(activity_ts)s, %(activity_type)s, %(activity_outcome)s, %(notes)s, %(created_at)s, %(updated_at)s
        )
        ON CONFLICT (sales_activity_id) DO UPDATE SET
            advertiser_id = EXCLUDED.advertiser_id,
            sales_rep_id = EXCLUDED.sales_rep_id,
            activity_ts = EXCLUDED.activity_ts,
            activity_type = EXCLUDED.activity_type,
            activity_outcome = EXCLUDED.activity_outcome,
            notes = EXCLUDED.notes,
            created_at = EXCLUDED.created_at,
            updated_at = EXCLUDED.updated_at
    """,
}


class PostgresWriter:
    def __init__(self, dsn: str) -> None:
        self.dsn = dsn

    def write_bundle(self, bundle: dict[str, list[dict[str, Any]]]) -> dict[str, int]:
        require_psycopg()
        ordered_tables = [
            "sales_rep",
            "customer",
            "advertiser",
            "product",
            "campaign",
            "campaign_product",
            "customer_session",
            "order_header",
            "order_item",
            "sales_activity",
        ]
        counts: dict[str, int] = {}
        with psycopg.connect(self.dsn) as connection:
            with connection.cursor() as cursor:
                for table_name in ordered_tables:
                    rows = bundle.get(table_name, [])
                    if not rows:
                        counts[table_name] = 0
                        continue
                    cursor.executemany(UPSERT_STATEMENTS[table_name], rows)
                    counts[table_name] = len(rows)
            connection.commit()
        return counts


def require_psycopg() -> None:
    if psycopg is None:
        raise RuntimeError("psycopg is required for Postgres output. Install generator dependencies first.")
