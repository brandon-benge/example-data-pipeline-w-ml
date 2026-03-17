CREATE OR REPLACE VIEW iceberg.silver.dq_invalid_session_event_type AS
SELECT *
FROM iceberg.silver.silver_session_event_clean
WHERE event_type NOT IN ('product_view', 'ad_impression', 'ad_click', 'add_to_cart', 'checkout_start');

CREATE OR REPLACE VIEW iceberg.silver.dq_invalid_order_header_amounts AS
SELECT *
FROM iceberg.silver.silver_order_header
WHERE subtotal_amount < 0 OR discount_amount < 0 OR tax_amount < 0 OR total_amount < 0;

CREATE OR REPLACE VIEW iceberg.silver.dq_invalid_order_item_line_amount AS
SELECT *
FROM iceberg.silver.silver_order_item
WHERE quantity <= 0 OR ABS(line_amount - (quantity * unit_price)) > 0.01;

CREATE OR REPLACE VIEW iceberg.silver.dq_invalid_session_windows AS
SELECT *
FROM iceberg.silver.silver_customer_session
WHERE session_end_ts IS NOT NULL AND session_start_ts > session_end_ts;

CREATE OR REPLACE VIEW iceberg.silver.dq_invalid_campaign_dates AS
SELECT *
FROM iceberg.silver.silver_campaign_current
WHERE start_date > end_date;
