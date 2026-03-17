CREATE OR REPLACE VIEW iceberg.bronze.v_bronze_customer_cdc_current_candidates AS
SELECT *
FROM iceberg.bronze.bronze_customer_cdc
WHERE is_tombstone = false;

CREATE OR REPLACE VIEW iceberg.bronze.v_bronze_campaign_cdc_current_candidates AS
SELECT *
FROM iceberg.bronze.bronze_campaign_cdc
WHERE is_tombstone = false;

CREATE OR REPLACE VIEW iceberg.bronze.v_bronze_session_event_raw_valid AS
SELECT *
FROM iceberg.bronze.bronze_session_event_raw
WHERE event_type IN ('product_view', 'ad_impression', 'ad_click', 'add_to_cart', 'checkout_start');
