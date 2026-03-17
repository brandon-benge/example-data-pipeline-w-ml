CREATE TABLE IF NOT EXISTS iceberg.silver.silver_sales_rep_current (
    sales_rep_id BIGINT,
    rep_name STRING,
    team_name STRING,
    region STRING,
    manager_name STRING,
    status STRING,
    created_at TIMESTAMP,
    updated_at TIMESTAMP,
    source_last_change_ts TIMESTAMP,
    silver_processed_ts TIMESTAMP,
    data_owner STRING,
    sensitivity_class STRING
) USING iceberg
PARTITIONED BY (days(updated_at))
TBLPROPERTIES ('format-version'='2', 'write.format.default'='parquet');

CREATE TABLE IF NOT EXISTS iceberg.silver.silver_customer_current (
    customer_id BIGINT,
    first_name_masked STRING,
    last_name_masked STRING,
    email_masked STRING,
    phone_masked STRING,
    city STRING,
    state STRING,
    zip_code_masked STRING,
    status STRING,
    created_at TIMESTAMP,
    updated_at TIMESTAMP,
    source_last_change_ts TIMESTAMP,
    silver_processed_ts TIMESTAMP,
    data_owner STRING,
    sensitivity_class STRING
) USING iceberg
PARTITIONED BY (days(updated_at))
TBLPROPERTIES ('format-version'='2', 'write.format.default'='parquet');

CREATE TABLE IF NOT EXISTS iceberg.silver.silver_advertiser_current (
    advertiser_id BIGINT,
    advertiser_name STRING,
    industry STRING,
    account_tier STRING,
    region STRING,
    owner_sales_rep_id BIGINT,
    status STRING,
    created_at TIMESTAMP,
    updated_at TIMESTAMP,
    source_last_change_ts TIMESTAMP,
    silver_processed_ts TIMESTAMP,
    data_owner STRING,
    sensitivity_class STRING
) USING iceberg
PARTITIONED BY (days(updated_at))
TBLPROPERTIES ('format-version'='2', 'write.format.default'='parquet');

CREATE TABLE IF NOT EXISTS iceberg.silver.silver_product_current (
    product_id BIGINT,
    sku STRING,
    product_name STRING,
    brand STRING,
    category STRING,
    subcategory STRING,
    list_price DECIMAL(12,2),
    cost DECIMAL(12,2),
    active_flag BOOLEAN,
    created_at TIMESTAMP,
    updated_at TIMESTAMP,
    source_last_change_ts TIMESTAMP,
    silver_processed_ts TIMESTAMP,
    data_owner STRING,
    sensitivity_class STRING
) USING iceberg
PARTITIONED BY (days(updated_at))
TBLPROPERTIES ('format-version'='2', 'write.format.default'='parquet');

CREATE TABLE IF NOT EXISTS iceberg.silver.silver_campaign_current (
    campaign_id BIGINT,
    advertiser_id BIGINT,
    campaign_name STRING,
    campaign_type STRING,
    objective STRING,
    budget_amount DECIMAL(14,2),
    start_date DATE,
    end_date DATE,
    status STRING,
    created_at TIMESTAMP,
    updated_at TIMESTAMP,
    source_last_change_ts TIMESTAMP,
    silver_processed_ts TIMESTAMP,
    data_owner STRING,
    sensitivity_class STRING
) USING iceberg
PARTITIONED BY (days(updated_at))
TBLPROPERTIES ('format-version'='2', 'write.format.default'='parquet');

CREATE TABLE IF NOT EXISTS iceberg.silver.silver_campaign_product_current (
    campaign_product_id BIGINT,
    campaign_id BIGINT,
    product_id BIGINT,
    bid_amount DECIMAL(10,2),
    priority INT,
    created_at TIMESTAMP,
    updated_at TIMESTAMP,
    source_last_change_ts TIMESTAMP,
    silver_processed_ts TIMESTAMP,
    data_owner STRING,
    sensitivity_class STRING
) USING iceberg
PARTITIONED BY (days(updated_at))
TBLPROPERTIES ('format-version'='2', 'write.format.default'='parquet');

CREATE TABLE IF NOT EXISTS iceberg.silver.silver_customer_session (
    session_id BIGINT,
    customer_id BIGINT,
    session_start_ts TIMESTAMP,
    session_end_ts TIMESTAMP,
    device_type STRING,
    channel STRING,
    referrer_type STRING,
    created_at TIMESTAMP,
    updated_at TIMESTAMP,
    source_last_change_ts TIMESTAMP,
    silver_processed_ts TIMESTAMP
) USING iceberg
PARTITIONED BY (days(updated_at))
TBLPROPERTIES ('format-version'='2', 'write.format.default'='parquet');

CREATE TABLE IF NOT EXISTS iceberg.silver.silver_session_event_clean (
    event_uuid STRING,
    session_id BIGINT,
    customer_id BIGINT,
    event_ts TIMESTAMP,
    event_date DATE,
    event_type STRING,
    product_id BIGINT,
    campaign_id BIGINT,
    page_type STRING,
    search_term STRING,
    position_in_list INT,
    ingest_ts TIMESTAMP,
    producer_version STRING,
    schema_version INT,
    silver_processed_ts TIMESTAMP
) USING iceberg
PARTITIONED BY (event_date)
TBLPROPERTIES ('format-version'='2', 'write.format.default'='parquet');

CREATE TABLE IF NOT EXISTS iceberg.silver.silver_order_header (
    order_id BIGINT,
    customer_id BIGINT,
    order_ts TIMESTAMP,
    order_status STRING,
    subtotal_amount DECIMAL(12,2),
    discount_amount DECIMAL(12,2),
    tax_amount DECIMAL(12,2),
    total_amount DECIMAL(12,2),
    payment_type STRING,
    created_at TIMESTAMP,
    updated_at TIMESTAMP,
    source_last_change_ts TIMESTAMP,
    silver_processed_ts TIMESTAMP
) USING iceberg
PARTITIONED BY (days(order_ts))
TBLPROPERTIES ('format-version'='2', 'write.format.default'='parquet');

CREATE TABLE IF NOT EXISTS iceberg.silver.silver_order_item (
    order_item_id BIGINT,
    order_id BIGINT,
    product_id BIGINT,
    quantity INT,
    unit_price DECIMAL(12,2),
    line_amount DECIMAL(12,2),
    attributed_campaign_id BIGINT,
    created_at TIMESTAMP,
    updated_at TIMESTAMP,
    source_last_change_ts TIMESTAMP,
    silver_processed_ts TIMESTAMP
) USING iceberg
PARTITIONED BY (days(updated_at))
TBLPROPERTIES ('format-version'='2', 'write.format.default'='parquet');

CREATE TABLE IF NOT EXISTS iceberg.silver.silver_sales_activity (
    sales_activity_id BIGINT,
    advertiser_id BIGINT,
    sales_rep_id BIGINT,
    activity_ts TIMESTAMP,
    activity_type STRING,
    activity_outcome STRING,
    created_at TIMESTAMP,
    updated_at TIMESTAMP,
    source_last_change_ts TIMESTAMP,
    silver_processed_ts TIMESTAMP
) USING iceberg
PARTITIONED BY (days(activity_ts))
TBLPROPERTIES ('format-version'='2', 'write.format.default'='parquet');

CREATE TABLE IF NOT EXISTS iceberg.silver.silver_customer_daily_metrics (
    metric_date DATE,
    customer_id BIGINT,
    views BIGINT,
    ad_clicks BIGINT,
    add_to_cart BIGINT,
    checkout_starts BIGINT,
    purchases BIGINT,
    order_amount DECIMAL(18,2),
    avg_order_value DOUBLE,
    processed_ts TIMESTAMP
) USING iceberg
PARTITIONED BY (metric_date)
TBLPROPERTIES ('format-version'='2', 'write.format.default'='parquet');

CREATE TABLE IF NOT EXISTS iceberg.silver.silver_product_daily_metrics (
    metric_date DATE,
    product_id BIGINT,
    product_views BIGINT,
    add_to_cart BIGINT,
    attributed_orders BIGINT,
    attributed_revenue DECIMAL(18,2),
    processed_ts TIMESTAMP
) USING iceberg
PARTITIONED BY (metric_date)
TBLPROPERTIES ('format-version'='2', 'write.format.default'='parquet');

CREATE TABLE IF NOT EXISTS iceberg.silver.silver_campaign_daily_metrics (
    metric_date DATE,
    campaign_id BIGINT,
    advertiser_id BIGINT,
    impressions BIGINT,
    clicks BIGINT,
    attributed_orders BIGINT,
    attributed_revenue DECIMAL(18,2),
    sales_contacts BIGINT,
    processed_ts TIMESTAMP
) USING iceberg
PARTITIONED BY (metric_date)
TBLPROPERTIES ('format-version'='2', 'write.format.default'='parquet');

CREATE TABLE IF NOT EXISTS iceberg.silver.silver_advertiser_daily_metrics (
    metric_date DATE,
    advertiser_id BIGINT,
    active_campaigns BIGINT,
    sales_contacts BIGINT,
    impressions BIGINT,
    clicks BIGINT,
    attributed_orders BIGINT,
    attributed_revenue DECIMAL(18,2),
    processed_ts TIMESTAMP
) USING iceberg
PARTITIONED BY (metric_date)
TBLPROPERTIES ('format-version'='2', 'write.format.default'='parquet');

CREATE TABLE IF NOT EXISTS iceberg.silver.customer_purchase_features_v1 (
    as_of_date DATE,
    customer_token STRING,
    purchases_30d BIGINT,
    avg_order_value_30d DOUBLE,
    customer_purchase_next_7d INT,
    feature_version STRING,
    generated_ts TIMESTAMP
) USING iceberg
PARTITIONED BY (as_of_date)
TBLPROPERTIES ('format-version'='2', 'write.format.default'='parquet');

CREATE TABLE IF NOT EXISTS iceberg.silver.customer_realtime_features_v1_parity (
    as_of_ts TIMESTAMP,
    customer_id BIGINT,
    views_1h BIGINT,
    views_24h BIGINT,
    ad_clicks_24h BIGINT,
    add_to_cart_24h BIGINT,
    feature_version STRING,
    last_event_ts TIMESTAMP,
    updated_at TIMESTAMP,
    ttl_seconds INT,
    generated_ts TIMESTAMP
) USING iceberg
PARTITIONED BY (days(as_of_ts))
TBLPROPERTIES ('format-version'='2', 'write.format.default'='parquet');
