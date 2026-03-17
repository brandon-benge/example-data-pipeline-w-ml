CREATE TABLE IF NOT EXISTS bronze_sales_rep_cdc (
    record_key STRING,
    source_table STRING,
    op STRING,
    source_ts_ms BIGINT,
    source_ts TIMESTAMP(3),
    ingest_ts TIMESTAMP(3),
    ingest_date DATE,
    event_uuid STRING,
    schema_version INT,
    source_partition INT,
    source_offset BIGINT,
    transaction_id STRING,
    is_tombstone BOOLEAN,
    payload_sales_rep_id BIGINT,
    payload_rep_name STRING,
    payload_team_name STRING,
    payload_region STRING,
    payload_manager_name STRING,
    payload_status STRING,
    payload_created_at TIMESTAMP(3),
    payload_updated_at TIMESTAMP(3)
) PARTITIONED BY (ingest_date)
WITH (
    'format-version' = '2',
    'write.format.default' = 'parquet'
);

CREATE TABLE IF NOT EXISTS bronze_customer_cdc (
    record_key STRING,
    source_table STRING,
    op STRING,
    source_ts_ms BIGINT,
    source_ts TIMESTAMP(3),
    ingest_ts TIMESTAMP(3),
    ingest_date DATE,
    event_uuid STRING,
    schema_version INT,
    source_partition INT,
    source_offset BIGINT,
    transaction_id STRING,
    is_tombstone BOOLEAN,
    payload_customer_id BIGINT,
    payload_first_name STRING,
    payload_last_name STRING,
    payload_email STRING,
    payload_phone STRING,
    payload_city STRING,
    payload_state STRING,
    payload_zip_code STRING,
    payload_status STRING,
    payload_created_at TIMESTAMP(3),
    payload_updated_at TIMESTAMP(3)
) PARTITIONED BY (ingest_date)
WITH (
    'format-version' = '2',
    'write.format.default' = 'parquet'
);

CREATE TABLE IF NOT EXISTS bronze_advertiser_cdc (
    record_key STRING,
    source_table STRING,
    op STRING,
    source_ts_ms BIGINT,
    source_ts TIMESTAMP(3),
    ingest_ts TIMESTAMP(3),
    ingest_date DATE,
    event_uuid STRING,
    schema_version INT,
    source_partition INT,
    source_offset BIGINT,
    transaction_id STRING,
    is_tombstone BOOLEAN,
    payload_advertiser_id BIGINT,
    payload_advertiser_name STRING,
    payload_industry STRING,
    payload_account_tier STRING,
    payload_region STRING,
    payload_owner_sales_rep_id BIGINT,
    payload_status STRING,
    payload_created_at TIMESTAMP(3),
    payload_updated_at TIMESTAMP(3)
) PARTITIONED BY (ingest_date)
WITH (
    'format-version' = '2',
    'write.format.default' = 'parquet'
);

CREATE TABLE IF NOT EXISTS bronze_product_cdc (
    record_key STRING,
    source_table STRING,
    op STRING,
    source_ts_ms BIGINT,
    source_ts TIMESTAMP(3),
    ingest_ts TIMESTAMP(3),
    ingest_date DATE,
    event_uuid STRING,
    schema_version INT,
    source_partition INT,
    source_offset BIGINT,
    transaction_id STRING,
    is_tombstone BOOLEAN,
    payload_product_id BIGINT,
    payload_sku STRING,
    payload_product_name STRING,
    payload_brand STRING,
    payload_category STRING,
    payload_subcategory STRING,
    payload_list_price DECIMAL(12,2),
    payload_cost DECIMAL(12,2),
    payload_active_flag BOOLEAN,
    payload_created_at TIMESTAMP(3),
    payload_updated_at TIMESTAMP(3)
) PARTITIONED BY (ingest_date)
WITH (
    'format-version' = '2',
    'write.format.default' = 'parquet'
);

CREATE TABLE IF NOT EXISTS bronze_campaign_cdc (
    record_key STRING,
    source_table STRING,
    op STRING,
    source_ts_ms BIGINT,
    source_ts TIMESTAMP(3),
    ingest_ts TIMESTAMP(3),
    ingest_date DATE,
    event_uuid STRING,
    schema_version INT,
    source_partition INT,
    source_offset BIGINT,
    transaction_id STRING,
    is_tombstone BOOLEAN,
    payload_campaign_id BIGINT,
    payload_advertiser_id BIGINT,
    payload_campaign_name STRING,
    payload_campaign_type STRING,
    payload_objective STRING,
    payload_budget_amount DECIMAL(14,2),
    payload_start_date DATE,
    payload_end_date DATE,
    payload_status STRING,
    payload_created_at TIMESTAMP(3),
    payload_updated_at TIMESTAMP(3)
) PARTITIONED BY (ingest_date)
WITH (
    'format-version' = '2',
    'write.format.default' = 'parquet'
);

CREATE TABLE IF NOT EXISTS bronze_campaign_product_cdc (
    record_key STRING,
    source_table STRING,
    op STRING,
    source_ts_ms BIGINT,
    source_ts TIMESTAMP(3),
    ingest_ts TIMESTAMP(3),
    ingest_date DATE,
    event_uuid STRING,
    schema_version INT,
    source_partition INT,
    source_offset BIGINT,
    transaction_id STRING,
    is_tombstone BOOLEAN,
    payload_campaign_product_id BIGINT,
    payload_campaign_id BIGINT,
    payload_product_id BIGINT,
    payload_bid_amount DECIMAL(10,2),
    payload_priority INT,
    payload_created_at TIMESTAMP(3),
    payload_updated_at TIMESTAMP(3)
) PARTITIONED BY (ingest_date)
WITH (
    'format-version' = '2',
    'write.format.default' = 'parquet'
);

CREATE TABLE IF NOT EXISTS bronze_customer_session_cdc (
    record_key STRING,
    source_table STRING,
    op STRING,
    source_ts_ms BIGINT,
    source_ts TIMESTAMP(3),
    ingest_ts TIMESTAMP(3),
    ingest_date DATE,
    event_uuid STRING,
    schema_version INT,
    source_partition INT,
    source_offset BIGINT,
    transaction_id STRING,
    is_tombstone BOOLEAN,
    payload_session_id BIGINT,
    payload_customer_id BIGINT,
    payload_session_start_ts TIMESTAMP(3),
    payload_session_end_ts TIMESTAMP(3),
    payload_device_type STRING,
    payload_channel STRING,
    payload_referrer_type STRING,
    payload_created_at TIMESTAMP(3),
    payload_updated_at TIMESTAMP(3)
) PARTITIONED BY (ingest_date)
WITH (
    'format-version' = '2',
    'write.format.default' = 'parquet'
);

CREATE TABLE IF NOT EXISTS bronze_order_header_cdc (
    record_key STRING,
    source_table STRING,
    op STRING,
    source_ts_ms BIGINT,
    source_ts TIMESTAMP(3),
    ingest_ts TIMESTAMP(3),
    ingest_date DATE,
    event_uuid STRING,
    schema_version INT,
    source_partition INT,
    source_offset BIGINT,
    transaction_id STRING,
    is_tombstone BOOLEAN,
    payload_order_id BIGINT,
    payload_customer_id BIGINT,
    payload_order_ts TIMESTAMP(3),
    payload_order_status STRING,
    payload_subtotal_amount DECIMAL(12,2),
    payload_discount_amount DECIMAL(12,2),
    payload_tax_amount DECIMAL(12,2),
    payload_total_amount DECIMAL(12,2),
    payload_payment_type STRING,
    payload_created_at TIMESTAMP(3),
    payload_updated_at TIMESTAMP(3)
) PARTITIONED BY (ingest_date)
WITH (
    'format-version' = '2',
    'write.format.default' = 'parquet'
);

CREATE TABLE IF NOT EXISTS bronze_order_item_cdc (
    record_key STRING,
    source_table STRING,
    op STRING,
    source_ts_ms BIGINT,
    source_ts TIMESTAMP(3),
    ingest_ts TIMESTAMP(3),
    ingest_date DATE,
    event_uuid STRING,
    schema_version INT,
    source_partition INT,
    source_offset BIGINT,
    transaction_id STRING,
    is_tombstone BOOLEAN,
    payload_order_item_id BIGINT,
    payload_order_id BIGINT,
    payload_product_id BIGINT,
    payload_quantity INT,
    payload_unit_price DECIMAL(12,2),
    payload_line_amount DECIMAL(12,2),
    payload_attributed_campaign_id BIGINT,
    payload_created_at TIMESTAMP(3),
    payload_updated_at TIMESTAMP(3)
) PARTITIONED BY (ingest_date)
WITH (
    'format-version' = '2',
    'write.format.default' = 'parquet'
);

CREATE TABLE IF NOT EXISTS bronze_sales_activity_cdc (
    record_key STRING,
    source_table STRING,
    op STRING,
    source_ts_ms BIGINT,
    source_ts TIMESTAMP(3),
    ingest_ts TIMESTAMP(3),
    ingest_date DATE,
    event_uuid STRING,
    schema_version INT,
    source_partition INT,
    source_offset BIGINT,
    transaction_id STRING,
    is_tombstone BOOLEAN,
    payload_sales_activity_id BIGINT,
    payload_advertiser_id BIGINT,
    payload_sales_rep_id BIGINT,
    payload_activity_ts TIMESTAMP(3),
    payload_activity_type STRING,
    payload_activity_outcome STRING,
    payload_notes STRING,
    payload_created_at TIMESTAMP(3),
    payload_updated_at TIMESTAMP(3)
) PARTITIONED BY (ingest_date)
WITH (
    'format-version' = '2',
    'write.format.default' = 'parquet'
);

CREATE TABLE IF NOT EXISTS bronze_session_event_raw (
    event_uuid STRING,
    event_id BIGINT,
    session_id BIGINT,
    customer_id BIGINT,
    event_ts TIMESTAMP(3),
    event_date DATE,
    event_type STRING,
    product_id BIGINT,
    campaign_id BIGINT,
    page_type STRING,
    search_term STRING,
    position_in_list INT,
    ingest_ts TIMESTAMP(3),
    ingest_date DATE,
    producer_version STRING,
    schema_version INT,
    source_partition INT,
    source_offset BIGINT
) PARTITIONED BY (event_date)
WITH (
    'format-version' = '2',
    'write.format.default' = 'parquet'
);
