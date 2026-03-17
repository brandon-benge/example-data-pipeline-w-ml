CREATE SCHEMA IF NOT EXISTS iceberg.bronze;
CREATE SCHEMA IF NOT EXISTS iceberg.silver;
CREATE SCHEMA IF NOT EXISTS iceberg.gold;

CREATE TABLE IF NOT EXISTS iceberg.bronze.bronze_sales_rep_cdc (
    sales_rep_id BIGINT,
    rep_name VARCHAR,
    team_name VARCHAR,
    region VARCHAR,
    manager_name VARCHAR,
    status VARCHAR,
    created_at TIMESTAMP(6),
    updated_at TIMESTAMP(6),
    _cdc ROW("key" VARCHAR, op VARCHAR, ts TIMESTAMP(6), "offset" BIGINT),
    _kafka_metadata_topic VARCHAR,
    _kafka_metadata_partition INTEGER,
    _kafka_metadata_offset BIGINT,
    _kafka_metadata_timestamp TIMESTAMP(6)
)
WITH (
    format = 'PARQUET',
    partitioning = ARRAY['day(_kafka_metadata_timestamp)']
);

CREATE TABLE IF NOT EXISTS iceberg.bronze.bronze_customer_cdc (
    customer_id BIGINT,
    first_name VARCHAR,
    last_name VARCHAR,
    email VARCHAR,
    phone VARCHAR,
    city VARCHAR,
    state VARCHAR,
    zip_code VARCHAR,
    status VARCHAR,
    created_at TIMESTAMP(6),
    updated_at TIMESTAMP(6),
    _cdc ROW("key" VARCHAR, op VARCHAR, ts TIMESTAMP(6), "offset" BIGINT),
    _kafka_metadata_topic VARCHAR,
    _kafka_metadata_partition INTEGER,
    _kafka_metadata_offset BIGINT,
    _kafka_metadata_timestamp TIMESTAMP(6)
)
WITH (
    format = 'PARQUET',
    partitioning = ARRAY['day(_kafka_metadata_timestamp)']
);

CREATE TABLE IF NOT EXISTS iceberg.bronze.bronze_advertiser_cdc (
    advertiser_id BIGINT,
    advertiser_name VARCHAR,
    industry VARCHAR,
    account_tier VARCHAR,
    region VARCHAR,
    owner_sales_rep_id BIGINT,
    status VARCHAR,
    created_at TIMESTAMP(6),
    updated_at TIMESTAMP(6),
    _cdc ROW("key" VARCHAR, op VARCHAR, ts TIMESTAMP(6), "offset" BIGINT),
    _kafka_metadata_topic VARCHAR,
    _kafka_metadata_partition INTEGER,
    _kafka_metadata_offset BIGINT,
    _kafka_metadata_timestamp TIMESTAMP(6)
)
WITH (
    format = 'PARQUET',
    partitioning = ARRAY['day(_kafka_metadata_timestamp)']
);

CREATE TABLE IF NOT EXISTS iceberg.bronze.bronze_product_cdc (
    product_id BIGINT,
    sku VARCHAR,
    product_name VARCHAR,
    brand VARCHAR,
    category VARCHAR,
    subcategory VARCHAR,
    list_price DECIMAL(12, 2),
    cost DECIMAL(12, 2),
    active_flag BOOLEAN,
    created_at TIMESTAMP(6),
    updated_at TIMESTAMP(6),
    _cdc ROW("key" VARCHAR, op VARCHAR, ts TIMESTAMP(6), "offset" BIGINT),
    _kafka_metadata_topic VARCHAR,
    _kafka_metadata_partition INTEGER,
    _kafka_metadata_offset BIGINT,
    _kafka_metadata_timestamp TIMESTAMP(6)
)
WITH (
    format = 'PARQUET',
    partitioning = ARRAY['day(_kafka_metadata_timestamp)']
);

CREATE TABLE IF NOT EXISTS iceberg.bronze.bronze_campaign_cdc (
    campaign_id BIGINT,
    advertiser_id BIGINT,
    campaign_name VARCHAR,
    campaign_type VARCHAR,
    objective VARCHAR,
    budget_amount DECIMAL(14, 2),
    start_date DATE,
    end_date DATE,
    status VARCHAR,
    created_at TIMESTAMP(6),
    updated_at TIMESTAMP(6),
    _cdc ROW("key" VARCHAR, op VARCHAR, ts TIMESTAMP(6), "offset" BIGINT),
    _kafka_metadata_topic VARCHAR,
    _kafka_metadata_partition INTEGER,
    _kafka_metadata_offset BIGINT,
    _kafka_metadata_timestamp TIMESTAMP(6)
)
WITH (
    format = 'PARQUET',
    partitioning = ARRAY['day(_kafka_metadata_timestamp)']
);

CREATE TABLE IF NOT EXISTS iceberg.bronze.bronze_campaign_product_cdc (
    campaign_product_id BIGINT,
    campaign_id BIGINT,
    product_id BIGINT,
    bid_amount DECIMAL(10, 2),
    priority INTEGER,
    created_at TIMESTAMP(6),
    updated_at TIMESTAMP(6),
    _cdc ROW("key" VARCHAR, op VARCHAR, ts TIMESTAMP(6), "offset" BIGINT),
    _kafka_metadata_topic VARCHAR,
    _kafka_metadata_partition INTEGER,
    _kafka_metadata_offset BIGINT,
    _kafka_metadata_timestamp TIMESTAMP(6)
)
WITH (
    format = 'PARQUET',
    partitioning = ARRAY['day(_kafka_metadata_timestamp)']
);

CREATE TABLE IF NOT EXISTS iceberg.bronze.bronze_customer_session_cdc (
    session_id BIGINT,
    customer_id BIGINT,
    session_start_ts TIMESTAMP(6),
    session_end_ts TIMESTAMP(6),
    device_type VARCHAR,
    channel VARCHAR,
    referrer_type VARCHAR,
    created_at TIMESTAMP(6),
    updated_at TIMESTAMP(6),
    _cdc ROW("key" VARCHAR, op VARCHAR, ts TIMESTAMP(6), "offset" BIGINT),
    _kafka_metadata_topic VARCHAR,
    _kafka_metadata_partition INTEGER,
    _kafka_metadata_offset BIGINT,
    _kafka_metadata_timestamp TIMESTAMP(6)
)
WITH (
    format = 'PARQUET',
    partitioning = ARRAY['day(_kafka_metadata_timestamp)']
);

CREATE TABLE IF NOT EXISTS iceberg.bronze.bronze_order_header_cdc (
    order_id BIGINT,
    customer_id BIGINT,
    order_ts TIMESTAMP(6),
    order_status VARCHAR,
    subtotal_amount DECIMAL(12, 2),
    discount_amount DECIMAL(12, 2),
    tax_amount DECIMAL(12, 2),
    total_amount DECIMAL(12, 2),
    payment_type VARCHAR,
    created_at TIMESTAMP(6),
    updated_at TIMESTAMP(6),
    _cdc ROW("key" VARCHAR, op VARCHAR, ts TIMESTAMP(6), "offset" BIGINT),
    _kafka_metadata_topic VARCHAR,
    _kafka_metadata_partition INTEGER,
    _kafka_metadata_offset BIGINT,
    _kafka_metadata_timestamp TIMESTAMP(6)
)
WITH (
    format = 'PARQUET',
    partitioning = ARRAY['day(_kafka_metadata_timestamp)']
);

CREATE TABLE IF NOT EXISTS iceberg.bronze.bronze_order_item_cdc (
    order_item_id BIGINT,
    order_id BIGINT,
    product_id BIGINT,
    quantity INTEGER,
    unit_price DECIMAL(12, 2),
    line_amount DECIMAL(12, 2),
    attributed_campaign_id BIGINT,
    created_at TIMESTAMP(6),
    updated_at TIMESTAMP(6),
    _cdc ROW("key" VARCHAR, op VARCHAR, ts TIMESTAMP(6), "offset" BIGINT),
    _kafka_metadata_topic VARCHAR,
    _kafka_metadata_partition INTEGER,
    _kafka_metadata_offset BIGINT,
    _kafka_metadata_timestamp TIMESTAMP(6)
)
WITH (
    format = 'PARQUET',
    partitioning = ARRAY['day(_kafka_metadata_timestamp)']
);

CREATE TABLE IF NOT EXISTS iceberg.bronze.bronze_sales_activity_cdc (
    sales_activity_id BIGINT,
    advertiser_id BIGINT,
    sales_rep_id BIGINT,
    activity_ts TIMESTAMP(6),
    activity_type VARCHAR,
    activity_outcome VARCHAR,
    notes VARCHAR,
    created_at TIMESTAMP(6),
    updated_at TIMESTAMP(6),
    _cdc ROW("key" VARCHAR, op VARCHAR, ts TIMESTAMP(6), "offset" BIGINT),
    _kafka_metadata_topic VARCHAR,
    _kafka_metadata_partition INTEGER,
    _kafka_metadata_offset BIGINT,
    _kafka_metadata_timestamp TIMESTAMP(6)
)
WITH (
    format = 'PARQUET',
    partitioning = ARRAY['day(_kafka_metadata_timestamp)']
);
