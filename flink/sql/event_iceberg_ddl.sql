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
