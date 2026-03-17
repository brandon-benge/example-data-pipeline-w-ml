select
    event_uuid,
    session_id,
    customer_id,
    event_ts,
    event_date,
    event_type,
    product_id,
    campaign_id,
    page_type,
    search_term,
    position_in_list,
    ingest_ts,
    producer_version,
    schema_version,
    silver_processed_ts
from silver.silver_session_event_clean