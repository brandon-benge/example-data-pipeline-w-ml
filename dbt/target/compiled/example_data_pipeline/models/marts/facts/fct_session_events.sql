

select
    e.event_uuid,
    e.event_date,
    e.event_ts,
    e.session_id,
    sha2(concat('local-demo-tokenization-salt', '::', cast(e.customer_id as string)), 256) as customer_token,
    e.product_id,
    e.campaign_id,
    e.event_type,
    e.page_type,
    e.search_term,
    e.position_in_list,
    s.device_type,
    s.channel,
    s.referrer_type,
    e.producer_version,
    e.schema_version,
    e.ingest_ts,
    e.silver_processed_ts,
    current_timestamp() as dbt_loaded_at
from gold.stg_silver_session_event_clean as e
left join gold.stg_silver_customer_session as s
    on e.session_id = s.session_id