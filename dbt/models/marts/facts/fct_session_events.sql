{{ config(partition_by=['event_date']) }}

select
    e.event_uuid,
    e.event_date,
    e.event_ts,
    e.session_id,
    {{ tokenize_identifier('e.customer_id') }} as customer_token,
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
from {{ ref('stg_silver_session_event_clean') }} as e
left join {{ ref('stg_silver_customer_session') }} as s
    on e.session_id = s.session_id
