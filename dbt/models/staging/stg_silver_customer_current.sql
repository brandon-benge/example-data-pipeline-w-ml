select
    customer_id,
    first_name_masked,
    last_name_masked,
    email_masked,
    phone_masked,
    city,
    state,
    zip_code_masked,
    status,
    created_at,
    updated_at,
    source_last_change_ts,
    silver_processed_ts
from {{ source('silver', 'silver_customer_current') }}
