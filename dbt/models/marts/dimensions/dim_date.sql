with all_dates as (
    select cast(metric_date as date) as date_day from {{ ref('stg_silver_customer_daily_metrics') }}
    union
    select cast(metric_date as date) as date_day from {{ ref('stg_silver_campaign_daily_metrics') }}
    union
    select cast(metric_date as date) as date_day from {{ ref('stg_silver_advertiser_daily_metrics') }}
    union
    select cast(order_date as date) as date_day from {{ ref('stg_silver_order_header') }}
    union
    select cast(event_date as date) as date_day from {{ ref('stg_silver_session_event_clean') }}
    union
    select cast(activity_date as date) as date_day from {{ ref('stg_silver_sales_activity') }}
)
select
    date_day,
    year(date_day) as calendar_year,
    quarter(date_day) as calendar_quarter,
    month(date_day) as calendar_month,
    day(date_day) as day_of_month,
    weekofyear(date_day) as week_of_year,
    date_format(date_day, 'E') as day_name,
    current_timestamp() as dbt_loaded_at
from all_dates
where date_day is not null
