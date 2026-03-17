
select *
from gold.mart_customer_conversion
where click_to_purchase_rate < 0
   or click_to_purchase_rate > 1
