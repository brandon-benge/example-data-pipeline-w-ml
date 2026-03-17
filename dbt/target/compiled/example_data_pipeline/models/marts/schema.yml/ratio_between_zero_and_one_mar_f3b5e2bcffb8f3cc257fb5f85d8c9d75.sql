
select *
from gold.mart_customer_conversion
where view_to_purchase_rate < 0
   or view_to_purchase_rate > 1
