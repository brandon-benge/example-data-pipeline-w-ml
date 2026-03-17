
select *
from gold.mart_customer_conversion
where cart_to_purchase_rate < 0
   or cart_to_purchase_rate > 1
