
    
    select
      count(*) as failures,
      count(*) != 0 as should_warn,
      count(*) != 0 as should_error
    from (
      
    
  
select *
from gold.mart_customer_conversion
where cart_to_purchase_rate < 0
   or cart_to_purchase_rate > 1

  
  
      
    ) dbt_internal_test