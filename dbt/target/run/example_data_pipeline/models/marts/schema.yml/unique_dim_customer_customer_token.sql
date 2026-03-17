
    
    select
      count(*) as failures,
      count(*) != 0 as should_warn,
      count(*) != 0 as should_error
    from (
      
    
  
    
    

select
    customer_token as unique_field,
    count(*) as n_records

from gold.dim_customer
where customer_token is not null
group by customer_token
having count(*) > 1



  
  
      
    ) dbt_internal_test