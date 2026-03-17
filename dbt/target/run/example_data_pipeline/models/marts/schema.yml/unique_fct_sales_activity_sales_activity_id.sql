
    
    select
      count(*) as failures,
      count(*) != 0 as should_warn,
      count(*) != 0 as should_error
    from (
      
    
  
    
    

select
    sales_activity_id as unique_field,
    count(*) as n_records

from gold.fct_sales_activity
where sales_activity_id is not null
group by sales_activity_id
having count(*) > 1



  
  
      
    ) dbt_internal_test