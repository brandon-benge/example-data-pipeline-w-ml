
    
    

select
    sales_activity_id as unique_field,
    count(*) as n_records

from gold.fct_sales_activity
where sales_activity_id is not null
group by sales_activity_id
having count(*) > 1


