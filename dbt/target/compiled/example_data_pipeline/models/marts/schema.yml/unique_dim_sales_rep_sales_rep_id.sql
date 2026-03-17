
    
    

select
    sales_rep_id as unique_field,
    count(*) as n_records

from gold.dim_sales_rep
where sales_rep_id is not null
group by sales_rep_id
having count(*) > 1


