
    
    

select
    customer_token as unique_field,
    count(*) as n_records

from gold.dim_customer
where customer_token is not null
group by customer_token
having count(*) > 1


