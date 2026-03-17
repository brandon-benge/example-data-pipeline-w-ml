
    
    select
      count(*) as failures,
      count(*) != 0 as should_warn,
      count(*) != 0 as should_error
    from (
      
    
  
    
    

with child as (
    select advertiser_id as from_field
    from gold.dim_campaign
    where advertiser_id is not null
),

parent as (
    select advertiser_id as to_field
    from gold.dim_advertiser
)

select
    from_field

from child
left join parent
    on child.from_field = parent.to_field

where parent.to_field is null



  
  
      
    ) dbt_internal_test