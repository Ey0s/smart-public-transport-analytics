
    
    select
      count(*) as failures,
      count(*) != 0 as should_warn,
      count(*) != 0 as should_error
    from (
      
    
  with duplicate_route_ids as (
  select
    route_id,
    count(*) as row_count
  from main.routes
  group by route_id
  having count(*) > 1
)

select *
from duplicate_route_ids
  
  
      
    ) dbt_internal_test