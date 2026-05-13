
    
    select
      count(*) as failures,
      count(*) != 0 as should_warn,
      count(*) != 0 as should_error
    from (
      
    
  with duplicate_trip_ids as (
  select
    trip_id,
    count(*) as row_count
  from main.trips
  group by trip_id
  having count(*) > 1
)

select *
from duplicate_trip_ids
  
  
      
    ) dbt_internal_test