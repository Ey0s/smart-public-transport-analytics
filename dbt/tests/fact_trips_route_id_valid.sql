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