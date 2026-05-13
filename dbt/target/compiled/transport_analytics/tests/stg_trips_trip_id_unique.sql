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