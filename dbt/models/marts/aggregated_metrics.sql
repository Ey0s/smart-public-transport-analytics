select
  operating_day,
  hour,
  source,
  count(*) as trip_count,
  sum(coalesce(passenger_count, 0)) as total_passengers,
  avg(delay_min) as avg_delay_min,
  sum(case when is_peak_hour then 1 else 0 end) as peak_hour_trip_count,
  sum(case when delay_reason = 'Weather' then 1 else 0 end) as weather_delay_trip_count
from {{ ref('fact_trips') }}
group by 1, 2, 3

