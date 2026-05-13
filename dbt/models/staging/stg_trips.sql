with source as (
  select *
  from main.trips
)
select
  source,
  trip_id,
  route_id,
  operating_day,
  start_ts,
  end_ts,
  start_lat,
  start_lon,
  end_lat,
  end_lon,
  distance_km,
  duration_min,
  avg_speed_kmh,
  passenger_count,
  delay_min,
  delay_reason,
  weather_main,
  weather_description,
  hour,
  day_of_week,
  is_peak_hour
from source

