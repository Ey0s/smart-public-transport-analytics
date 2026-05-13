select
  trip_id,
  source,
  route_id,
  operating_day,
  hour,
  day_of_week,
  start_ts,
  end_ts,
  duration_min,
  distance_km,
  avg_speed_kmh,
  passenger_count,
  delay_min,
  delay_reason,
  weather_main,
  weather_description,
  is_peak_hour
from {{ ref('stg_trips') }}

