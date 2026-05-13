-- DuckDB analytical schema for transport analytics pipeline

CREATE TABLE IF NOT EXISTS trips (
  source              VARCHAR,
  trip_id             VARCHAR,
  route_id            VARCHAR,
  operating_day       DATE,
  start_ts            TIMESTAMP,
  end_ts              TIMESTAMP,
  start_lat           DOUBLE,
  start_lon           DOUBLE,
  end_lat             DOUBLE,
  end_lon             DOUBLE,
  distance_km         DOUBLE,
  duration_min        DOUBLE,
  avg_speed_kmh       DOUBLE,
  passenger_count     INTEGER,
  delay_min           DOUBLE,
  delay_reason        VARCHAR,
  weather_main        VARCHAR,
  weather_description VARCHAR,
  hour                INTEGER,
  day_of_week         INTEGER,
  is_peak_hour        BOOLEAN
);

CREATE TABLE IF NOT EXISTS routes (
  route_id   VARCHAR,
  route_name VARCHAR
);

CREATE TABLE IF NOT EXISTS analytics_summary (
  operating_day        DATE,
  hour                 INTEGER,
  source               VARCHAR,
  trip_count           BIGINT,
  total_passengers     BIGINT,
  avg_delay_min        DOUBLE,
  peak_hour_trip_count BIGINT,
  created_at           TIMESTAMP
);

CREATE INDEX IF NOT EXISTS idx_trips_operating_day ON trips(operating_day);
CREATE INDEX IF NOT EXISTS idx_trips_route_id ON trips(route_id);
CREATE INDEX IF NOT EXISTS idx_summary_operating_day ON analytics_summary(operating_day);

