-- ============================================================================
-- DuckDB Analytical Schema
-- Transport Analytics Data Pipeline
-- ============================================================================

-- ----------------------------------------------------------------------------
-- Table: trips
-- Stores detailed trip-level transportation records collected from
-- multiple transport data sources.
-- ----------------------------------------------------------------------------

CREATE TABLE IF NOT EXISTS trips (

  -- Source system or provider name
  source              VARCHAR,

  -- Unique trip identifier
  trip_id             VARCHAR,

  -- Route identifier associated with the trip
  route_id            VARCHAR,

  -- Operational service date
  operating_day       DATE,

  -- Trip start timestamp
  start_ts            TIMESTAMP,

  -- Trip end timestamp
  end_ts              TIMESTAMP,

  -- Geographic coordinates of trip start point
  start_lat           DOUBLE,
  start_lon           DOUBLE,

  -- Geographic coordinates of trip end point
  end_lat             DOUBLE,
  end_lon             DOUBLE,

  -- Total distance traveled in kilometers
  distance_km         DOUBLE,

  -- Trip duration in minutes
  duration_min        DOUBLE,

  -- Average trip speed in kilometers per hour
  avg_speed_kmh       DOUBLE,

  -- Number of passengers transported
  passenger_count     INTEGER,

  -- Delay duration in minutes
  delay_min           DOUBLE,

  -- Reason associated with trip delay
  delay_reason        VARCHAR,

  -- Main weather category during the trip
  weather_main        VARCHAR,

  -- Detailed weather condition description
  weather_description VARCHAR,

  -- Hour extracted from trip timing
  hour                INTEGER,

  -- Day of the week indicator
  day_of_week         INTEGER,

  -- Peak-hour operational flag
  is_peak_hour        BOOLEAN
);

-- ----------------------------------------------------------------------------
-- Table: routes
-- Stores route reference and lookup information.
-- ----------------------------------------------------------------------------

CREATE TABLE IF NOT EXISTS routes (

  -- Unique route identifier
  route_id   VARCHAR,

  -- Human-readable route name
  route_name VARCHAR
);

-- ----------------------------------------------------------------------------
-- Table: analytics_summary
-- Stores aggregated analytics metrics for reporting and dashboarding.
-- ----------------------------------------------------------------------------

CREATE TABLE IF NOT EXISTS analytics_summary (

  -- Operational service date
  operating_day        DATE,

  -- Hourly aggregation bucket
  hour                 INTEGER,

  -- Source system or provider
  source               VARCHAR,

  -- Total number of trips
  trip_count           BIGINT,

  -- Total passenger volume
  total_passengers     BIGINT,

  -- Average trip delay in minutes
  avg_delay_min        DOUBLE,

  -- Number of trips during peak hours
  peak_hour_trip_count BIGINT,

  -- Record creation timestamp
  created_at           TIMESTAMP
);

-- ============================================================================
-- Index Definitions
-- Improve query performance for filtering and aggregations
-- ============================================================================

-- Index for trip queries filtered by operating day
CREATE INDEX IF NOT EXISTS idx_trips_operating_day
ON trips(operating_day);

-- Index for trip queries filtered by route identifier
CREATE INDEX IF NOT EXISTS idx_trips_route_id
ON trips(route_id);

-- Index for summary queries filtered by operating day
CREATE INDEX IF NOT EXISTS idx_summary_operating_day
ON analytics_summary(operating_day);

