# Power BI Dashboard Setup

This guide connects the DuckDB analytics database to Power BI for interactive dashboarding.

## Prerequisites

- Power BI Desktop ([Download](https://powerbi.microsoft.com/en-us/desktop/))
- DuckDB database: `duckdb/transport_analytics.duckdb` (created after running the ETL pipeline)
- The pipeline has been executed: `python -m spark.etl_job`

## Connecting DuckDB to Power BI

### Option 1: Direct ODBC Connection (Recommended)

#### 1. Install DuckDB ODBC Driver

1. Download the ODBC driver from [DuckDB releases](https://github.com/duckdb/duckdb/releases)
2. Install the appropriate version for your system (Windows 64-bit or 32-bit)
3. Restart your computer after installation

#### 2. Configure ODBC Data Source

1. Open **ODBC Data Source Administrator**:
   - Windows: Press `Win + R`, type `odbcad32.exe`, press Enter
2. Click **System DSN** tab
3. Click **Add** and select **DuckDB Driver**
4. Fill in:
   - **Data Source Name**: `transport_analytics`
   - **Database**: `C:\Users\eyos\Documents\bigdata\SmartPublicTransport\duckdb\transport_analytics.duckdb` (full path)
5. Click **OK** to save

#### 3. Connect in Power BI

1. Open **Power BI Desktop**
2. Go to **Get Data** → **ODBC**
3. Select data source: `transport_analytics`
4. Click **Connect**
5. Select tables: `trips`, `routes`, `analytics_summary`
6. Click **Load**

### Option 2: CSV Export (Alternative)

If ODBC is not available, export from DuckDB to CSV and import into Power BI:

```bash
duckdb duckdb/transport_analytics.duckdb
SELECT * FROM trips TO 'data/powerbi/trips.csv' (FORMAT CSV);
SELECT * FROM routes TO 'data/powerbi/routes.csv' (FORMAT CSV);
SELECT * FROM analytics_summary TO 'data/powerbi/analytics_summary.csv' (FORMAT CSV);
```

Then in Power BI:
1. **Get Data** → **Text/CSV**
2. Select each CSV file
3. Load and create relationships on `route_id`

## Available Tables

### `trips`
- `trip_id` (PK) — Unique trip identifier
- `route_id` (FK) — Route reference
- `operating_day` — Date of trip
- `departure_time`, `arrival_time` — Timestamps
- `passenger_count` — Number of passengers
- `delay_min` — Delay in minutes
- `delay_reason` — Weather or Traffic
- `avg_speed_kmh` — Average speed
- `duration_min` — Trip duration

### `routes`
- `route_id` (PK) — Unique route identifier
- `route_name` — Route name/number
- `start_location` — Origin
- `end_location` — Destination
- `distance_km` — Route distance

### `analytics_summary`
- `summary_id` (PK) — Unique summary identifier
- `operating_day` — Date
- `total_trips` — Trip count
- `total_passengers` — Passenger count
- `peak_hour` — Hour with highest trip count (0–23)
- `avg_delay_min` — Average delay
- `weather_condition` — Prevailing weather

## Creating Relationships

After loading tables, configure relationships in Power BI:

1. Go to **Model** view
2. Create relationship: `trips.route_id` → `routes.route_id`
3. Create relationship: `trips.operating_day` → `analytics_summary.operating_day`
4. Set cardinality to **Many-to-One** for each

## Dashboard Examples

See `dashboard/screenshots/` for Power BI dashboard examples showing:
- Trip count and total passengers
- Peak hours analysis
- Peak vs off-peak trip analysis

## Troubleshooting

**"ODBC Driver not found"**
- Reinstall the DuckDB ODBC driver from [releases](https://github.com/duckdb/duckdb/releases)
- Match your system architecture (64-bit vs 32-bit)

**"Database file not found"**
- Use the full absolute path in the ODBC DSN configuration
- Verify the file exists: `duckdb/transport_analytics.duckdb`

**"Empty tables in Power BI"**
- Ensure the ETL pipeline has been run: `python -m spark.etl_job`
- Check that data files exist in `data/raw/`

