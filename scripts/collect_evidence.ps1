$ErrorActionPreference = "Stop"

param(
  [string]$DbPath = "duckdb\transport_analytics.duckdb",
  [string]$OutputRoot = "evidence"
)

# Require duckdb CLI on PATH
$duckdbExe = Get-Command duckdb -ErrorAction SilentlyContinue
if (-not $duckdbExe) {
  Write-Error "duckdb CLI not found on PATH. Install from https://duckdb.org/docs/installation/"
  exit 1
}

if (-not (Test-Path -LiteralPath $DbPath)) {
  Write-Error "DuckDB file not found: $DbPath`nRun the pipeline first: python -m spark.etl_job"
  exit 1
}

$timestamp = Get-Date -Format "yyyyMMdd_HHmmss"
$outDir = Join-Path $OutputRoot $timestamp
New-Item -ItemType Directory -Force -Path $outDir | Out-Null

$sqlPath  = Join-Path $outDir "evidence.sql"
$txtPath  = Join-Path $outDir "evidence.txt"
$tripsCsv = Join-Path $outDir "sample_trips.csv"
$summCsv  = Join-Path $outDir "analytics_summary.csv"
$routesCsv = Join-Path $outDir "routes.csv"

# Escape backslashes for DuckDB string literals
$tripsEsc  = $tripsCsv  -replace "\\", "/"
$summEsc   = $summCsv   -replace "\\", "/"
$routesEsc = $routesCsv -replace "\\", "/"

@"
-- Transport Analytics Pipeline – Evidence Queries
.mode column
.headers on

SELECT '=== Row counts ===' AS section;
SELECT
  (SELECT COUNT(*) FROM trips)             AS trips_rows,
  (SELECT COUNT(*) FROM routes)            AS routes_rows,
  (SELECT COUNT(*) FROM analytics_summary) AS summary_rows;

SELECT '=== Date coverage ===' AS section;
SELECT MIN(operating_day) AS min_day, MAX(operating_day) AS max_day FROM trips;

SELECT '=== Sources ===' AS section;
SELECT source, COUNT(*) AS trip_count FROM trips GROUP BY source ORDER BY source;

SELECT '=== Top 10 routes by trip count ===' AS section;
SELECT route_id, COUNT(*) AS trips, AVG(delay_min) AS avg_delay_min
FROM trips
WHERE route_id IS NOT NULL
GROUP BY route_id
ORDER BY trips DESC
LIMIT 10;

SELECT '=== Peak vs off-peak ===' AS section;
SELECT is_peak_hour, COUNT(*) AS trips, AVG(delay_min) AS avg_delay_min
FROM trips
GROUP BY is_peak_hour;

SELECT '=== Weather impact on delays ===' AS section;
SELECT weather_main, delay_reason, COUNT(*) AS trips, AVG(delay_min) AS avg_delay_min
FROM trips
GROUP BY weather_main, delay_reason
ORDER BY weather_main, delay_reason;

SELECT '=== Sample trips (20 rows) ===' AS section;
SELECT trip_id, source, route_id, operating_day, hour, distance_km,
       duration_min, delay_min, delay_reason, weather_main, is_peak_hour
FROM trips
ORDER BY operating_day, hour
LIMIT 20;

COPY (SELECT * FROM trips LIMIT 500) TO '$tripsEsc' (HEADER, DELIMITER ',');
COPY (SELECT * FROM analytics_summary) TO '$summEsc' (HEADER, DELIMITER ',');
COPY (SELECT * FROM routes) TO '$routesEsc' (HEADER, DELIMITER ',');
"@ | Set-Content -LiteralPath $sqlPath -Encoding UTF8

Write-Host "Running evidence queries against: $DbPath"
& $duckdbExe.Path $DbPath < $sqlPath | Tee-Object -FilePath $txtPath

Write-Host ""
Write-Host "Evidence written to: $outDir"
Write-Host "  $txtPath"
Write-Host "  $tripsCsv"
Write-Host "  $summCsv"
Write-Host "  $routesCsv"
