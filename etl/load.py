from __future__ import annotations

import logging
from pathlib import Path
from typing import Optional

import duckdb
from pyspark.sql import DataFrame

logger = logging.getLogger(__name__)

TRIPS_TABLE_COLS = [
    "source",
    "trip_id",
    "route_id",
    "operating_day",
    "start_ts",
    "end_ts",
    "start_lat",
    "start_lon",
    "end_lat",
    "end_lon",
    "distance_km",
    "duration_min",
    "avg_speed_kmh",
    "passenger_count",
    "delay_min",
    "delay_reason",
    "weather_main",
    "weather_description",
    "hour",
    "day_of_week",
    "is_peak_hour",
]

ROUTES_TABLE_COLS = ["route_id", "route_name"]

SUMMARY_TABLE_COLS = [
    "operating_day",
    "hour",
    "source",
    "trip_count",
    "total_passengers",
    "avg_delay_min",
    "peak_hour_trip_count",
    "created_at",
]


def ensure_duckdb_schema(db_path: Path, schema_sql_path: Path) -> None:
    db_path.parent.mkdir(parents=True, exist_ok=True)
    con = duckdb.connect(str(db_path))
    try:
        sql = schema_sql_path.read_text(encoding="utf-8")
        con.execute(sql)
    finally:
        con.close()


def _write_df_to_parquet(df: DataFrame, output_path: Path, mode: str = "overwrite") -> None:
    output_path.parent.mkdir(parents=True, exist_ok=True)
    df.write.mode(mode).parquet(str(output_path))


def load_parquet_into_duckdb(
    *,
    db_path: Path,
    table_name: str,
    parquet_path: Path,
    schema_sql_path: Optional[Path] = None,
) -> None:
    con = duckdb.connect(str(db_path))
    try:
        if schema_sql_path:
            con.execute(schema_sql_path.read_text(encoding="utf-8"))

        # Efficient DuckDB load from parquet
        con.execute(f"INSERT INTO {table_name} SELECT * FROM read_parquet(?)", [str(parquet_path)])
        con.execute("CHECKPOINT;")
    finally:
        con.close()


def load_to_duckdb(
    *,
    trips_df: DataFrame,
    routes_df: DataFrame,
    summary_df: DataFrame,
    processed_dir: Path,
    db_path: Path,
    schema_sql_path: Path,
) -> None:
    """
    Write parquet intermediates (Spark) then bulk-load into DuckDB.
    """

    trips_path = processed_dir / "trips_unified.parquet"
    routes_path = processed_dir / "routes.parquet"
    summary_path = processed_dir / "analytics_summary.parquet"

    logger.info("Writing parquet intermediates to %s", processed_dir)
    _write_df_to_parquet(trips_df.select(*TRIPS_TABLE_COLS), trips_path)
    _write_df_to_parquet(routes_df.select(*ROUTES_TABLE_COLS), routes_path)
    _write_df_to_parquet(summary_df.select(*SUMMARY_TABLE_COLS), summary_path)

    logger.info("Ensuring DuckDB schema at %s", db_path)
    ensure_duckdb_schema(db_path, schema_sql_path)

    con = duckdb.connect(str(db_path))
    try:
        # Replace data atomically
        con.execute("BEGIN TRANSACTION;")
        con.execute("DELETE FROM trips;")
        con.execute("DELETE FROM routes;")
        con.execute("DELETE FROM analytics_summary;")

        con.execute("INSERT INTO trips SELECT * FROM read_parquet(?)", [str(trips_path)])
        con.execute("INSERT INTO routes SELECT * FROM read_parquet(?)", [str(routes_path)])
        con.execute("INSERT INTO analytics_summary SELECT * FROM read_parquet(?)", [str(summary_path)])
        con.execute("COMMIT;")
        con.execute("CHECKPOINT;")
    except Exception:
        con.execute("ROLLBACK;")
        raise
    finally:
        con.close()
