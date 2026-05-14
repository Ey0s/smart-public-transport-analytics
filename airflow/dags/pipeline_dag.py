from __future__ import annotations

import os
from datetime import datetime, timedelta
from pathlib import Path

from airflow import DAG
from airflow.operators.python import PythonOperator


def _duckdb_path() -> Path:
    repo_root = Path(__file__).resolve().parents[2]
    return repo_root / "duckdb" / "transport_analytics.duckdb"


def _run_spark_etl() -> None:
    # Ensure repo root is on the path for imports when running in Airflow.
    repo_root = Path(__file__).resolve().parents[2]
    os.environ.setdefault("PYTHONPATH", str(repo_root))

    from spark.etl_job import run  # noqa: WPS433 (runtime import is intentional)

    run(project_root=repo_root)


def _verify_duckdb_min_rows() -> None:
    """
    Fail fast if the ETL produced an empty (or unexpectedly tiny) load.
    """
    import duckdb  # noqa: WPS433 (runtime import is intentional)

    db_path = _duckdb_path()
    con = duckdb.connect(str(db_path))
    try:
        trips = con.execute("select count(1) from trips").fetchone()[0]
        routes = con.execute("select count(1) from routes").fetchone()[0]
        summary = con.execute("select count(1) from analytics_summary").fetchone()[0]
    finally:
        con.close()

    if trips < 1_000:
        raise RuntimeError(f"DuckDB trips row count too small: {trips}")
    if routes <= 0:
        raise RuntimeError(f"DuckDB routes row count too small: {routes}")
    if summary <= 0:
        raise RuntimeError(f"DuckDB analytics_summary row count too small: {summary}")


def _export_powerbi_assets() -> None:
    repo_root = Path(__file__).resolve().parents[2]
    os.environ.setdefault("PYTHONPATH", str(repo_root))

    from scripts.export_powerbi import export_powerbi  # noqa: WPS433 (runtime import is intentional)

    export_powerbi(
        db_path=_duckdb_path(),
        out_dir=repo_root / "data" / "powerbi",
        trips_limit=1_000_000,
    )


default_args = {
    "owner": "transport-analytics",
    "depends_on_past": False,
    "retries": 2,
    "retry_delay": timedelta(minutes=5),
}



