# Airflow orchestration

The DAG is located at `airflow/dags/pipeline_dag.py`.

## Notes (Windows)

Apache Airflow is easiest to run via **Docker** or **WSL** on Windows.

## Quick start (Docker)

1. Follow the official Airflow Docker Compose quick start.
2. Mount this repo into the Airflow container.
3. Point `AIRFLOW__CORE__DAGS_FOLDER` to the mounted `airflow/dags`.

The DAG `transport_analytics_pipeline` runs daily and executes the PySpark ETL job via `PythonOperator`.

## Power BI

Power BI Desktop doesn't read DuckDB files directly. This project exports Power BI-friendly CSVs from DuckDB:

- `data/powerbi/routes.csv`
- `data/powerbi/analytics_summary.csv`
- `data/powerbi/trips_sample_limit_1000000.csv` (sample; the full `trips` table can be very large)

Manual export:

- `python -m scripts.export_powerbi --trips-limit 1000000`

Airflow export:

The DAG runs three tasks in order:

1. `extract_transform_load` (runs the Spark ETL -> DuckDB)
2. `verify_duckdb` (checks minimum row counts)
3. `export_powerbi` (writes the CSVs under `data/powerbi/`)
