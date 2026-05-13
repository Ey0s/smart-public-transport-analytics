# dbt (DuckDB) models

This folder contains an optional dbt layer over the DuckDB database produced by the ETL job.

## Quick start

1. Run the ETL once to create and populate the DuckDB database:
   - `python -m spark.etl_job`

2. Install dbt:
   - `pip install dbt-duckdb`

3. Point dbt at the included profile (recommended for this repo):
   - PowerShell:
     - `$env:DBT_PROFILES_DIR = (Resolve-Path .\\dbt).Path`

4. Run dbt:
   - `dbt debug --project-dir dbt`
   - `dbt run --project-dir dbt`
   - `dbt test --project-dir dbt`

