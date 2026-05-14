from __future__ import annotations

import argparse
from pathlib import Path

import duckdb


def _sql_path_literal(path: Path) -> str:
    
    return str(path).replace("'", "''")


def export_powerbi(*, db_path: Path, out_dir: Path, trips_limit: int = 1_000_000) -> None:
    """
    Export Power BI-friendly flat files from DuckDB.

    Power BI Desktop can import CSV easily. The full `trips` table can be very
    large, so we export `routes` + `analytics_summary` fully and a capped sample
    of `trips` by default.
    """
    out_dir.mkdir(parents=True, exist_ok=True)

    con = duckdb.connect(str(db_path), read_only=True)
    try:
        routes_csv = _sql_path_literal(out_dir / "routes.csv")
        summary_csv = _sql_path_literal(out_dir / "analytics_summary.csv")
        trips_csv = _sql_path_literal(out_dir / f"trips_sample_limit_{trips_limit}.csv")

        con.execute(f"copy (select * from routes) to '{routes_csv}' (format csv, header true)")
        con.execute(f"copy (select * from analytics_summary) to '{summary_csv}' (format csv, header true)")
        con.execute(f"copy (select * from trips limit {int(trips_limit)}) to '{trips_csv}' (format csv, header true)")
    finally:
        con.close()

def main() -> int:
    parser = argparse.ArgumentParser(description="Export DuckDB tables for Power BI (CSV).")
    parser.add_argument(
        "--db",
        default=str(Path("duckdb") / "transport_analytics.duckdb"),
        help="Path to DuckDB database file.",
    )
    parser.add_argument(
        "--out",
        default=str(Path("data") / "powerbi"),
        help="Output directory for CSV files.",
    )
    parser.add_argument(
        "--trips-limit",
        type=int,
        default=1_000_000,
        help="Row limit for trips sample export (full trips can be very large).",
    )
    args = parser.parse_args()

    export_powerbi(db_path=Path(args.db), out_dir=Path(args.out), trips_limit=args.trips_limit)
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
