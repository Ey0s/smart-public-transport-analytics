from __future__ import annotations

import logging
import subprocess
import re
import os
import sys
from pathlib import Path

from etl.config import load_config
from etl.extract import discover_sources, read_addis_final_csv, read_addis_trips_csv, read_passengers_parquet
from etl.load import load_to_duckdb
from etl.logging_utils import setup_logging
from etl.transform import build_analytics_summary, transform_addis_trips, transform_passengers_parquet, unify_trips
from spark.spark_session import build_spark
from pyspark import StorageLevel

logger = logging.getLogger(__name__)

_JAVA_MAJOR_RE = re.compile(r'version "(?P<major>\d+)(?:\.(?P<minor>\d+)\.(?P<patch>\d+))?')


def _java_major_version(java_exe: str = "java") -> int | None:
    try:
        proc = subprocess.run([java_exe, "-version"], capture_output=True, text=True, check=False)
    except FileNotFoundError:
        return None

    text = (proc.stderr or "") + "\n" + (proc.stdout or "")
    m = _JAVA_MAJOR_RE.search(text)
    if not m:
        return None
    major = int(m.group("major"))
    # Legacy format "1.8.0_..." would be major=1
    if major == 1 and m.group("minor"):
        return int(m.group("minor"))
    return major


def _candidate_java_homes() -> list[Path]:
    """
    Return candidate JAVA_HOME directories (best-effort).

    On Windows, it's common to have multiple JDKs installed under:
      C:\\Program Files\\Java\\
    """

    homes: list[Path] = []

    env_home = os.environ.get("JAVA_HOME")
    if env_home:
        homes.append(Path(env_home))

    pf_java = Path(r"C:\Program Files\Java")
    if pf_java.exists():
        # Prefer JDKs; include any directory that looks like a JDK install.
        for child in sorted(pf_java.iterdir()):
            if not child.is_dir():
                continue
            name = child.name.lower()
            if name.startswith("jdk"):
                homes.append(child)
    seen: set[str] = set()
    unique: list[Path] = []
    for h in homes:
        key = str(h).lower()
        if key not in seen:
            unique.append(h)
            seen.add(key)
    return unique


def _pick_best_java_home(min_major: int = 17) -> tuple[Path, int] | None:
    """
    Choose the highest Java major version available from known install locations.
    """

    best: tuple[Path, int] | None = None
    for home in _candidate_java_homes():
        java_exe = home / "bin" / ("java.exe" if os.name == "nt" else "java")
        if not java_exe.exists():
            continue
        major = _java_major_version(str(java_exe))
        if major is None or major < min_major:
            continue
        if best is None or major > best[1]:
            best = (home, major)
    return best


def _set_java_home(java_home: Path) -> None:
    os.environ["JAVA_HOME"] = str(java_home)
    bin_dir = java_home / "bin"
    current_path = os.environ.get("PATH", "")
    # Prepend to PATH so `java` resolves to the chosen JDK.
    os.environ["PATH"] = str(bin_dir) + os.pathsep + current_path


def _preflight_java() -> None:
    """
    Ensure Spark can launch with Java 17+.

    If `java` on PATH is too old, attempt to auto-select a newer JDK from common
    install locations and set JAVA_HOME/PATH for the current process.
    """

    major = _java_major_version()
    if major is not None and major >= 17:
        return

    picked = _pick_best_java_home(min_major=17)
    if picked is not None:
        java_home, picked_major = picked
        _set_java_home(java_home)
        logger.info("Using Java %s from JAVA_HOME=%s", picked_major, java_home)
        return

    if major is None:
        raise RuntimeError(
            "Java not found. Install Java 17+ and set JAVA_HOME, then ensure `java` is on PATH."
        )

    raise RuntimeError(
        f"Java {major} detected on PATH, but Spark 3.5 requires Java 17+. "
        "Install Java 17+ and set JAVA_HOME (or ensure a newer JDK is earlier on PATH)."
    )


def run(project_root: Path | None = None) -> None:
    setup_logging()
    if os.name == "nt" and sys.version_info >= (3, 12) and os.environ.get("ALLOW_PYSPARK_PY312_PLUS") != "1":
        raise RuntimeError(
            "On Windows, run this project with Python 3.11 for best PySpark stability. "
            "You are running Python {}.{}. Create the venv with `py -3.11 -m venv .venv` "
            "and reinstall requirements, then rerun. (Set ALLOW_PYSPARK_PY312_PLUS=1 to bypass.)".format(
                sys.version_info[0], sys.version_info[1]
            )
        )
    cfg = load_config(project_root=project_root)
    cfg.processed_dir.mkdir(parents=True, exist_ok=True)

    _preflight_java()
    spark = build_spark(cfg.spark_app_name)
    try:
        sources = discover_sources(cfg.raw_dir)

        # Read and transform all CSV sources (two schemas supported)
        transformed_csvs = []
        if sources.addis_trips_csv:
            addis_raw = read_addis_trips_csv(spark, sources.addis_trips_csv)
            transformed_csvs.append(transform_addis_trips(addis_raw))
        if sources.addis_final_csv:
            final_raw = read_addis_final_csv(spark, sources.addis_final_csv)
            transformed_csvs.append(transform_addis_trips(final_raw))

        if not transformed_csvs:
            raise FileNotFoundError("No Addis CSV files found under data/raw.")

        # Union all CSV-derived DataFrames
        addis = transformed_csvs[0]
        for extra in transformed_csvs[1:]:
            addis = addis.unionByName(extra, allowMissingColumns=True)

        passengers_raw = read_passengers_parquet(spark, sources.passengers_parquet)
        passengers = transform_passengers_parquet(passengers_raw)

        trips = unify_trips(
            addis,
            passengers,
            weather_api_key=cfg.openweathermap_api_key,
            weather_base_url=cfg.openweathermap_base_url,
            weather_cache_path=str(cfg.weather_cache_path),
        ).persist(StorageLevel.DISK_ONLY)
        routes, summary = build_analytics_summary(trips)

        load_to_duckdb(
            trips_df=trips,
            routes_df=routes,
            summary_df=summary,
            processed_dir=cfg.processed_dir,
            db_path=cfg.duckdb_path,
            schema_sql_path=cfg.project_root / "duckdb" / "schema.sql",
        )

        logger.info("ETL completed. DuckDB: %s", cfg.duckdb_path)
        trips.unpersist(blocking=False)
    finally:
        spark.stop()


if __name__ == "__main__":
    run()
