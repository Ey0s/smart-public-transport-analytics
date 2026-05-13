from __future__ import annotations

import os
import subprocess
import sys
from pathlib import Path

from pyspark.sql import SparkSession


def _env(key: str, default: str) -> str:
    value = os.environ.get(key)
    return value if value not in (None, "") else default


def _env_int(key: str, default: int) -> int:
    value = os.environ.get(key)
    if value is None or value == "":
        return default
    try:
        return int(value)
    except ValueError:
        return default


def _find_python311() -> str | None:
    """
    Return the path to a Python 3.11 executable, or None if not found.
    PySpark local mode is most reliable on Windows with Python 3.11.
    """
    # 1. Already running 3.11 — use current interpreter
    if sys.version_info[:2] == (3, 11):
        return sys.executable

    # 2. Try Windows py launcher
    try:
        result = subprocess.run(
            ["py", "-3.11", "-c", "import sys; print(sys.executable)"],
            capture_output=True, text=True, check=True,
        )
        path = result.stdout.strip()
        if path and Path(path).exists():
            return path
    except Exception:
        pass

    # 3. Common install locations on Windows
    for candidate in [
        r"C:\Users\{}\AppData\Local\Programs\Python\Python311\python.exe".format(os.environ.get("USERNAME", "")),
        r"C:\Python311\python.exe",
        r"C:\Program Files\Python311\python.exe",
    ]:
        if Path(candidate).exists():
            return candidate

    return None


def build_spark(app_name: str) -> SparkSession:
    """
    Create a SparkSession tuned for local development.

    Forces Python 3.11 as the interpreter on Windows when available, because
    PySpark local mode can crash on Python 3.12+ in some environments.
    """

    # Always set explicit interpreter paths so Spark never falls back to the
    # Windows "python" app-execution alias (Microsoft Store), which breaks
    # worker startup with: "Python was not found; run without arguments...".
    # Spark requires the driver and worker Python minor versions to match.
    # We therefore default to the currently-running interpreter.
    python_exe = sys.executable

    # If the user is *already* running Python 3.11, keep it.
    # If they're on 3.12+, they should run the whole job under 3.11 (see
    # spark/etl_job.py guard). We do NOT silently swap only the worker Python
    # to 3.11 because that causes a driver/worker mismatch.
    if os.name == "nt" and os.environ.get("FORCE_PYSPARK_PYTHON311") == "1":
        py311 = _find_python311()
        if py311:
            python_exe = py311

    os.environ["PYSPARK_PYTHON"] = python_exe
    os.environ["PYSPARK_DRIVER_PYTHON"] = python_exe

    # Prefer a repo-local Spark temp dir to avoid permission issues on some
    # Windows setups (and in sandboxed environments) where %TEMP% cleanup or
    # directory creation can fail.
    repo_root = Path(__file__).resolve().parents[1]
    spark_local_dir = repo_root / ".spark_local"
    spark_local_dir.mkdir(parents=True, exist_ok=True)
    # Spark can override `spark.local.dir` from env; force a writable location.
    os.environ["SPARK_LOCAL_DIRS"] = str(spark_local_dir)

    # Memory / execution tuning (env-overridable via .env; loaded by etl.config).
    driver_memory = _env("SPARK_DRIVER_MEMORY", "4g")
    executor_memory = _env("SPARK_EXECUTOR_MEMORY", driver_memory)
    driver_max_result = _env("SPARK_DRIVER_MAX_RESULT_SIZE", "2g")
    shuffle_partitions = _env_int("SPARK_SQL_SHUFFLE_PARTITIONS", 64)
    default_parallelism = _env_int("SPARK_DEFAULT_PARALLELISM", 64)
    max_partition_bytes = _env("SPARK_SQL_FILES_MAX_PARTITION_BYTES", "134217728")  # 128 MiB

    builder = (
        SparkSession.builder.appName(app_name)
        .config("spark.sql.session.timeZone", "UTC")
        .config("spark.driver.memory", driver_memory)
        .config("spark.executor.memory", executor_memory)
        .config("spark.driver.maxResultSize", driver_max_result)
        .config("spark.sql.shuffle.partitions", str(shuffle_partitions))
        .config("spark.default.parallelism", str(default_parallelism))
        .config("spark.sql.files.maxPartitionBytes", max_partition_bytes)
        # More resilient execution on skewed / variable inputs.
        .config("spark.sql.adaptive.enabled", "true")
        .config("spark.sql.adaptive.coalescePartitions.enabled", "true")
        .config("spark.local.dir", str(spark_local_dir))
        .config("spark.pyspark.python", python_exe)
        .config("spark.pyspark.driver.python", python_exe)
        # Better tracebacks when Python workers crash.
        .config("spark.python.worker.faulthandler.enabled", "true")
        .config("spark.sql.execution.pyspark.udf.faulthandler.enabled", "true")
        # Avoid importing/using Arrow in worker processes unless explicitly needed.
        .config("spark.sql.execution.arrow.pyspark.enabled", "false")
        # Ensure Python workers have faulthandler enabled at interpreter level too.
        .config("spark.executorEnv.PYTHONFAULTHANDLER", "1")
        .config("spark.executorEnv.PYSPARK_PYTHON", python_exe)
    )

    return builder.getOrCreate()
