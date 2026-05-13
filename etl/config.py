from __future__ import annotations

import os
from dataclasses import dataclass
from pathlib import Path
from typing import Optional

from dotenv import load_dotenv


@dataclass(frozen=True)
class PipelineConfig:
    project_root: Path
    raw_dir: Path
    processed_dir: Path
    duckdb_path: Path
    weather_cache_path: Path
    openweathermap_api_key: Optional[str]
    openweathermap_base_url: str
    spark_app_name: str


def _first_env(*names: str) -> Optional[str]:
    for name in names:
        value = os.getenv(name)
        if value:
            return value
    return None


def load_config(project_root: Optional[Path] = None) -> PipelineConfig:
    root = project_root or Path(__file__).resolve().parents[1]

    # Load .env from repo root if present
    load_dotenv(dotenv_path=root / ".env", override=False)

    raw_dir = root / "data" / "raw"
    processed_dir = root / "data" / "processed"
    duckdb_path = root / "duckdb" / "transport_analytics.duckdb"
    weather_cache_path = processed_dir / "weather_cache.json"

    api_key = _first_env(
        "OPENWEATHERMAP_API_KEY",
        "OPEN_WEATHER_MAP_API_KEY",
        "SmartPublicTransport_weather_api_key",
    )

    return PipelineConfig(
        project_root=root,
        raw_dir=raw_dir,
        processed_dir=processed_dir,
        duckdb_path=duckdb_path,
        weather_cache_path=weather_cache_path,
        openweathermap_api_key=api_key,
        openweathermap_base_url=os.getenv("OPENWEATHERMAP_BASE_URL", "https://api.openweathermap.org/data/2.5"),
        spark_app_name=os.getenv("SPARK_APP_NAME", "transport-analytics-pipeline"),
    )

