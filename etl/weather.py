from __future__ import annotations

import json
import logging
import time
from dataclasses import dataclass
from pathlib import Path
from typing import Any, Dict, Optional, Tuple

import requests
from requests.adapters import HTTPAdapter
from urllib3.util.retry import Retry

logger = logging.getLogger(__name__)


@dataclass(frozen=True)
class WeatherSnapshot:
    weather_main: str
    weather_description: str


def _session() -> requests.Session:
    sess = requests.Session()
    retry = Retry(
        total=3,
        backoff_factor=0.5,
        status_forcelist=(429, 500, 502, 503, 504),
        allowed_methods=("GET",),
        raise_on_status=False,
    )
    adapter = HTTPAdapter(max_retries=retry)
    sess.mount("https://", adapter)
    sess.mount("http://", adapter)
    return sess


def _load_cache(path: Path) -> Dict[str, Any]:
    if not path.exists():
        return {}
    try:
        return json.loads(path.read_text(encoding="utf-8"))
    except Exception:
        logger.warning("Weather cache read failed; starting fresh: %s", path)
        return {}


def _save_cache(path: Path, cache: Dict[str, Any]) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    path.write_text(json.dumps(cache, indent=2, sort_keys=True), encoding="utf-8")


def _bucket(lat: float, lon: float, epoch_hour: int) -> str:
    # Avoid too many unique keys: round geo, bucket by hour.
    return f"{round(lat, 2)}|{round(lon, 2)}|{epoch_hour}"


def get_weather_snapshot(
    *,
    lat: float,
    lon: float,
    epoch_hour: int,
    api_key: Optional[str],
    base_url: str,
    cache_path: Path,
    allow_mock: bool = True,
) -> WeatherSnapshot:
    """
    Fetch current weather for a lat/lon (cached) or generate a deterministic mock.

    `epoch_hour` should be an integer representing UTC hours since epoch, used for caching.
    """

    cache = _load_cache(cache_path)
    key = _bucket(lat, lon, epoch_hour)
    if key in cache:
        return WeatherSnapshot(**cache[key])

    if not api_key:
        if not allow_mock:
            raise RuntimeError("OpenWeatherMap API key not set and mocks disabled.")
        snap = mock_weather(epoch_hour)
        cache[key] = snap.__dict__
        _save_cache(cache_path, cache)
        return snap

    url = f"{base_url.rstrip('/')}/weather"
    params = {"lat": lat, "lon": lon, "appid": api_key, "units": "metric"}

    sess = _session()
    start = time.time()
    resp = sess.get(url, params=params, timeout=10)
    elapsed = time.time() - start
    if resp.status_code != 200:
        logger.warning("Weather API error %s in %.2fs; using mock. Body: %s", resp.status_code, elapsed, resp.text[:200])
        snap = mock_weather(epoch_hour)
        cache[key] = snap.__dict__
        _save_cache(cache_path, cache)
        return snap

    body = resp.json()
    weather = (body.get("weather") or [{}])[0] or {}
    snap = WeatherSnapshot(
        weather_main=str(weather.get("main") or "Clear"),
        weather_description=str(weather.get("description") or "clear sky"),
    )
    cache[key] = snap.__dict__
    _save_cache(cache_path, cache)
    return snap


def mock_weather(epoch_hour: int) -> WeatherSnapshot:
    # Deterministic mock: "worse" weather more likely during afternoon/evening.
    hour = epoch_hour % 24
    if 16 <= hour <= 20:
        return WeatherSnapshot(weather_main="Rain", weather_description="light rain")
    if 7 <= hour <= 9:
        return WeatherSnapshot(weather_main="Clouds", weather_description="broken clouds")
    return WeatherSnapshot(weather_main="Clear", weather_description="clear sky")

