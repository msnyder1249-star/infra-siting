from __future__ import annotations

import json
import math
import re
import time
from datetime import datetime, timedelta, timezone
from pathlib import Path
from typing import Any, Iterable

import numpy as np
import pandas as pd
import requests

from config import CACHE_TTL_SECONDS, DEFAULT_TIMEOUT_SECONDS

TEXAS_POLYGON = [
    (-106.6456, 31.8958),
    (-106.5070, 31.7863),
    (-106.3825, 31.7547),
    (-106.2332, 31.7936),
    (-105.9989, 31.3934),
    (-105.6293, 31.3945),
    (-105.0031, 31.0010),
    (-104.7050, 30.6620),
    (-104.4564, 29.9306),
    (-104.2500, 29.7323),
    (-103.1126, 28.9956),
    (-102.4805, 29.7548),
    (-101.4917, 29.7599),
    (-100.9577, 29.3911),
    (-100.1106, 28.1100),
    (-99.5204, 27.5400),
    (-97.4206, 25.8371),
    (-96.7990, 27.4200),
    (-96.0000, 28.5000),
    (-95.2190, 29.0000),
    (-94.7000, 29.6000),
    (-93.5080, 29.7000),
    (-93.5850, 30.2000),
    (-93.8438, 31.0012),
    (-94.0429, 31.9779),
    (-94.4846, 33.5666),
    (-94.0431, 33.5693),
    (-100.0000, 36.5000),
    (-103.0000, 36.5000),
    (-106.6456, 31.8958),
]


def ensure_directory(path: Path) -> None:
    path.mkdir(parents=True, exist_ok=True)


def utc_now() -> datetime:
    return datetime.now(timezone.utc)


def iso_timestamp() -> str:
    return utc_now().isoformat()


def cache_is_fresh(path: Path, ttl_seconds: int = CACHE_TTL_SECONDS) -> bool:
    if not path.exists():
        return False
    age_seconds = time.time() - path.stat().st_mtime
    return age_seconds < ttl_seconds


def write_json(path: Path, payload: Any) -> None:
    ensure_directory(path.parent)
    path.write_text(json.dumps(payload, indent=2, default=str), encoding="utf-8")


def read_json(path: Path, default: Any = None) -> Any:
    if not path.exists():
        return default
    return json.loads(path.read_text(encoding="utf-8"))


def clean_name(value: Any) -> str:
    if value is None or (isinstance(value, float) and math.isnan(value)):
        return ""
    text = str(value).upper().strip()
    text = re.sub(r"[^A-Z0-9]+", " ", text)
    return re.sub(r"\s+", " ", text).strip()


def token_set(value: Any) -> set[str]:
    return {token for token in clean_name(value).split(" ") if token}


def parse_lat_long(value: Any) -> tuple[float | None, float | None]:
    if value is None or (isinstance(value, float) and math.isnan(value)):
        return None, None
    parts = [part.strip() for part in str(value).split(",")]
    if len(parts) != 2:
        return None, None
    try:
        return float(parts[0]), float(parts[1])
    except ValueError:
        return None, None


def parse_voltage_values(value: Any) -> list[float]:
    if value is None or (isinstance(value, float) and math.isnan(value)):
        return []
    numbers = re.findall(r"\d+(?:\.\d+)?", str(value))
    parsed = [float(number) for number in numbers]
    if not parsed:
        return []
    if max(parsed) > 5000:
        return [round(number / 1000.0, 3) for number in parsed]
    return parsed


def coerce_numeric(series: pd.Series) -> pd.Series:
    return pd.to_numeric(series, errors="coerce")


def bounded(value: float, lower: float, upper: float) -> float:
    return max(lower, min(upper, value))


def safe_mean(values: Iterable[float]) -> float | None:
    values_list = [value for value in values if pd.notna(value)]
    if not values_list:
        return None
    return float(np.mean(values_list))


def lookback_dates(days: int) -> tuple[str, str]:
    end_date = utc_now().date()
    start_date = end_date - timedelta(days=max(days - 1, 0))
    return start_date.isoformat(), end_date.isoformat()


def requests_session() -> requests.Session:
    session = requests.Session()
    session.headers.update({"User-Agent": "ercot-capacity/1.0"})
    return session


def request_with_retries(
    session: requests.Session,
    method: str,
    url: str,
    *,
    retries: int = 3,
    backoff_seconds: float = 1.5,
    timeout: int = DEFAULT_TIMEOUT_SECONDS,
    **kwargs: Any,
) -> requests.Response:
    last_error: Exception | None = None
    for attempt in range(1, retries + 1):
        try:
            response = session.request(method, url, timeout=timeout, **kwargs)
            response.raise_for_status()
            return response
        except Exception as exc:  # pragma: no cover - network failure path
            last_error = exc
            if attempt == retries:
                raise
            time.sleep(backoff_seconds ** attempt)
    raise RuntimeError(f"Request failed for {url}: {last_error}")


def point_in_polygon(latitude: float | None, longitude: float | None, polygon: list[tuple[float, float]]) -> bool:
    if latitude is None or longitude is None or pd.isna(latitude) or pd.isna(longitude):
        return False
    x = float(longitude)
    y = float(latitude)
    inside = False
    j = len(polygon) - 1
    for i in range(len(polygon)):
        xi, yi = polygon[i]
        xj, yj = polygon[j]
        intersects = ((yi > y) != (yj > y)) and (
            x < ((xj - xi) * (y - yi) / ((yj - yi) or 1e-12)) + xi
        )
        if intersects:
            inside = not inside
        j = i
    return inside
