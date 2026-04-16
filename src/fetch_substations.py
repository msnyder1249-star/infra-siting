from __future__ import annotations

from pathlib import Path
from typing import Any

import pandas as pd

from config import LEGACY_OUTPUT_DIR, RAW_DIR
from src.utils import (
    cache_is_fresh,
    clean_name,
    ensure_directory,
    parse_lat_long,
    parse_voltage_values,
    point_in_polygon,
    request_with_retries,
    requests_session,
    TEXAS_POLYGON,
)

HIFLD_URL = (
    "https://services1.arcgis.com/Hp6G80Pky0om7QvQ/arcgis/rest/services/"
    "Electric_Substations/FeatureServer/0/query"
)
RAW_OUTPUT_PATH = RAW_DIR / "tx_substations.csv"
LEGACY_SUBSTATIONS_PATH = LEGACY_OUTPUT_DIR / "texas_private_substations.csv"

OUTPUT_COLUMNS = [
    "NAME",
    "CITY",
    "COUNTY",
    "STATE",
    "LATITUDE",
    "LONGITUDE",
    "LINES",
    "MAX_VOLT",
    "MIN_VOLT",
    "OWNER",
    "TYPE",
    "STATUS",
    "source_dataset",
]


def _normalize_legacy_substations(path: Path) -> pd.DataFrame:
    if not path.exists():
        return pd.DataFrame(columns=OUTPUT_COLUMNS)

    legacy_df = pd.read_csv(path)
    rows: list[dict[str, Any]] = []
    for row in legacy_df.to_dict(orient="records"):
        latitude, longitude = parse_lat_long(row.get("lat_long"))
        voltages = parse_voltage_values(row.get("voltage_kv"))
        notes = str(row.get("notes", ""))
        substation_type = ""
        if "substation_type=" in notes:
            substation_type = notes.split("substation_type=", 1)[1].split(";", 1)[0].strip()

        rows.append(
            {
                "NAME": row.get("name"),
                "CITY": None,
                "COUNTY": row.get("county"),
                "STATE": "TX",
                "LATITUDE": latitude,
                "LONGITUDE": longitude,
                "LINES": 1,
                "MAX_VOLT": max(voltages) if voltages else None,
                "MIN_VOLT": min(voltages) if voltages else None,
                "OWNER": row.get("owner_operator"),
                "TYPE": substation_type or None,
                "STATUS": "IN SERVICE",
                "source_dataset": row.get("source", "legacy"),
            }
        )

    df = pd.DataFrame(rows, columns=OUTPUT_COLUMNS)
    return _finalize_substation_frame(df)


def _fetch_hifld_substations() -> pd.DataFrame:
    session = requests_session()
    response = request_with_retries(
        session,
        "GET",
        HIFLD_URL,
        params={"where": "STATE='TX'", "outFields": "*", "f": "json"},
    )
    payload = response.json()
    features = payload.get("features", [])
    rows = [feature.get("attributes", {}) for feature in features]
    df = pd.DataFrame(rows)
    if df.empty:
        return pd.DataFrame(columns=OUTPUT_COLUMNS)

    df = df.rename(columns=str.upper)
    for column in OUTPUT_COLUMNS:
        if column not in df.columns:
            df[column] = None
    df["source_dataset"] = "HIFLD"
    return _finalize_substation_frame(df[OUTPUT_COLUMNS])


def _finalize_substation_frame(df: pd.DataFrame) -> pd.DataFrame:
    if df.empty:
        return pd.DataFrame(columns=OUTPUT_COLUMNS)

    df = df.copy()
    df = df[df["STATUS"].fillna("").str.upper().eq("IN SERVICE")]
    df["LINES"] = pd.to_numeric(df["LINES"], errors="coerce").fillna(1)
    df = df[df["LINES"] >= 1]
    df["MAX_VOLT"] = pd.to_numeric(df["MAX_VOLT"], errors="coerce")
    df["MIN_VOLT"] = pd.to_numeric(df["MIN_VOLT"], errors="coerce")
    df["LATITUDE"] = pd.to_numeric(df["LATITUDE"], errors="coerce")
    df["LONGITUDE"] = pd.to_numeric(df["LONGITUDE"], errors="coerce")
    df["is_texas_point"] = [
        point_in_polygon(lat, lon, TEXAS_POLYGON) if pd.notna(lat) and pd.notna(lon) else True
        for lat, lon in zip(df["LATITUDE"], df["LONGITUDE"])
    ]
    df = df[df["is_texas_point"]]
    df["clean_name"] = df["NAME"].map(clean_name)
    df["coord_key"] = (
        df["LATITUDE"].round(4).astype(str).fillna("")
        + "|"
        + df["LONGITUDE"].round(4).astype(str).fillna("")
    )
    df = df.sort_values(["source_dataset", "MAX_VOLT"], ascending=[True, False], na_position="last")
    df = df.drop_duplicates(subset=["clean_name", "coord_key"], keep="first")
    df = df.drop(columns=["clean_name", "coord_key", "is_texas_point"])
    return df.reset_index(drop=True)[OUTPUT_COLUMNS]


def get_tx_substations(refresh_cache: bool = False) -> pd.DataFrame:
    ensure_directory(RAW_OUTPUT_PATH.parent)
    if RAW_OUTPUT_PATH.exists() and cache_is_fresh(RAW_OUTPUT_PATH) and not refresh_cache:
        return pd.read_csv(RAW_OUTPUT_PATH)

    legacy_df = _normalize_legacy_substations(LEGACY_SUBSTATIONS_PATH)
    hifld_df = pd.DataFrame(columns=OUTPUT_COLUMNS)
    try:
        hifld_df = _fetch_hifld_substations()
    except Exception as exc:  # pragma: no cover - network failure path
        print(f"Warning: HIFLD fetch failed, continuing with existing Texas substations only ({exc}).")

    frames = [df for df in [legacy_df, hifld_df] if not df.empty]
    combined = pd.concat(frames, ignore_index=True) if frames else pd.DataFrame(columns=OUTPUT_COLUMNS)
    combined = _finalize_substation_frame(combined)
    combined.to_csv(RAW_OUTPUT_PATH, index=False)
    return combined
