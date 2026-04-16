from __future__ import annotations

import re
from typing import Any

import pandas as pd

from src.utils import clean_name


def _canonical_root(value: Any) -> str:
    text = clean_name(value)
    text = re.sub(r"\b(?:SUBSTATION|SWITCHYARD|SWITCHING STATION|STATION)\b", " ", text)
    text = re.sub(r"\b(?:UNIT|CC|CT|GT|ST)\s*\d+\b", " ", text)
    text = re.sub(r"\b(?:RN|ALL|LOAD|BESS|ESS|SOLAR|WIND)\b", " ", text)
    text = re.sub(r"\s+", " ", text).strip()
    return text


def _normalize_external_frame(df: pd.DataFrame) -> pd.DataFrame:
    if df.empty:
        return df
    frame = df.copy()
    frame.columns = [str(column) for column in frame.columns]
    likely_name_columns = [
        column
        for column in frame.columns
        if any(token in column.lower() for token in ["station", "substation", "node", "resource", "bus", "point", "project", "facility"])
    ]
    if not likely_name_columns:
        likely_name_columns = list(frame.columns[:3])
    frame["match_blob"] = frame[likely_name_columns].fillna("").astype(str).agg(" ".join, axis=1)
    frame["match_key"] = frame["match_blob"].map(_canonical_root)
    return frame


def apply_hosting_bands(
    scored_df: pd.DataFrame,
    queue_df: pd.DataFrame,
    project_df: pd.DataFrame,
) -> pd.DataFrame:
    df = scored_df.copy()
    df["substation_key"] = df["NAME"].map(_canonical_root)

    queue_df = _normalize_external_frame(queue_df)
    project_df = _normalize_external_frame(project_df)

    if not queue_df.empty:
        queue_counts = queue_df[queue_df["match_key"] != ""].groupby("match_key").size().rename("queue_hits")
        df["queue_hits"] = df["substation_key"].map(queue_counts).fillna(0).astype(int)
    else:
        df["queue_hits"] = 0

    if not project_df.empty:
        project_counts = project_df[project_df["match_key"] != ""].groupby("match_key").size().rename("project_hits")
        df["project_hits"] = df["substation_key"].map(project_counts).fillna(0).astype(int)
    else:
        df["project_hits"] = 0

    df["upgrade_pressure"] = "medium"
    df.loc[df["project_hits"] >= 2, "upgrade_pressure"] = "high"
    df.loc[df["project_hits"] == 0, "upgrade_pressure"] = "low"

    df["hosting_band"] = "UNKNOWN"
    df["hosting_confidence"] = "LOW"
    df["primary_limiter"] = "unknown"

    for idx, row in df.iterrows():
        if row["TIER"] == "UNSCORED":
            df.at[idx, "hosting_band"] = "UNKNOWN"
            df.at[idx, "hosting_confidence"] = "LOW"
            df.at[idx, "primary_limiter"] = "unknown"
            continue

        score = float(row["CAPACITY_SCORE"]) if pd.notna(row["CAPACITY_SCORE"]) else 0.0
        max_voltage = float(row["MAX_VOLT"]) if pd.notna(row["MAX_VOLT"]) else 0.0
        queue_hits = int(row["queue_hits"])
        project_hits = int(row["project_hits"])
        shadow = float(row["shadow_price_nearby"]) if pd.notna(row["shadow_price_nearby"]) else 0.0
        constraint_hours = float(row["constraint_hours"]) if pd.notna(row["constraint_hours"]) else 0.0

        if score >= 80 and max_voltage >= 345 and queue_hits == 0 and shadow < 15:
            band = "300+"
        elif score >= 70 and max_voltage >= 138 and queue_hits <= 1:
            band = "150-300"
        elif score >= 50:
            band = "50-150"
        else:
            band = "0-50"

        if queue_hits >= 3:
            if band == "300+":
                band = "150-300"
            elif band == "150-300":
                band = "50-150"
            else:
                band = "0-50"

        limiter = "unknown"
        if queue_hits >= 3:
            limiter = "queue"
        elif shadow >= 25 or constraint_hours >= 4:
            limiter = "thermal"
        elif max_voltage < 138:
            limiter = "station_limit"
        elif row["lmp_hub_spread"] >= 10:
            limiter = "voltage"

        confidence = "MEDIUM"
        if row["match_confidence"] >= 0.97 and row["data_source"] != "unscored":
            confidence = "HIGH"
        if queue_hits == 0 and project_hits == 0:
            confidence = "LOW" if confidence == "MEDIUM" else confidence

        df.at[idx, "hosting_band"] = band
        df.at[idx, "hosting_confidence"] = confidence
        df.at[idx, "primary_limiter"] = limiter

    return df.drop(columns=["substation_key"])
