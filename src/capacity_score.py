from __future__ import annotations

from typing import Any

import numpy as np
import pandas as pd

from src.utils import clean_name, iso_timestamp

HUBS = {"HB_NORTH", "HB_SOUTH", "HB_WEST", "HB_HOUSTON"}
LZ_ZONES = {"LZ_NORTH", "LZ_SOUTH", "LZ_WEST", "LZ_HOUSTON"}
ZONE_ESTIMATE_HAIRCUT = 0.80


def _series_or_default(df: pd.DataFrame, column: str, default: Any = None) -> pd.Series:
    if column in df.columns:
        return df[column]
    return pd.Series([default] * len(df), index=df.index)


def _tier_from_score(score: float | None, data_source: str) -> str:
    if data_source == "unscored" or pd.isna(score):
        return "UNSCORED"
    # Zone estimates capped at MARGINAL — never AVAILABLE (conservative screening)
    if data_source == "zone_estimate":
        return "MARGINAL" if score >= 40 else "CONSTRAINED"
    if score >= 70:
        return "AVAILABLE"
    if score >= 40:
        return "MARGINAL"
    return "CONSTRAINED"


def score_substations(
    substations_df: pd.DataFrame,
    crosswalk_df: pd.DataFrame,
    ercot_bundle: Any,
    zone_series: "pd.Series | None" = None,
) -> pd.DataFrame:
    substations = substations_df.copy()
    substations["substation_key"] = substations["NAME"].map(clean_name)

    lmp_df = ercot_bundle.lmp_by_bus.copy()
    if not lmp_df.empty:
        lmp_df["busName"] = lmp_df["busName"].astype(str)
        lmp_df["LMP"] = pd.to_numeric(lmp_df["LMP"], errors="coerce")

    resource_node_lmp_df = ercot_bundle.resource_node_lmp.copy()
    if not resource_node_lmp_df.empty:
        resource_node_lmp_df = resource_node_lmp_df.rename(
            columns={"SCEDTimestamp": "deliveryDate", "SettlementPoint": "busName"}
        )
        resource_node_lmp_df["deliveryDate"] = pd.to_datetime(
            resource_node_lmp_df["deliveryDate"], errors="coerce"
        )
        resource_node_lmp_df["hourEnding"] = resource_node_lmp_df["deliveryDate"]
        resource_node_lmp_df["busName"] = resource_node_lmp_df["busName"].astype(str)
        resource_node_lmp_df["LMP"] = pd.to_numeric(resource_node_lmp_df["LMP"], errors="coerce")

    shadow_df = ercot_bundle.shadow_prices.copy()
    if not shadow_df.empty:
        shadow_df["shadowPrice"] = pd.to_numeric(shadow_df["shadowPrice"], errors="coerce").fillna(0)
        shadow_df["fromStationKey"] = _series_or_default(shadow_df, "fromStation", "").map(clean_name)
        shadow_df["toStationKey"] = _series_or_default(shadow_df, "toStation", "").map(clean_name)

    hub_avg = None
    hub_prices = pd.DataFrame(columns=["timestamp", "hub_avg"])
    settlement_prices_df = ercot_bundle.settlement_point_prices.copy()
    if not settlement_prices_df.empty:
        settlement_prices_df["SettlementPointName"] = settlement_prices_df["SettlementPointName"].astype(str)
        settlement_prices_df["SettlementPointPrice"] = pd.to_numeric(
            settlement_prices_df["SettlementPointPrice"], errors="coerce"
        )
        hub_rows = settlement_prices_df[
            settlement_prices_df["SettlementPointName"].isin(HUBS)
        ][["timestamp", "SettlementPointPrice"]].dropna()
        if not hub_rows.empty:
            hub_prices = (
                hub_rows.groupby("timestamp", as_index=False)
                .agg(hub_avg=("SettlementPointPrice", "mean"))
                .sort_values("timestamp")
            )
            hub_avg = float(hub_prices["hub_avg"].mean())

    if hub_avg is None:
        rtmgr_df = ercot_bundle.rtmgr.copy()
        if not rtmgr_df.empty:
            rtmgr_df["settlementPoint"] = rtmgr_df["settlementPoint"].astype(str)
            rtmgr_df["LMP"] = pd.to_numeric(rtmgr_df["LMP"], errors="coerce")
            hub_rows = rtmgr_df[rtmgr_df["settlementPoint"].isin(HUBS)]
            if not hub_rows.empty:
                hub_avg = float(hub_rows["LMP"].mean())

    if hub_avg is None and not lmp_df.empty:
        hub_avg = float(lmp_df["LMP"].mean())

    # Zone-level LMP stats for fallback scoring of unmatched substations
    zone_lmp_stats: dict[str, tuple[float, float]] = {}
    if not settlement_prices_df.empty:
        lz_rows = settlement_prices_df[
            settlement_prices_df["SettlementPointName"].isin(LZ_ZONES)
        ]
        if not lz_rows.empty:
            for zone, grp in lz_rows.groupby("SettlementPointName"):
                zone_lmp_stats[str(zone)] = (
                    float(grp["SettlementPointPrice"].mean()),
                    float(grp["SettlementPointPrice"].std(ddof=0)),
                )

    crosswalk = crosswalk_df.copy()
    if crosswalk.empty:
        crosswalk = pd.DataFrame(columns=["ercot_bus", "substation_name", "match_confidence"])
    crosswalk["substation_key"] = crosswalk["substation_name"].map(clean_name)
    crosswalk["match_confidence"] = pd.to_numeric(crosswalk["match_confidence"], errors="coerce")

    bus_stats = pd.DataFrame(columns=["substation_key", "lmp_avg", "lmp_std", "ercot_bus_matched"])
    lmp_candidates = [lmp_df, resource_node_lmp_df]
    for candidate_df in lmp_candidates:
        if candidate_df.empty or crosswalk.empty:
            continue
        merged_lmp = candidate_df.merge(crosswalk, left_on="busName", right_on="ercot_bus", how="inner")
        if merged_lmp.empty:
            continue
        if not hub_prices.empty:
            merged_lmp["timestamp"] = pd.to_datetime(merged_lmp["deliveryDate"], errors="coerce").dt.floor("15min")
            merged_lmp = merged_lmp.merge(hub_prices, on="timestamp", how="left")
            merged_lmp["lmp_hub_spread_obs"] = (merged_lmp["LMP"] - merged_lmp["hub_avg"]).abs()
        bus_stats = (
            merged_lmp.groupby("substation_key")
            .agg(
                lmp_avg=("LMP", "mean"),
                lmp_std=("LMP", "std"),
                lmp_hub_spread=("lmp_hub_spread_obs", "mean")
                if "lmp_hub_spread_obs" in merged_lmp.columns
                else ("LMP", lambda _: np.nan),
                ercot_bus_matched=("ercot_bus", lambda s: ", ".join(sorted(set(map(str, s))))),
                match_confidence=("match_confidence", "max"),
            )
            .reset_index()
        )
        break

    shadow_stats = pd.DataFrame(columns=["substation_key", "shadow_price_nearby", "constraint_hours"])
    if not shadow_df.empty:
        relevant = pd.concat(
            [
                shadow_df[["fromStationKey", "shadowPrice"]].rename(columns={"fromStationKey": "substation_key"}),
                shadow_df[["toStationKey", "shadowPrice"]].rename(columns={"toStationKey": "substation_key"}),
            ],
            ignore_index=True,
        )
        relevant = relevant[relevant["substation_key"] != ""]
        if not relevant.empty:
            relevant["positive_constraint"] = (relevant["shadowPrice"] > 0).astype(int)
            shadow_stats = (
                relevant.groupby("substation_key")
                .agg(
                    shadow_price_nearby=("shadowPrice", "max"),
                    constraint_hours=("positive_constraint", "sum"),
                )
                .reset_index()
            )

    scored = substations.merge(bus_stats, on="substation_key", how="left")
    scored = scored.merge(shadow_stats, on="substation_key", how="left")
    scored["match_confidence"] = scored["substation_key"].map(
        crosswalk.drop_duplicates("substation_key").set_index("substation_key")["match_confidence"]
        if not crosswalk.empty
        else {}
    )

    if "lmp_hub_spread" not in scored.columns:
        scored["lmp_hub_spread"] = np.nan
    scored["lmp_hub_spread"] = scored["lmp_hub_spread"].where(
        scored["lmp_hub_spread"].notna(),
        (scored["lmp_avg"] - hub_avg).abs() if hub_avg is not None else np.nan,
    )
    scored["lmp_std"] = pd.to_numeric(scored["lmp_std"], errors="coerce").fillna(0)
    scored["shadow_price_nearby"] = pd.to_numeric(scored["shadow_price_nearby"], errors="coerce").fillna(0)
    scored["constraint_hours"] = pd.to_numeric(scored["constraint_hours"], errors="coerce").fillna(0)
    scored["max_voltage"] = pd.to_numeric(scored["MAX_VOLT"], errors="coerce").fillna(0)
    scored["line_count"] = pd.to_numeric(scored["LINES"], errors="coerce").fillna(0)

    scored["lmp_score"] = (100 - (scored["lmp_hub_spread"] * 5)).clip(lower=0)
    scored["volatility_score"] = (100 - (scored["lmp_std"] * 2)).clip(lower=0)
    scored["constraint_score"] = (
        100 - (scored["shadow_price_nearby"] * 0.5) - (scored["constraint_hours"] * 3)
    ).clip(lower=0)
    scored["voltage_score"] = ((scored["max_voltage"] / 500.0) * 100).clip(lower=0, upper=100)

    has_ercot_match = scored["lmp_avg"].notna()
    scored["CAPACITY_SCORE"] = np.where(
        has_ercot_match,
        (
            (scored["lmp_score"] * 0.35)
            + (scored["volatility_score"] * 0.20)
            + (scored["constraint_score"] * 0.30)
            + (scored["voltage_score"] * 0.15)
        ).round(2),
        np.nan,
    )

    default_source = (
        ercot_bundle.data_source
        if ercot_bundle.data_source in {"live", "sample", "local_zip"}
        else "unscored"
    )
    scored["data_source"] = np.where(has_ercot_match, default_source, "unscored")
    scored["TIER"] = [
        _tier_from_score(score, data_source)
        for score, data_source in zip(scored["CAPACITY_SCORE"], scored["data_source"])
    ]

    # Assign ERCOT zone to all rows
    scored["ercot_zone"] = zone_series.values if zone_series is not None else ""

    # Zone-level fallback: score the UNSCORED rows that have a valid ERCOT zone
    if zone_lmp_stats and hub_avg is not None and zone_series is not None:
        unscored_mask = scored["data_source"] == "unscored"
        for idx in scored.index[unscored_mask]:
            zone = str(scored.at[idx, "ercot_zone"])
            if zone not in zone_lmp_stats:
                continue
            zone_avg, zone_std = zone_lmp_stats[zone]
            hub_spread = abs(zone_avg - hub_avg)
            lmp_score = max(0.0, 100.0 - hub_spread * 5.0)
            vol_score = max(0.0, 100.0 - zone_std * 2.0)
            constraint_score = 100.0  # no shadow price data; assume uncongested
            max_volt = float(scored.at[idx, "max_voltage"])
            volt_score = min(100.0, (max_volt / 500.0) * 100.0) if max_volt > 0 else 0.0
            raw = lmp_score * 0.35 + vol_score * 0.20 + constraint_score * 0.30 + volt_score * 0.15
            zone_score = round(raw * ZONE_ESTIMATE_HAIRCUT, 2)
            scored.at[idx, "lmp_avg"] = round(zone_avg, 2)
            scored.at[idx, "lmp_hub_spread"] = round(hub_spread, 2)
            scored.at[idx, "lmp_std"] = round(zone_std, 2)
            scored.at[idx, "CAPACITY_SCORE"] = zone_score
            scored.at[idx, "data_source"] = "zone_estimate"
        # Re-compute tier for updated rows
        scored["TIER"] = [
            _tier_from_score(score, ds)
            for score, ds in zip(scored["CAPACITY_SCORE"], scored["data_source"])
        ]

    scored["score_basis"] = scored["data_source"].map(
        lambda ds: "unscored" if ds == "unscored" else ("zone_estimate" if ds == "zone_estimate" else "bus_match")
    )
    scored["score_timestamp"] = iso_timestamp()

    output_columns = [
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
        "lmp_avg",
        "lmp_hub_spread",
        "lmp_std",
        "shadow_price_nearby",
        "constraint_hours",
        "CAPACITY_SCORE",
        "TIER",
        "ercot_bus_matched",
        "match_confidence",
        "data_source",
        "score_basis",
        "ercot_zone",
        "score_timestamp",
    ]
    return scored[output_columns].sort_values(["TIER", "CAPACITY_SCORE", "MAX_VOLT"], ascending=[True, False, False])
