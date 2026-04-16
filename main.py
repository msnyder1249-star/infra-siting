from __future__ import annotations

import argparse
import sys
from pathlib import Path

sys.path.insert(0, str((Path(__file__).resolve().parent / "src")))

import pandas as pd

from config import PROCESSED_DIR
from src.capacity_score import score_substations
from src.crosswalk import build_crosswalk
from src.fetch_ercot import fetch_all_ercot_data
from src.fetch_substations import get_tx_substations
from src.map_output import build_capacity_map


def parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser(description="Build the ERCOT substation capacity index.")
    parser.add_argument("--refresh-cache", action="store_true", help="Force refetch of cached raw data.")
    parser.add_argument("--lookback", type=int, default=7, help="Lookback window in days for ERCOT data.")
    parser.add_argument("--min-voltage", type=float, default=0, help="Minimum kV threshold for rendered map markers.")
    parser.add_argument("--live", action="store_true", help="Stub for future ERCOT API integration.")
    return parser.parse_args()


def main() -> None:
    args = parse_args()
    print("1. Fetching Texas substations...")
    substations_df = get_tx_substations(refresh_cache=args.refresh_cache)
    print(f"   Substations loaded: {len(substations_df):,}")

    print("2. Fetching ERCOT data...")
    ercot_bundle = fetch_all_ercot_data(
        lookback_days=args.lookback,
        refresh_cache=args.refresh_cache,
        live=args.live,
    )
    lmp_rows = len(ercot_bundle.lmp_by_bus)
    shadow_rows = len(ercot_bundle.shadow_prices)
    rtmgr_rows = len(ercot_bundle.rtmgr)
    print(f"   ERCOT source: {ercot_bundle.data_source} | LMP rows={lmp_rows:,}, shadow rows={shadow_rows:,}, RTMGR rows={rtmgr_rows:,}")
    print(f"   {ercot_bundle.message}")
    for dataset_name, date_range in ercot_bundle.date_ranges.items():
        print(f"   {dataset_name} date range: {date_range}")

    print("3. Building bus-to-substation crosswalk...")
    crosswalk_df = build_crosswalk(substations_df, ercot_bundle, refresh_cache=args.refresh_cache)
    match_rate = 0.0
    if lmp_rows:
        unique_buses = ercot_bundle.lmp_by_bus["busName"].dropna().nunique()
        match_rate = (crosswalk_df["ercot_bus"].nunique() / unique_buses * 100) if unique_buses else 0.0
    print(f"   Crosswalk rows: {len(crosswalk_df):,} | Match rate: {match_rate:.1f}%")

    print("4. Scoring substations...")
    scored_df = score_substations(substations_df, crosswalk_df, ercot_bundle)
    PROCESSED_DIR.mkdir(parents=True, exist_ok=True)
    csv_path = PROCESSED_DIR / "substation_capacity_scores.csv"
    scored_df.to_csv(csv_path, index=False)
    tier_counts = scored_df["TIER"].value_counts(dropna=False).to_dict()
    print(f"   Tier distribution: {tier_counts}")

    print("5. Generating interactive map...")
    timestamp = pd.Timestamp.utcnow().isoformat()
    map_path = build_capacity_map(scored_df, timestamp=timestamp, min_voltage=args.min_voltage)
    print(f"   Map written to: {map_path}")

    print("6. Export complete.")
    print(f"   CSV written to: {csv_path}")


if __name__ == "__main__":
    main()
