from __future__ import annotations

import argparse
import sys
from pathlib import Path

sys.path.insert(0, str((Path(__file__).resolve().parent / "src")))

import pandas as pd

from config import PROCESSED_DIR
from src.capacity_score import score_substations
from src.crosswalk import build_crosswalk, load_zone_reference
from src.zone_lookup import assign_zones
from src.fetch_ercot import fetch_all_ercot_data
from src.fetch_projects import load_project_data
from src.fetch_queue import load_queue_data
from src.fetch_substations import get_tx_substations
from src.hosting_band import apply_hosting_bands
from src.map_output import build_capacity_map
from src.publish_site import publish_docs


def parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser(description="Build the ERCOT substation capacity index.")
    parser.add_argument("--refresh-cache", action="store_true", help="Force refetch of cached raw data.")
    parser.add_argument("--lookback", type=int, default=7, help="Lookback window in days for ERCOT data.")
    parser.add_argument("--min-voltage", type=float, default=69, help="Minimum kV threshold for rendered map markers.")
    parser.add_argument("--live", action="store_true", help="Stub for future ERCOT API integration.")
    parser.add_argument("--publish-docs", action="store_true", help="Sync latest outputs into docs/ for GitHub Pages.")
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

    print("3b. Assigning ERCOT zones...")
    settlement_zone_ref = load_zone_reference()
    zone_series = assign_zones(substations_df, crosswalk_df, settlement_zone_ref)
    zone_counts = zone_series[zone_series != ""].value_counts().to_dict()
    print(f"   Zone assignments: {zone_counts}")

    print("4. Scoring substations...")
    scored_df = score_substations(substations_df, crosswalk_df, ercot_bundle, zone_series=zone_series)
    print("5. Applying Phase 2 hosting bands...")
    queue_bundle = load_queue_data()
    project_bundle = load_project_data()
    scored_df = apply_hosting_bands(scored_df, queue_bundle.df, project_bundle.df)
    print(
        f"   Queue files: {len(queue_bundle.source_files)} | "
        f"Project files: {len(project_bundle.source_files)}"
    )
    PROCESSED_DIR.mkdir(parents=True, exist_ok=True)
    csv_path = PROCESSED_DIR / "substation_capacity_scores.csv"
    scored_df.to_csv(csv_path, index=False)
    tier_counts = scored_df["TIER"].value_counts(dropna=False).to_dict()
    print(f"   Tier distribution: {tier_counts}")
    print(f"   Hosting bands: {scored_df['hosting_band'].value_counts(dropna=False).to_dict()}")

    print("6. Generating interactive map...")
    timestamp = pd.Timestamp.utcnow().isoformat()
    map_path = build_capacity_map(scored_df, timestamp=timestamp, min_voltage=args.min_voltage)
    print(f"   Map written to: {map_path}")

    print("7. Export complete.")
    print(f"   CSV written to: {csv_path}")
    if args.publish_docs:
        published = publish_docs()
        print("8. Published docs for GitHub Pages.")
        print(f"   Docs map: {published['map']}")
        print(f"   Docs CSV: {published['scores_csv']}")


if __name__ == "__main__":
    main()
