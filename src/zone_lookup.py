from __future__ import annotations

import pandas as pd

# ERCOT load zone boundaries follow county lines but are well-approximated by geography.
# Priority: bus-match path (exact) → lat/lon geographic fallback.

# NOIE = Non-Opt-In Entity zones (CPS/AEP/LCRA/AEN service territories within ERCOT footprint,
# plus Panhandle counties connected to SPP rather than the main ERCOT transmission system).
ERCOT_ZONES = {"LZ_NORTH", "LZ_SOUTH", "LZ_WEST", "LZ_HOUSTON"}


def _zone_from_coords(lat: float, lon: float) -> str:
    """Approximate ERCOT load zone from lat/lon using geographic boundaries."""
    # Panhandle / SPP-connected (north of ~34.5°N and west of 100°W)
    if lat > 34.5 and lon < -100.0:
        return "NOIE"
    # El Paso / far-west (WECC, outside ERCOT)
    if lon < -104.0:
        return "NOIE"
    # West Texas / Permian Basin / Trans-Pecos (LZ_WEST)
    if lon < -100.0:
        return "LZ_WEST"
    # Houston metro — tight bounding box around the 6 core counties
    # (Harris, Fort Bend, Montgomery, Galveston, Waller, Chambers)
    if 29.3 <= lat <= 30.4 and -96.2 <= lon <= -94.8:
        return "LZ_HOUSTON"
    # South Texas: Rio Grande Valley, Corpus Christi, San Antonio, Austin corridor
    if lat < 30.5:
        return "LZ_SOUTH"
    # Default: North / Central / East Texas (DFW, Waco, East Texas)
    return "LZ_NORTH"


def assign_zones(
    substations_df: pd.DataFrame,
    crosswalk_df: pd.DataFrame,
    settlement_zone_df: pd.DataFrame,
) -> pd.Series:
    """
    Return a Series of ERCOT zone strings indexed like substations_df.

    Priority:
      1. Bus-match path: crosswalk_df ercot_bus → settlement_zone_df SETTLEMENT_LOAD_ZONE
      2. Geographic fallback: lat/lon → _zone_from_coords
    """
    result = pd.Series("", index=substations_df.index, dtype=str)

    # Step 1: bus-matched substations get their zone from the settlement reference
    if not crosswalk_df.empty and not settlement_zone_df.empty:
        bus_zone: dict[str, str] = (
            settlement_zone_df[["ELECTRICAL_BUS", "SETTLEMENT_LOAD_ZONE"]]
            .dropna()
            .drop_duplicates("ELECTRICAL_BUS")
            .set_index("ELECTRICAL_BUS")["SETTLEMENT_LOAD_ZONE"]
            .to_dict()
        )
        sub_zone: dict[str, str] = {}
        for _, row in crosswalk_df.iterrows():
            bus = str(row["ercot_bus"])
            sub = str(row["substation_name"])
            zone = bus_zone.get(bus, "")
            if zone and sub not in sub_zone:
                sub_zone[sub] = zone

        matched = substations_df["NAME"].isin(sub_zone)
        result[matched] = substations_df.loc[matched, "NAME"].map(sub_zone)

    # Step 2: geographic fallback for everything without a bus-matched zone
    unresolved = result == ""
    if unresolved.any():
        for idx in substations_df.index[unresolved]:
            lat = substations_df.at[idx, "LATITUDE"]
            lon = substations_df.at[idx, "LONGITUDE"]
            if pd.notna(lat) and pd.notna(lon):
                result[idx] = _zone_from_coords(float(lat), float(lon))

    return result
