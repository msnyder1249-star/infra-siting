from __future__ import annotations

from html import escape
from pathlib import Path

import folium
import pandas as pd

from config import OUTPUT_DIR

COLOR_BY_TIER = {
    "AVAILABLE": "#22c55e",
    "MARGINAL": "#f59e0b",
    "CONSTRAINED": "#ef4444",
    "UNSCORED": "#94a3b8",
}


def _marker_radius(max_voltage: float | None) -> float:
    if pd.isna(max_voltage):
        return 6
    return max(6, min(20, float(max_voltage) / 100.0))


def _score_bar(score: float | None) -> str:
    if pd.isna(score):
        width = 0
        label = "N/A"
    else:
        width = max(0, min(100, round(float(score))))
        label = f"{float(score):.1f}"
    return (
        "<div style='width:220px;background:#e2e8f0;border-radius:999px;height:10px;overflow:hidden;'>"
        f"<div style='width:{width}%;background:#2563eb;height:10px;'></div>"
        "</div>"
        f"<div style='margin-top:4px;font-weight:600;'>{label}</div>"
    )


def _popup_html(row: pd.Series) -> str:
    tier_color = COLOR_BY_TIER.get(row["TIER"], "#94a3b8")
    score_basis = str(row.get("score_basis", ""))
    zone = str(row.get("ercot_zone", ""))
    zone_label = f"{escape(zone)}" if zone else "unknown"
    if score_basis == "zone_estimate":
        zone_label += " <small style='color:#888'>(zone estimate)</small>"
    queue_hits = row.get("queue_hits", 0)
    return f"""
    <div style="min-width: 260px; font-family: Arial, sans-serif;">
      <div style="font-size: 16px; font-weight: 700; margin-bottom: 6px;">{escape(str(row['NAME']))}</div>
      <div><strong>Owner:</strong> {escape(str(row.get('OWNER', '')))}</div>
      <div><strong>County:</strong> {escape(str(row.get('COUNTY', '')))}</div>
      <div><strong>ERCOT Zone:</strong> {zone_label}</div>
      <div><strong>Voltage:</strong> {row.get('MAX_VOLT', 'N/A')} kV</div>
      <div><strong>Lines:</strong> {row.get('LINES', 'N/A')}</div>
      <div style="margin-top: 10px;"><strong>Capacity Score</strong></div>
      {_score_bar(row.get('CAPACITY_SCORE'))}
      <div style="margin-top: 8px;">
        <span style="display:inline-block;padding:3px 8px;border-radius:999px;background:{tier_color};color:white;font-weight:700;">
          {escape(str(row['TIER']))}
        </span>
      </div>
      <div style="margin-top: 8px;"><strong>Hosting band:</strong> {escape(str(row.get('hosting_band', 'UNKNOWN')))}</div>
      <div><strong>Confidence:</strong> {escape(str(row.get('hosting_confidence', 'LOW')))} | <strong>Limiter:</strong> {escape(str(row.get('primary_limiter', 'unknown')))}</div>
      <div style="margin-top: 8px;"><strong>LMP avg:</strong> {row.get('lmp_avg', 'N/A')}</div>
      <div><strong>Hub spread:</strong> {row.get('lmp_hub_spread', 'N/A')}</div>
      <div><strong>Shadow price:</strong> {row.get('shadow_price_nearby', 'N/A')}</div>
      <div style="margin-top: 8px; color: #475569;">
        <strong>Score basis:</strong> {escape(score_basis)} |
        <strong>Queue:</strong> ~{queue_hits} proj/sub in zone
      </div>
      <div style="color: #475569;"><strong>Data:</strong> {escape(str(row.get('data_source', '')))} | {escape(str(row.get('score_timestamp', '')))}</div>
    </div>
    """


def _add_marker(group: folium.FeatureGroup, row: pd.Series) -> None:
    is_estimate = str(row.get("score_basis", "")) == "zone_estimate"
    folium.CircleMarker(
        location=[row["LATITUDE"], row["LONGITUDE"]],
        radius=_marker_radius(row["MAX_VOLT"]),
        color=COLOR_BY_TIER.get(row["TIER"], "#94a3b8"),
        weight=1,
        fill=True,
        fill_opacity=0.5 if is_estimate else 0.8,
        popup=folium.Popup(_popup_html(row), max_width=320),
        tooltip=f"{row['NAME']} — {row['TIER']} — {row['CAPACITY_SCORE'] if pd.notna(row['CAPACITY_SCORE']) else 'N/A'}{'*' if is_estimate else ''}",
    ).add_to(group)


def build_capacity_map(
    scored_df: pd.DataFrame,
    *,
    timestamp: str,
    min_voltage: float = 0,
) -> Path:
    output_path = OUTPUT_DIR / "capacity_map.html"
    OUTPUT_DIR.mkdir(parents=True, exist_ok=True)

    df = scored_df.copy()
    df["LATITUDE"] = pd.to_numeric(df["LATITUDE"], errors="coerce")
    df["LONGITUDE"] = pd.to_numeric(df["LONGITUDE"], errors="coerce")
    df["MAX_VOLT"] = pd.to_numeric(df["MAX_VOLT"], errors="coerce")
    df = df.dropna(subset=["LATITUDE", "LONGITUDE"])
    df = df[df["MAX_VOLT"].fillna(0) >= min_voltage]

    fmap = folium.Map(location=[31.0, -99.0], zoom_start=6, tiles="CartoDB positron")
    title_html = f"""
    <div style="position: fixed; top: 10px; left: 50px; z-index: 9999; background: white;
                padding: 10px 14px; border: 1px solid #cbd5e1; border-radius: 8px;
                box-shadow: 0 4px 12px rgba(0,0,0,0.08); font-family: Arial, sans-serif;">
      <div style="font-size: 18px; font-weight: 700;">Infrastructure Siting Index</div>
      <div style="font-size: 12px; color: #475569;">{escape(timestamp)}</div>
    </div>
    """
    fmap.get_root().html.add_child(folium.Element(title_html))

    layers = {
        "All substations": folium.FeatureGroup(name="All substations", show=True),
        "AVAILABLE only": folium.FeatureGroup(name="AVAILABLE only", show=False),
        "MARGINAL only (bus-matched)": folium.FeatureGroup(name="MARGINAL only (bus-matched)", show=False),
        "CONSTRAINED only": folium.FeatureGroup(name="CONSTRAINED only", show=False),
        "Zone estimates only": folium.FeatureGroup(name="Zone estimates only", show=False),
        "345kV+ substations": folium.FeatureGroup(name="345kV+ substations", show=False),
        "LZ_NORTH": folium.FeatureGroup(name="Zone: LZ_NORTH", show=False),
        "LZ_SOUTH": folium.FeatureGroup(name="Zone: LZ_SOUTH", show=False),
        "LZ_WEST": folium.FeatureGroup(name="Zone: LZ_WEST", show=False),
        "LZ_HOUSTON": folium.FeatureGroup(name="Zone: LZ_HOUSTON", show=False),
    }

    for _, row in df.iterrows():
        _add_marker(layers["All substations"], row)
        score_basis = str(row.get("score_basis", ""))
        if row["TIER"] == "AVAILABLE":
            _add_marker(layers["AVAILABLE only"], row)
        if row["TIER"] == "MARGINAL" and score_basis == "bus_match":
            _add_marker(layers["MARGINAL only (bus-matched)"], row)
        if row["TIER"] == "CONSTRAINED":
            _add_marker(layers["CONSTRAINED only"], row)
        if score_basis == "zone_estimate":
            _add_marker(layers["Zone estimates only"], row)
        if pd.notna(row["MAX_VOLT"]) and row["MAX_VOLT"] >= 345:
            _add_marker(layers["345kV+ substations"], row)
        zone = str(row.get("ercot_zone", ""))
        if zone in layers:
            _add_marker(layers[zone], row)

    for layer in layers.values():
        layer.add_to(fmap)

    folium.LayerControl(collapsed=False).add_to(fmap)

    legend_html = """
    <div style="position: fixed; bottom: 24px; right: 24px; z-index: 9999; background: white;
                padding: 12px 14px; border: 1px solid #cbd5e1; border-radius: 8px;
                box-shadow: 0 4px 12px rgba(0,0,0,0.08); font-family: Arial, sans-serif; font-size: 13px;">
      <div style="font-weight: 700; margin-bottom: 8px;">Index Legend</div>
      <div><span style="color:#22c55e;">●</span> AVAILABLE (score 70-100, bus-matched)</div>
      <div><span style="color:#f59e0b;">●</span> MARGINAL (score 40-69)</div>
      <div><span style="color:#ef4444;">●</span> CONSTRAINED (score &lt;40)</div>
      <div><span style="color:#94a3b8;">●</span> UNSCORED (no zone or ERCOT match)</div>
      <div style="margin-top: 6px; color: #475569; font-size: 11px;">
        Faded markers = zone-level estimate (not bus-matched)
      </div>
    </div>
    """
    fmap.get_root().html.add_child(folium.Element(legend_html))
    fmap.save(str(output_path))
    return output_path
