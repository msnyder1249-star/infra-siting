from __future__ import annotations

import shutil
from pathlib import Path

import pandas as pd

from config import OUTPUT_DIR, PROCESSED_DIR, PROJECT_ROOT
from src.utils import ensure_directory

DOCS_DIR = PROJECT_ROOT / "docs"
DOCS_DATA_DIR = DOCS_DIR / "data" / "processed"


def publish_docs() -> dict[str, str]:
    ensure_directory(DOCS_DIR)
    ensure_directory(DOCS_DATA_DIR)

    copies = {
        OUTPUT_DIR / "capacity_map.html": DOCS_DIR / "capacity_map.html",
        PROCESSED_DIR / "substation_capacity_scores.csv": DOCS_DATA_DIR / "substation_capacity_scores.csv",
        PROCESSED_DIR / "bus_substation_crosswalk.csv": DOCS_DATA_DIR / "bus_substation_crosswalk.csv",
    }
    for src, dest in copies.items():
        if src.exists():
            shutil.copy2(src, dest)

    summary_path = DOCS_DIR / "summary.csv"
    scored_path = PROCESSED_DIR / "substation_capacity_scores.csv"
    if scored_path.exists():
        scored = pd.read_csv(scored_path)
        tier_counts = scored["TIER"].value_counts().rename_axis("TIER").reset_index(name="count")
        tier_counts.to_csv(summary_path, index=False)

    return {
        "map": str(DOCS_DIR / "capacity_map.html"),
        "scores_csv": str(DOCS_DATA_DIR / "substation_capacity_scores.csv"),
        "crosswalk_csv": str(DOCS_DATA_DIR / "bus_substation_crosswalk.csv"),
        "summary_csv": str(summary_path),
    }
