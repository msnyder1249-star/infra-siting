from __future__ import annotations

from dataclasses import dataclass
from pathlib import Path

import pandas as pd

from config import RAW_DIR

PROJECT_PATTERNS = [
    "*project*.csv",
    "*upgrade*.csv",
    "*planning*.csv",
    "*transmission*.csv",
]


@dataclass
class ProjectBundle:
    df: pd.DataFrame
    source_files: list[str]


def load_project_data() -> ProjectBundle:
    paths: list[Path] = []
    for pattern in PROJECT_PATTERNS:
        paths.extend(RAW_DIR.glob(pattern))
    unique_paths = sorted({path for path in paths if path.is_file()})
    if not unique_paths:
        return ProjectBundle(df=pd.DataFrame(), source_files=[])

    frames: list[pd.DataFrame] = []
    for path in unique_paths:
        try:
            frame = pd.read_csv(path)
        except Exception:
            continue
        if frame.empty:
            continue
        frame = frame.copy()
        frame["_source_file"] = path.name
        frames.append(frame)

    if not frames:
        return ProjectBundle(df=pd.DataFrame(), source_files=[])
    return ProjectBundle(df=pd.concat(frames, ignore_index=True), source_files=[path.name for path in unique_paths])
