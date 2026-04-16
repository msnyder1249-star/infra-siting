from __future__ import annotations

import os
from dataclasses import dataclass
from pathlib import Path

try:
    from dotenv import load_dotenv
except ImportError:  # pragma: no cover - optional dependency at runtime
    load_dotenv = None

if load_dotenv is not None:
    load_dotenv()

PROJECT_ROOT = Path(__file__).resolve().parent
SRC_DIR = PROJECT_ROOT / "src"
DATA_DIR = PROJECT_ROOT / "data"
RAW_DIR = DATA_DIR / "raw"
PROCESSED_DIR = DATA_DIR / "processed"
OUTPUT_DIR = PROJECT_ROOT / "output"
REPO_ROOT = PROJECT_ROOT.parent
LEGACY_OUTPUT_DIR = REPO_ROOT / "output"

CACHE_TTL_SECONDS = 2 * 60 * 60
DEFAULT_TIMEOUT_SECONDS = 30


@dataclass(frozen=True)
class ErcotCredentials:
    username: str
    password: str
    subscription_key: str

    @property
    def is_configured(self) -> bool:
        return all((self.username, self.password, self.subscription_key))


def get_ercot_credentials() -> ErcotCredentials:
    return ErcotCredentials(
        username=os.environ.get("ERCOT_USERNAME", "").strip(),
        password=os.environ.get("ERCOT_PASSWORD", "").strip(),
        subscription_key=os.environ.get("ERCOT_SUBSCRIPTION_KEY", "").strip(),
    )
