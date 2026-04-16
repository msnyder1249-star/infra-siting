from __future__ import annotations

import re
import zipfile
from dataclasses import dataclass, field
from pathlib import Path
from typing import Any

import pandas as pd

from config import RAW_DIR, get_ercot_credentials
from src.utils import ensure_directory, read_json, request_with_retries, requests_session

TOKEN_URL = (
    "https://ercotb2c.b2clogin.com/ercotb2c.onmicrosoft.com/"
    "B2C_1_SIGNIN/oauth2/v2.0/token"
)
BASE_URL = "https://api.ercot.com/api/public-data"
SAMPLE_PATH = RAW_DIR / "ercot_sample.json"

ZIP_PATTERNS = {
    "lmp_by_bus": "*LMPSELECTBUSNP6787*.zip",
    "resource_node_lmp": "*LMPSROSNODENP6788*.zip",
    "rtmgr": "*RTDLMPRNLZHUBNP6970*.zip",
    "shadow_prices": "*SCEDBTCNP686*.zip",
    "settlement_point_prices": "*SPPHLZNP6905*.zip",
}

FILENAME_TIMESTAMP_PATTERNS = [
    re.compile(r"_(\d{8})_(\d{6})_"),
    re.compile(r"_(\d{8})_(\d{4})_"),
]


@dataclass
class ErcotDatasetBundle:
    lmp_by_bus: pd.DataFrame
    shadow_prices: pd.DataFrame
    rtmgr: pd.DataFrame
    resource_node_lmp: pd.DataFrame = field(default_factory=pd.DataFrame)
    settlement_point_prices: pd.DataFrame = field(default_factory=pd.DataFrame)
    data_source: str = "unscored"
    message: str = ""
    date_ranges: dict[str, str] = field(default_factory=dict)
    detected_columns: dict[str, list[str]] = field(default_factory=dict)


def _empty_bundle(message: str, data_source: str = "unscored") -> ErcotDatasetBundle:
    return ErcotDatasetBundle(
        lmp_by_bus=pd.DataFrame(),
        shadow_prices=pd.DataFrame(),
        rtmgr=pd.DataFrame(),
        resource_node_lmp=pd.DataFrame(),
        settlement_point_prices=pd.DataFrame(),
        data_source=data_source,
        message=message,
    )


class ErcotClient:
    def __init__(self) -> None:
        self.credentials = get_ercot_credentials()
        self.session = requests_session()
        self.token: str | None = None

    def authenticate(self) -> str:
        if self.token:
            return self.token
        if not self.credentials.is_configured:
            raise RuntimeError("Missing ERCOT credentials.")

        payload = {
            "grant_type": "password",
            "username": self.credentials.username,
            "password": self.credentials.password,
            "scope": "openid offline_access",
            "response_type": "id_token",
            "client_id": "public-client",
        }
        response = request_with_retries(self.session, "POST", TOKEN_URL, data=payload)
        token_payload = response.json()
        token = token_payload.get("id_token") or token_payload.get("access_token")
        if not token:
            raise RuntimeError(f"ERCOT auth response did not include a token: {token_payload}")
        self.token = token
        return token

    def fetch_endpoint(self, endpoint: str, params: dict[str, Any]) -> pd.DataFrame:
        token = self.authenticate()
        headers = {
            "Authorization": f"Bearer {token}",
            "Ocp-Apim-Subscription-Key": self.credentials.subscription_key,
            "Accept": "application/json",
        }
        response = request_with_retries(
            self.session,
            "GET",
            f"{BASE_URL}{endpoint}",
            headers=headers,
            params=params,
        )
        payload = response.json()
        rows = payload.get("data") or payload.get("items") or payload.get("results") or payload
        if isinstance(rows, dict):
            rows = rows.get("records", [])
        if not isinstance(rows, list):
            rows = []
        return pd.DataFrame(rows)


def _normalize_frame(df: pd.DataFrame, columns: list[str]) -> pd.DataFrame:
    if df.empty:
        return pd.DataFrame(columns=columns)

    normalized = df.copy()
    lowered = {column.lower(): column for column in normalized.columns}
    rename_map: dict[str, str] = {}
    for column in columns:
        source = lowered.get(column.lower())
        if source:
            rename_map[source] = column
    normalized = normalized.rename(columns=rename_map)
    for column in columns:
        if column not in normalized.columns:
            normalized[column] = None
    return normalized[columns].copy()


def _parse_filename_timestamp(path: Path) -> pd.Timestamp | None:
    for pattern in FILENAME_TIMESTAMP_PATTERNS:
        match = pattern.search(path.name)
        if match:
            date_part, time_part = match.groups()
            fmt = "%Y%m%d%H%M%S" if len(time_part) == 6 else "%Y%m%d%H%M"
            return pd.to_datetime(f"{date_part}{time_part}", format=fmt, errors="coerce")
    return None


def _find_csv_name(zip_path: Path) -> str | None:
    with zipfile.ZipFile(zip_path) as archive:
        csv_names = [name for name in archive.namelist() if name.lower().endswith(".csv")]
    return csv_names[0] if csv_names else None


def _read_zip_csv(zip_path: Path) -> pd.DataFrame:
    with zipfile.ZipFile(zip_path) as archive:
        csv_name = _find_csv_name(zip_path)
        if not csv_name:
            return pd.DataFrame()
        with archive.open(csv_name) as handle:
            return pd.read_csv(handle)


def _load_local_zip_dataset(dataset_key: str) -> tuple[pd.DataFrame, list[str], str]:
    zip_paths = sorted(RAW_DIR.glob(ZIP_PATTERNS[dataset_key]))
    if not zip_paths:
        return pd.DataFrame(), [], ""

    frames: list[pd.DataFrame] = []
    column_names: list[str] = []
    timestamps: list[pd.Timestamp] = []

    for zip_path in zip_paths:
        frame = _read_zip_csv(zip_path)
        if frame.empty:
            continue
        frame = frame.copy()
        frame["_source_zip"] = zip_path.name
        frame["_source_timestamp"] = _parse_filename_timestamp(zip_path)
        frames.append(frame)
        if not column_names:
            column_names = list(frame.columns)
        timestamp = _parse_filename_timestamp(zip_path)
        if timestamp is not None and not pd.isna(timestamp):
            timestamps.append(timestamp)

    if not frames:
        return pd.DataFrame(), column_names, ""

    combined = pd.concat(frames, ignore_index=True)
    date_range = ""
    if timestamps:
        date_range = f"{min(timestamps).isoformat()} -> {max(timestamps).isoformat()}"
    return combined, column_names, date_range


def _normalize_lmp_by_bus(df: pd.DataFrame) -> pd.DataFrame:
    normalized = _normalize_frame(df, ["SCEDTimestamp", "RepeatedHourFlag", "ElectricalBus", "LMP"])
    normalized = normalized.rename(columns={"SCEDTimestamp": "deliveryDate", "ElectricalBus": "busName"})
    normalized["hourEnding"] = pd.to_datetime(normalized["deliveryDate"], errors="coerce")
    normalized["LMP"] = pd.to_numeric(normalized["LMP"], errors="coerce")
    normalized["congestionComponent"] = None
    normalized["lossComponent"] = None
    return normalized[
        ["deliveryDate", "hourEnding", "busName", "LMP", "congestionComponent", "lossComponent"]
    ]


def _normalize_resource_node_lmp(df: pd.DataFrame) -> pd.DataFrame:
    normalized = _normalize_frame(df, ["SCEDTimestamp", "RepeatedHourFlag", "SettlementPoint", "LMP"])
    normalized["SCEDTimestamp"] = pd.to_datetime(normalized["SCEDTimestamp"], errors="coerce")
    normalized["LMP"] = pd.to_numeric(normalized["LMP"], errors="coerce")
    return normalized


def _normalize_rtmgr(df: pd.DataFrame) -> pd.DataFrame:
    normalized = _normalize_frame(df, ["RTDTimestamp", "SettlementPoint", "SettlementPointType", "LMP"])
    normalized = normalized.rename(columns={"RTDTimestamp": "deliveryDate", "SettlementPoint": "settlementPoint"})
    normalized["deliveryDate"] = pd.to_datetime(normalized["deliveryDate"], errors="coerce")
    normalized["hourEnding"] = normalized["deliveryDate"]
    normalized["LMP"] = pd.to_numeric(normalized["LMP"], errors="coerce")
    return normalized[["deliveryDate", "hourEnding", "settlementPoint", "LMP", "SettlementPointType"]]


def _normalize_shadow_prices(df: pd.DataFrame) -> pd.DataFrame:
    normalized = _normalize_frame(
        df,
        [
            "SCEDTimeStamp",
            "ConstraintName",
            "ShadowPrice",
            "MaxShadowPrice",
            "FromStation",
            "ToStation",
            "FromStationkV",
            "ToStationkV",
            "Limit",
            "Value",
        ],
    )
    normalized = normalized.rename(
        columns={
            "SCEDTimeStamp": "deliveryDate",
            "FromStationkV": "fromKV",
            "ToStationkV": "toKV",
            "Limit": "overloadedElementLimit",
            "Value": "overloadedElementFlow",
        }
    )
    normalized["deliveryDate"] = pd.to_datetime(normalized["deliveryDate"], errors="coerce")
    normalized["hourEnding"] = normalized["deliveryDate"]
    normalized["constraintName"] = normalized["ConstraintName"]
    normalized["fromStation"] = normalized["FromStation"]
    normalized["toStation"] = normalized["ToStation"]
    normalized["shadowPrice"] = pd.to_numeric(normalized["ShadowPrice"], errors="coerce")
    normalized["maxShadowPrice"] = pd.to_numeric(normalized["MaxShadowPrice"], errors="coerce")
    return normalized[
        [
            "deliveryDate",
            "hourEnding",
            "constraintName",
            "fromStation",
            "toStation",
            "fromKV",
            "toKV",
            "shadowPrice",
            "maxShadowPrice",
            "overloadedElementLimit",
            "overloadedElementFlow",
        ]
    ]


def _normalize_settlement_point_prices(df: pd.DataFrame) -> pd.DataFrame:
    normalized = _normalize_frame(
        df,
        [
            "DeliveryDate",
            "DeliveryHour",
            "DeliveryInterval",
            "SettlementPointName",
            "SettlementPointType",
            "SettlementPointPrice",
        ],
    )
    normalized["DeliveryDate"] = pd.to_datetime(normalized["DeliveryDate"], errors="coerce")
    normalized["DeliveryHour"] = pd.to_numeric(normalized["DeliveryHour"], errors="coerce")
    normalized["DeliveryInterval"] = pd.to_numeric(normalized["DeliveryInterval"], errors="coerce")
    normalized["SettlementPointPrice"] = pd.to_numeric(
        normalized["SettlementPointPrice"], errors="coerce"
    )
    normalized["timestamp"] = (
        normalized["DeliveryDate"]
        + pd.to_timedelta(normalized["DeliveryHour"].fillna(0) - 1, unit="h")
        + pd.to_timedelta((normalized["DeliveryInterval"].fillna(1) - 1) * 15, unit="m")
    )
    return normalized


def _load_sample_bundle() -> ErcotDatasetBundle | None:
    if not SAMPLE_PATH.exists():
        return None
    payload = read_json(SAMPLE_PATH, default={})
    return ErcotDatasetBundle(
        lmp_by_bus=pd.DataFrame(payload.get("lmp_by_bus", [])),
        shadow_prices=pd.DataFrame(payload.get("shadow_prices", [])),
        rtmgr=pd.DataFrame(payload.get("rtmgr", [])),
        resource_node_lmp=pd.DataFrame(payload.get("resource_node_lmp", [])),
        settlement_point_prices=pd.DataFrame(payload.get("settlement_point_prices", [])),
        data_source="sample",
        message=f"Loaded sample ERCOT data from {SAMPLE_PATH}.",
    )


def _print_detected_columns(detected_columns: dict[str, list[str]]) -> None:
    for dataset_name, columns in detected_columns.items():
        print(f"   Detected columns for {dataset_name}: {columns}")


def _load_local_ercot_bundle() -> ErcotDatasetBundle | None:
    loaded: dict[str, pd.DataFrame] = {}
    detected_columns: dict[str, list[str]] = {}
    date_ranges: dict[str, str] = {}

    for dataset_key in ZIP_PATTERNS:
        df, columns, date_range = _load_local_zip_dataset(dataset_key)
        if not df.empty:
            loaded[dataset_key] = df
        if columns:
            detected_columns[dataset_key] = columns
        if date_range:
            date_ranges[dataset_key] = date_range

    if not loaded:
        return None

    _print_detected_columns(detected_columns)
    return ErcotDatasetBundle(
        lmp_by_bus=_normalize_lmp_by_bus(loaded.get("lmp_by_bus", pd.DataFrame())),
        shadow_prices=_normalize_shadow_prices(loaded.get("shadow_prices", pd.DataFrame())),
        rtmgr=_normalize_rtmgr(loaded.get("rtmgr", pd.DataFrame())),
        resource_node_lmp=_normalize_resource_node_lmp(
            loaded.get("resource_node_lmp", pd.DataFrame())
        ),
        settlement_point_prices=_normalize_settlement_point_prices(
            loaded.get("settlement_point_prices", pd.DataFrame())
        ),
        data_source="local_zip",
        message="Loaded ERCOT datasets from local ZIP archives in data/raw.",
        date_ranges=date_ranges,
        detected_columns=detected_columns,
    )


def fetch_all_ercot_data(
    lookback_days: int = 7,
    refresh_cache: bool = False,
    live: bool = False,
) -> ErcotDatasetBundle:
    del lookback_days, refresh_cache
    ensure_directory(RAW_DIR)

    local_bundle = _load_local_ercot_bundle()
    if local_bundle is not None:
        return local_bundle

    sample_bundle = _load_sample_bundle()
    if sample_bundle:
        return sample_bundle

    if not live:
        return _empty_bundle("No local ERCOT ZIP files found. Live mode is disabled; continuing unscored.")

    credentials = get_ercot_credentials()
    if not credentials.is_configured:
        return _empty_bundle(
            "ERCOT credentials not set and no local ZIP files found. Continuing with substation-only scoring."
        )

    client = ErcotClient()
    try:  # pragma: no cover - future API path
        lmp_by_bus = client.fetch_endpoint("/np6-787-cd/lmp_electrical_bus", {})
        shadow_prices = client.fetch_endpoint("/np6-86-cd/shdw_prices_bnd_trns_const", {})
        rtmgr = client.fetch_endpoint("/np6-970-cd/rtd_lmp_node_zone_hub", {})
        return ErcotDatasetBundle(
            lmp_by_bus=_normalize_lmp_by_bus(lmp_by_bus),
            shadow_prices=_normalize_shadow_prices(shadow_prices),
            rtmgr=_normalize_rtmgr(rtmgr),
            data_source="live",
            message="Fetched live ERCOT data.",
        )
    except Exception as exc:  # pragma: no cover - network failure path
        print(f"Warning: ERCOT live fetch failed ({exc}).")
        return _empty_bundle("ERCOT fetch failed; continuing with substation-only scoring.")
