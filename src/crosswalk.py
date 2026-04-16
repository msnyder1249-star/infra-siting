from __future__ import annotations

import io
import re
import zipfile
from difflib import SequenceMatcher
from pathlib import Path
from typing import Any

import pandas as pd

from config import PROCESSED_DIR, RAW_DIR
from src.utils import clean_name, ensure_directory, request_with_retries, requests_session

SETTLEMENT_POINTS_PAGE = "https://www.ercot.com/mp/data-products/data-product-details?id=NP4-160-SG"
SETTLEMENT_ZIP_PATH = RAW_DIR / "ercot_settlement_points.zip"
SETTLEMENT_CSV_CACHE = RAW_DIR / "ercot_settlement_points.csv"
REFERENCE_SETTLEMENT_POINTS_PATH = RAW_DIR / "Settlement_Points_04022026_094343.csv"
RESOURCE_NODE_TO_UNIT_PATH = RAW_DIR / "Resource_Node_to_Unit_04022026_094343.csv"
NOIE_MAPPING_PATH = RAW_DIR / "NOIE_Mapping_04022026_094343.csv"
ALIASES_PATH = RAW_DIR / "ercot_name_aliases.csv"
OUTPUT_PATH = PROCESSED_DIR / "bus_substation_crosswalk.csv"
REVIEW_OUTPUT_PATH = PROCESSED_DIR / "ercot_unmatched_review.csv"

GENERIC_TOKENS = {
    "RN",
    "ALL",
    "GEN",
    "LOAD",
    "NODE",
    "BUS",
    "EBUS",
    "ELECTRICAL",
    "POINT",
    "SUB",
    "SUBSTATION",
    "UNIT",
    "PLANT",
}
SUFFIX_PATTERNS = [
    re.compile(r"^(?:CC|CT|GT|ST|U|UNIT)\d+$"),
    re.compile(r"^(?:SLR|SOLAR|ESS|ESR|BESS|WIND|BT|EN)$"),
    re.compile(r"^[A-Z]$"),
]
TOKEN_REPLACEMENTS = {
    "SA": "SAN",
    "ST": "SAINT",
    "MT": "MOUNT",
    "FT": "FORT",
}


def load_settlement_points(refresh_cache: bool = False) -> pd.DataFrame:
    if SETTLEMENT_CSV_CACHE.exists() and not refresh_cache:
        return pd.read_csv(SETTLEMENT_CSV_CACHE)

    ensure_directory(SETTLEMENT_ZIP_PATH.parent)
    zip_path = _download_settlement_points_zip(refresh_cache=refresh_cache)
    if not zip_path or not zip_path.exists():
        return pd.DataFrame(columns=["settlementPoint", "settlementPointType", "busName", "substationName", "voltage"])

    with zipfile.ZipFile(zip_path) as archive:
        csv_names = [name for name in archive.namelist() if name.lower().endswith(".csv")]
        if not csv_names:
            return pd.DataFrame(columns=["settlementPoint", "settlementPointType", "busName", "substationName", "voltage"])
        with archive.open(csv_names[0]) as handle:
            df = pd.read_csv(io.BytesIO(handle.read()))

    lowered = {column.lower(): column for column in df.columns}
    columns = ["settlementPoint", "settlementPointType", "busName", "substationName", "voltage"]
    rename_map = {}
    for column in columns:
        source = lowered.get(column.lower())
        if source:
            rename_map[source] = column
    df = df.rename(columns=rename_map)
    for column in columns:
        if column not in df.columns:
            df[column] = None
    df = df[columns].copy()
    df.to_csv(SETTLEMENT_CSV_CACHE, index=False)
    return df


def _download_settlement_points_zip(refresh_cache: bool = False) -> Path | None:
    if SETTLEMENT_ZIP_PATH.exists() and not refresh_cache:
        return SETTLEMENT_ZIP_PATH

    session = requests_session()
    page_response = request_with_retries(session, "GET", SETTLEMENT_POINTS_PAGE)
    html = page_response.text

    zip_url: str | None = None
    for marker in ['.zip"', ".zip'"]:
        if marker not in html:
            continue
        prefix = html.split(marker, 1)[0]
        start = max(prefix.rfind("https://"), prefix.rfind("http://"), prefix.rfind("/"))
        candidate = prefix[start:] + ".zip"
        zip_url = f"https://www.ercot.com{candidate}" if candidate.startswith("/") else candidate
        break

    if not zip_url:
        return None

    zip_response = request_with_retries(session, "GET", zip_url)
    SETTLEMENT_ZIP_PATH.write_bytes(zip_response.content)
    return SETTLEMENT_ZIP_PATH


def _is_name_like_bus(value: str) -> bool:
    cleaned = clean_name(value)
    return any(character.isalpha() for character in cleaned)


def _load_reference_csv(path: Path, expected_columns: list[str]) -> pd.DataFrame:
    if not path.exists():
        return pd.DataFrame(columns=expected_columns)
    df = pd.read_csv(path)
    for column in expected_columns:
        if column not in df.columns:
            df[column] = None
    return df[expected_columns].copy()


def load_ercot_reference_points() -> pd.DataFrame:
    df = _load_reference_csv(
        REFERENCE_SETTLEMENT_POINTS_PATH,
        [
            "ELECTRICAL_BUS",
            "NODE_NAME",
            "PSSE_BUS_NAME",
            "VOLTAGE_LEVEL",
            "SUBSTATION",
            "SETTLEMENT_LOAD_ZONE",
            "RESOURCE_NODE",
            "HUB_BUS_NAME",
            "HUB",
            "PSSE_BUS_NUMBER",
        ],
    )
    if df.empty:
        return df
    df["ELECTRICAL_BUS"] = df["ELECTRICAL_BUS"].astype(str).str.strip()
    df["RESOURCE_NODE"] = df["RESOURCE_NODE"].fillna("").astype(str).str.strip()
    df["SUBSTATION"] = df["SUBSTATION"].fillna("").astype(str).str.strip()
    return df


def load_resource_node_to_unit() -> pd.DataFrame:
    df = _load_reference_csv(
        RESOURCE_NODE_TO_UNIT_PATH,
        ["RESOURCE_NODE", "UNIT_SUBSTATION", "UNIT_NAME"],
    )
    if df.empty:
        return df
    df["RESOURCE_NODE"] = df["RESOURCE_NODE"].fillna("").astype(str).str.strip()
    df["UNIT_SUBSTATION"] = df["UNIT_SUBSTATION"].fillna("").astype(str).str.strip()
    return df


def load_noie_mapping() -> pd.DataFrame:
    df = _load_reference_csv(
        NOIE_MAPPING_PATH,
        ["PHYSICAL_LOAD", "NOIE", "VOLTAGE_NAME", "SUBSTATION", "ELECTRICAL_BUS"],
    )
    if df.empty:
        return df
    df["ELECTRICAL_BUS"] = df["ELECTRICAL_BUS"].fillna("").astype(str).str.strip()
    df["SUBSTATION"] = df["SUBSTATION"].fillna("").astype(str).str.strip()
    return df


def _canonical_tokens(value: Any) -> list[str]:
    cleaned = clean_name(value)
    if not cleaned:
        return []
    tokens = [TOKEN_REPLACEMENTS.get(token, token) for token in cleaned.split()]
    filtered: list[str] = []
    for token in tokens:
        if token in GENERIC_TOKENS:
            continue
        if any(pattern.match(token) for pattern in SUFFIX_PATTERNS):
            continue
        filtered.append(token)
    while len(filtered) >= 2 and filtered[-2] in {"V", "L"} and len(filtered[-1]) == 1:
        filtered = filtered[:-2]
    return filtered


def _canonical_name(value: Any) -> str:
    return " ".join(_canonical_tokens(value))


def _token_overlap(left: str, right: str) -> float:
    left_tokens = set(_canonical_tokens(left))
    right_tokens = set(_canonical_tokens(right))
    if not left_tokens or not right_tokens:
        return 0.0
    return len(left_tokens & right_tokens) / len(left_tokens | right_tokens)


def _sequence_ratio(left: str, right: str) -> float:
    if not left or not right:
        return 0.0
    return SequenceMatcher(None, left, right).ratio()


def _build_token_index(substations: pd.DataFrame) -> dict[str, set[int]]:
    token_index: dict[str, set[int]] = {}
    for idx, row in substations.iterrows():
        combined_tokens = set(_canonical_tokens(row["NAME"])) | set(_canonical_tokens(row.get("OWNER")))
        for token in combined_tokens:
            token_index.setdefault(token, set()).add(idx)
    return token_index


def _load_aliases(substations: pd.DataFrame) -> dict[str, str]:
    ensure_directory(ALIASES_PATH.parent)
    if not ALIASES_PATH.exists():
        template = pd.DataFrame(
            [
                {"ercot_name": "PALMVIEW_RN", "substation_name": "", "notes": "Fill in curated mappings here."},
                {"ercot_name": "PANDA_T1_CC1", "substation_name": "", "notes": "One row per ERCOT name alias."},
            ]
        )
        template.to_csv(ALIASES_PATH, index=False)
        return {}

    aliases_df = pd.read_csv(ALIASES_PATH).fillna("")
    valid_names = set(substations["NAME"].astype(str))
    alias_map: dict[str, str] = {}
    for row in aliases_df.to_dict(orient="records"):
        ercot_name = str(row.get("ercot_name", "")).strip()
        substation_name = str(row.get("substation_name", "")).strip()
        if ercot_name and substation_name and substation_name in valid_names:
            alias_map[ercot_name] = substation_name
    return alias_map


def promote_review_aliases(
    *,
    min_confidence: float = 0.66,
    min_group_size: int = 2,
) -> pd.DataFrame:
    ensure_directory(ALIASES_PATH.parent)
    if not ALIASES_PATH.exists():
        pd.DataFrame(columns=["ercot_name", "substation_name", "notes"]).to_csv(ALIASES_PATH, index=False)
    if not REVIEW_OUTPUT_PATH.exists():
        raise FileNotFoundError(f"Review file not found: {REVIEW_OUTPUT_PATH}")

    aliases_df = pd.read_csv(ALIASES_PATH).fillna("")
    review_df = pd.read_csv(REVIEW_OUTPUT_PATH).fillna("")

    if review_df.empty:
        return aliases_df

    review_df["suggested_confidence"] = pd.to_numeric(review_df["suggested_confidence"], errors="coerce").fillna(0)
    review_df["canonical_token_count"] = review_df["canonical_name"].map(lambda value: len(_canonical_tokens(value)))
    review_df["canonical_in_substation"] = review_df.apply(
        lambda row: set(_canonical_tokens(row["canonical_name"])).issubset(set(_canonical_tokens(row["suggested_substation"])))
        if row["canonical_name"] and row["suggested_substation"]
        else False,
        axis=1,
    )
    review_df["suffix_like_name"] = review_df["ercot_name"].astype(str).str.contains(
        r"_(?:L|V|RN|CC|CT|ST|GT|K|A|B|C|D|E|F|1|2)",
        regex=True,
        na=False,
    )
    review_df["suggested_substation_group_size"] = review_df.groupby("suggested_substation")["ercot_name"].transform("count")

    promoted = review_df[
        (review_df["suggested_confidence"] >= min_confidence)
        & (review_df["canonical_in_substation"])
        & (review_df["suffix_like_name"])
        & (review_df["suggested_substation_group_size"] >= min_group_size)
        & (review_df["suggested_substation"] != "")
    ][["ercot_name", "suggested_substation", "suggested_confidence"]].copy()

    promoted = promoted.rename(columns={"suggested_substation": "substation_name"})
    promoted["notes"] = promoted["suggested_confidence"].map(
        lambda score: f"auto-promoted from review export; confidence={score:.3f}"
    )
    promoted = promoted.drop(columns=["suggested_confidence"]).drop_duplicates(subset=["ercot_name"], keep="first")

    existing_mapped = aliases_df[aliases_df["substation_name"].astype(str).str.strip() != ""].copy()
    existing_unmapped = aliases_df[aliases_df["substation_name"].astype(str).str.strip() == ""].copy()
    existing_names = set(existing_mapped["ercot_name"].astype(str))
    promoted = promoted[~promoted["ercot_name"].astype(str).isin(existing_names)]

    combined = pd.concat([existing_mapped, promoted, existing_unmapped], ignore_index=True)
    combined = combined.drop_duplicates(subset=["ercot_name"], keep="first")
    combined.to_csv(ALIASES_PATH, index=False)
    return combined


def _candidate_indexes(
    bus_name: str,
    reference_name: str | None,
    substations: pd.DataFrame,
    token_index: dict[str, set[int]],
) -> set[int]:
    candidate_tokens = set(_canonical_tokens(bus_name)) | set(_canonical_tokens(reference_name))
    indexes: set[int] = set()
    for token in candidate_tokens:
        indexes.update(token_index.get(token, set()))
    return indexes


def _match_substation_reference(
    substations: pd.DataFrame,
    reference_name: str | None,
    token_index: dict[str, set[int]],
) -> tuple[pd.Series | None, float, str]:
    if not reference_name:
        return None, 0.0, ""
    reference_clean = clean_name(reference_name)
    reference_canonical = _canonical_name(reference_name)

    direct = substations[substations["clean_name"] == reference_clean]
    if direct.empty and reference_canonical:
        direct = substations[substations["canonical_name"] == reference_canonical]
    if not direct.empty:
        return direct.iloc[0], 1.0, "ercot_reference_exact"

    best_row, best_score, best_method = _best_match(reference_name, None, substations, token_index)
    if best_row is not None and best_score >= 0.68:
        return best_row, best_score, f"ercot_reference_{best_method}"
    return None, 0.0, ""


def _build_reference_maps(
    reference_points_df: pd.DataFrame,
    resource_node_to_unit_df: pd.DataFrame,
    noie_mapping_df: pd.DataFrame,
) -> tuple[dict[str, list[str]], dict[str, list[str]]]:
    bus_to_substations: dict[str, set[str]] = {}
    resource_to_substations: dict[str, set[str]] = {}

    if not reference_points_df.empty:
        for row in reference_points_df.to_dict(orient="records"):
            electrical_bus = str(row.get("ELECTRICAL_BUS", "")).strip()
            resource_node = str(row.get("RESOURCE_NODE", "")).strip()
            substation = str(row.get("SUBSTATION", "")).strip()
            if electrical_bus and substation:
                bus_to_substations.setdefault(electrical_bus, set()).add(substation)
            if resource_node and substation:
                resource_to_substations.setdefault(resource_node, set()).add(substation)

    if not resource_node_to_unit_df.empty:
        for row in resource_node_to_unit_df.to_dict(orient="records"):
            resource_node = str(row.get("RESOURCE_NODE", "")).strip()
            unit_substation = str(row.get("UNIT_SUBSTATION", "")).strip()
            if resource_node and unit_substation:
                resource_to_substations.setdefault(resource_node, set()).add(unit_substation)

    if not noie_mapping_df.empty:
        for row in noie_mapping_df.to_dict(orient="records"):
            electrical_bus = str(row.get("ELECTRICAL_BUS", "")).strip()
            substation = str(row.get("SUBSTATION", "")).strip()
            if electrical_bus and substation:
                bus_to_substations.setdefault(electrical_bus, set()).add(substation)

    bus_map = {key: sorted(values) for key, values in bus_to_substations.items()}
    resource_map = {key: sorted(values) for key, values in resource_to_substations.items()}
    return bus_map, resource_map


def _score_candidate(bus_name: str, reference_name: str | None, candidate_row: pd.Series) -> tuple[float, str]:
    bus_canonical = _canonical_name(bus_name)
    ref_canonical = _canonical_name(reference_name)
    sub_canonical = candidate_row["canonical_name"]
    owner_canonical = candidate_row["canonical_owner"]
    is_placeholder_name = str(candidate_row["NAME"]).startswith("Osm_")

    if bus_canonical and bus_canonical == sub_canonical:
        return 0.99, "canonical_exact"
    if ref_canonical and ref_canonical == sub_canonical:
        return 0.97, "reference_canonical_exact"
    if bus_canonical and bus_canonical == owner_canonical and not is_placeholder_name:
        return 0.92, "owner_canonical_exact"

    token_score = max(
        _token_overlap(bus_name, candidate_row["NAME"]),
        _token_overlap(bus_name, candidate_row.get("OWNER")),
        _token_overlap(reference_name, candidate_row["NAME"]),
    )
    sequence_score = max(
        _sequence_ratio(bus_canonical, sub_canonical),
        _sequence_ratio(ref_canonical, sub_canonical),
        _sequence_ratio(bus_canonical, owner_canonical),
    )

    if bus_canonical and sub_canonical and bus_canonical in sub_canonical:
        sequence_score = max(sequence_score, 0.9)
    if ref_canonical and sub_canonical and ref_canonical in sub_canonical:
        sequence_score = max(sequence_score, 0.9)

    score = (token_score * 0.6) + (sequence_score * 0.4)
    method = "hybrid_fuzzy"
    return score, method


def _best_match(
    bus_name: str,
    reference_name: str | None,
    substations: pd.DataFrame,
    token_index: dict[str, set[int]],
) -> tuple[pd.Series | None, float, str]:
    candidate_idxs = _candidate_indexes(bus_name, reference_name, substations, token_index)
    if not candidate_idxs:
        return None, 0.0, ""

    best_row = None
    best_score = 0.0
    best_method = ""
    for idx in candidate_idxs:
        candidate = substations.loc[idx]
        score, method = _score_candidate(bus_name, reference_name, candidate)
        if score > best_score:
            best_row = candidate
            best_score = score
            best_method = method
    return best_row, best_score, best_method


def _bus_names_from_bundle(ercot_bundle: Any) -> pd.Series:
    names: list[str] = []
    if not getattr(ercot_bundle, "resource_node_lmp", pd.DataFrame()).empty:
        names.extend(
            value
            for value in ercot_bundle.resource_node_lmp["SettlementPoint"].dropna().astype(str)
            if _is_name_like_bus(value)
        )
    if not getattr(ercot_bundle, "lmp_by_bus", pd.DataFrame()).empty:
        names.extend(
            value
            for value in ercot_bundle.lmp_by_bus["busName"].dropna().astype(str)
            if _is_name_like_bus(value)
        )
    return pd.Series(names, dtype="object").drop_duplicates().sort_values().reset_index(drop=True)


def _build_review_export(
    bus_names: pd.Series,
    crosswalk_df: pd.DataFrame,
    substations: pd.DataFrame,
    token_index: dict[str, set[int]],
) -> pd.DataFrame:
    matched = set(crosswalk_df["ercot_bus"].astype(str))
    unmatched = [name for name in bus_names.astype(str) if name not in matched]
    review_rows: list[dict[str, Any]] = []
    counts = bus_names.astype(str).value_counts()
    for name in unmatched:
        suggestion, score, method = _best_match(name, None, substations, token_index)
        review_rows.append(
            {
                "ercot_name": name,
                "canonical_name": _canonical_name(name),
                "occurrences": int(counts.get(name, 0)),
                "suggested_substation": suggestion["NAME"] if suggestion is not None else "",
                "suggested_confidence": round(score, 3) if suggestion is not None else 0.0,
                "suggested_method": method,
            }
        )
    review_df = pd.DataFrame(review_rows).sort_values(
        ["occurrences", "suggested_confidence", "ercot_name"], ascending=[False, False, True]
    )
    review_df = review_df[
        (review_df["canonical_name"].str.len() >= 4) | (review_df["suggested_confidence"] >= 0.7)
    ].head(750)
    review_df.to_csv(REVIEW_OUTPUT_PATH, index=False)
    return review_df


def load_zone_reference() -> pd.DataFrame:
    """Return ELECTRICAL_BUS → SETTLEMENT_LOAD_ZONE from the local reference file."""
    return _load_reference_csv(
        REFERENCE_SETTLEMENT_POINTS_PATH,
        ["ELECTRICAL_BUS", "SETTLEMENT_LOAD_ZONE"],
    ).drop_duplicates("ELECTRICAL_BUS")


def build_crosswalk(
    substations_df: pd.DataFrame,
    ercot_bundle: Any,
    refresh_cache: bool = False,
) -> pd.DataFrame:
    ensure_directory(OUTPUT_PATH.parent)
    try:
        settlement_points_df = load_settlement_points(refresh_cache=refresh_cache)
    except Exception as exc:  # pragma: no cover
        print(f"Warning: settlement point download failed, continuing with bus-name heuristics only ({exc}).")
        settlement_points_df = pd.DataFrame(
            columns=["settlementPoint", "settlementPointType", "busName", "substationName", "voltage"]
        )

    substations = substations_df.copy()
    substations["clean_name"] = substations["NAME"].map(clean_name)
    substations["canonical_name"] = substations["NAME"].map(_canonical_name)
    substations["canonical_owner"] = substations["OWNER"].map(_canonical_name)
    token_index = _build_token_index(substations)
    alias_map = _load_aliases(substations)
    reference_points_df = load_ercot_reference_points()
    resource_node_to_unit_df = load_resource_node_to_unit()
    noie_mapping_df = load_noie_mapping()
    bus_reference_map, resource_reference_map = _build_reference_maps(
        reference_points_df,
        resource_node_to_unit_df,
        noie_mapping_df,
    )
    bus_names = _bus_names_from_bundle(ercot_bundle)

    if bus_names.empty and not settlement_points_df.empty:
        bus_names = settlement_points_df["busName"].dropna().astype(str).drop_duplicates().sort_values().reset_index(drop=True)

    matched_rows: list[dict[str, Any]] = []
    for bus_name in bus_names:
        if bus_name in alias_map:
            matched = substations[substations["NAME"] == alias_map[bus_name]]
            if not matched.empty:
                best = matched.iloc[0]
                matched_rows.append(
                    {
                        "ercot_bus": bus_name,
                        "substation_name": best["NAME"],
                        "match_method": "alias",
                        "match_confidence": 1.0,
                        "latitude": best["LATITUDE"],
                        "longitude": best["LONGITUDE"],
                    }
                )
                continue

        if bus_name in bus_reference_map:
            for reference_name in bus_reference_map[bus_name]:
                best, score, method = _match_substation_reference(substations, reference_name, token_index)
                if best is not None:
                    matched_rows.append(
                        {
                            "ercot_bus": bus_name,
                            "substation_name": best["NAME"],
                            "match_method": method,
                            "match_confidence": round(score, 3),
                            "latitude": best["LATITUDE"],
                            "longitude": best["LONGITUDE"],
                        }
                    )
                    break
            if matched_rows and matched_rows[-1]["ercot_bus"] == bus_name:
                continue

        if bus_name in resource_reference_map:
            for reference_name in resource_reference_map[bus_name]:
                best, score, method = _match_substation_reference(substations, reference_name, token_index)
                if best is not None:
                    matched_rows.append(
                        {
                            "ercot_bus": bus_name,
                            "substation_name": best["NAME"],
                            "match_method": method,
                            "match_confidence": round(score, 3),
                            "latitude": best["LATITUDE"],
                            "longitude": best["LONGITUDE"],
                        }
                    )
                    break
            if matched_rows and matched_rows[-1]["ercot_bus"] == bus_name:
                continue

        bus_clean = clean_name(bus_name)
        settlement_rows = settlement_points_df[
            settlement_points_df["busName"].astype(str).map(clean_name) == bus_clean
        ]
        reference_name = None
        if not settlement_rows.empty and settlement_rows["substationName"].notna().any():
            reference_name = settlement_rows["substationName"].dropna().astype(str).iloc[0]

        exact_match = substations[substations["clean_name"] == bus_clean]
        if exact_match.empty:
            bus_canonical = _canonical_name(bus_name)
            if bus_canonical:
                exact_match = substations[substations["canonical_name"] == bus_canonical]
        if exact_match.empty and reference_name:
            reference_canonical = _canonical_name(reference_name)
            exact_match = substations[substations["canonical_name"] == reference_canonical]

        if not exact_match.empty:
            best = exact_match.iloc[0]
            matched_rows.append(
                {
                    "ercot_bus": bus_name,
                    "substation_name": best["NAME"],
                    "match_method": "exact",
                    "match_confidence": 0.99,
                    "latitude": best["LATITUDE"],
                    "longitude": best["LONGITUDE"],
                }
            )
            continue

        best_row, best_score, best_method = _best_match(bus_name, reference_name, substations, token_index)
        if best_row is not None and best_score >= 0.68:
            matched_rows.append(
                {
                    "ercot_bus": bus_name,
                    "substation_name": best_row["NAME"],
                    "match_method": best_method,
                    "match_confidence": round(best_score, 3),
                    "latitude": best_row["LATITUDE"],
                    "longitude": best_row["LONGITUDE"],
                }
            )

    crosswalk_df = pd.DataFrame(
        matched_rows,
        columns=["ercot_bus", "substation_name", "match_method", "match_confidence", "latitude", "longitude"],
    ).drop_duplicates(subset=["ercot_bus"], keep="first")
    crosswalk_df.to_csv(OUTPUT_PATH, index=False)

    review_df = _build_review_export(bus_names, crosswalk_df, substations, token_index)
    if not reference_points_df.empty:
        print(
            f"   ERCOT reference rows: settlement_points={len(reference_points_df):,}, "
            f"resource_node_to_unit={len(resource_node_to_unit_df):,}, noie_mapping={len(noie_mapping_df):,}"
        )
    print(f"   Review candidates exported: {len(review_df):,} to {REVIEW_OUTPUT_PATH}")
    return crosswalk_df
