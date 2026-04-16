# CLAUDE.md

## Project Purpose

This repository builds a public-data screening model for Texas energy infrastructure siting.

Current outputs:

- processed CSV of scored substations
- interactive HTML map
- GitHub Pages snapshot under `docs/`

This is a screening/indexing tool, not a formal engineering capacity determination.

## Core Workflow

Run from repo root:

```bash
../.venv/bin/python main.py --publish-docs
```

That pipeline currently does:

1. load normalized Texas substations
2. load ERCOT local ZIP market datasets
3. build a bus/resource-node to substation crosswalk
4. score substations with market/reference signals
5. apply Phase 2 hosting-band logic
6. generate the interactive map
7. sync publishable outputs into `docs/`

## Important Files

Main entrypoints:

- `main.py`
- `src/fetch_ercot.py`
- `src/fetch_substations.py`
- `src/crosswalk.py`
- `src/capacity_score.py`
- `src/hosting_band.py`
- `src/map_output.py`
- `src/publish_site.py`

Published artifacts:

- `output/capacity_map.html`
- `data/processed/substation_capacity_scores.csv`
- `data/processed/bus_substation_crosswalk.csv`
- `docs/index.html`
- `docs/capacity_map.html`

## Data Priorities

The most valuable inputs are authoritative ERCOT reference files, not just more price history.

High-value local files already in use:

- `Settlement_Points_*.csv`
- `Resource_Node_to_Unit_*.csv`
- `NOIE_Mapping_*.csv`
- `LMPSELECTBUSNP6787*.zip`
- `LMPSROSNODENP6788*.zip`
- `RTDLMPRNLZHUBNP6970*.zip`
- `SCEDBTCNP686*.zip`
- `SPPHLZNP6905*.zip`

Planned Phase 2 inputs:

- queue / interconnection data
- transmission project / upgrade data

The Phase 2 loaders exist now:

- `src/fetch_queue.py`
- `src/fetch_projects.py`

They currently auto-scan `data/raw/` for likely CSV filenames.

## Crosswalk Rules

Crosswalk quality is the main bottleneck.

Matching priority:

1. manual alias file in `data/raw/ercot_name_aliases.csv`
2. ERCOT reference file mappings
3. exact/canonical name match
4. constrained fuzzy fallback

Be conservative with new matching logic. False positives are worse than leaving a row unmatched.

Known bad pattern to avoid:

- owner-only matches to placeholder OSM names such as `Osm_Way_*`

## Hosting Band Logic

Phase 2 currently adds:

- `hosting_band`
- `hosting_confidence`
- `primary_limiter`
- `upgrade_pressure`
- `queue_hits`
- `project_hits`

Right now these are still a public-data approximation.
Without queue/project files, the model falls back to score/voltage/congestion-derived heuristics.

Do not describe these outputs as exact MW headroom.

## Public Positioning

Use neutral language in public-facing pages and docs.

Preferred terms:

- `Infrastructure Siting Index`
- `screening`
- `follow-up diligence`
- `hosting estimate`

Avoid overly explicit public language like:

- `exact capacity`
- `true available MW`
- `definitive interconnection headroom`

## Publishing Workflow

GitHub Pages serves from `docs/`.

After regenerating outputs:

```bash
git add docs data/processed output
git commit -m "Refresh published outputs"
git push
```

Pages site should auto-refresh after push.

## Repo Hygiene

Do not commit:

- `.env`
- `.venv/`
- raw ZIP archives under `data/raw/`
- transient cache files

Usually safe to commit:

- processed CSV outputs
- publishable HTML outputs
- reference CSVs that are important for reproducibility

## Current Reality

This project is now substantially better because ERCOT reference CSVs were added.
Those files improved the crosswalk far more than API-only work.

If choosing between:

- more API work for duplicate market reports
- better reference/queue/planning files

prefer better reference/queue/planning files.
