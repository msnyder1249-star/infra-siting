# Infrastructure Siting Index

Python project for screening Texas energy infrastructure locations using public reference data, market signals, and normalized substation inventory inputs.

## Project Layout

```text
ercot-capacity/
├── src/
│   ├── fetch_ercot.py
│   ├── fetch_substations.py
│   ├── capacity_score.py
│   ├── crosswalk.py
│   └── map_output.py
├── data/
│   ├── raw/
│   └── processed/
├── output/
│   └── capacity_map.html
├── config.py
├── main.py
├── requirements.txt
└── README.md
```

## Setup

```bash
python -m venv .venv
source .venv/bin/activate
pip install -r requirements.txt
cp .env.example .env
```

## ERCOT Registration

Register for ERCOT Public API access at:

`https://apiexplorer.ercot.com`

## Run

```bash
python main.py
```

Optional flags:

```bash
python main.py --refresh-cache --lookback 7 --min-voltage 138
```

To also sync the latest map and CSVs into `docs/` for GitHub Pages:

```bash
python main.py --publish-docs
```

## Dev Mode Without ERCOT Credentials

If `ERCOT_USERNAME`, `ERCOT_PASSWORD`, and `ERCOT_SUBSCRIPTION_KEY` are not set, the pipeline:

- warns that live ERCOT data is unavailable
- loads `data/raw/ercot_sample.json` if present
- otherwise continues end-to-end with `UNSCORED` substations and still renders the map

## Open the Map

Open `output/capacity_map.html` in a browser.

## GitHub Pages

This repo includes a publish-friendly `docs/` folder for GitHub Pages.

After generating fresh outputs:

```bash
python main.py --publish-docs
```

Then in GitHub:

1. Open `Settings`
2. Open `Pages`
3. Set `Source` to `Deploy from a branch`
4. Select branch `main`
5. Select folder `/docs`

The Pages site will serve:

- `docs/index.html`
- `docs/capacity_map.html`
- `docs/data/processed/substation_capacity_scores.csv`
- `docs/data/processed/bus_substation_crosswalk.csv`

## CSV Outputs

- `data/raw/tx_substations.csv`: normalized Texas substation inventory
- `data/processed/bus_substation_crosswalk.csv`: matched ERCOT buses to substations
- `data/processed/substation_capacity_scores.csv`: scored substation output

Important score fields:

- `lmp_avg`: average matched bus LMP
- `lmp_hub_spread`: absolute spread versus ERCOT hub average
- `lmp_std`: matched bus LMP volatility
- `shadow_price_nearby`: max nearby transmission shadow price
- `constraint_hours`: number of shadow-price-positive hours
- `CAPACITY_SCORE`: weighted 0-100 composite score
- `TIER`: `AVAILABLE`, `MARGINAL`, `CONSTRAINED`, or `UNSCORED`
- `hosting_band`: Phase 2 public-data hosting estimate band
- `hosting_confidence`: confidence in the hosting-band estimate
- `primary_limiter`: dominant public-data limiter signal
- `upgrade_pressure`: rough pressure indicator from future project data when available
- `ercot_bus_matched`: matched ERCOT bus name(s)
- `match_confidence`: crosswalk confidence
- `data_source`: `live`, `sample`, or `unscored`

## Scoring Methodology

Each substation is scored from 0 to 100 using:

- LMP spread to ERCOT hub averages
- LMP volatility
- Nearby binding constraint shadow prices and hours
- Maximum substation voltage as a rough transmission-capacity proxy

Formula:

```text
lmp_score         = max(0, 100 - (lmp_hub_spread * 5))
volatility_score  = max(0, 100 - (lmp_std * 2))
constraint_score  = max(0, 100 - (shadow_price_nearby * 0.5) - (constraint_hours * 3))
voltage_score     = min(100, (max_voltage / 500) * 100)

CAPACITY_SCORE = (lmp_score * 0.35) +
                 (volatility_score * 0.20) +
                 (constraint_score * 0.30) +
                 (voltage_score * 0.15)
```

Tier cutoffs:

- `AVAILABLE`: score >= 70
- `MARGINAL`: score 40-69
- `CONSTRAINED`: score < 40
- `UNSCORED`: no ERCOT match

## Known Limitations

- Bus-to-substation matching is approximate and depends on settlement point metadata plus name heuristics.
- True thermal capacity and interconnection feasibility require formal transmission studies and utility review.
- The project reuses the existing private-substation CSV, so public-only substations may still depend on HIFLD fetch availability.
- The published Pages site is a static snapshot of the latest generated outputs, not a live application.
