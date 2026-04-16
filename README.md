# ERCOT Capacity

Python project for identifying Texas substations with indicative grid capacity for large-load interconnection, using the existing `output/texas_private_substations.csv` dataset as the primary seed list and supplementing from HIFLD when available.

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

## Dev Mode Without ERCOT Credentials

If `ERCOT_USERNAME`, `ERCOT_PASSWORD`, and `ERCOT_SUBSCRIPTION_KEY` are not set, the pipeline:

- warns that live ERCOT data is unavailable
- loads `data/raw/ercot_sample.json` if present
- otherwise continues end-to-end with `UNSCORED` substations and still renders the map

## Open the Map

Open `output/capacity_map.html` in a browser.

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
