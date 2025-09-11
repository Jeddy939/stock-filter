# Stock Filter

This repository now exposes a reusable Python package located in `src/moneymaker`.
The package provides utilities for fetching market data and for applying
screening filters used by the desktop tools.

## Command Line Interface

The package exposes a small CLI. To fetch data into `stock_data.json` run:

```bash
python -m moneymaker fetch asx_200_tickers.txt
```

## Legacy Scripts

The GUI application (`moneymaker_pro_alpha.py`) now depends on the new package and shares the
same filtering logic.

## Configuration

Default filter parameters are stored in `default filter settings.json`. The
file now includes a `lookback_weeks` key used by advanced filtering routines.
