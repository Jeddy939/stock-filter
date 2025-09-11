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

The GUI applications (`moneymaker_app.py`, `moneymaker_pro.py`,
`moneymaker_pro_alpha.py`, `MoneymakerPro_Alpha_fetchfix.py`) now depend on the new package and share the
same filtering logic.

## Configuration

Default filter parameters are stored in `default filter settings.json`. The
file now includes a `lookback_weeks` key used by advanced filtering routines.
