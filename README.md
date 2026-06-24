# Stock Filter

This repository now exposes a reusable Python package located in `src/moneymaker`.
The package provides utilities for fetching market data and for applying
screening filters used by the desktop tools.

## Command Line Interface

The package exposes a small CLI. To fetch data into `stock_data.json` run:

```bash
python -m moneymaker fetch asx_yfinance_valid_stocks_2026-05-11.txt
```

Fetches use a SQLite cache by default (`stock_cache.sqlite`). The first run
fills the cache, and later runs only request missing or stale price ranges
before exporting the same JSON format used by the GUI and filters. This keeps
the durable store compact and queryable while preserving compatibility with the
existing desktop app.

You can control the number of threads used when downloading data with the
`--workers` option (default is 10):

```bash
python -m moneymaker fetch asx_yfinance_valid_stocks_2026-05-11.txt --workers 20
```

To run a cached incremental update and export JSON:

```bash
python -m moneymaker fetch asx_yfinance_valid_stocks_2026-05-11.txt --cache-file stock_cache.sqlite --output stock_data.json
```

To force a direct uncached fetch:

```bash
python -m moneymaker fetch asx_yfinance_valid_stocks_2026-05-11.txt --no-cache --output stock_data.json
```

The cache also stores company info. By default, company info is refreshed after
7 days, and recent price history refetches overlap the last 5 days to catch
late adjustments or corrections. You can tune that behavior:

```bash
python -m moneymaker fetch asx_yfinance_valid_stocks_2026-05-11.txt --info-refresh-days 14 --history-refresh-days 7
```

`yfinance` remains the default data provider for compatibility. For a smaller
smoke test, limit the run to the first 100 symbols without editing the source
file:

```bash
python -m moneymaker fetch us_tickers_nasdaqtrader.txt --limit 100 --output stock_data_100.json
```

An opt-in Stooq historical OHLCV source is also available:

```bash
python -m moneymaker fetch us_tickers_nasdaqtrader.txt --provider stooq --limit 100 --output stock_data_100_stooq.json
```

Stooq is used for daily historical prices only; company info and market cap are
still fetched through `yfinance` so the JSON schema used by filters and the GUI
is preserved. Keyless CSV coverage varies by exchange. Plain US symbols such as
`AAPL` are queried as `aapl.us`; ASX symbols may require a Stooq API key and are
reported as missing history when the keyless endpoint does not return CSV data.

Fetch output includes extraction metadata such as provider, cache file,
requested tickers, successful and missing histories, missing info counts,
missing market cap counts, provider limitations, cache hit/download counts, and
a `success` flag. CLI fetches exit non-zero when no requested ticker returns
historical data.

## Browser UI

On Windows, double-click:

```text
START_MONEYMAKER.bat
```

The launcher opens the browser UI and starts the local server. Leave the command
window open while using the app.

If you do not want a command window, double-click:

```text
START_MONEYMAKER_NO_CMD.vbs
```

That starts the app hidden and opens the browser. To stop the hidden server,
double-click:

```text
STOP_MONEYMAKER_NO_CMD.vbs
```

To export saved scan labels such as Winner, Potential Winner, Maybe, and Bad,
double-click:

```text
EXPORT_SCAN_LABELS.bat
```

It writes timestamped TXT and CSV files into the local `exports/` folder. If
someone else labelled stocks on their machine, copy their `stock_cache.sqlite`
or `stock_cache_us.sqlite` into this folder first, then run the exporter.

Run the local browser UI from the repository root:

```bash
python web_app.py
```

Then open:

```text
http://localhost:8000/
```

The web UI reads `stock_cache.sqlite` directly for cache status and filter
scans. Fetches run in the background and update the cache before exporting a
JSON compatibility file.

The UI has separate market defaults:

```text
ASX -> stock_cache.sqlite, yfinance, ASX ticker files
US  -> stock_cache_us.sqlite, yfinance, us_tickers_nasdaqtrader.txt
```

The active ASX ticker file is `asx_yfinance_valid_stocks_2026-05-11.txt`.
Older ticker lists and test screener inputs are kept under
`legacy/unused_screeners/` so they do not clutter the app dropdown.

Use the **Market** selector to switch between ASX and US mode. US mode keeps its
own SQLite cache so American data does not mix with ASX data. Click **Download
US Tickers** to refresh `us_tickers_nasdaqtrader.txt` from the official Nasdaq
Trader symbol directories. The same command is available from the CLI:

```bash
python -m moneymaker us-tickers -o us_tickers_nasdaqtrader.txt
```

US mode uses yfinance by default and keeps its own cache. The browser UI writes
to SQLite only and skips the legacy JSON export, which avoids multi-GB JSON
files at the end of large US fetches. Stooq remains available as an optional
provider, but recent live checks showed Stooq's CSV endpoint can ask for an API
key, so it is not used as the default no-key US path.

The fetch panel lists local `.txt` ticker files from the repository root. If
you enable the cleaned ticker file option, a fetch will create a new
`*_cleaned_YYYY-MM-DD.txt` file excluding attempted tickers that returned no
historical data. The original ticker file is not overwritten.

## Legacy Scripts

The GUI application (`moneymaker_pro_alpha.py`) now depends on the new package and shares the
same filtering logic.

## Configuration

Default filter parameters are stored in `default filter settings.json`. The
file now includes a `lookback_weeks` key used by advanced filtering routines.
