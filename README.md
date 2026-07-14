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

For the easiest online central ratings file, use Google Sheets. Every rating
click can be sent straight to one Sheet, while still being saved locally as a
backup.

Setup:

1. Create a blank Google Sheet.
2. In that Sheet, open Extensions > Apps Script.
3. Copy the contents of `google_sheets_rating_webhook.gs` into Apps Script.
4. Deploy it as a Web App.
5. Set access to allow the people using MoneyMaker.
6. Copy the Web App URL.
7. Double-click:

```text
CONFIGURE_GOOGLE_SHEETS_RATINGS.bat
```

After that, start MoneyMaker normally. When you click Winner, Potential Winner,
Maybe, or Bad, the app saves the rating locally and sends it to the Sheet. If
Google Sheets cannot be reached, the event is queued locally. Retry queued
events with:

```text
SYNC_PENDING_SHEETS_RATINGS.bat
```

To send ratings that were made on another computer before Sheets was set up,
copy these two files to that computer's MoneyMaker folder and double-click the
BAT file:

```text
SEND_EXISTING_RATINGS_TO_SHEETS.bat
send_existing_ratings_to_sheets.py
```

It scans old scan labels in `stock_cache.sqlite` and `stock_cache_us.sqlite`,
newer central ratings in `ratings\central_stock_ratings.sqlite`, and the
central JSON/JSONL backup files. It sends all found ratings to the configured
Google Sheet and writes an audit file into `exports`.

Ratings are also appended to the local analysis database:

```text
ratings\central_stock_ratings.sqlite
```

That SQLite file is the local backup/source for deeper analysis. Every rating
click adds a timestamped event, so you can track what was selected, when it was
selected, who selected it, and whether the price later moved in the right
direction.
Open the folder with:

```text
OPEN_RATINGS_FOLDER.bat
```

Ratings are also written into GitHub-friendly backup files in the repository:

```text
central_stock_ratings.json
central_stock_ratings.jsonl
```

Each event records the rating timestamp, rater name, ticker, label, scan id,
rank, signal date, close price, volume ratio, market cap, sector, industry,
cache source, and Yahoo Finance link.

If you want a GitHub backup, double-click this after rating stocks:

```text
PUSH_RATINGS_TO_GITHUB.bat
```

Double-click this to export the SQLite rating history with latest cached price
and return percentage:

```text
EXPORT_RATING_ANALYSIS.bat
```

If you want the central SQLite file somewhere else, set the environment variable
`MONEYMAKER_CENTRAL_RATINGS_DB` before launching the app. Set
`MONEYMAKER_RATER_NAME` if you want a friendly name instead of your Windows
username in the export.

For a OneDrive-backed ratings file, double-click:

```text
USE_ONEDRIVE_RATINGS.bat
```

That points MoneyMaker at:

```text
%OneDrive%\MoneyMaker\ratings\central_stock_ratings.sqlite
```

Then start the app with:

```text
START_MONEYMAKER_ONEDRIVE.bat
```

Open the OneDrive ratings folder with:

```text
OPEN_ONEDRIVE_RATINGS_FOLDER.bat
```

Export the OneDrive ratings database with:

```text
EXPORT_RATING_ANALYSIS_ONEDRIVE.bat
```

OneDrive is file sync, not a live database server. It works best for one main
user, or for a small number of people who are not rating at the exact same time.
Anyone else must have the shared OneDrive folder synced on their computer and
must launch MoneyMaker with the OneDrive launcher.

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

To send saved Winner / Potential / Maybe / Bad labels to Google Docs, place
your Google OAuth desktop credential JSON in this folder as:

```text
google_client_secret.json
```

Google's original downloaded `client_secret_*.json` filename also works.

Then double-click:

```text
SEND_LABELS_TO_GOOGLE_DOCS.bat
```

The first run opens a Google approval flow and then creates a Google Doc from
the labels saved in `stock_cache.sqlite`. The same export is also available
inside the browser UI from **Send Labels to Google Docs**.

Saved picks are also kept in a permanent list in the browser UI. The categories
are colour coded:

```text
Winner              -> green
Potential Winner    -> blue
Needs Confirmation  -> orange
Maybe               -> yellow
Bad                 -> red
```

The **Needs Confirmation** list stays visible above the chart whenever the app
opens. The full saved-picks table shows the ticker, category, market, added
date, updated date, source user, signal date, close, and market cap. Use the
market, category, and ticker filters above the table to narrow the list.

### Shared picks between computers/users

Shared picks use one central Google Sheet. Each computer keeps a local copy in
SQLite. Once a shared Sheet is linked, the app syncs when it opens and after a
pick is marked. The same saved ratings are applied to new filter results, so a
stock that was already rated appears with its category button highlighted.

In Google Cloud, enable both APIs for the same OAuth app:

```text
Google Docs API
Google Sheets API
```

The first run after this change may ask for Google approval again because the
app now needs Sheets access as well as Docs access.

Use this one file for setup and syncing:

```text
SEND_PICKS_TO_GOOGLE_SHEETS.bat
```

First computer:

```text
1. Run SEND_PICKS_TO_GOOGLE_SHEETS.bat.
2. Enter your display name.
3. Type CREATE when asked for the Sheet.
4. Copy the Google Sheet link printed by the batch file.
5. Share that Sheet with the other Google account as Editor.
```

Second computer:

```text
1. Run SEND_PICKS_TO_GOOGLE_SHEETS.bat.
2. Enter the other user's display name.
3. Paste the real shared Google Sheet link.
```

After the Sheet is linked, future app starts sync automatically. You can also
run `SEND_PICKS_TO_GOOGLE_SHEETS.bat` any time to force a send/receive sync.
Do not paste the local Moneymaker browser URL, and do not paste anything from
the Google secret JSON.

If the Google app is still in Testing mode, every Google account that syncs the
shared list must also be added as a Google Cloud test user.

The shared Sheet setting is stored locally in:

```text
moneymaker_shared_google.json
```

That file is ignored by Git. Do not copy `google_docs_token.json` between users;
each user should approve Google access with their own account.

To export an existing user's old saved picks from their local cache, copy their
old `stock_cache.sqlite` into this folder and double-click:

```text
SEND_EXISTING_PICKS_TO_GOOGLE_DOCS.bat
```

If the cache file has a different name, drag the `.sqlite` file onto
`SEND_EXISTING_PICKS_TO_GOOGLE_DOCS.bat`. This exports every saved Winner /
Potential Winner / Maybe / Bad label in that cache.

### Fresh computer setup for Google Docs exports

On a new computer, clone the repo:

```bash
git clone https://github.com/Jeddy939/stock-filter.git
cd stock-filter
```

If the repo is already there, update it instead:

```bash
git pull
```

Then copy these local files into the `stock-filter` folder:

```text
google_client_secret.json
```

or Google's original downloaded file:

```text
client_secret_*.json
```

To export old saved picks, also copy the user's old cache:

```text
stock_cache.sqlite
```

Do not copy `google_docs_token.json` between users. That file is created after
the Google login and belongs to the signed-in Google account. If the wrong
Google account is being used, delete `google_docs_token.json` and run the export
batch file again.

If the Google app is still in Testing mode, add the user's Google email address
as a test user in Google Cloud before they sign in.

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
