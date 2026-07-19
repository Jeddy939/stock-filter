"""Data fetching utilities for the moneymaker package."""


import json
import re
import sqlite3
import time
from concurrent.futures import ThreadPoolExecutor, as_completed
from datetime import date, datetime, timedelta, timezone
from io import StringIO
from pathlib import Path
from typing import Callable, Dict, Iterable, List, Optional, Sequence, Set, Tuple
from urllib.request import urlopen

import pandas as pd
import yfinance as yf
from tqdm import tqdm

DEFAULT_OUTPUT_FILE = "stock_data.json"
DEFAULT_CACHE_FILE = "stock_cache.sqlite"
DEFAULT_US_CACHE_FILE = "stock_cache_us.sqlite"
DEFAULT_US_TICKER_FILE = "us_tickers_nasdaqtrader.txt"
DEFAULT_DATA_YEARS = 15
DEFAULT_WORKERS = 10
DEFAULT_PROVIDER = "yfinance"
SUPPORTED_PROVIDERS = ("yfinance", "stooq")
INFO_WORKER_LIMIT = 8
DEFAULT_INFO_REFRESH_DAYS = 7
DEFAULT_HISTORY_REFRESH_DAYS = 5
DEFAULT_HISTORY_CHUNK_SIZE = 100
DEFAULT_HISTORY_PAUSE_SECONDS = 0.0
DEFAULT_INFO_PAUSE_SECONDS = 0.0
DEFAULT_RATE_LIMIT_PAUSE_SECONDS = 180.0
DEFAULT_RATE_LIMIT_RETRIES = 8
DEFAULT_STOP_ON_RATE_LIMIT = False
DEFAULT_CACHE_WRITE_BATCH_SIZE = 50000

_HEADER_TOKENS = {"SYMBOL", "CODE", "ASX CODE", "TICKER"}
_ASX_FILENAME_TOKENS = ("asx", "all_ords", "all ords", "ordinaries")
_STOOQ_COLUMNS = ["Open", "High", "Low", "Close", "Volume"]
_OHLCV_COLUMNS = ["Open", "High", "Low", "Close", "Volume"]
NASDAQ_LISTED_URL = "https://www.nasdaqtrader.com/dynamic/SymDir/nasdaqlisted.txt"
NASDAQ_OTHER_LISTED_URL = "https://www.nasdaqtrader.com/dynamic/SymDir/otherlisted.txt"
ProgressCallback = Optional[Callable[[str, int, Optional[int], str], None]]


def _emit_progress(
    callback: ProgressCallback,
    stage: str,
    current: int,
    total: Optional[int],
    message: str,
) -> None:
    """Send structured progress updates to callers that need live status."""

    if callback:
        callback(stage, current, total, message)


def normalize_provider(provider: str) -> str:
    """Return a supported provider key."""

    normalized = (provider or DEFAULT_PROVIDER).strip().lower()
    if normalized not in SUPPORTED_PROVIDERS:
        supported = ", ".join(SUPPORTED_PROVIDERS)
        raise ValueError(f"Unsupported provider '{provider}'. Supported providers: {supported}.")
    return normalized


def apply_ticker_limit(tickers: List[str], limit: Optional[int]) -> List[str]:
    """Return the first ``limit`` tickers, preserving unlimited legacy behavior."""

    if limit is None:
        return tickers
    if limit < 1:
        raise ValueError("limit must be a positive integer when provided.")
    return tickers[:limit]


def _coerce_date(value) -> date:
    """Return a date from pandas, datetime, date, or ISO string input."""

    if hasattr(value, "date"):
        return value.date()
    return pd.to_datetime(value).date()


def _date_string(value) -> str:
    """Normalize supported date inputs to YYYY-MM-DD."""

    return _coerce_date(value).isoformat()


def _history_download_end(value=None, now=None) -> datetime:
    """Return the exclusive yfinance cutoff at a stable UTC date boundary."""

    if value:
        return datetime.combine(_coerce_date(value), datetime.min.time())
    reference = now or datetime.now(timezone.utc)
    return datetime.combine(_coerce_date(reference) + timedelta(days=1), datetime.min.time())


def _cache_connect(cache_file: str) -> sqlite3.Connection:
    """Open and initialize the SQLite cache."""

    cache_path = Path(cache_file)
    if cache_path.parent and str(cache_path.parent) not in ("", "."):
        cache_path.parent.mkdir(parents=True, exist_ok=True)

    conn = sqlite3.connect(str(cache_path))
    conn.execute("PRAGMA journal_mode=WAL")
    conn.execute("PRAGMA synchronous=NORMAL")
    _init_cache(conn)
    return conn


def _init_cache(conn: sqlite3.Connection) -> None:
    """Create cache tables if they are missing."""

    conn.executescript(
        """
        CREATE TABLE IF NOT EXISTS price_history (
            provider TEXT NOT NULL,
            ticker TEXT NOT NULL,
            date TEXT NOT NULL,
            open REAL,
            high REAL,
            low REAL,
            close REAL,
            volume REAL,
            fetched_at_utc TEXT NOT NULL,
            PRIMARY KEY (provider, ticker, date)
        );

        CREATE INDEX IF NOT EXISTS idx_price_history_ticker_date
            ON price_history (provider, ticker, date);

        CREATE TABLE IF NOT EXISTS company_info (
            ticker TEXT PRIMARY KEY,
            info_json TEXT NOT NULL,
            fetched_at_utc TEXT NOT NULL
        );
        """
    )
    conn.commit()


_HISTORY_UPSERT_SQL = """
    INSERT INTO price_history
        (provider, ticker, date, open, high, low, close, volume, fetched_at_utc)
    VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?)
    ON CONFLICT(provider, ticker, date) DO UPDATE SET
        open = excluded.open,
        high = excluded.high,
        low = excluded.low,
        close = excluded.close,
        volume = excluded.volume,
        fetched_at_utc = excluded.fetched_at_utc
    """


def _store_history_cache(
    conn: sqlite3.Connection,
    provider: str,
    histories: Dict[str, pd.DataFrame],
    progress_callback: ProgressCallback = None,
    batch_size: int = DEFAULT_CACHE_WRITE_BATCH_SIZE,
) -> int:
    """Upsert historical OHLCV rows into the cache."""

    fetched_at = datetime.now(timezone.utc).isoformat()
    rows = []
    rows_written = 0
    processed_tickers = 0
    total_tickers = len(histories)
    batch_size = max(1, int(batch_size or DEFAULT_CACHE_WRITE_BATCH_SIZE))

    def flush_rows() -> None:
        nonlocal rows_written
        if not rows:
            return
        conn.executemany(_HISTORY_UPSERT_SQL, rows)
        conn.commit()
        rows_written += len(rows)
        rows.clear()

    if total_tickers:
        _emit_progress(
            progress_callback,
            "History cache",
            0,
            total_tickers,
            f"Writing downloaded history for {total_tickers} tickers to SQLite.",
        )

    for ticker, frame in histories.items():
        if frame is None or frame.empty:
            processed_tickers += 1
            continue
        history = frame.copy()
        history.index = pd.to_datetime(history.index)
        for column in _OHLCV_COLUMNS:
            if column not in history.columns:
                history[column] = None
            else:
                history[column] = pd.to_numeric(history[column], errors="coerce")
        history = history.dropna(subset=["Close"], how="any")
        for index, row in history.iterrows():
            rows.append(
                (
                    provider,
                    ticker,
                    _date_string(index),
                    _nullable_float(row.get("Open")),
                    _nullable_float(row.get("High")),
                    _nullable_float(row.get("Low")),
                    _nullable_float(row.get("Close")),
                    _nullable_float(row.get("Volume")),
                    fetched_at,
                )
            )
            if len(rows) >= batch_size:
                flush_rows()

        processed_tickers += 1
        if processed_tickers == total_tickers or processed_tickers % 25 == 0:
            _emit_progress(
                progress_callback,
                "History cache",
                processed_tickers,
                total_tickers,
                (
                    f"Wrote {rows_written + len(rows):,} history rows "
                    f"from {processed_tickers}/{total_tickers} tickers."
                ),
            )

    flush_rows()
    if total_tickers:
        _emit_progress(
            progress_callback,
            "History cache",
            total_tickers,
            total_tickers,
            f"Finished writing {rows_written:,} history rows to SQLite.",
        )
    return rows_written


def _nullable_float(value) -> Optional[float]:
    """Convert pandas/numpy scalar values to JSON/SQLite-friendly floats."""

    if pd.isna(value):
        return None
    return float(value)


def _load_cached_histories(
    conn: sqlite3.Connection,
    provider: str,
    tickers: Sequence[str],
    start: datetime,
    end: datetime,
    progress_callback: ProgressCallback = None,
) -> Dict[str, pd.DataFrame]:
    """Load cached histories for tickers within the requested date range."""

    start_key = _date_string(start)
    end_key = _date_string(end)
    cached: Dict[str, pd.DataFrame] = {}

    total_tickers = len(tickers)
    if total_tickers:
        _emit_progress(
            progress_callback,
            "Cache export",
            0,
            total_tickers,
            f"Loading cached history for {total_tickers} tickers.",
        )

    for index, ticker in enumerate(tickers, 1):
        rows = conn.execute(
            """
            SELECT date, open, high, low, close, volume
            FROM price_history
            WHERE provider = ? AND ticker = ? AND date >= ? AND date <= ?
            ORDER BY date
            """,
            (provider, ticker, start_key, end_key),
        ).fetchall()
        if rows:
            frame = pd.DataFrame(
                rows,
                columns=["Date", "Open", "High", "Low", "Close", "Volume"],
            )
            frame["Date"] = pd.to_datetime(frame["Date"])
            frame = frame.set_index("Date")
            cached[ticker] = frame[_OHLCV_COLUMNS]
        if index == total_tickers or index % 25 == 0:
            _emit_progress(
                progress_callback,
                "Cache export",
                index,
                total_tickers,
                f"Loaded {len(cached)}/{total_tickers} histories from cache.",
            )

    return cached


def _cached_history_tickers(
    conn: sqlite3.Connection,
    provider: str,
    tickers: Sequence[str],
    start: datetime,
    end: datetime,
    progress_callback: ProgressCallback = None,
) -> Set[str]:
    """Return tickers that have at least one cached history row in range."""

    if not tickers:
        return set()

    start_key = _date_string(start)
    end_key = _date_string(end)
    cached: Set[str] = set()
    ticker_chunks = list(_chunked(list(tickers), 500))
    total_chunks = len(ticker_chunks)
    _emit_progress(
        progress_callback,
        "Cache export",
        0,
        len(tickers),
        f"Checking cached history availability for {len(tickers)} tickers.",
    )

    for chunk_index, chunk in enumerate(ticker_chunks, 1):
        placeholders = ",".join("?" for _ in chunk)
        rows = conn.execute(
            f"""
            SELECT DISTINCT ticker
            FROM price_history
            WHERE provider = ?
              AND date >= ?
              AND date <= ?
              AND ticker IN ({placeholders})
            """,
            (provider, start_key, end_key, *chunk),
        ).fetchall()
        cached.update(row[0] for row in rows)
        _emit_progress(
            progress_callback,
            "Cache export",
            min(chunk_index * 500, len(tickers)),
            len(tickers),
            (
                f"Found cached history for {len(cached)}/{len(tickers)} tickers "
                f"({chunk_index}/{total_chunks})."
            ),
        )

    return cached


def _cached_history_bounds(
    conn: sqlite3.Connection,
    provider: str,
    ticker: str,
) -> Tuple[Optional[str], Optional[str]]:
    """Return earliest and latest cached date for one ticker/provider."""

    row = conn.execute(
        """
        SELECT MIN(date), MAX(date)
        FROM price_history
        WHERE provider = ? AND ticker = ?
        """,
        (provider, ticker),
    ).fetchone()
    if not row:
        return None, None
    return row[0], row[1]


def _history_fetch_groups(
    conn: sqlite3.Connection,
    provider: str,
    tickers: Sequence[str],
    start: datetime,
    end: datetime,
    refresh_days: int = DEFAULT_HISTORY_REFRESH_DAYS,
) -> Dict[str, List[str]]:
    """Group tickers by start date needed for incremental refresh."""

    groups: Dict[str, List[str]] = {}
    requested_start = _coerce_date(start)
    requested_end = _coerce_date(end)
    refresh_delta = timedelta(days=max(0, refresh_days))
    fresh_enough_date = requested_end - timedelta(days=max(1, refresh_days))

    for ticker in tickers:
        earliest, latest = _cached_history_bounds(conn, provider, ticker)
        fetch_start = requested_start
        if earliest and latest:
            earliest_date = datetime.fromisoformat(earliest).date()
            latest_date = datetime.fromisoformat(latest).date()
            if earliest_date <= requested_start and latest_date >= fresh_enough_date:
                continue
            if latest_date >= requested_start:
                fetch_start = max(requested_start, latest_date - refresh_delta)
        groups.setdefault(fetch_start.isoformat(), []).append(ticker)

    return groups


def _store_info_cache(conn: sqlite3.Connection, info_data: Dict[str, dict]) -> None:
    """Upsert yfinance info payloads into the cache."""

    if not info_data:
        return
    fetched_at = datetime.now(timezone.utc).isoformat()
    rows = [
        (ticker, json.dumps(info, separators=(",", ":")), fetched_at)
        for ticker, info in info_data.items()
        if info
    ]
    if not rows:
        return
    conn.executemany(
        """
        INSERT INTO company_info (ticker, info_json, fetched_at_utc)
        VALUES (?, ?, ?)
        ON CONFLICT(ticker) DO UPDATE SET
            info_json = excluded.info_json,
            fetched_at_utc = excluded.fetched_at_utc
        """,
        rows,
    )
    conn.commit()


def _load_info_cache(
    conn: sqlite3.Connection,
    tickers: Sequence[str],
    max_age_days: int = DEFAULT_INFO_REFRESH_DAYS,
) -> Tuple[Dict[str, dict], List[str]]:
    """Return fresh cached info and tickers that need a refresh."""

    cutoff = datetime.now(timezone.utc) - timedelta(days=max(0, max_age_days))
    cached: Dict[str, dict] = {}
    refresh: List[str] = []

    for ticker in tickers:
        row = conn.execute(
            "SELECT info_json, fetched_at_utc FROM company_info WHERE ticker = ?",
            (ticker,),
        ).fetchone()
        if not row:
            refresh.append(ticker)
            continue
        try:
            fetched_at = datetime.fromisoformat(row[1])
            info = json.loads(row[0])
        except (TypeError, ValueError, json.JSONDecodeError):
            refresh.append(ticker)
            continue
        if fetched_at < cutoff:
            refresh.append(ticker)
            continue
        cached[ticker] = info

    return cached, refresh


def _provider_limitations(provider: str) -> List[str]:
    """Human-readable caveats written to extraction metadata."""

    if provider == "stooq":
        return [
            "Stooq is used for historical OHLCV only; company info and market cap are still fetched from yfinance.",
            "Stooq keyless CSV coverage varies by exchange. US symbols are queried with a .us suffix. Some non-US exchanges, including ASX, may require a Stooq API key and will be reported as missing history.",
        ]
    return [
        "yfinance is the default provider for historical OHLCV and company info.",
    ]


def _read_url_text(url: str, timeout: int = 30) -> str:
    """Read UTF-8-ish text from a public data endpoint."""

    with urlopen(url, timeout=timeout) as response:  # nosec B310 - fixed public market-data endpoint
        return response.read().decode("utf-8", errors="replace")


def _normalize_us_symbol(symbol: str) -> str:
    """Normalize Nasdaq Trader symbols for Stooq/yfinance-style US lookups."""

    ticker = str(symbol or "").strip().upper()
    if not ticker or ticker in {"SYMBOL", "ACT SYMBOL"}:
        return ""
    ticker = ticker.replace("/", "-")
    ticker = ticker.replace("^", "-")
    ticker = re.sub(r"[^A-Z0-9.\-]", "", ticker)
    return ticker.strip(".-")


def _parse_nasdaq_trader_symbols(content: str, symbol_column: str) -> List[str]:
    """Parse Nasdaq Trader pipe-delimited symbol directory content."""

    lines = [line.strip() for line in content.splitlines() if line.strip()]
    if not lines:
        return []
    headers = [part.strip() for part in lines[0].split("|")]
    try:
        symbol_index = headers.index(symbol_column)
    except ValueError:
        return []

    test_issue_index = headers.index("Test Issue") if "Test Issue" in headers else None
    symbols: List[str] = []
    seen: Set[str] = set()
    for line in lines[1:]:
        if line.lower().startswith("file creation time"):
            break
        parts = line.split("|")
        if len(parts) <= symbol_index:
            continue
        if test_issue_index is not None and len(parts) > test_issue_index and parts[test_issue_index].upper() == "Y":
            continue
        ticker = _normalize_us_symbol(parts[symbol_index])
        if ticker and ticker not in seen:
            seen.add(ticker)
            symbols.append(ticker)
    return symbols


def fetch_us_tickers_from_nasdaq_trader() -> List[str]:
    """Return active US-listed symbols from Nasdaq Trader symbol directories."""

    nasdaq = _parse_nasdaq_trader_symbols(_read_url_text(NASDAQ_LISTED_URL), "Symbol")
    other = _parse_nasdaq_trader_symbols(_read_url_text(NASDAQ_OTHER_LISTED_URL), "ACT Symbol")
    seen: Set[str] = set()
    tickers: List[str] = []
    for ticker in nasdaq + other:
        if ticker not in seen:
            seen.add(ticker)
            tickers.append(ticker)
    return sorted(tickers)


def write_us_ticker_file(output_file: str = DEFAULT_US_TICKER_FILE) -> Dict[str, object]:
    """Download current US symbols and write a local ticker file."""

    tickers = fetch_us_tickers_from_nasdaq_trader()
    output = Path(output_file)
    generated_at = datetime.now(timezone.utc).isoformat()
    with output.open("w", encoding="utf-8") as handle:
        handle.write("Symbol\n")
        handle.write(f"# Generated from Nasdaq Trader symbol directories at {generated_at}\n")
        handle.write(f"# Sources: {NASDAQ_LISTED_URL} and {NASDAQ_OTHER_LISTED_URL}\n")
        handle.write("# Includes common stocks and ETFs. Test issues are excluded.\n")
        for ticker in tickers:
            handle.write(f"{ticker}\n")
    return {
        "output_file": str(output),
        "ticker_count": len(tickers),
        "generated_at_utc": generated_at,
        "sources": [NASDAQ_LISTED_URL, NASDAQ_OTHER_LISTED_URL],
    }


def get_tickers_from_file(filename: str) -> List[str]:
    """Reads tickers from a text file, handling different formats."""
    tickers: List[str] = []
    seen: Set[str] = set()
    is_asx_list = _looks_like_asx_file(filename)
    try:
        with open(filename, "r", encoding="utf-8") as f:
            first_line = f.readline()
            if "Symbol" in first_line and "Security Name" in first_line:
                pass
            else:
                _process_line(first_line, is_asx_list, tickers, seen)
            for line_content in f:
                _process_line(line_content, is_asx_list, tickers, seen)
        if not tickers:
            print(f"Warning: No tickers found in {filename}.")
        return tickers
    except FileNotFoundError:
        print(f"Error: Ticker file '{filename}' not found.")
        return []
    except Exception as e:
        print(f"Error reading ticker file: {str(e)}")
        return []


def _looks_like_asx_file(filename: str) -> bool:
    """Infer ASX ticker files from common local filenames."""

    normalized = filename.lower().replace("-", "_")
    return any(token in normalized for token in _ASX_FILENAME_TOKENS)


def _process_line(
    line_content: str, is_asx_list: bool, tickers: List[str], seen: Set[str]
) -> None:
    """Helper for ``get_tickers_from_file``."""
    line = line_content.strip()
    if not line or line.startswith("#"):
        return
    if "|" in line:
        raw_ticker = line.split("|")[0]
    elif "," in line:
        raw_ticker = line.split(",")[0]
    else:
        raw_ticker = line

    ticker = _normalize_ticker(raw_ticker, is_asx_list)
    if ticker and ticker not in seen:
        seen.add(ticker)
        tickers.append(ticker)


def _normalize_ticker(raw_ticker: str, is_asx_list: bool) -> str:
    """Clean ticker text from CSV, pipe-delimited, and scraped table sources."""

    ticker = str(raw_ticker).strip().upper()
    ticker = re.sub(r"\[[^\]]+\]", "", ticker)
    ticker = re.sub(r"\s+", "", ticker)
    if ticker.startswith("ASX:"):
        ticker = ticker[4:]
    if ticker.startswith("^"):
        index_symbol = re.sub(r"[^A-Z0-9.\-]", "", ticker[1:])
        return f"^{index_symbol}" if index_symbol else ""
    ticker = re.sub(r"[^A-Z0-9.\-]", "", ticker)

    if not ticker or ticker in _HEADER_TOKENS:
        return ""
    if is_asx_list and not ticker.endswith(".AX"):
        ticker += ".AX"
    return ticker


def _write_cleaned_ticker_file(
    ticker_file: str,
    requested_tickers: Sequence[str],
    removed_tickers: Sequence[str],
) -> Optional[str]:
    """Write a new ticker file with missing-history tickers removed."""

    removed = set(removed_tickers)
    if not removed:
        return None

    source = Path(ticker_file)
    output = source.with_name(
        f"{source.stem}_cleaned_{datetime.now().strftime('%Y-%m-%d')}{source.suffix or '.txt'}"
    )
    counter = 2
    while output.exists():
        output = source.with_name(
            f"{source.stem}_cleaned_{datetime.now().strftime('%Y-%m-%d')}_{counter}{source.suffix or '.txt'}"
        )
        counter += 1

    kept = [ticker for ticker in requested_tickers if ticker not in removed]
    with open(output, "w", encoding="utf-8") as handle:
        handle.write("Symbol\n")
        handle.write(f"# Generated from {ticker_file} on {datetime.now().strftime('%Y-%m-%d %H:%M:%S')}\n")
        handle.write("# Removed tickers with missing historical data from the latest fetch.\n")
        for ticker in kept:
            handle.write(f"{ticker}\n")

    return str(output)


def _fetch_info(ticker: str) -> Tuple[str, dict]:
    """Fetch one ticker's yfinance ``info`` payload."""

    stock = yf.Ticker(ticker)
    info = stock.info
    if info and info.get("regularMarketPrice") is not None:
        return ticker, info
    return ticker, {}


def fetch_info_individual(
    tickers: List[str],
    workers: int = DEFAULT_WORKERS,
    progress_callback: ProgressCallback = None,
    info_pause_seconds: float = DEFAULT_INFO_PAUSE_SECONDS,
    rate_limit_pause_seconds: float = DEFAULT_RATE_LIMIT_PAUSE_SECONDS,
    max_rate_limit_retries: int = DEFAULT_RATE_LIMIT_RETRIES,
) -> Dict[str, dict]:
    """Fetch ``.info`` data for tickers concurrently with a conservative cap."""

    all_info_data: Dict[str, dict] = {}
    info_workers = max(1, min(int(workers or 1), INFO_WORKER_LIMIT, len(tickers)))
    print(f"\n--- Step 2 of 3: Fetching company info ({info_workers} workers) ---")
    _emit_progress(
        progress_callback,
        "Company info",
        0,
        len(tickers),
        f"Fetching company info for {len(tickers)} tickers with {info_workers} workers.",
    )

    if info_workers == 1:
        for completed, ticker in enumerate(tqdm(tickers, desc="Fetching Info", unit="ticker"), 1):
            attempts = 0
            while True:
                try:
                    fetched_ticker, info = _fetch_info(ticker)
                    break
                except Exception as e:  # pragma: no cover - network error path
                    if _is_rate_limit_error(e) and attempts < max_rate_limit_retries:
                        wait_time = rate_limit_pause_seconds * (attempts + 1)
                        tqdm.write(
                            f"   [!] Rate limit hit while fetching info for {ticker}. "
                            f"Sleeping {wait_time:.0f}s before retrying."
                        )
                        time.sleep(wait_time)
                        attempts += 1
                        continue
                    tqdm.write(f"[-] Warning: Failed to fetch info for {ticker}. Error: {str(e)[:100]}")
                    fetched_ticker, info = ticker, {}
                    break
            if info:
                all_info_data[fetched_ticker] = info
            else:
                tqdm.write(f"[-] Warning: No valid info found for {ticker}.")
            _emit_progress(
                progress_callback,
                "Company info",
                completed,
                len(tickers),
                f"Fetched company info {completed}/{len(tickers)}: {ticker}",
            )
            if info_pause_seconds > 0 and completed < len(tickers):
                time.sleep(info_pause_seconds)
        _emit_progress(
            progress_callback,
            "Company info",
            len(tickers),
            len(tickers),
            f"Company info complete. Found info for {len(all_info_data)} tickers.",
        )
        return all_info_data

    with ThreadPoolExecutor(max_workers=info_workers) as executor:
        futures = {executor.submit(_fetch_info, ticker): ticker for ticker in tickers}
        completed = 0
        for future in tqdm(
            as_completed(futures),
            total=len(futures),
            desc="Fetching Info",
            unit="ticker",
        ):
            ticker = futures[future]
            try:
                fetched_ticker, info = future.result()
            except Exception as e:  # pragma: no cover - network error path
                tqdm.write(f"[-] Warning: Failed to fetch info for {ticker}. Error: {str(e)[:100]}")
                completed += 1
                _emit_progress(
                    progress_callback,
                    "Company info",
                    completed,
                    len(tickers),
                    f"Failed company info {completed}/{len(tickers)}: {ticker}",
                )
                continue
            if info:
                all_info_data[fetched_ticker] = info
            else:
                tqdm.write(f"[-] Warning: No valid info found for {ticker}.")
            completed += 1
            _emit_progress(
                progress_callback,
                "Company info",
                completed,
                len(tickers),
                f"Fetched company info {completed}/{len(tickers)}: {ticker}",
            )
    _emit_progress(
        progress_callback,
        "Company info",
        len(tickers),
        len(tickers),
        f"Company info complete. Found info for {len(all_info_data)} tickers.",
    )
    return all_info_data


def _chunked(iterable: List[str], size: int) -> Iterable[List[str]]:
    """Simple chunk generator used for batch downloads."""

    for idx in range(0, len(iterable), size):
        yield iterable[idx : idx + size]


def _is_rate_limit_error(error: Exception) -> bool:
    """Detects yfinance rate-limit style errors without direct imports."""

    message = str(error)
    return "rate limit" in message.lower() or "too many requests" in message.lower()


def _is_invalid_period_error(error: Exception) -> bool:
    """Detects invalid-period errors produced by yfinance."""

    return "period 'max' is invalid" in str(error).lower()


def _stooq_symbol(ticker: str) -> str:
    """Map common Yahoo-style tickers to Stooq query symbols."""

    normalized = ticker.strip().lower()
    if normalized.endswith(".ax"):
        return f"{normalized[:-3]}.au"
    normalized = normalized.replace("/", "-")
    normalized = normalized.replace("^", "-")
    if "." not in normalized:
        return f"{normalized}.us"
    return normalized


def _read_stooq_history(ticker: str, start: datetime, end: datetime) -> pd.DataFrame:
    """Read daily historical OHLCV from Stooq's unauthenticated CSV endpoint."""

    symbol = _stooq_symbol(ticker)
    url = (
        "https://stooq.com/q/d/l/"
        f"?s={symbol}&d1={start.strftime('%Y%m%d')}&d2={end.strftime('%Y%m%d')}&i=d"
    )

    with urlopen(url, timeout=30) as response:  # nosec B310 - user-selected public data source
        content = response.read().decode("utf-8", errors="replace").strip()

    if not content or content.lower().startswith("no data"):
        return pd.DataFrame()

    first_line = content.splitlines()[0].strip().lower()
    if "apikey" in content[:500].lower() and not first_line.startswith("date,"):
        raise ValueError(
            f"Stooq did not return keyless CSV data for {ticker} ({symbol}); API key may be required."
        )
    if not first_line.startswith("date,"):
        raise ValueError(f"Stooq returned an unexpected non-CSV response for {ticker} ({symbol}).")

    data = pd.read_csv(StringIO(content))
    if data.empty or "Date" not in data.columns or data.iloc[0].astype(str).str.contains("No data").any():
        return pd.DataFrame()

    data["Date"] = pd.to_datetime(data["Date"], errors="coerce")
    data = data.dropna(subset=["Date"]).set_index("Date")
    available_columns = [column for column in _STOOQ_COLUMNS if column in data.columns]
    data = data[available_columns].dropna(how="all")
    return data


def _download_stooq_historical_data(
    tickers: List[str],
    start: datetime,
    end: datetime,
    progress_callback: ProgressCallback = None,
) -> Tuple[Dict[str, pd.DataFrame], Set[str]]:
    """Download historical data from Stooq one ticker at a time."""

    historical_data: Dict[str, pd.DataFrame] = {}
    missing_tickers: Set[str] = set()

    for idx, ticker in enumerate(tqdm(tickers, desc="Fetching Stooq History", unit="ticker"), 1):
        _emit_progress(
            progress_callback,
            "Historical prices",
            idx - 1,
            len(tickers),
            f"Fetching Stooq history {idx}/{len(tickers)}: {ticker}",
        )
        try:
            data = _read_stooq_history(ticker, start, end)
        except Exception as exc:  # pragma: no cover - network error path
            tqdm.write(
                f"[-] Warning: Failed to fetch Stooq historical data for {ticker}. "
                f"Error: {str(exc)[:120]}"
            )
            missing_tickers.add(ticker)
            continue

        if data.empty:
            missing_tickers.add(ticker)
        else:
            historical_data[ticker] = data
        _emit_progress(
            progress_callback,
            "Historical prices",
            idx,
            len(tickers),
            f"Completed Stooq history {idx}/{len(tickers)}: {ticker}",
        )

    return historical_data, missing_tickers


def _extract_histories_from_frame(chunk: List[str], data: pd.DataFrame) -> Dict[str, pd.DataFrame]:
    """Normalises the output of ``yfinance.download`` into ticker keyed frames."""

    if data is None or data.empty:
        return {}

    if isinstance(data, pd.Series):
        data = data.to_frame()

    histories: Dict[str, pd.DataFrame] = {}
    if isinstance(data.columns, pd.MultiIndex):
        available = set(data.columns.get_level_values(0))
        for ticker in chunk:
            if ticker in available:
                hist = data[ticker].dropna(how="all")
                if not hist.empty:
                    histories[ticker] = hist
    else:
        ticker = chunk[0]
        hist = data.dropna(how="all")
        if not hist.empty:
            histories[ticker] = hist

    return histories


def _download_chunk_sequential(
    chunk: List[str],
    start: datetime,
    end: datetime,
    pause_seconds: float = DEFAULT_HISTORY_PAUSE_SECONDS,
    rate_limit_pause_seconds: float = DEFAULT_RATE_LIMIT_PAUSE_SECONDS,
    max_rate_limit_retries: int = DEFAULT_RATE_LIMIT_RETRIES,
    stop_on_rate_limit: bool = DEFAULT_STOP_ON_RATE_LIMIT,
) -> Tuple[Dict[str, pd.DataFrame], Set[str], bool]:
    """Sequentially downloads historical data for each ticker in ``chunk``."""

    downloaded: Dict[str, pd.DataFrame] = {}
    failures: Set[str] = set()
    stopped_on_rate_limit = False

    for ticker in chunk:
        attempts = 0
        while attempts <= max_rate_limit_retries:
            try:
                data = yf.download(
                    ticker,
                    start=start.strftime("%Y-%m-%d"),
                    end=end.strftime("%Y-%m-%d"),
                    interval="1d",
                    auto_adjust=False,
                    progress=False,
                )
            except Exception as exc:  # pragma: no cover - network error path
                if _is_rate_limit_error(exc):
                    if stop_on_rate_limit:
                        print(
                            f"   [!] Rate limit hit while fetching {ticker} individually. "
                            "Stopping now so cached progress can be resumed later."
                        )
                        stopped_on_rate_limit = True
                        return downloaded, failures, stopped_on_rate_limit
                    wait_time = rate_limit_pause_seconds * (attempts + 1)
                    print(
                        f"   [!] Rate limit hit while fetching {ticker} individually. "
                        f"Sleeping {wait_time:.0f}s before retrying."
                    )
                    time.sleep(wait_time)
                    attempts += 1
                    continue
                if _is_invalid_period_error(exc):
                    failures.add(ticker)
                    break
                print(
                    f"[-] Warning: Failed to fetch historical data for {ticker}. "
                    f"Error: {str(exc)[:120]}"
                )
                failures.add(ticker)
                break
            else:
                data = data.dropna(how="all")
                if data.empty:
                    failures.add(ticker)
                else:
                    downloaded[ticker] = data
                break
            if pause_seconds > 0:
                time.sleep(pause_seconds)
        else:
            failures.add(ticker)
    return downloaded, failures, stopped_on_rate_limit


def _download_historical_data(
    tickers: List[str],
    start: datetime,
    end: datetime,
    workers: int,
    progress_callback: ProgressCallback = None,
    chunk_size: int = DEFAULT_HISTORY_CHUNK_SIZE,
    pause_seconds: float = DEFAULT_HISTORY_PAUSE_SECONDS,
    rate_limit_pause_seconds: float = DEFAULT_RATE_LIMIT_PAUSE_SECONDS,
    max_rate_limit_retries: int = DEFAULT_RATE_LIMIT_RETRIES,
    stop_on_rate_limit: bool = DEFAULT_STOP_ON_RATE_LIMIT,
) -> Tuple[Dict[str, pd.DataFrame], Set[str], bool]:
    """Downloads historical data in manageable batches with fallbacks."""

    historical_data: Dict[str, pd.DataFrame] = {}
    missing_tickers: Set[str] = set()
    stopped_on_rate_limit = False

    chunk_size = max(1, int(chunk_size or DEFAULT_HISTORY_CHUNK_SIZE))
    chunks = list(_chunked(tickers, chunk_size))
    processed = 0
    for chunk_number, chunk in enumerate(chunks, 1):
        _emit_progress(
            progress_callback,
            "Historical prices",
            processed,
            len(tickers),
            f"Downloading yfinance history batch {chunk_number}/{len(chunks)} ({len(chunk)} tickers).",
        )
        attempts = 0
        while attempts <= max_rate_limit_retries:
            try:
                data = yf.download(
                    chunk,
                    start=start.strftime("%Y-%m-%d"),
                    end=end.strftime("%Y-%m-%d"),
                    interval="1d",
                    group_by="ticker",
                    auto_adjust=False,
                    threads=workers,
                    progress=False,
                )
            except RuntimeError as exc:  # pragma: no cover - network error path
                if "dictionary changed size" in str(exc).lower():
                    print(
                        "   [!] Encountered yfinance internal error while downloading "
                        f"chunk starting with {chunk[0]}. Falling back to sequential downloads."
                    )
                    sequential_data, seq_failures, sequential_stopped = _download_chunk_sequential(
                        chunk,
                        start,
                        end,
                        pause_seconds,
                        rate_limit_pause_seconds,
                        max_rate_limit_retries,
                        stop_on_rate_limit,
                    )
                    historical_data.update(sequential_data)
                    missing_tickers.update(seq_failures)
                    if sequential_stopped:
                        stopped_on_rate_limit = True
                        _emit_progress(
                            progress_callback,
                            "Rate limit stopped",
                            processed,
                            len(tickers),
                            "Rate limit reached. Stopping history fetch so cached progress can resume later.",
                        )
                        return historical_data, missing_tickers, stopped_on_rate_limit
                    break
                raise
            except Exception as exc:  # pragma: no cover - network error path
                if _is_rate_limit_error(exc):
                    if stop_on_rate_limit:
                        print(
                            f"   [!] Rate limit reached for chunk starting with {chunk[0]}. "
                            "Stopping now so cached progress can be resumed later."
                        )
                        stopped_on_rate_limit = True
                        _emit_progress(
                            progress_callback,
                            "Rate limit stopped",
                            processed,
                            len(tickers),
                            "Rate limit reached. Stopping history fetch so cached progress can resume later.",
                        )
                        return historical_data, missing_tickers, stopped_on_rate_limit
                    wait_time = rate_limit_pause_seconds * (attempts + 1)
                    print(
                        f"   [!] Rate limit reached for chunk starting with {chunk[0]}. "
                        f"Sleeping {wait_time:.0f}s before retrying."
                    )
                    time.sleep(wait_time)
                    attempts += 1
                    continue
                if _is_invalid_period_error(exc):
                    missing_tickers.update(chunk)
                    break
                print(
                    f"[-] Warning: Failed to download chunk starting with {chunk[0]}. "
                    f"Error: {str(exc)[:120]}"
                )
                missing_tickers.update(chunk)
                break
            else:
                chunk_histories = _extract_histories_from_frame(chunk, data)
                historical_data.update(chunk_histories)
                missing = {ticker for ticker in chunk if ticker not in chunk_histories}
                missing_tickers.update(missing)
                processed += len(chunk)
                _emit_progress(
                    progress_callback,
                    "Historical prices",
                    processed,
                    len(tickers),
                    f"Finished yfinance history batch {chunk_number}/{len(chunks)}.",
                )
                break
        else:
            print(
                f"[-] Warning: Exhausted retries while downloading chunk starting with {chunk[0]} due to rate limits."
            )
            if stop_on_rate_limit:
                stopped_on_rate_limit = True
                _emit_progress(
                    progress_callback,
                    "Rate limit stopped",
                    processed,
                    len(tickers),
                    "Rate-limit retries exhausted. Stopping so cached progress can resume later.",
                )
                return historical_data, missing_tickers, stopped_on_rate_limit
            missing_tickers.update(chunk)
            processed += len(chunk)
            _emit_progress(
                progress_callback,
                "Historical prices",
                processed,
                len(tickers),
                f"Failed yfinance history batch {chunk_number}/{len(chunks)} after retries.",
            )
        if pause_seconds > 0 and chunk_number < len(chunks):
            _emit_progress(
                progress_callback,
                "Rate limit pause",
                processed,
                len(tickers),
                f"Pausing {pause_seconds:.0f}s before next history batch.",
            )
            time.sleep(pause_seconds)

    return historical_data, missing_tickers, stopped_on_rate_limit


def download_historical_data(
    tickers: List[str],
    start: datetime,
    end: datetime,
    workers: int,
    provider: str = DEFAULT_PROVIDER,
    progress_callback: ProgressCallback = None,
    chunk_size: int = DEFAULT_HISTORY_CHUNK_SIZE,
    pause_seconds: float = DEFAULT_HISTORY_PAUSE_SECONDS,
    rate_limit_pause_seconds: float = DEFAULT_RATE_LIMIT_PAUSE_SECONDS,
    max_rate_limit_retries: int = DEFAULT_RATE_LIMIT_RETRIES,
    stop_on_rate_limit: bool = DEFAULT_STOP_ON_RATE_LIMIT,
) -> Tuple[Dict[str, pd.DataFrame], Set[str], bool]:
    """Dispatch historical OHLCV downloads through the selected provider."""

    provider = normalize_provider(provider)
    if provider == "stooq":
        histories, missing = _download_stooq_historical_data(tickers, start, end, progress_callback)
        return histories, missing, False
    return _download_historical_data(
        tickers,
        start,
        end,
        workers,
        progress_callback,
        chunk_size,
        pause_seconds,
        rate_limit_pause_seconds,
        max_rate_limit_retries,
        stop_on_rate_limit,
    )


def _fetch_historical_data_with_cache(
    tickers: List[str],
    start: datetime,
    end: datetime,
    workers: int,
    provider: str,
    cache_file: Optional[str],
    history_refresh_days: int = DEFAULT_HISTORY_REFRESH_DAYS,
    progress_callback: ProgressCallback = None,
    history_chunk_size: int = DEFAULT_HISTORY_CHUNK_SIZE,
    history_pause_seconds: float = DEFAULT_HISTORY_PAUSE_SECONDS,
    rate_limit_pause_seconds: float = DEFAULT_RATE_LIMIT_PAUSE_SECONDS,
    max_rate_limit_retries: int = DEFAULT_RATE_LIMIT_RETRIES,
    stop_on_rate_limit: bool = DEFAULT_STOP_ON_RATE_LIMIT,
    load_history_frames: bool = True,
) -> Tuple[Dict[str, pd.DataFrame], Set[str], Dict[str, object]]:
    """Fetch only missing/stale history ranges and return cache-backed histories."""

    if not cache_file:
        hist_data, missing, stopped_on_rate_limit = download_historical_data(
            tickers,
            start,
            end,
            workers,
            provider,
            progress_callback,
            history_chunk_size,
            history_pause_seconds,
            rate_limit_pause_seconds,
            max_rate_limit_retries,
            stop_on_rate_limit,
        )
        return hist_data, missing, {
            "cache_enabled": False,
            "cache_file": None,
            "history_download_ticker_count": len(tickers),
            "history_cache_hit_count": 0,
            "history_rows_written": 0,
            "stopped_on_rate_limit": stopped_on_rate_limit,
            "cached_history_tickers": sorted(hist_data.keys()),
        }

    rows_written = 0
    downloaded_tickers: Set[str] = set()
    stopped_on_rate_limit = False
    with _cache_connect(cache_file) as conn:
        _emit_progress(
            progress_callback,
            "Cache check",
            0,
            len(tickers),
            f"Checking cached history for {len(tickers)} tickers.",
        )
        fetch_groups = _history_fetch_groups(
            conn,
            provider,
            tickers,
            start,
            end,
            history_refresh_days,
        )
        cache_hit_count = len(tickers) - sum(len(group) for group in fetch_groups.values())
        _emit_progress(
            progress_callback,
            "Cache check",
            cache_hit_count,
            len(tickers),
            f"{cache_hit_count}/{len(tickers)} tickers have fresh cached history.",
        )

        for group_start, group_tickers in sorted(fetch_groups.items()):
            group_start_dt = datetime.fromisoformat(group_start)
            group_histories, _, group_stopped_on_rate_limit = download_historical_data(
                group_tickers,
                group_start_dt,
                end,
                workers,
                provider,
                progress_callback,
                history_chunk_size,
                history_pause_seconds,
                rate_limit_pause_seconds,
                max_rate_limit_retries,
                stop_on_rate_limit,
            )
            downloaded_tickers.update(group_tickers)
            rows_written += _store_history_cache(conn, provider, group_histories, progress_callback)
            if group_stopped_on_rate_limit:
                stopped_on_rate_limit = True
                break

        if load_history_frames:
            cached_histories = _load_cached_histories(conn, provider, tickers, start, end, progress_callback)
            cached_history_tickers = set(cached_histories)
        else:
            cached_histories = {}
            cached_history_tickers = _cached_history_tickers(conn, provider, tickers, start, end, progress_callback)
        _emit_progress(
            progress_callback,
            "Cache export",
            len(cached_history_tickers),
            len(tickers),
            f"Found usable cached history for {len(cached_history_tickers)}/{len(tickers)} tickers.",
        )

    missing = {ticker for ticker in tickers if ticker not in cached_history_tickers}
    return cached_histories, missing, {
        "cache_enabled": True,
        "cache_file": str(Path(cache_file)),
        "history_download_ticker_count": len(downloaded_tickers),
        "history_cache_hit_count": cache_hit_count,
        "history_rows_written": rows_written,
        "stopped_on_rate_limit": stopped_on_rate_limit,
        "cached_history_tickers": sorted(cached_history_tickers),
    }


def _fetch_info_with_cache(
    tickers: List[str],
    workers: int,
    cache_file: Optional[str],
    info_refresh_days: int = DEFAULT_INFO_REFRESH_DAYS,
    progress_callback: ProgressCallback = None,
    info_pause_seconds: float = DEFAULT_INFO_PAUSE_SECONDS,
    rate_limit_pause_seconds: float = DEFAULT_RATE_LIMIT_PAUSE_SECONDS,
    max_rate_limit_retries: int = DEFAULT_RATE_LIMIT_RETRIES,
) -> Tuple[Dict[str, dict], Dict[str, object]]:
    """Fetch stale/missing company info and return cache-backed info."""

    if not cache_file:
        info = fetch_info_individual(
            tickers,
            workers,
            progress_callback,
            info_pause_seconds,
            rate_limit_pause_seconds,
            max_rate_limit_retries,
        )
        return info, {
            "info_cache_hit_count": 0,
            "info_download_ticker_count": len(tickers),
        }

    with _cache_connect(cache_file) as conn:
        _emit_progress(
            progress_callback,
            "Info cache",
            0,
            len(tickers),
            f"Checking cached company info for {len(tickers)} tickers.",
        )
        cached_info, refresh_tickers = _load_info_cache(conn, tickers, info_refresh_days)
        _emit_progress(
            progress_callback,
            "Info cache",
            len(tickers) - len(refresh_tickers),
            len(tickers),
            f"{len(tickers) - len(refresh_tickers)}/{len(tickers)} tickers have fresh cached company info.",
        )
        downloaded_info = (
            fetch_info_individual(
                refresh_tickers,
                workers,
                progress_callback,
                info_pause_seconds,
                rate_limit_pause_seconds,
                max_rate_limit_retries,
            )
            if refresh_tickers
            else {}
        )
        _store_info_cache(conn, downloaded_info)
        cached_info.update(downloaded_info)

    return cached_info, {
        "info_cache_hit_count": len(tickers) - len(refresh_tickers),
        "info_download_ticker_count": len(refresh_tickers),
    }


def fetch_stock_data(
    ticker_file: str,
    output: str = DEFAULT_OUTPUT_FILE,
    years: int = DEFAULT_DATA_YEARS,
    workers: int = DEFAULT_WORKERS,
    provider: str = DEFAULT_PROVIDER,
    limit: Optional[int] = None,
    cache_file: Optional[str] = DEFAULT_CACHE_FILE,
    info_refresh_days: int = DEFAULT_INFO_REFRESH_DAYS,
    history_refresh_days: int = DEFAULT_HISTORY_REFRESH_DAYS,
    progress_callback: ProgressCallback = None,
    prune_missing_tickers: bool = False,
    history_chunk_size: int = DEFAULT_HISTORY_CHUNK_SIZE,
    history_pause_seconds: float = DEFAULT_HISTORY_PAUSE_SECONDS,
    info_pause_seconds: float = DEFAULT_INFO_PAUSE_SECONDS,
    rate_limit_pause_seconds: float = DEFAULT_RATE_LIMIT_PAUSE_SECONDS,
    max_rate_limit_retries: int = DEFAULT_RATE_LIMIT_RETRIES,
    stop_on_rate_limit: bool = DEFAULT_STOP_ON_RATE_LIMIT,
    export_json: bool = True,
    history_end_date: Optional[str] = None,
) -> bool:
    """Fetches historical and info data for tickers and saves to ``output``.

    Parameters
    ----------
    ticker_file:
        Path to a text file containing ticker symbols.
    output:
        Destination JSON file for fetched data.
    years:
        Number of years of historical data to retrieve.
    workers:
        Number of worker threads used by ``yfinance`` when downloading
        historical prices.
    provider:
        Historical OHLCV provider. ``yfinance`` remains the default. ``stooq``
        is an opt-in free CSV source for supported symbols.
    limit:
        Optional first-N ticker cap for smoke tests.
    cache_file:
        SQLite cache path. Pass ``None`` to bypass cache and fetch directly.
    info_refresh_days:
        Maximum age for cached company info before refetching.
    history_refresh_days:
        Number of days before the latest cached bar to refetch so recent bars
        can be corrected or adjusted.
    history_end_date:
        Exclusive history cutoff shared by every batch. Defaults to tomorrow's
        UTC date so yfinance includes the latest completed daily session.
    prune_missing_tickers:
        Create a new ticker file without attempted missing-history tickers.
        The source ticker file is left unchanged.
    export_json:
        Write the legacy JSON export. The browser UI reads SQLite directly and
        can disable this to avoid multi-GB end-of-fetch exports.
    """
    provider = normalize_provider(provider)
    print("--- Starting Data Fetcher (Concurrent Mode) ---")
    _emit_progress(progress_callback, "Starting", 0, None, "Starting data fetch.")
    print(f"Ticker File: {ticker_file}")
    print(f"Data Years: {years}")
    print(f"Provider: {provider}")
    print(f"Cache File: {cache_file if cache_file else 'disabled'}")
    print(f"JSON Export: {'enabled' if export_json else 'disabled'}")
    print(
        "Rate limit settings: "
        f"history_chunk_size={history_chunk_size}, "
        f"history_pause_seconds={history_pause_seconds}, "
        f"info_pause_seconds={info_pause_seconds}, "
        f"rate_limit_pause_seconds={rate_limit_pause_seconds}, "
        f"max_rate_limit_retries={max_rate_limit_retries}, "
        f"stop_on_rate_limit={stop_on_rate_limit}"
    )
    if limit is not None:
        print(f"Ticker Limit: {limit}")
    print("-------------------------------------------------")

    requested_tickers = get_tickers_from_file(ticker_file)
    if not requested_tickers:
        print("No tickers to process. Exiting.")
        _emit_progress(progress_callback, "Failed", 0, 0, "No tickers to process.")
        return False
    tickers = apply_ticker_limit(requested_tickers, limit)

    total_requested_tickers = len(requested_tickers)
    total_tickers = len(tickers)
    print(f"Found {total_requested_tickers} tickers. Starting data fetch for {total_tickers} tickers...")
    _emit_progress(
        progress_callback,
        "Preparing",
        0,
        total_tickers,
        f"Loaded {total_requested_tickers} tickers; processing {total_tickers}.",
    )

    all_stock_data: Dict[str, dict] = {}
    start_time = time.time()

    print("\n--- Step 1 of 3: Batch fetching historical data ---")
    end_date = _history_download_end(history_end_date)
    start_date = end_date - pd.DateOffset(years=years)
    start_dt = start_date.to_pydatetime() if hasattr(start_date, "to_pydatetime") else start_date
    end_dt = end_date.to_pydatetime() if hasattr(end_date, "to_pydatetime") else end_date
    hist_data, missing_hist_tickers, history_cache_metadata = _fetch_historical_data_with_cache(
        tickers,
        start_dt,
        end_dt,
        workers,
        provider,
        cache_file,
        history_refresh_days,
        progress_callback,
        history_chunk_size,
        history_pause_seconds,
        rate_limit_pause_seconds,
        max_rate_limit_retries,
        stop_on_rate_limit,
        load_history_frames=export_json,
    )
    stopped_on_rate_limit = bool(history_cache_metadata.get("stopped_on_rate_limit"))
    if missing_hist_tickers:
        print(
            f"   [!] Unable to fetch historical data for {len(missing_hist_tickers)} tickers during the batch step."
        )
    if stopped_on_rate_limit:
        print("   [!] History fetch stopped early because yfinance returned a rate-limit response.")
    print("Historical data fetch complete.")

    all_info_data, info_cache_metadata = _fetch_info_with_cache(
        tickers,
        workers,
        cache_file,
        info_refresh_days,
        progress_callback,
        info_pause_seconds,
        rate_limit_pause_seconds,
        max_rate_limit_retries,
    )
    print(f"Company info fetch complete. Found info for {len(all_info_data)} tickers.")

    print("\n--- Step 3 of 3: Combining and saving data ---")
    tickers_no_hist = 0
    missing_info_tickers = sorted(ticker for ticker in tickers if not all_info_data.get(ticker))
    missing_market_cap_tickers = sorted(
        ticker for ticker, info in all_info_data.items() if ticker in tickers and info.get("marketCap") is None
    )

    cached_history_tickers = set(history_cache_metadata.get("cached_history_tickers", []))
    if not cached_history_tickers:
        cached_history_tickers = set(hist_data)
    combine_message = "Counting usable cached histories." if not export_json else "Combining history and company info."
    _emit_progress(progress_callback, "Combining", 0, total_tickers, combine_message)
    for processed_count, ticker in enumerate(tqdm(tickers, desc="Processing Tickers"), 1):
        info = all_info_data.get(ticker)
        if export_json:
            hist_single = hist_data.get(ticker)
            if hist_single is None or hist_single.empty:
                tickers_no_hist += 1
                _emit_progress(
                    progress_callback,
                    "Combining",
                    processed_count,
                    total_tickers,
                    f"Skipped {processed_count}/{total_tickers}: {ticker} has no history.",
                )
                continue
        elif ticker not in cached_history_tickers:
            tickers_no_hist += 1
            _emit_progress(
                progress_callback,
                "Combining",
                processed_count,
                total_tickers,
                f"Skipped {processed_count}/{total_tickers}: {ticker} has no cached history.",
            )
            continue
        if export_json:
            hist_json = json.loads(hist_single.to_json(orient="split", date_format="iso"))
            all_stock_data[ticker] = {"info": info if info else {}, "history": hist_json}
        _emit_progress(
            progress_callback,
            "Combining",
            processed_count,
            total_tickers,
            (
                f"Combined {processed_count}/{total_tickers}: {ticker}"
                if export_json
                else f"Counted {processed_count}/{total_tickers}: {ticker}"
            ),
        )

    print("\n--- Fetch Complete ---")
    successful_fetches = len(cached_history_tickers) if not export_json else len(all_stock_data)
    successful_histories = sorted(cached_history_tickers if not export_json else hist_data.keys())
    missing_histories = sorted(set(tickers) - set(successful_histories))
    cleaned_ticker_file = None
    if prune_missing_tickers and missing_histories and not stopped_on_rate_limit:
        cleaned_ticker_file = _write_cleaned_ticker_file(
            ticker_file,
            requested_tickers,
            missing_histories,
        )
        if cleaned_ticker_file:
            print(f"Cleaned ticker file written to {cleaned_ticker_file}")
    elif prune_missing_tickers and stopped_on_rate_limit:
        print("Cleaned ticker file was not written because the run stopped on a rate limit.")
    print(f"Successfully cached usable data for {successful_fetches}/{total_tickers} requested tickers.")
    print(f"  - Skipped {tickers_no_hist} tickers with no historical data.")
    print(f"  - {len(missing_info_tickers)} tickers had no company info (e.g., market cap).")
    print(f"  - Of those with info, {len(missing_market_cap_tickers)} were missing a market cap value.")
    print("------------------------")

    output_data = {
        "metadata": {
            "fetch_date_utc": datetime.now(timezone.utc).isoformat(),
            "source_ticker_file": ticker_file,
            "data_years_fetched": years,
            "success": successful_fetches > 0,
            "error": None
            if successful_fetches > 0
            else "No historical data was fetched for any requested ticker.",
            "provider": provider,
            "storage": (
                "sqlite-cache"
                if cache_file and not export_json
                else "sqlite-cache-plus-json-export"
                if cache_file
                else "json-export"
            ),
            "cache_file": cache_file,
            "json_exported": export_json,
            "json_output": output if export_json else None,
            "requested_tickers": tickers,
            "requested_ticker_count": total_tickers,
            "source_ticker_count": total_requested_tickers,
            "limit": limit,
            "successful_histories": successful_histories,
            "successful_history_count": len(successful_histories),
            "missing_histories": missing_histories,
            "missing_history_count": len(missing_histories),
            "prune_missing_tickers": prune_missing_tickers,
            "cleaned_ticker_file": cleaned_ticker_file,
            "removed_tickers": missing_histories if prune_missing_tickers else [],
            "missing_info_count": len(missing_info_tickers),
            "missing_market_cap_count": len(missing_market_cap_tickers),
            "provider_limitations": _provider_limitations(provider),
            "info_refresh_days": info_refresh_days,
            "history_refresh_days": history_refresh_days,
            "history_chunk_size": history_chunk_size,
            "history_pause_seconds": history_pause_seconds,
            "info_pause_seconds": info_pause_seconds,
            "rate_limit_pause_seconds": rate_limit_pause_seconds,
            "max_rate_limit_retries": max_rate_limit_retries,
            "stop_on_rate_limit": stop_on_rate_limit,
            **history_cache_metadata,
            **info_cache_metadata,
        },
        "stocks": all_stock_data,
    }

    if export_json:
        _emit_progress(progress_callback, "JSON export", 0, 1, f"Writing JSON export to {output}.")
        with open(output, "w", encoding="utf-8") as f:
            json.dump(output_data, f, indent=2)
        _emit_progress(progress_callback, "JSON export", 1, 1, f"JSON export written to {output}.")
    end_time = time.time()
    if total_tickers > 0 and successful_fetches == 0:
        if export_json:
            print(f"Diagnostic data saved to {output}")
        else:
            print("Diagnostic JSON export skipped.")
        print(f"Total execution time: {end_time - start_time:.2f} seconds.")
        print("ERROR: No historical data was fetched for any requested ticker.")
        _emit_progress(progress_callback, "Failed", 0, total_tickers, "No historical data was fetched.")
        return False
    if export_json:
        print(f"Data successfully saved to {output}")
    else:
        print("SQLite cache updated. JSON export skipped.")
    print(f"Total execution time: {end_time - start_time:.2f} seconds.")
    _emit_progress(
        progress_callback,
        "Complete",
        total_tickers,
        total_tickers,
        (
            f"Fetch stopped on rate limit. Cached usable data for {successful_fetches}/{total_tickers} requested tickers."
            if stopped_on_rate_limit
            else f"Fetch complete. Cached usable data for {successful_fetches}/{total_tickers} requested tickers."
        ),
    )
    return True


def cli() -> None:
    """Simple CLI entry point for data fetching."""
    import argparse

    parser = argparse.ArgumentParser(description="Stock Data Fetcher")
    sub = parser.add_subparsers(dest="command")

    us_tickers_cmd = sub.add_parser("us-tickers", help="Download a US ticker file from Nasdaq Trader")
    us_tickers_cmd.add_argument(
        "-o",
        "--output",
        default=DEFAULT_US_TICKER_FILE,
        help=f"Output ticker file name. (Default: {DEFAULT_US_TICKER_FILE})",
    )

    fetch_cmd = sub.add_parser("fetch", help="Fetch stock data")
    fetch_cmd.add_argument("ticker_file", help="Path to the text file containing stock tickers.")
    fetch_cmd.add_argument(
        "-o", "--output", default=DEFAULT_OUTPUT_FILE, help=f"Output JSON file name. (Default: {DEFAULT_OUTPUT_FILE})"
    )
    fetch_cmd.add_argument(
        "-y", "--years", type=int, default=DEFAULT_DATA_YEARS, help=f"Number of years of historical data to fetch. (Default: {DEFAULT_DATA_YEARS})"
    )
    fetch_cmd.add_argument(
        "-w",
        "--workers",
        type=int,
        default=DEFAULT_WORKERS,
        help=f"Number of threads for downloading data. (Default: {DEFAULT_WORKERS})",
    )
    fetch_cmd.add_argument(
        "--provider",
        choices=SUPPORTED_PROVIDERS,
        default=DEFAULT_PROVIDER,
        help=f"Historical OHLCV provider. (Default: {DEFAULT_PROVIDER})",
    )
    fetch_cmd.add_argument(
        "--limit",
        type=int,
        help="Only fetch the first N tickers from the ticker file.",
    )
    fetch_cmd.add_argument(
        "--cache-file",
        default=DEFAULT_CACHE_FILE,
        help=f"SQLite cache file for incremental fetches. (Default: {DEFAULT_CACHE_FILE})",
    )
    fetch_cmd.add_argument(
        "--no-cache",
        action="store_true",
        help="Disable SQLite caching and fetch the requested data directly.",
    )
    fetch_cmd.add_argument(
        "--no-json-export",
        action="store_true",
        help="Update SQLite cache without writing the legacy JSON export.",
    )
    fetch_cmd.add_argument(
        "--info-refresh-days",
        type=int,
        default=DEFAULT_INFO_REFRESH_DAYS,
        help=f"Refresh cached company info after this many days. (Default: {DEFAULT_INFO_REFRESH_DAYS})",
    )
    fetch_cmd.add_argument(
        "--history-refresh-days",
        type=int,
        default=DEFAULT_HISTORY_REFRESH_DAYS,
        help=(
            "Refetch this many days before the latest cached price bar "
            f"to catch corrections. (Default: {DEFAULT_HISTORY_REFRESH_DAYS})"
        ),
    )
    fetch_cmd.add_argument(
        "--prune-missing-tickers",
        action="store_true",
        help=(
            "Create a new ticker file with attempted tickers that had missing "
            "historical data removed. The original file is not changed."
        ),
    )
    fetch_cmd.add_argument(
        "--history-chunk-size",
        type=int,
        default=DEFAULT_HISTORY_CHUNK_SIZE,
        help=f"Tickers per yfinance history batch. Lower is slower but gentler. (Default: {DEFAULT_HISTORY_CHUNK_SIZE})",
    )
    fetch_cmd.add_argument(
        "--history-pause-seconds",
        type=float,
        default=DEFAULT_HISTORY_PAUSE_SECONDS,
        help=f"Pause between yfinance history batches. (Default: {DEFAULT_HISTORY_PAUSE_SECONDS})",
    )
    fetch_cmd.add_argument(
        "--info-pause-seconds",
        type=float,
        default=DEFAULT_INFO_PAUSE_SECONDS,
        help=f"Pause between company info requests when workers=1. (Default: {DEFAULT_INFO_PAUSE_SECONDS})",
    )
    fetch_cmd.add_argument(
        "--rate-limit-pause-seconds",
        type=float,
        default=DEFAULT_RATE_LIMIT_PAUSE_SECONDS,
        help=f"Base pause after a yfinance rate-limit response. (Default: {DEFAULT_RATE_LIMIT_PAUSE_SECONDS})",
    )
    fetch_cmd.add_argument(
        "--max-rate-limit-retries",
        type=int,
        default=DEFAULT_RATE_LIMIT_RETRIES,
        help=f"Retries for rate-limit responses before giving up on a batch/ticker. (Default: {DEFAULT_RATE_LIMIT_RETRIES})",
    )
    fetch_cmd.add_argument(
        "--stop-on-rate-limit",
        action="store_true",
        default=DEFAULT_STOP_ON_RATE_LIMIT,
        help="Stop the fetch when yfinance rate-limits history data so cached progress can resume later.",
    )
    args = parser.parse_args()
    if args.command == "us-tickers":
        result = write_us_ticker_file(args.output)
        print(f"Wrote {result['ticker_count']} US tickers to {result['output_file']}")
        return
    if args.command != "fetch":
        parser.print_help()
        return
    success = fetch_stock_data(
        args.ticker_file,
        args.output,
        args.years,
        args.workers,
        args.provider,
        args.limit,
        None if args.no_cache else args.cache_file,
        args.info_refresh_days,
        args.history_refresh_days,
        None,
        args.prune_missing_tickers,
        args.history_chunk_size,
        args.history_pause_seconds,
        args.info_pause_seconds,
        args.rate_limit_pause_seconds,
        args.max_rate_limit_retries,
        args.stop_on_rate_limit,
        not args.no_json_export,
    )
    if not success:
        raise SystemExit(1)


if __name__ == "__main__":
    cli()
