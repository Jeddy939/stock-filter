"""Data fetching utilities for the moneymaker package."""


import json
import time
from datetime import datetime, timezone
from typing import Dict, List

import pandas as pd
import yfinance as yf
from tqdm import tqdm

DEFAULT_OUTPUT_FILE = "stock_data.json"
DEFAULT_DATA_YEARS = 15
DEFAULT_WORKERS = 10


def get_tickers_from_file(filename: str) -> List[str]:
    """Reads tickers from a text file, handling different formats."""
    tickers: List[str] = []
    is_asx_list = "asx" in filename.lower()
    try:
        with open(filename, "r", encoding="utf-8") as f:
            first_line = f.readline()
            if "Symbol" in first_line and "Security Name" in first_line:
                pass
            else:
                _process_line(first_line, is_asx_list, tickers)
            for line_content in f:
                _process_line(line_content, is_asx_list, tickers)
        if not tickers:
            print(f"Warning: No tickers found in {filename}.")
        return tickers
    except FileNotFoundError:
        print(f"Error: Ticker file '{filename}' not found.")
        return []
    except Exception as e:
        print(f"Error reading ticker file: {str(e)}")
        return []


def _process_line(line_content: str, is_asx_list: bool, tickers: List[str]) -> None:
    """Helper for ``get_tickers_from_file``."""
    line = line_content.strip()
    if not line or line.startswith("#"):
        return
    if "|" in line:
        ticker = line.split("|")[0].strip().upper()
    else:
        ticker = line.strip().upper()
    if ticker and ticker != "SYMBOL":
        if is_asx_list and not ticker.endswith(".AX"):
            ticker += ".AX"
        tickers.append(ticker)


def fetch_info_individual(tickers: List[str]) -> Dict[str, dict]:
    """Fetches ``.info`` data for a list of tickers one by one using yfinance."""
    all_info_data: Dict[str, dict] = {}
    print("\n--- Step 2 of 3: Fetching company info individually ---")
    for ticker in tqdm(tickers, desc="Fetching Info", unit="ticker"):
        try:
            stock = yf.Ticker(ticker)
            info = stock.info
            if info and info.get("regularMarketPrice") is not None:
                all_info_data[ticker] = info
            else:
                tqdm.write(f"[-] Warning: No valid info found for {ticker}.")
        except Exception as e:  # pragma: no cover - network error path
            tqdm.write(f"[-] Warning: Failed to fetch info for {ticker}. Error: {str(e)[:100]}")
    return all_info_data


def fetch_stock_data(
    ticker_file: str,
    output: str = DEFAULT_OUTPUT_FILE,
    years: int = DEFAULT_DATA_YEARS,
    workers: int = DEFAULT_WORKERS,
) -> None:
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
    """
    print("--- Starting Data Fetcher (Concurrent Mode) ---")
    print(f"Ticker File: {ticker_file}")
    print(f"Data Years: {years}")
    print("-------------------------------------------------")

    tickers = get_tickers_from_file(ticker_file)
    if not tickers:
        print("No tickers to process. Exiting.")
        return

    total_tickers = len(tickers)
    print(f"Found {total_tickers} tickers. Starting data fetch...")

    all_stock_data: Dict[str, dict] = {}
    start_time = time.time()

    print("\n--- Step 1 of 3: Batch fetching historical data ---")
    end_date = datetime.now()
    start_date = end_date - pd.DateOffset(years=years)
    hist_data_multi = yf.download(
        tickers,
        start=start_date.strftime("%Y-%m-%d"),
        end=end_date.strftime("%Y-%m-%d"),
        interval="1d",
        group_by="ticker",
        threads=workers,
        progress=True,
    )
    print("Historical data fetch complete.")

    all_info_data = fetch_info_individual(tickers)
    print(f"Company info fetch complete. Found info for {len(all_info_data)} tickers.")

    print("\n--- Step 3 of 3: Combining and saving data ---")
    tickers_no_hist = 0
    tickers_no_info = 0
    tickers_no_mcap = 0

    for ticker in tqdm(tickers, desc="Processing Tickers"):
        info = all_info_data.get(ticker)
        if ticker not in hist_data_multi.columns.get_level_values(0):
            tickers_no_hist += 1
            continue
        hist_single = hist_data_multi[ticker].dropna(how="all")
        if hist_single.empty:
            tickers_no_hist += 1
            continue
        if not info:
            tickers_no_info += 1
        elif info.get("marketCap") is None:
            tickers_no_mcap += 1
        hist_json = json.loads(hist_single.to_json(orient="split", date_format="iso"))
        all_stock_data[ticker] = {"info": info if info else {}, "history": hist_json}

    print("\n--- Fetch Complete ---")
    successful_fetches = len(all_stock_data)
    print(f"Successfully processed data for {successful_fetches}/{total_tickers} tickers.")
    print(f"  - Skipped {tickers_no_hist} tickers with no historical data.")
    print(f"  - {tickers_no_info} tickers had no company info (e.g., market cap).")
    print(f"  - Of those with info, {tickers_no_mcap} were missing a market cap value.")
    print("------------------------")

    output_data = {
        "metadata": {
            "fetch_date_utc": datetime.now(timezone.utc).isoformat(),
            "source_ticker_file": ticker_file,
            "data_years_fetched": years,
        },
        "stocks": all_stock_data,
    }

    with open(output, "w", encoding="utf-8") as f:
        json.dump(output_data, f, indent=2)
    print(f"Data successfully saved to {output}")
    end_time = time.time()
    print(f"Total execution time: {end_time - start_time:.2f} seconds.")


def cli() -> None:
    """Simple CLI entry point for data fetching."""
    import argparse

    parser = argparse.ArgumentParser(description="Stock Data Fetcher")
    parser.add_argument("ticker_file", help="Path to the text file containing stock tickers.")
    parser.add_argument(
        "-o", "--output", default=DEFAULT_OUTPUT_FILE, help=f"Output JSON file name. (Default: {DEFAULT_OUTPUT_FILE})"
    )
    parser.add_argument(
        "-y", "--years", type=int, default=DEFAULT_DATA_YEARS, help=f"Number of years of historical data to fetch. (Default: {DEFAULT_DATA_YEARS})"
    )
    parser.add_argument(
        "-w",
        "--workers",
        type=int,
        default=DEFAULT_WORKERS,
        help=f"Number of threads for downloading data. (Default: {DEFAULT_WORKERS})",
    )
    args = parser.parse_args()
    fetch_stock_data(args.ticker_file, args.output, args.years, args.workers)


if __name__ == "__main__":
    cli()
