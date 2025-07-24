import yfinance as yf
import pandas as pd
import json
import argparse
import time
from datetime import datetime
import os
from concurrent.futures import ThreadPoolExecutor, as_completed
from queue import Queue
from tqdm import tqdm

# --- Configuration ---
DEFAULT_OUTPUT_FILE = "stock_data.json"
DEFAULT_DATA_YEARS = 15
DEFAULT_MAX_WORKERS = 10

"""def get_tickers_from_file(filename):
    """Reads tickers from a text file, handling different formats."""
    tickers = []
    is_asx_list = "asx" in filename.lower()
    try:
        with open(filename, 'r', encoding='utf-8') as f:
            # More robust header skipping for files that might contain one
            first_line = f.readline()
            if "Symbol" in first_line and "Security Name" in first_line:
                 # This looks like a header, so we process the rest of the file
                 pass
            else:
                # This is not a header, so we process the first line
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

def _process_line(line_content, is_asx_list, tickers):
    """Helper function to process a single line from a ticker file."""
    line = line_content.strip()
    if not line or line.startswith("#"):
        return

    if '|' in line:
        ticker = line.split('|')[0].strip().upper()
    else:
        ticker = line.strip().upper()

    if ticker and ticker != "SYMBOL": # Explicitly skip the header ticker
        if is_asx_list and not ticker.endswith(".AX"):
            ticker += ".AX"
        tickers.append(ticker)
""

def fetch_stock_data(ticker, years):
    """Fetches historical data and info for a single stock with robust retry logic."""
    retry_delay = 30
    max_delay = 300
    attempt = 1
    while True:
        try:
            stock = yf.Ticker(ticker)
            info = stock.info
            if info.get('marketCap') is None and info.get('regularMarketPrice') is None:
                return ticker, None, "Insufficient data (no market cap or price)"
            
            end_date = datetime.now()
            start_date = end_date - pd.DateOffset(years=years)
            hist = stock.history(start=start_date.strftime('%Y-%m-%d'), end=end_date.strftime('%Y-%m-%d'), interval="1d")
            
            if hist.empty:
                return ticker, None, "No historical data found"

            hist_json = json.loads(hist.to_json(orient='split', date_format='iso'))
            return ticker, {"info": info, "history": hist_json}, "Success"
        
        except Exception as e:
            error_str = str(e).lower()
            if "failed to decrypt" in error_str or "404" in error_str or "429" in error_str or "failed to get data" in error_str or "too many requests" in error_str:
                time.sleep(retry_delay)
                attempt += 1
                retry_delay = min(retry_delay + 30, max_delay)
                continue
            else:
                return ticker, None, f"Unhandled error: {str(e)[:100]}"

def main():
    parser = argparse.ArgumentParser(description="Stock Data Fetcher for Moneymaker Pro")
    parser.add_argument("ticker_file", help="Path to the text file containing stock tickers.")
    parser.add_argument("-o", "--output", default=DEFAULT_OUTPUT_FILE, help=f"Output JSON file name. (Default: {DEFAULT_OUTPUT_FILE})")
    parser.add_argument("-y", "--years", type=int, default=DEFAULT_DATA_YEARS, help=f"Number of years of historical data to fetch. (Default: {DEFAULT_DATA_YEARS})")
    parser.add_argument("-w", "--workers", type=int, default=DEFAULT_MAX_WORKERS, help=f"Number of concurrent workers. (Default: {DEFAULT_MAX_WORKERS})")
    args = parser.parse_args()

    print("--- Starting Data Fetcher (Concurrent Mode) ---")
    print(f"Ticker File: {args.ticker_file}")
    print(f"Data Years: {args.years}")
    print(f"Max Workers: {args.workers}")
    print("-------------------------------------------------")

    tickers = get_tickers_from_file(args.ticker_file)
    if not tickers:
        print("No tickers to process. Exiting.")
        return

    total_tickers = len(tickers)
    print(f"Found {total_tickers} tickers. Starting data fetch...")

    all_stock_data = {}
    start_time = time.time()

    with ThreadPoolExecutor(max_workers=args.workers) as executor:
        with tqdm(total=total_tickers, desc="Fetching Data", unit="ticker") as pbar:
            futures = {}
            for ticker in tickers:
                futures[executor.submit(fetch_stock_data, ticker, args.years)] = ticker
                time.sleep(0.05)  # Stagger requests to reduce rate limiting issues
            for future in as_completed(futures):
                ticker, result, status = future.result()
                if result:
                    all_stock_data[ticker] = result
                else:
                    tqdm.write(f"- Skipping {ticker}: {status}")
                pbar.update(1)

    print("\n--- Fetch Complete ---")
    successful_fetches = len(all_stock_data)
    print(f"Successfully fetched data for {successful_fetches}/{total_tickers} tickers.")

    output_data = {
        "metadata": {
            "fetch_date_utc": datetime.utcnow().isoformat(),
            "source_ticker_file": args.ticker_file,
            "data_years_fetched": args.years
        },
        "stocks": all_stock_data
    }

    try:
        with open(args.output, 'w', encoding='utf-8') as f:
            json.dump(output_data, f, indent=2)
        print(f"Data successfully saved to {args.output}")
    except Exception as e:
        print(f"Error saving data to file: {e}")

    end_time = time.time()
    print(f"Total execution time: {end_time - start_time:.2f} seconds.")

if __name__ == "__main__":
    main()
