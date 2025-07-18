import yfinance as yf
import pandas as pd
import json
import argparse
import time
from datetime import datetime
import concurrent.futures
import os

# --- Configuration ---
DEFAULT_OUTPUT_FILE = "stock_data.json"
DEFAULT_DATA_YEARS = 15
DEFAULT_MAX_WORKERS = 10

def get_tickers_from_file(filename):
    """Reads tickers from a text file, handling different formats."""
    tickers = []
    is_asx_list = "asx" in filename.lower()
    try:
        with open(filename, 'r', encoding='utf-8') as f:
            header_line_content = "Symbol|Security Name"
            skipped_header = False
            for line_content in f:
                line = line_content.strip()
                if not line or line.startswith("#"):
                    continue
                if not skipped_header and header_line_content in line:
                    skipped_header = True
                    continue
                
                if '|' in line:
                    ticker = line.split('|')[0].strip().upper()
                else:
                    ticker = line.strip().upper()

                if ticker:
                    if is_asx_list and not ticker.endswith(".AX"):
                        ticker += ".AX"
                    tickers.append(ticker)
        if not tickers:
            print(f"Warning: No tickers found in {filename}.")
        return tickers
    except FileNotFoundError:
        print(f"Error: Ticker file '{filename}' not found.")
        return []
    except Exception as e:
        print(f"Error reading ticker file: {str(e)}")
        return []

def fetch_stock_data(ticker, years):
    """Fetches historical data and info for a single stock with retry logic."""
    max_retries = 3
    retry_delay = 10 # seconds

    for attempt in range(max_retries):
        try:
            stock = yf.Ticker(ticker)
            
            # Fetch basic info
            info = stock.info
            # Ensure market cap is present, otherwise the data is often not useful
            if info.get('marketCap') is None and info.get('regularMarketPrice') is None:
                # This is a data issue, not a rate limit, so don't retry.
                print(f"- Skipping {ticker}: Insufficient data (no market cap or price).")
                return None, None

            # Fetch historical data
            end_date = datetime.now()
            start_date = end_date - pd.DateOffset(years=years)
            hist = stock.history(start=start_date.strftime('%Y-%m-%d'), 
                                 end=end_date.strftime('%Y-%m-%d'), 
                                 interval="1d")
            
            if hist.empty:
                # Data is empty, not a rate limit, don't retry.
                print(f"- Skipping {ticker}: No historical data found for the period.")
                return None, None

            # Convert history to a JSON-serializable format
            hist_json = json.loads(hist.to_json(orient='split', date_format='iso'))

            # Success
            return info, hist_json

        except Exception as e:
            # yfinance can throw various errors. We'll treat most network-related ones as retryable.
            error_str = str(e).lower()
            # Common indicators of transient network/API issues.
            if "failed to decrypt" in error_str or "404" in error_str or "429" in error_str or "failed to get data" in error_str:
                 if attempt < max_retries - 1:
                    print(f"! Rate limit or transient error for {ticker}. Retrying in {retry_delay}s... (Attempt {attempt + 1}/{max_retries})")
                    time.sleep(retry_delay)
                    continue # Go to the next attempt
                 else:
                    print(f"!! Failed to fetch {ticker} after {max_retries} attempts. Last error: {str(e)[:100]}")
                    return None, None
            else:
                # For other errors (like JSON parsing, etc.), don't retry.
                print(f"! Unhandled error fetching data for {ticker}: {str(e)[:100]}")
                return None, None
    
    return None, None # Should be unreachable if loop logic is correct, but good for safety.

def main():
    parser = argparse.ArgumentParser(description="Stock Data Fetcher for Moneymaker Pro")
    parser.add_argument("ticker_file", help="Path to the text file containing stock tickers.")
    parser.add_argument("-o", "--output", default=DEFAULT_OUTPUT_FILE, 
                        help=f"Output JSON file name. (Default: {DEFAULT_OUTPUT_FILE})")
    parser.add_argument("-y", "--years", type=int, default=DEFAULT_DATA_YEARS, 
                        help=f"Number of years of historical data to fetch. (Default: {DEFAULT_DATA_YEARS})")
    parser.add_argument("-w", "--workers", type=int, default=DEFAULT_MAX_WORKERS, 
                        help=f"Number of parallel workers for fetching data. (Default: {DEFAULT_MAX_WORKERS})")
    args = parser.parse_args()

    print("--- Starting Data Fetcher ---")
    print(f"Ticker File: {args.ticker_file}")
    print(f"Data Years: {args.years}")
    print(f"Max Workers: {args.workers}")
    print("-----------------------------")

    tickers = get_tickers_from_file(args.ticker_file)
    if not tickers:
        print("No tickers to process. Exiting.")
        return

    total_tickers = len(tickers)
    print(f"Found {total_tickers} tickers. Starting data fetch...")

    all_stock_data = {}
    start_time = time.time()

    with concurrent.futures.ThreadPoolExecutor(max_workers=args.workers) as executor:
        future_to_ticker = {executor.submit(fetch_stock_data, ticker, args.years): ticker for ticker in tickers}
        
        completed_count = 0
        for future in concurrent.futures.as_completed(future_to_ticker):
            ticker = future_to_ticker[future]
            completed_count += 1
            try:
                info, hist = future.result()
                if info and hist:
                    all_stock_data[ticker] = {"info": info, "history": hist}
            except Exception as exc:
                print(f"!! Critical error processing {ticker}: {exc}")

            # Progress Indicator
            progress = (completed_count / total_tickers) * 100
            print(f"Progress: {completed_count}/{total_tickers} ({progress:.2f}%) - Last processed: {ticker}")

    print("\n--- Fetch Complete ---")
    successful_fetches = len(all_stock_data)
    print(f"Successfully fetched data for {successful_fetches}/{total_tickers} tickers.")

    # Add metadata
    output_data = {
        "metadata": {
            "fetch_date_utc": datetime.utcnow().isoformat(),
            "source_ticker_file": args.ticker_file,
            "data_years_fetched": args.years
        },
        "stocks": all_stock_data
    }

    # Save to file
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
