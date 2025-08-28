import yfinance as yf
import pandas as pd
import json
import argparse
import time
from datetime import datetime, timezone, timedelta
import os
from tqdm import tqdm
from curl_cffi import requests as cffi_requests

# --- Configuration ---
DEFAULT_OUTPUT_FILE = "stock_data.json"
DEFAULT_DATA_YEARS = 15

def get_tickers_from_file(filename):
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

def fetch_info_individual(tickers):
    """
    Fetches .info data for a list of tickers one by one using yfinance.
    This is slower but can be more reliable than batch API calls if they are getting blocked.
    """
    all_info_data = {}
    print(f"\n--- Step 2 of 3: Fetching company info individually ---")
    
    for ticker in tqdm(tickers, desc="Fetching Info", unit="ticker"):
        try:
            stock = yf.Ticker(ticker)
            info = stock.info
            # Check if we got meaningful data, not just an empty dict for a dead ticker
            if info and info.get('regularMarketPrice') is not None:
                all_info_data[ticker] = info
            else:
                tqdm.write(f"[-] Warning: No valid info found for {ticker}.")
        except Exception as e:
            # This will catch network errors or errors for delisted tickers
            tqdm.write(f"[-] Warning: Failed to fetch info for {ticker}. Error: {str(e)[:100]}")
            
    return all_info_data



def main():
    parser = argparse.ArgumentParser(description="Stock Data Fetcher for Moneymaker Pro")
    parser.add_argument("ticker_file", help="Path to the text file containing stock tickers.")
    parser.add_argument("-o", "--output", default=DEFAULT_OUTPUT_FILE, help=f"Output JSON file name. (Default: {DEFAULT_OUTPUT_FILE})")
    parser.add_argument("-y", "--years", type=int, default=DEFAULT_DATA_YEARS, help=f"Number of years of historical data to fetch. (Default: {DEFAULT_DATA_YEARS})")
    args = parser.parse_args()

    print("--- Starting Data Fetcher (Concurrent Mode) ---")
    print(f"Ticker File: {args.ticker_file}")
    print(f"Data Years: {args.years}")
    print("-------------------------------------------------")

    tickers = get_tickers_from_file(args.ticker_file)
    if not tickers:
        print("No tickers to process. Exiting.")
        return

    total_tickers = len(tickers)
    print(f"Found {total_tickers} tickers. Starting data fetch...")

    all_stock_data = {}
    start_time = time.time()
    
    # --- Step 1: Batch download all historical data ---
    # This is MUCH more efficient than one-by-one calls.
    print("\n--- Step 1 of 3: Batch fetching historical data ---")
    end_date = datetime.now()
    start_date = end_date - pd.DateOffset(years=args.years)

    hist_data_multi = yf.download(
        tickers,
        start=start_date.strftime('%Y-%m-%d'),
        end=end_date.strftime('%Y-%m-%d'),
        interval="1d",
        group_by='ticker',
        threads=True, # Let yfinance handle threading for this part
        progress=True,
        # session=session # REMOVED: Incompatible with recent yfinance versions that use curl_cffi
    )
    print("Historical data fetch complete.")

    # --- Step 2: Fetch '.info' data (market cap, etc.) ---
    # We now fetch individually as batch requests are often blocked. This is slower.
    all_info_data = fetch_info_individual(tickers)
    print(f"Company info fetch complete. Found info for {len(all_info_data)} tickers.")

    # --- Step 3: Combine historical and info data ---
    print("\n--- Step 3 of 3: Combining and saving data ---")
    tickers_no_hist = 0
    tickers_no_info = 0
    tickers_no_mcap = 0

    for ticker in tqdm(tickers, desc="Processing Tickers"):
        info = all_info_data.get(ticker)
        
        # Check if we have historical data for this ticker. Info is optional.
        if ticker not in hist_data_multi.columns.get_level_values(0):
            tickers_no_hist += 1
            continue

        hist_single = hist_data_multi[ticker].dropna(how='all')
        if hist_single.empty:
            tickers_no_hist += 1
            continue
        
        # If info is missing, we'll still save the history.
        # The filter app can handle missing info.
        if not info:
            tickers_no_info += 1
        elif info.get('marketCap') is None:
            # This is common for indices, warrants, or delisted stocks.
            tickers_no_mcap += 1

        # Convert to the same JSON format as the original script
        hist_json = json.loads(hist_single.to_json(orient='split', date_format='iso'))
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
