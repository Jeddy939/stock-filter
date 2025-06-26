# volume_scanner_v3_plus_pipe_delimited.py
import yfinance as yf
import pandas as pd
from datetime import datetime, timedelta

# --- Configuration ---
# CHANGE THIS TO YOUR PIPE-DELIMITED FILE NAME
TICKER_FILE = "your_nasdaq_screener_list.txt" # e.g., the file you provided

# Volume Spike Configuration
VOLUME_SPIKE_MULTIPLIER = 2.0
AVG_VOLUME_WEEKS = 52

# Moving Average Periods (in weeks)
MA_PERIODS = {
    "short": 90,
    "medium": 360,
    "long": 700
}
DATA_FETCH_PERIOD_YEARS = 16

DEBUG_SPECIFIC_TICKER = None
DEBUG_FIRST_N_TICKERS = 0
debug_counter_main = 0

# NEW get_tickers_from_file function
def get_tickers_from_file(filename=TICKER_FILE, is_asx_list=False): # Added a flag
    """
    Reads tickers from a file.
    Can handle simple one-ticker-per-line files or pipe-delimited files
    where the ticker is the first |-separated value.
    Skips lines starting with # and the specific header "Symbol|Security Name..."
    """
    tickers = []
    try:
        with open(filename, 'r', encoding='utf-8') as f: # Added encoding for broader compatibility
            header_line_content = "Symbol|Security Name" # Specific header to skip
            skipped_header = False

            for line_number, line in enumerate(f):
                line = line.strip()
                if not line or line.startswith("#"): # Skip empty lines and comments
                    continue

                # Skip the known header line
                if not skipped_header and header_line_content in line:
                    print(f"  Info: Detected and skipped header row in {filename}: '{line[:60]}...'")
                    skipped_header = True
                    continue
                
                # If it was a file that *only* had a header and no data, or if we are past the header
                # Try to parse as pipe-delimited if it contains a pipe
                if '|' in line:
                    parts = line.split('|')
                    if parts:
                        ticker = parts[0].strip().upper() # Symbol is in the first column
                        if ticker: # Ensure ticker is not empty
                            # For non-ASX lists, we assume the ticker is yfinance-ready
                            # If it were an ASX list in this format, it should already have .AX
                            tickers.append(ticker)
                else: # Fallback: Assume simple one-ticker-per-line format
                    ticker = line.strip().upper()
                    if ticker: # Ensure ticker is not empty
                        if is_asx_list and not ticker.endswith(".AX"):
                            # print(f"  Info: Appending .AX to ASX ticker {ticker}")
                            ticker += ".AX"
                        tickers.append(ticker)
                        
        if not tickers:
            print(f"Warning: No tickers extracted from {filename}. Please check the file content and format.")
        else:
            print(f"Loaded {len(tickers)} tickers from {filename}.")
        return tickers
    except FileNotFoundError:
        print(f"Error: Ticker file '{filename}' not found.")
        print(f"Please create this file with tickers.")
        return []
    except Exception as e:
        print(f"Error reading ticker file {filename}: {e}")
        return []


# ... (analyze_stock_volume function remains the same as in volume_scanner_v3.py) ...
def analyze_stock_volume(ticker, global_debug_counter_ref):
    try:
        # --- Debug Setup ---
        do_debug_print = (DEBUG_SPECIFIC_TICKER and ticker == DEBUG_SPECIFIC_TICKER) or \
                         (DEBUG_FIRST_N_TICKERS > 0 and global_debug_counter_ref[0] < DEBUG_FIRST_N_TICKERS)
        if do_debug_print:
            print(f"\n--- DEBUGGING {ticker} ---")
            print(f"Script run time (approx): {datetime.now().strftime('%Y-%m-%d %H:%M:%S')}")
        # --- End Debug Setup ---

        end_date = datetime.now()
        start_date = end_date - timedelta(days=DATA_FETCH_PERIOD_YEARS * 365.25) # More precise for years

        stock = yf.Ticker(ticker)
        hist_daily = stock.history(start=start_date.strftime('%Y-%m-%d'),
                                   end=end_date.strftime('%Y-%m-%d'),
                                   interval="1d")

        if hist_daily.empty:
            if do_debug_print: print(f"  DEBUG: No daily history for {ticker}.")
            return None
        
        if do_debug_print:
            print(f"  DEBUG: Daily history: {len(hist_daily)} days from {hist_daily.index.min().date()} to {hist_daily.index.max().date()}")

        # Resample to weekly data, using Monday as the week's anchor
        # We need Open, High, Low, Close for price, and Volume
        agg_functions = {'Open': 'first', 'High': 'max', 'Low': 'min', 'Close': 'last', 'Volume': 'sum'}
        weekly_data = hist_daily.resample('W-MON').agg(agg_functions)
        weekly_data = weekly_data.dropna(subset=['Close', 'Volume']) # Ensure Close and Volume are present
        
        # Filter out weeks with zero volume as they are not meaningful for volume spike analysis
        weekly_data = weekly_data[weekly_data['Volume'] > 0]

        if weekly_data.empty:
            if do_debug_print: print(f"  DEBUG: No valid weekly data after resampling for {ticker}.")
            return None

        # --- 1. Volume Spike Condition ---
        min_weeks_for_volume_avg = AVG_VOLUME_WEEKS + 1 # Need 52 prior weeks + current week
        if len(weekly_data) < min_weeks_for_volume_avg:
            if do_debug_print: print(f"  DEBUG: Not enough weekly data ({len(weekly_data)}) for volume avg calc (need {min_weeks_for_volume_avg}).")
            return None

        avg_weekly_volume_series = weekly_data['Volume'].shift(1).rolling(window=AVG_VOLUME_WEEKS, min_periods=int(AVG_VOLUME_WEEKS * 0.8)).mean()
        
        current_week_volume = weekly_data['Volume'].iloc[-1]
        target_week_start_date = weekly_data.index[-1]
        preceding_avg_volume = avg_weekly_volume_series.loc[target_week_start_date]

        if pd.isna(preceding_avg_volume) or preceding_avg_volume == 0:
            if do_debug_print: print(f"  DEBUG: Volume: Preceding avg volume is NaN or zero.")
            return None

        volume_condition_met = current_week_volume >= VOLUME_SPIKE_MULTIPLIER * preceding_avg_volume
        volume_ratio = current_week_volume / preceding_avg_volume if preceding_avg_volume > 0 else float('inf')

        if do_debug_print:
            print(f"  DEBUG: Target Week Start: {target_week_start_date.strftime('%Y-%m-%d')}")
            print(f"  DEBUG: Volume: Current={current_week_volume:,.0f}, Avg={preceding_avg_volume:,.0f}, Ratio={volume_ratio:.2f}x")
            print(f"  DEBUG: Volume Condition Met: {volume_condition_met} (Threshold: {VOLUME_SPIKE_MULTIPLIER}x)")

        if not volume_condition_met:
            if do_debug_print: print(f"  DEBUG: Failed volume condition. Skipping price checks.")
            if do_debug_print: print(f"--- END DEBUGGING {ticker} ---\n")
            return None 

        # --- 2. Price Moving Average Conditions ---
        current_week_close_price = weekly_data['Close'].iloc[-1]
        price_conditions_met = True 
        ma_values = {}
        ma_passes = {}

        if do_debug_print:
            print(f"  DEBUG: Price: Current Week Close = {current_week_close_price:.2f}")

        for ma_key, period in MA_PERIODS.items():
            min_weeks_for_ma = period + 1 
            
            if len(weekly_data) >= min_weeks_for_ma:
                ma_series = weekly_data['Close'].shift(1).rolling(window=period, min_periods=int(period * 0.8)).mean()
                ma_value_for_current_week = ma_series.loc[target_week_start_date]
                ma_values[ma_key] = ma_value_for_current_week

                if pd.isna(ma_value_for_current_week):
                    if do_debug_print: print(f"  DEBUG: Price MA ({ma_key} {period}w): MA is NaN. Condition considered FAILED.")
                    price_conditions_met = False 
                    ma_passes[ma_key] = "N/A (NaN)"
                    # No break, collect all MA info
                else:
                    condition_passes = current_week_close_price > ma_value_for_current_week
                    ma_passes[ma_key] = condition_passes
                    if do_debug_print:
                        print(f"  DEBUG: Price MA ({ma_key} {period}w): Val={ma_value_for_current_week:.2f}, Close > MA? {condition_passes}")
                    if not condition_passes:
                        price_conditions_met = False
            else:
                if do_debug_print: print(f"  DEBUG: Price MA ({ma_key} {period}w): Stock too young ({len(weekly_data)}w < {min_weeks_for_ma}w). Omitted.")
                ma_values[ma_key] = "Too Young"
                ma_passes[ma_key] = "Omitted"
        
        if do_debug_print:
            print(f"  DEBUG: Overall Price Conditions Met: {price_conditions_met}")

        if volume_condition_met and price_conditions_met:
            if do_debug_print: print(f"  DEBUG: ALL CONDITIONS MET for {ticker}!")
            return {
                "ticker": ticker,
                "date": target_week_start_date.strftime('%Y-%m-%d'),
                "close_price": current_week_close_price,
                "volume": current_week_volume,
                "avg_volume": preceding_avg_volume,
                "volume_ratio": volume_ratio,
                "ma_values": ma_values,
                "ma_passes": ma_passes
            }

        if do_debug_print: print(f"--- END DEBUGGING {ticker} ---\n")

    except Exception as e:
        if do_debug_print: print(f"  DEBUG: Error processing {ticker}: {e}")
        pass
    finally:
        if DEBUG_FIRST_N_TICKERS > 0 and (DEBUG_SPECIFIC_TICKER is None or ticker != DEBUG_SPECIFIC_TICKER):
            global_debug_counter_ref[0] += 1
    return None


# ... (main function remains the same, but ensure it calls the new get_tickers_from_file correctly) ...
def main():
    print("--- Weekly Volume & Price MA Filter ---")
    print(f"Volume: Current Week >= {VOLUME_SPIKE_MULTIPLIER:.1f}x Avg ({AVG_VOLUME_WEEKS} weeks)")
    print(f"Price: Current Close > {MA_PERIODS['short']}w MA, > {MA_PERIODS['medium']}w MA, > {MA_PERIODS['long']}w MA (omitted if stock too young)")
    print(f"Using tickers from: {TICKER_FILE}\n")

    # Decide if the list is an ASX list needing .AX suffix, or a general list (like the pipe-delimited one)
    # For your pipe-delimited NASDAQ list, is_asx_list should be False.
    # If TICKER_FILE was, say, "asx_200_tickers.txt" (simple list), you might set it to True.
    # We'll make a simple assumption based on file name for now, or you can hardcode it.
    is_asx_format = ".ax" in TICKER_FILE.lower() or "asx" in TICKER_FILE.lower()
    
    # If your pipe-delimited file is NOT for ASX, then is_asx_list should be False
    # For the NASDAQ example file you gave, it's not ASX.
    if "nasdaq_screener_list.txt" in TICKER_FILE: # Example check for your specific file
        is_asx_format = False

    tickers_to_scan = get_tickers_from_file(TICKER_FILE, is_asx_list=is_asx_format) # Pass the flag

    if not tickers_to_scan:
        print("Exiting due to no tickers provided.")
        return

    spiked_stocks_details = []
    processed_count = 0
    total_tickers = len(tickers_to_scan)
    global_debug_counter_ref = [0]

    print(f"Starting scan of {total_tickers} tickers (this may take a while due to extended history fetch)...")
    for i, ticker in enumerate(tickers_to_scan):
        do_debug_print_for_current = (DEBUG_SPECIFIC_TICKER and ticker == DEBUG_SPECIFIC_TICKER) or \
                                     (DEBUG_FIRST_N_TICKERS > 0 and global_debug_counter_ref[0] < DEBUG_FIRST_N_TICKERS)
        if not do_debug_print_for_current:
            print(f"Processing ({i+1}/{total_tickers}): {ticker:<15}         ", end='\r') # Increased ticker width
        else:
            print(f"Processing ({i+1}/{total_tickers}): {ticker:<15}")

        result = analyze_stock_volume(ticker, global_debug_counter_ref)
        if result:
            spiked_stocks_details.append(result)
        processed_count += 1

    print("\n\n--- Scan Complete ---")
    print(f"Processed {processed_count} tickers.")

    if spiked_stocks_details:
        print("\nStocks meeting all criteria for the most recent completed week:")
        header = f"{'Ticker':<15} | {'Date':<10} | {'Close':>7} | {'Vol':>12} | {'AvgVol':>12} | {'VolRatio':>6} | " # Adjusted spacing
        header += f"{MA_PERIODS['short']}w MA | Pass | {MA_PERIODS['medium']}w MA | Pass | {MA_PERIODS['long']}w MA | Pass"
        print("-" * len(header))
        print(header)
        print("-" * len(header))
        for stock_info in spiked_stocks_details:
            row = f"{stock_info['ticker']:<15} | {stock_info['date']:<10} | {stock_info['close_price']:>7.2f} | "
            row += f"{stock_info['volume']:>12,.0f} | {stock_info['avg_volume']:>12,.0f} | {stock_info['volume_ratio']:>6.2f}x | "
            
            for ma_key in ["short", "medium", "long"]:
                ma_val_str = f"{stock_info['ma_values'][ma_key]:>7.2f}" if isinstance(stock_info['ma_values'][ma_key], (int, float)) else f"{str(stock_info['ma_values'][ma_key]):>7}"
                pass_str = str(stock_info['ma_passes'][ma_key]) if stock_info['ma_passes'][ma_key] != "Omitted" else "Omit"
                row += f"{ma_val_str} | {pass_str:<4} | "
            print(row.strip().rstrip('|').strip())
        print("-" * len(header))
    else:
        print("No stocks found with the specified criteria for the most recent week.")

if __name__ == "__main__":
    main()