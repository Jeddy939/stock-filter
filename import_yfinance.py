# volume_scanner.py (with yfinance fix and debugging)
import yfinance as yf
import pandas as pd
from datetime import datetime, timedelta

# --- Configuration ---
TICKER_FILE = "asx_200_tickers.txt"
VOLUME_SPIKE_MULTIPLIER = 1.5
AVG_VOLUME_WEEKS = 52
DATA_FETCH_PERIOD_MONTHS = 18

# !!! --- DEBUGGING CONFIG --- !!!
DEBUG_SPECIFIC_TICKER = "BHP.AX" # Or another ticker, or None
DEBUG_FIRST_N_TICKERS = 3       # Debug first N tickers if DEBUG_SPECIFIC_TICKER is None or not matched
debug_counter = 0

def get_tickers_from_file(filename=TICKER_FILE):
    tickers = []
    try:
        with open(filename, 'r') as f:
            for line in f:
                ticker = line.strip().upper()
                if ticker and not ticker.startswith("#"):
                    if not ticker.endswith(".AX"):
                        ticker += ".AX"
                    tickers.append(ticker)
        if not tickers:
            print(f"Warning: No tickers found in {filename}. Please add tickers to the file.")
        else:
            print(f"Loaded {len(tickers)} tickers from {filename}.")
        return tickers
    except FileNotFoundError:
        print(f"Error: Ticker file '{filename}' not found.")
        print(f"Please create this file and add tickers (e.g., CBA.AX), one per line.")
        return []

def analyze_stock_volume(ticker):
    global debug_counter
    try:
        do_debug_print = (DEBUG_SPECIFIC_TICKER and ticker == DEBUG_SPECIFIC_TICKER) or \
                         (DEBUG_FIRST_N_TICKERS > 0 and debug_counter < DEBUG_FIRST_N_TICKERS)
        if do_debug_print:
            print(f"\n--- DEBUGGING {ticker} ---")
            print(f"Script run time (approx): {datetime.now().strftime('%Y-%m-%d %H:%M:%S')}")

        end_date = datetime.now()
        start_date = end_date - timedelta(days=DATA_FETCH_PERIOD_MONTHS * 30)

        stock = yf.Ticker(ticker)
        # CORRECTED LINE: Removed 'progress=False' and 'show_errors=False'
        hist_daily = stock.history(start=start_date.strftime('%Y-%m-%d'),
                                   end=end_date.strftime('%Y-%m-%d'),
                                   interval="1d")

        if hist_daily.empty:
            if do_debug_print: print(f"  DEBUG: No daily history returned for {ticker}.")
            return None

        if do_debug_print:
            print(f"  DEBUG: Daily history fetched from {hist_daily.index.min().strftime('%Y-%m-%d')} to {hist_daily.index.max().strftime('%Y-%m-%d')}")
            if len(hist_daily) < (AVG_VOLUME_WEEKS * 5 * 0.8):
                 print(f"  DEBUG: Warning - limited daily data points: {len(hist_daily)}")

        weekly_data_volume = hist_daily['Volume'].resample('W-MON').sum()
        weekly_data_volume = weekly_data_volume[weekly_data_volume > 0]

        if do_debug_print and not weekly_data_volume.empty:
            print(f"  DEBUG: Last 5 weekly volumes (summed, week starting Mon):")
            for date_idx, vol in weekly_data_volume.tail(5).items():
                print(f"    {date_idx.strftime('%Y-%m-%d')}: {vol:,.0f}")
        elif do_debug_print and weekly_data_volume.empty:
            print(f"  DEBUG: No weekly volume data after resampling and filtering zero volume weeks.")

        if len(weekly_data_volume) < AVG_VOLUME_WEEKS + 1:
            if do_debug_print: print(f"  DEBUG: Not enough weekly data points after resampling ({len(weekly_data_volume)}) for {ticker} (need {AVG_VOLUME_WEEKS + 1}).")
            return None

        avg_volume_series = weekly_data_volume.shift(1).rolling(window=AVG_VOLUME_WEEKS, min_periods=int(AVG_VOLUME_WEEKS * 0.8)).mean()
        
        current_week_volume = weekly_data_volume.iloc[-1]
        target_week_start_date = weekly_data_volume.index[-1]
        preceding_avg_volume = avg_volume_series.loc[target_week_start_date]

        if do_debug_print:
            print(f"  DEBUG: Target week for spike check (starts on Mon): {target_week_start_date.strftime('%Y-%m-%d')}")
            print(f"  DEBUG: Current Week Volume ({target_week_start_date.strftime('%Y-%m-%d')}): {current_week_volume:,.0f}")
            if pd.notna(preceding_avg_volume):
                print(f"  DEBUG: Preceding {AVG_VOLUME_WEEKS}-week Avg Volume: {preceding_avg_volume:,.0f}")
                print(f"  DEBUG: Threshold for spike ({VOLUME_SPIKE_MULTIPLIER} * Avg): {VOLUME_SPIKE_MULTIPLIER * preceding_avg_volume:,.0f}")
            else:
                print(f"  DEBUG: Preceding {AVG_VOLUME_WEEKS}-week Avg Volume: NaN or not available")

        if pd.isna(preceding_avg_volume) or preceding_avg_volume == 0:
            if do_debug_print: print(f"  DEBUG: Skipping {ticker} due to NaN or zero preceding_avg_volume.")
            return None

        if current_week_volume >= VOLUME_SPIKE_MULTIPLIER * preceding_avg_volume:
            if do_debug_print: print(f"  DEBUG: SPIKE DETECTED for {ticker}!")
            return {
                "ticker": ticker,
                "spike_date": target_week_start_date.strftime('%Y-%m-%d'),
                "current_volume": current_week_volume,
                "average_volume": preceding_avg_volume,
                "ratio": current_week_volume / preceding_avg_volume
            }
        else:
            if do_debug_print: print(f"  DEBUG: No spike for {ticker}. Ratio: {current_week_volume / preceding_avg_volume:.2f}x (Threshold was {VOLUME_SPIKE_MULTIPLIER}x)")

        if do_debug_print: print(f"--- END DEBUGGING {ticker} ---\n")

    except Exception as e:
        if do_debug_print: print(f"  DEBUG: Error processing {ticker}: {e}")
        pass
    finally:
        if DEBUG_FIRST_N_TICKERS > 0 and (DEBUG_SPECIFIC_TICKER is None or ticker != DEBUG_SPECIFIC_TICKER):
            debug_counter += 1
    return None

def main():
    print("--- ASX Weekly Volume Spike Detector ---")
    print(f"Looking for weekly volume 50% OVER the average (i.e., current >= {VOLUME_SPIKE_MULTIPLIER}x average)")
    print(f"of the previous {AVG_VOLUME_WEEKS} weeks.")
    print(f"Using tickers from: {TICKER_FILE}\n")

    tickers_to_scan = get_tickers_from_file()
    if not tickers_to_scan:
        print("Exiting due to no tickers provided.")
        return

    spiked_stocks_details = []
    processed_count = 0
    total_tickers = len(tickers_to_scan)
    global debug_counter
    debug_counter = 0

    print(f"Starting scan of {total_tickers} tickers...")
    for i, ticker in enumerate(tickers_to_scan):
        if not (DEBUG_SPECIFIC_TICKER and ticker == DEBUG_SPECIFIC_TICKER) and \
           not (DEBUG_FIRST_N_TICKERS > 0 and debug_counter < DEBUG_FIRST_N_TICKERS):
            print(f"Processing ({i+1}/{total_tickers}): {ticker:<10}         ", end='\r')
        else:
            print(f"Processing ({i+1}/{total_tickers}): {ticker:<10}")

        result = analyze_stock_volume(ticker)
        if result:
            spiked_stocks_details.append(result)
        processed_count += 1

    print("\n\n--- Scan Complete ---")
    print(f"Processed {processed_count} tickers.")

    if spiked_stocks_details:
        print("\nStocks with significant weekly volume spikes in the most recent completed week:")
        print("--------------------------------------------------------------------------------")
        print(f"{'Ticker':<10} | {'Week Of':<12} | {'Volume':>15} | {'Avg Volume':>15} | {'Ratio':>6}")
        print("--------------------------------------------------------------------------------")
        for stock_info in spiked_stocks_details:
            print(f"{stock_info['ticker']:<10} | {stock_info['spike_date']:<12} | {stock_info['current_volume']:>15,.0f} | {stock_info['average_volume']:>15,.0f} | {stock_info['ratio']:>6.2f}x")
        print("--------------------------------------------------------------------------------")
    else:
        print("No stocks found with the specified volume spike criteria for the most recent week.")

if __name__ == "__main__":
    main()