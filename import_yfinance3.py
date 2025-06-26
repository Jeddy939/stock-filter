import tkinter as tk
from tkinter import ttk, filedialog, messagebox
import yfinance as yf
import pandas as pd
from datetime import datetime, timedelta
import threading
import queue
import webbrowser
import time # For ETA calculation

# --- Default Configuration --- (These will be initial UI values)
DEFAULT_TICKER_FILE = "asx_200_tickers.txt" # Or your preferred default
DEFAULT_VOLUME_MULTIPLIER = 2.0
DEFAULT_MA_SHORT = 90
DEFAULT_MA_MEDIUM = 360
DEFAULT_MA_LONG = 700
DEFAULT_AVG_VOLUME_WEEKS = 52

# --- Core Scanning Logic (adapted from previous script) ---
# This will be run in a separate thread

def get_tickers_from_file_core(filename, is_asx_list=False, progress_queue=None):
    tickers = []
    try:
        with open(filename, 'r', encoding='utf-8') as f:
            header_line_content = "Symbol|Security Name"
            skipped_header = False
            lines = f.readlines() # Read all lines to count for progress
            total_lines = len(lines)
            line_num = 0

            for line_content in lines:
                line_num += 1
                if progress_queue:
                    progress_queue.put(f"Reading ticker file: line {line_num}/{total_lines}")

                line = line_content.strip()
                if not line or line.startswith("#"):
                    continue
                if not skipped_header and header_line_content in line:
                    skipped_header = True
                    continue
                
                if '|' in line:
                    parts = line.split('|')
                    if parts:
                        ticker = parts[0].strip().upper()
                        if ticker: tickers.append(ticker)
                else:
                    ticker = line.strip().upper()
                    if ticker:
                        if is_asx_list and not ticker.endswith(".AX"):
                            ticker += ".AX"
                        tickers.append(ticker)
        if not tickers:
            if progress_queue: progress_queue.put(f"Warning: No tickers found in {filename}.")
        return tickers
    except FileNotFoundError:
        if progress_queue: progress_queue.put(f"Error: Ticker file '{filename}' not found.")
        return []
    except Exception as e:
        if progress_queue: progress_queue.put(f"Error reading ticker file: {str(e)}")
        return []

def analyze_stock_core(ticker, config, progress_queue=None):
    # config is a dictionary holding volume_multiplier, avg_volume_weeks, ma_periods, data_fetch_years
    try:
        end_date = datetime.now()
        start_date = end_date - timedelta(days=config['data_fetch_years'] * 365.25)

        stock = yf.Ticker(ticker)
        hist_daily = stock.history(start=start_date.strftime('%Y-%m-%d'),
                                   end=end_date.strftime('%Y-%m-%d'),
                                   interval="1d")

        if hist_daily.empty: return None
        
        agg_functions = {'Open': 'first', 'High': 'max', 'Low': 'min', 'Close': 'last', 'Volume': 'sum'}
        weekly_data = hist_daily.resample('W-MON').agg(agg_functions)
        weekly_data = weekly_data.dropna(subset=['Close', 'Volume'])
        weekly_data = weekly_data[weekly_data['Volume'] > 0]

        if weekly_data.empty: return None

        # --- Exclude if too young for shortest MA ---
        shortest_ma_period = config['ma_periods']['short']
        min_weeks_for_shortest_ma = shortest_ma_period + 1
        if len(weekly_data) < min_weeks_for_shortest_ma:
            if progress_queue: progress_queue.put(f"Status: {ticker} too young for {shortest_ma_period}w MA. Skipping.")
            return None # Exclude based on new rule

        # --- Volume Spike Condition ---
        min_weeks_for_volume_avg = config['avg_volume_weeks'] + 1
        if len(weekly_data) < min_weeks_for_volume_avg: return None

        avg_weekly_volume_series = weekly_data['Volume'].shift(1).rolling(window=config['avg_volume_weeks'], min_periods=int(config['avg_volume_weeks'] * 0.8)).mean()
        
        current_week_volume = weekly_data['Volume'].iloc[-1]
        target_week_start_date = weekly_data.index[-1]
        preceding_avg_volume = avg_weekly_volume_series.get(target_week_start_date, float('nan')) # Use .get for safety

        if pd.isna(preceding_avg_volume) or preceding_avg_volume == 0: return None

        volume_condition_met = current_week_volume >= config['volume_multiplier'] * preceding_avg_volume
        volume_ratio = current_week_volume / preceding_avg_volume if preceding_avg_volume > 0 else float('inf')

        if not volume_condition_met: return None

        # --- Price Moving Average Conditions ---
        current_week_close_price = weekly_data['Close'].iloc[-1]
        price_conditions_met = True
        ma_values = {}
        ma_passes = {}

        for ma_key, period in config['ma_periods'].items():
            min_weeks_for_ma = period + 1
            
            if len(weekly_data) >= min_weeks_for_ma:
                ma_series = weekly_data['Close'].shift(1).rolling(window=period, min_periods=int(period * 0.8)).mean()
                ma_value_for_current_week = ma_series.get(target_week_start_date, float('nan')) # Use .get
                ma_values[ma_key] = ma_value_for_current_week

                if pd.isna(ma_value_for_current_week):
                    price_conditions_met = False
                    ma_passes[ma_key] = "N/A (NaN)"
                else:
                    condition_passes = current_week_close_price > ma_value_for_current_week
                    ma_passes[ma_key] = condition_passes
                    if not condition_passes:
                        price_conditions_met = False
            else:
                ma_values[ma_key] = "Too Young"
                ma_passes[ma_key] = "Omitted" # This MA condition is effectively passed/ignored

        if price_conditions_met: # volume_condition_met is already true if we reached here
            return {
                "ticker": ticker,
                "date": target_week_start_date.strftime('%Y-%m-%d'),
                "close_price": current_week_close_price,
                "volume": current_week_volume,
                "avg_volume": preceding_avg_volume,
                "volume_ratio": volume_ratio,
                "ma_values": ma_values,
                "ma_passes": ma_passes,
                "ma_periods_config": config['ma_periods'] # Store for display consistency
            }
    except Exception as e:
        if progress_queue: progress_queue.put(f"Error processing {ticker}: {str(e)[:100]}") # Log error
        return None
    return None

def run_scan_thread(config, progress_queue, results_queue):
    progress_queue.put("Status: Starting scan...")
    is_asx = ".ax" in config['ticker_file'].lower() or "asx" in config['ticker_file'].lower()
    
    # Simple check for the NASDAQ list filename you provided earlier
    if "nasdaq_screener_list.txt" in config['ticker_file'].lower():
        is_asx = False

    tickers = get_tickers_from_file_core(config['ticker_file'], is_asx, progress_queue)
    
    if not tickers:
        progress_queue.put("Status: No tickers to scan.")
        progress_queue.put("DONE")
        return

    progress_queue.put(f"Status: Loaded {len(tickers)} tickers. Starting analysis...")
    
    results = []
    total_tickers = len(tickers)
    start_time = time.time()

    for i, ticker in enumerate(tickers):
        elapsed_time = time.time() - start_time
        avg_time_per_ticker = elapsed_time / (i + 1) if i > 0 else 1 # Avoid division by zero, estimate 1s for first
        tickers_remaining = total_tickers - (i + 1)
        eta_seconds = tickers_remaining * avg_time_per_ticker
        eta_str = f"{int(eta_seconds // 60)}m {int(eta_seconds % 60)}s" if eta_seconds > 0 else "---"
        
        progress_queue.put(f"Status: Scanning {ticker} ({i+1}/{total_tickers}) ETA: {eta_str}")
        
        analysis_result = analyze_stock_core(ticker, config, progress_queue)
        if analysis_result:
            results.append(analysis_result)
            results_queue.put(analysis_result) # Send result immediately for live update

    progress_queue.put("Status: Scan complete!")
    progress_queue.put("DONE") # Signal completion

# --- Tkinter Application ---
class StockScannerApp:
    def __init__(self, root):
        self.root = root
        root.title("Stock Scanner")
        root.geometry("1200x700") # Adjusted for wider table

        self.config = {} # To store UI settings
        self.scan_thread = None
        self.progress_queue = queue.Queue()
        self.results_queue = queue.Queue()

        # --- UI Elements ---
        # Frame for inputs
        input_frame = ttk.LabelFrame(root, text="Scan Parameters", padding="10")
        input_frame.pack(side=tk.TOP, fill=tk.X, padx=10, pady=5)

        # Ticker File
        ttk.Label(input_frame, text="Ticker File:").grid(row=0, column=0, sticky=tk.W, padx=5, pady=2)
        self.ticker_file_var = tk.StringVar(value=DEFAULT_TICKER_FILE)
        self.ticker_file_entry = ttk.Entry(input_frame, textvariable=self.ticker_file_var, width=50)
        self.ticker_file_entry.grid(row=0, column=1, sticky=tk.EW, padx=5, pady=2)
        self.browse_button = ttk.Button(input_frame, text="Browse", command=self.browse_file)
        self.browse_button.grid(row=0, column=2, sticky=tk.W, padx=5, pady=2)

        # Volume Spike
        ttk.Label(input_frame, text="Volume Multiplier (e.g., 2.0):").grid(row=1, column=0, sticky=tk.W, padx=5, pady=2)
        self.volume_mult_var = tk.DoubleVar(value=DEFAULT_VOLUME_MULTIPLIER)
        self.volume_mult_entry = ttk.Entry(input_frame, textvariable=self.volume_mult_var, width=10)
        self.volume_mult_entry.grid(row=1, column=1, sticky=tk.W, padx=5, pady=2)

        # MA Periods
        ttk.Label(input_frame, text="MA Short (weeks):").grid(row=2, column=0, sticky=tk.W, padx=5, pady=2)
        self.ma_short_var = tk.IntVar(value=DEFAULT_MA_SHORT)
        self.ma_short_entry = ttk.Entry(input_frame, textvariable=self.ma_short_var, width=10)
        self.ma_short_entry.grid(row=2, column=1, sticky=tk.W, padx=5, pady=2)

        ttk.Label(input_frame, text="MA Medium (weeks):").grid(row=2, column=2, sticky=tk.W, padx=5, pady=2)
        self.ma_medium_var = tk.IntVar(value=DEFAULT_MA_MEDIUM)
        self.ma_medium_entry = ttk.Entry(input_frame, textvariable=self.ma_medium_var, width=10)
        self.ma_medium_entry.grid(row=2, column=3, sticky=tk.W, padx=5, pady=2)
        
        ttk.Label(input_frame, text="MA Long (weeks):").grid(row=2, column=4, sticky=tk.W, padx=5, pady=2)
        self.ma_long_var = tk.IntVar(value=DEFAULT_MA_LONG)
        self.ma_long_entry = ttk.Entry(input_frame, textvariable=self.ma_long_var, width=10)
        self.ma_long_entry.grid(row=2, column=5, sticky=tk.W, padx=5, pady=2)
        
        input_frame.columnconfigure(1, weight=1) # Make entry field expand

        # Run Button
        self.run_button = ttk.Button(input_frame, text="Run Scan", command=self.start_scan)
        self.run_button.grid(row=3, column=0, columnspan=6, pady=10)

        # Status Label
        self.status_var = tk.StringVar(value="Ready.")
        status_label = ttk.Label(root, textvariable=self.status_var, relief=tk.SUNKEN, anchor=tk.W, padding="2")
        status_label.pack(side=tk.BOTTOM, fill=tk.X, padx=10, pady=5)

        # Results Treeview
        results_frame = ttk.LabelFrame(root, text="Results", padding="10")
        results_frame.pack(expand=True, fill=tk.BOTH, padx=10, pady=5)

        self.columns = ("Ticker", "Date", "Close", "AvgVol", "VolRatio", "MA_S_Pass", "MA_M_Pass", "MA_L_Pass")
        self.tree = ttk.Treeview(results_frame, columns=self.columns, show="headings")
        
        col_widths = {
            "Ticker": 80, "Date": 80, "Close": 60, "AvgVol": 90, "VolRatio": 70,
            "MA_S_Pass": 90, "MA_M_Pass": 90, "MA_L_Pass": 90
        }

        header_texts = {
            "AvgVol": "Avg Vol", "VolRatio": "Vol Ratio", "MA_S_Pass": "MA Short Pass",
            "MA_M_Pass": "MA Medium Pass", "MA_L_Pass": "MA Long Pass"
        }

        for col in self.columns:
            self.tree.heading(col, text=header_texts.get(col, col))
            self.tree.column(col, width=col_widths.get(col, 80), anchor=tk.CENTER)

        # Special handling for Ticker column for hyperlink feel
        self.tree.tag_configure("hyperlink", foreground="blue", font=('TkDefaultFont', 9, 'underline'))
        self.tree.bind("<Button-1>", self.on_tree_click)

        # Scrollbars for Treeview
        vsb = ttk.Scrollbar(results_frame, orient="vertical", command=self.tree.yview)
        hsb = ttk.Scrollbar(results_frame, orient="horizontal", command=self.tree.xview)
        self.tree.configure(yscrollcommand=vsb.set, xscrollcommand=hsb.set)
        
        vsb.pack(side=tk.RIGHT, fill=tk.Y)
        hsb.pack(side=tk.BOTTOM, fill=tk.X)
        self.tree.pack(expand=True, fill=tk.BOTH)

        self.check_queues() # Start queue checker

    def browse_file(self):
        filename = filedialog.askopenfilename(
            title="Select Ticker File",
            filetypes=(("Text files", "*.txt"), ("CSV files", "*.csv"), ("All files", "*.*"))
        )
        if filename:
            self.ticker_file_var.set(filename)

    def on_tree_click(self, event):
        region = self.tree.identify_region(event.x, event.y)
        if region == "cell":
            column_id = self.tree.identify_column(event.x)
            column_index = int(column_id.replace("#", "")) -1 # Column index is 0-based
            
            if self.columns[column_index] == "Ticker":
                item_id = self.tree.identify_row(event.y)
                if item_id:
                    ticker_symbol = self.tree.item(item_id, "values")[0]
                    if ticker_symbol:
                        # Construct Yahoo Finance URL (adjust for non-US if needed later)
                        # Basic US ticker URL:
                        url = f"https://finance.yahoo.com/quote/{ticker_symbol}"
                        # For ASX tickers (if this logic were adapted):
                        # if ticker_symbol.endswith(".AX"):
                        #    url = f"https://au.finance.yahoo.com/quote/{ticker_symbol}"
                        try:
                            webbrowser.open_new_tab(url)
                        except Exception as e:
                            self.status_var.set(f"Error opening browser: {e}")


    def start_scan(self):
        if self.scan_thread and self.scan_thread.is_alive():
            messagebox.showwarning("Scan in Progress", "A scan is already running.")
            return

        try:
            self.config['ticker_file'] = self.ticker_file_var.get()
            self.config['volume_multiplier'] = self.volume_mult_var.get()
            self.config['avg_volume_weeks'] = DEFAULT_AVG_VOLUME_WEEKS # Stays default for now

            ma_s = self.ma_short_var.get()
            ma_m = self.ma_medium_var.get()
            ma_l = self.ma_long_var.get()
            
            if not (ma_s > 0 and ma_m > 0 and ma_l > 0 and ma_s < ma_m < ma_l):
                 messagebox.showerror("Invalid MA Periods", "MA periods must be positive and in increasing order (Short < Medium < Long).")
                 return
            if self.config['volume_multiplier'] <= 0:
                messagebox.showerror("Invalid Volume Multiplier", "Volume multiplier must be positive.")
                return


            self.config['ma_periods'] = {"short": ma_s, "medium": ma_m, "long": ma_l}
            
            # Dynamic data fetch period
            max_ma_period_weeks = max(ma_s, ma_m, ma_l)
            # Add avg volume weeks + buffer
            self.config['data_fetch_years'] = (max_ma_period_weeks / 52) + (DEFAULT_AVG_VOLUME_WEEKS / 52) + 2 # +2 years buffer

            self.run_button.config(state=tk.DISABLED)
            self.status_var.set("Status: Initializing scan...")
            self.tree.delete(*self.tree.get_children()) # Clear previous results

            # Update treeview headers with dynamic MA periods from the current scan settings
            self.tree.heading("MA_S_Pass", text=f"{ma_s}w Pass")
            self.tree.heading("MA_M_Pass", text=f"{ma_m}w Pass")
            self.tree.heading("MA_L_Pass", text=f"{ma_l}w Pass")

            # Ensure queues are empty before starting
            while not self.progress_queue.empty(): self.progress_queue.get_nowait()
            while not self.results_queue.empty(): self.results_queue.get_nowait()
            
            self.scan_thread = threading.Thread(
                target=run_scan_thread,
                args=(self.config.copy(), self.progress_queue, self.results_queue) # Pass a copy of config
            )
            self.scan_thread.daemon = True # Allows main program to exit even if thread is running
            self.scan_thread.start()

        except ValueError:
            messagebox.showerror("Input Error", "Please enter valid numbers for volume multiplier and MA periods.")
            self.run_button.config(state=tk.NORMAL)
        except Exception as e:
            messagebox.showerror("Error", f"An unexpected error occurred: {e}")
            self.run_button.config(state=tk.NORMAL)

    def check_queues(self):
        # Process progress queue
        try:
            while True:
                message = self.progress_queue.get_nowait()
                if message == "DONE":
                    self.run_button.config(state=tk.NORMAL)
                    # Final status update might be handled by the last message before "DONE"
                elif message.startswith("Status:"):
                     self.status_var.set(message)
                elif message.startswith("Error:") or message.startswith("Warning:"):
                    # Could log these to a more detailed log area if needed
                    self.status_var.set(message) # Show last critical message
                # else:
                    # Could be other debug messages from core, ignore for status bar
                    # print(f"Core Log: {message}")

        except queue.Empty:
            pass # No new messages

        # Process results queue (add items to treeview)
        try:
            while True:
                result = self.results_queue.get_nowait()
                self.add_result_to_treeview(result)
        except queue.Empty:
            pass

        self.root.after(100, self.check_queues) # Check again in 100ms

    def add_result_to_treeview(self, result_item):
        # Ensure MA keys match the order/names in self.columns for MA_S_Val, MA_M_Val etc.
        # The ma_periods_config stored in result_item ensures we use the correct labels
        ma_p_conf = result_item['ma_periods_config']
        
        values = [
            result_item['ticker'],
            result_item['date'],
            f"{result_item['close_price']:.2f}",
            f"{result_item['avg_volume']:,.0f}",
            f"{result_item['volume_ratio']:.2f}x"
        ]
        
        # Add MA passes dynamically based on the config used for that scan
        for ma_key in ["short", "medium", "long"]: # Order matters for columns
            ma_pass_raw = result_item['ma_passes'].get(ma_key)

            ma_pass_str = "Omit" if ma_pass_raw == "Omitted" else ("Yes" if ma_pass_raw is True else ("No" if ma_pass_raw is False else "N/A"))
            
            values.append(ma_pass_str)
        
        item_id = self.tree.insert("", tk.END, values=tuple(values))
        # Apply hyperlink tag to the first cell (ticker)
        self.tree.item(item_id, tags=("hyperlink",))


if __name__ == "__main__":
    root = tk.Tk()
    app = StockScannerApp(root)
    root.mainloop()