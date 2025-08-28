import tkinter as tk
from tkinter import ttk, filedialog, messagebox, scrolledtext
import yfinance as yf
import pandas as pd
from datetime import datetime, timedelta
import threading
import queue
import webbrowser
import json
import sv_ttk
import subprocess
import sys
import os
from io import StringIO

# --- Default Configuration ---
DEFAULT_DATA_FILE = "stock_data.json"
DEFAULT_TICKER_FILE = "asx_200_tickers.txt"
DEFAULT_VOLUME_MULTIPLIER = 2.0
DEFAULT_MA_SHORT = 90
DEFAULT_MA_INTERMEDIATE = 180
DEFAULT_MA_MEDIUM = 360
DEFAULT_MA_LONG = 700
DEFAULT_AVG_VOLUME_WEEKS = 52
DEFAULT_PRICE_AVG_WEEKS = 1
DEFAULT_DATA_YEARS = 15
DEFAULT_MAX_WORKERS = 10

# --- Core Filtering Logic (operates on pre-loaded data) ---
def analyze_stock_from_local_data(ticker, data, config, progress_queue=None, log_queue=None):
    try:
        info = data.get('info', {})
        history_json = data.get('history')
        
        if not history_json:
            if log_queue: log_queue.put(f"  -> SKIPPED: {ticker} - Missing essential history data.")
            return None

        market_cap = info.get('marketCap') if info else None
        min_cap_m = config.get('min_market_cap', 0)
        max_cap_m = config.get('max_market_cap', 0)
        
        hist_daily = pd.read_json(StringIO(json.dumps(history_json)), orient='split')
        if hist_daily.empty:
            if log_queue: log_queue.put(f"  -> SKIPPED: {ticker} - No historical data after processing.")
            return None

        agg_functions = {'Open': 'first', 'High': 'max', 'Low': 'min', 'Close': 'last', 'Volume': 'sum'}
        weekly_data = hist_daily.resample('W-MON').agg(agg_functions).dropna(subset=['Close', 'Volume'])
        weekly_data = weekly_data[weekly_data['Volume'] > 0]
        if weekly_data.empty:
            if log_queue: log_queue.put(f"  -> SKIPPED: {ticker} - No valid weekly data.")
            return None

        if datetime.now().date() < weekly_data.index[-1].date():
            weekly_data = weekly_data.iloc[:-1]
        if weekly_data.empty:
            if log_queue: log_queue.put(f"  -> SKIPPED: {ticker} - No weekly data after removing incomplete week.")
            return None

        lookback_weeks = config.get('lookback_weeks', 1)

        for i in range(1, lookback_weeks + 1):
            if len(weekly_data) < i:
                break 

            target_week_index = -i
            
            # --- Data Age / MA Period Checks for the target week ---
            if len(weekly_data.iloc[:target_week_index]) < config['ma_periods']['short'] + 1:
                if log_queue and i == 1: log_queue.put(f"  -> SKIPPED: {ticker} - Too young for shortest MA ({config['ma_periods']['short']} weeks).")
                continue
            if len(weekly_data.iloc[:target_week_index]) < config['avg_volume_weeks'] + 1:
                if log_queue and i == 1: log_queue.put(f"  -> SKIPPED: {ticker} - Not enough data for volume average ({config['avg_volume_weeks']} weeks).")
                continue

            # --- Volume Spike Condition ---
            avg_weekly_volume_series = weekly_data['Volume'].shift(1).rolling(window=config['avg_volume_weeks'], min_periods=int(config['avg_volume_weeks'] * 0.8)).mean()
            current_week_volume = weekly_data['Volume'].iloc[target_week_index]
            target_week_start_date = weekly_data.index[target_week_index]
            preceding_avg_volume = avg_weekly_volume_series.get(target_week_start_date, float('nan'))
            
            if pd.isna(preceding_avg_volume) or preceding_avg_volume == 0:
                continue

            if not current_week_volume >= config['volume_multiplier'] * preceding_avg_volume:
                continue

            # --- Price vs. Preceding Weeks Average Condition ---
            if len(weekly_data.iloc[:target_week_index]) < config.get('price_avg_weeks', 1):
                continue
            current_week_close_price = weekly_data['Close'].iloc[target_week_index]
            
            price_avg_start_index = target_week_index - config.get('price_avg_weeks', 1)
            if not weekly_data['Close'].iloc[price_avg_start_index:target_week_index].empty:
                if current_week_close_price <= weekly_data['Close'].iloc[price_avg_start_index:target_week_index].mean():
                    continue
            else:
                continue

            # --- Price Moving Average Conditions ---
            price_conditions_met = True
            for ma_name, period in config['ma_periods'].items():
                if len(weekly_data.iloc[:target_week_index]) >= period:
                    ma_series = weekly_data['Close'].shift(1).rolling(window=period, min_periods=int(period * 0.8)).mean()
                    ma_value = ma_series.get(target_week_start_date, float('nan'))
                    if pd.isna(ma_value) or current_week_close_price <= ma_value:
                        price_conditions_met = False
                        break
            
            if price_conditions_met:
                # Market cap check is performed last, only if all other conditions are met for a given week
                if market_cap is None:
                    if min_cap_m > 0:
                        if log_queue: log_queue.put(f"  -> WEEK SKIPPED ({target_week_start_date.date()}): No market cap data, but min cap filter is active.")
                        continue
                else:
                    market_cap_in_millions = market_cap / 1_000_000
                    if min_cap_m > 0 and market_cap_in_millions < min_cap_m:
                        if log_queue: log_queue.put(f"  -> WEEK SKIPPED ({target_week_start_date.date()}): Mkt Cap ({market_cap_in_millions:.2f}M) is below min ({min_cap_m}M).")
                        continue
                    if max_cap_m > 0 and market_cap_in_millions > max_cap_m:
                        if log_queue: log_queue.put(f"  -> WEEK SKIPPED ({target_week_start_date.date()}): Mkt Cap ({market_cap_in_millions:.2f}M) is above max ({max_cap_m}M).")
                        continue
                
                # If we get here, this week is a match
                return {"ticker": ticker, "date": target_week_start_date.strftime('%Y-%m-%d'), "close_price": current_week_close_price, "market_cap": market_cap, "avg_volume": preceding_avg_volume, "volume_ratio": current_week_volume / preceding_avg_volume if preceding_avg_volume > 0 else float('inf')}

        # If the loop finishes without finding a match
        if log_queue: log_queue.put(f"  -> SKIPPED: {ticker} - No week in the lookback period met all criteria.")
        return None

    except Exception as e:
        if progress_queue: progress_queue.put(f"Error processing {ticker}: {str(e)[:100]}")
        if log_queue: log_queue.put(f"  -> ERROR: {ticker} - {str(e)[:100]}")
    return None

def run_filter_thread(config, stock_data, results_queue, progress_queue):
    progress_queue.put("Status: Starting filter...")
    results = []
    total_stocks = len(stock_data)
    processed_count = 0

    for ticker, data in stock_data.items():
        result = analyze_stock_from_local_data(ticker, data, config, progress_queue)
        if result:
            results.append(result)
        
        processed_count += 1
        if processed_count % 100 == 0:
            progress_queue.put(f"Status: Analyzed {processed_count}/{total_stocks} stocks...")

    results.sort(key=lambda x: x['volume_ratio'], reverse=True)
    for res in results:
        results_queue.put(res)
    
    if not results:
        progress_queue.put(f"Status: Filter complete! No matching stocks found out of {total_stocks}.")
    else:
        progress_queue.put(f"Status: Filter complete! Found {len(results)} matching stocks.")
        
    progress_queue.put("DONE")

class MoneymakerProAlphaApp:
    def __init__(self, root):
        self.root = root
        root.title("Moneymaker Pro Alpha")
        root.geometry("1300x800")
        sv_ttk.set_theme("dark")

        self.stock_data = {}
        self.filter_thread = None
        self.data_loading_thread = None
        self.results_queue = queue.Queue()
        self.progress_queue = queue.Queue()
        self.log_queue = queue.Queue()
        self.filter_log_queue = queue.Queue()
        self.status_var = tk.StringVar()
        self.filter_log_window = None
        self.filter_log_window = None

        self.notebook = ttk.Notebook(root)
        self.notebook.pack(fill=tk.BOTH, expand=True, padx=10, pady=10)

        self.fetch_tab = ttk.Frame(self.notebook, padding=10)
        self.filter_tab = ttk.Frame(self.notebook, padding=10)
        self.notebook.add(self.fetch_tab, text='  Fetch Data  ')
        self.notebook.add(self.filter_tab, text='  Filter & Screen  ')

        self._create_fetch_widgets(self.fetch_tab)
        self._create_filter_widgets(self.filter_tab)

        self.status_var.set("Ready. Please load a data file to begin.")
        self.check_queues()

    def _create_fetch_widgets(self, parent):
        parent.columnconfigure(0, weight=1)
        parent.rowconfigure(1, weight=1)

        controls_frame = ttk.LabelFrame(parent, text="Fetch Parameters", padding=15)
        controls_frame.grid(row=0, column=0, sticky="ew", pady=(0, 10))
        controls_frame.columnconfigure(1, weight=1)

        ttk.Label(controls_frame, text="Ticker File:").grid(row=0, column=0, sticky=tk.W, padx=5, pady=5)
        self.ticker_file_var = tk.StringVar(value=DEFAULT_TICKER_FILE)
        self.ticker_file_entry = ttk.Entry(controls_frame, textvariable=self.ticker_file_var, width=50)
        self.ticker_file_entry.grid(row=0, column=1, sticky=tk.EW, padx=5, pady=5)
        
        browse_button = ttk.Button(controls_frame, text="Browse", command=self.browse_ticker_file)
        browse_button.grid(row=0, column=2, padx=5, pady=5)

        fetch_ords_button = ttk.Button(controls_frame, text="Fetch All Ords", command=self.fetch_all_ords_tickers)
        fetch_ords_button.grid(row=0, column=3, padx=5, pady=5)

        ttk.Label(controls_frame, text="Data Years:").grid(row=1, column=0, sticky=tk.W, padx=5, pady=5)
        self.years_var = tk.IntVar(value=DEFAULT_DATA_YEARS)
        ttk.Entry(controls_frame, textvariable=self.years_var, width=10).grid(row=1, column=1, sticky=tk.W, padx=5, pady=5)

        ttk.Label(controls_frame, text="Output File:").grid(row=2, column=0, sticky=tk.W, padx=5, pady=5)
        self.output_file_var = tk.StringVar(value=DEFAULT_DATA_FILE)
        ttk.Entry(controls_frame, textvariable=self.output_file_var, width=50).grid(row=2, column=1, columnspan=3, sticky=tk.EW, padx=5, pady=5)

        self.run_fetch_button = ttk.Button(controls_frame, text="Start Fetch", command=self.start_fetch, style="Accent.TButton")
        self.run_fetch_button.grid(row=0, column=4, rowspan=3, sticky="ns", padx=20, pady=5)

        log_frame = ttk.LabelFrame(parent, text="Logs", padding=10)
        log_frame.grid(row=1, column=0, sticky="nsew")
        self.log_text = scrolledtext.ScrolledText(log_frame, wrap=tk.WORD, bg="#222222", fg="#DDDDDD", font=("Consolas", 9))
        self.log_text.pack(fill=tk.BOTH, expand=True)
        self.log_text.configure(state='disabled')

    def fetch_all_ords_tickers(self):
        self.log_text.config(state='normal')
        self.log_text.delete(1.0, tk.END)
        self.log_text.insert(tk.END, "Fetching All Ordinaries tickers from Wikipedia...\n")
        self.log_text.config(state='disabled')
        
        def do_fetch():
            try:
                url = 'https://en.wikipedia.org/wiki/S%26P/ASX_All_Ordinaries'
                tables = pd.read_html(url)
                # Usually the first table is the one we want
                df = tables[0]
                
                # Find the column with the tickers. It's often named 'Code' or 'ASX code'.
                # Let's be flexible.
                ticker_col = None
                for col in df.columns:
                    if 'code' in col.lower():
                        ticker_col = col
                        break
                
                if ticker_col is None:
                    self.log_queue.put("Could not find ticker column in the table.")
                    return

                tickers = df[ticker_col].tolist()
                
                # Clean up tickers (remove any extra text if necessary)
                cleaned_tickers = [str(t).strip().upper() for t in tickers]
                
                filename = f"all_ords_tickers_Current_{datetime.now().strftime('%Y-%m-%d')}.txt"
                with open(filename, 'w', encoding='utf-8') as f:
                    f.write("Symbol\n")
                    for ticker in cleaned_tickers:
                        f.write(f"{ticker}\n")
                
                self.log_queue.put(f"Successfully fetched {len(cleaned_tickers)} tickers.")
                self.log_queue.put(f"Saved to {filename}")
                
                # Update the UI
                self.ticker_file_var.set(filename)
                base_name = filename.split('/')[-1].split('\\')[-1]
                name_without_ext = base_name.rsplit('.', 1)[0]
                current_date = datetime.now().strftime('%Y-%m-%d')
                new_output_filename = f"{name_without_ext}_{current_date}.json"
                self.output_file_var.set(new_output_filename)

            except Exception as e:
                self.log_queue.put(f"Failed to fetch All Ords tickers: {e}")

        threading.Thread(target=do_fetch, daemon=True).start()

    def browse_ticker_file(self):
        filename = filedialog.askopenfilename(title="Select Ticker File", filetypes=(("Text files", "*.txt"), ("All files", "*.* sviluppo")))
        if filename:
            self.ticker_file_var.set(filename)
            base_name = filename.split('/')[-1].split('\\')[-1]
            name_without_ext = base_name.rsplit('.', 1)[0]
            current_date = datetime.now().strftime('%Y-%m-%d')
            new_output_filename = f"{name_without_ext}_{current_date}.json"
            self.output_file_var.set(new_output_filename)

    def start_fetch(self):
        if not self.ticker_file_var.get():
            messagebox.showerror("Error", "Please select a ticker file.")
            return

        self.run_fetch_button.config(state=tk.DISABLED)
        self.log_text.config(state='normal'); self.log_text.delete(1.0, tk.END); self.log_text.config(state='disabled')

        script_dir = os.path.dirname(os.path.abspath(__file__))
        command = [
            sys.executable, "-u", 
            os.path.join(script_dir, "data_fetcher.py"), 
            self.ticker_file_var.get(), 
            "-y", str(self.years_var.get()), 
            "-o", self.output_file_var.get()]
        threading.Thread(target=self.run_process, args=(command, script_dir), daemon=True).start()

    def run_process(self, command, script_dir):
        try:
            process = subprocess.Popen(command, stdout=subprocess.PIPE, stderr=subprocess.STDOUT, text=True, encoding='utf-8', creationflags=subprocess.CREATE_NO_WINDOW, cwd=script_dir)
            for line in iter(process.stdout.readline, ''): self.log_queue.put(line)
            process.stdout.close(); process.wait()
        except Exception as e:
            self.log_queue.put(f"\n--- FATAL ERROR ---\n{str(e)}")
        finally:
            self.log_queue.put(None)

    def show_filter_log(self):
        if self.filter_log_window and self.filter_log_window.winfo_exists():
            self.filter_log_window.lift()
            return

        self.filter_log_window = tk.Toplevel(self.root)
        self.filter_log_window.title("Filter Log")
        self.filter_log_window.geometry("800x600")

        log_frame = ttk.LabelFrame(self.filter_log_window, text="Live Filter Log", padding=10)
        log_frame.pack(fill=tk.BOTH, expand=True, padx=10, pady=10)
        
        self.filter_log_text = scrolledtext.ScrolledText(log_frame, wrap=tk.WORD, bg="#222222", fg="#DDDDDD", font=("Consolas", 9))
        self.filter_log_text.pack(fill=tk.BOTH, expand=True)
        self.filter_log_text.configure(state='disabled')

        # Immediately populate with any existing log messages
        self.update_filter_log()

    def update_filter_log(self):
        """Checks the queue and updates the log window if it's open."""
        if self.filter_log_window and self.filter_log_window.winfo_exists():
            try:
                while True:
                    log_line = self.filter_log_queue.get_nowait()
                    self.filter_log_text.config(state='normal')
                    self.filter_log_text.insert(tk.END, log_line + '\n')
                    self.filter_log_text.see(tk.END)
                    self.filter_log_text.config(state='disabled')
            except queue.Empty:
                pass # No new messages

    def _create_filter_widgets(self, parent):
        main_paned_window = ttk.PanedWindow(parent, orient=tk.HORIZONTAL)
        main_paned_window.pack(fill=tk.BOTH, expand=True)

        controls_frame = ttk.Frame(main_paned_window, padding=10)
        main_paned_window.add(controls_frame, weight=1)
        results_frame = ttk.Frame(main_paned_window, padding=10)
        main_paned_window.add(results_frame, weight=4)

        file_frame = ttk.LabelFrame(controls_frame, text="Stock Data File", padding=10)
        file_frame.pack(fill=tk.X, pady=5)
        self.data_file_var = tk.StringVar(value=DEFAULT_DATA_FILE)
        self.data_file_entry = ttk.Entry(file_frame, textvariable=self.data_file_var, width=40, state='readonly')
        self.data_file_entry.pack(side=tk.LEFT, fill=tk.X, expand=True, padx=(0, 5))
        ttk.Button(file_frame, text="Load File", command=self.load_data_file).pack(side=tk.LEFT)

        params_frame = ttk.LabelFrame(controls_frame, text="Filter Parameters", padding=10)
        params_frame.pack(fill=tk.X, pady=5)
        params_frame.columnconfigure(1, weight=1); params_frame.columnconfigure(3, weight=1)

        self.min_cap_var = tk.DoubleVar(value=0.0)
        self.max_cap_var = tk.DoubleVar(value=0.0)
        self.volume_mult_var = tk.DoubleVar(value=DEFAULT_VOLUME_MULTIPLIER)
        self.price_avg_weeks_var = tk.IntVar(value=DEFAULT_PRICE_AVG_WEEKS)
        self.lookback_weeks_var = tk.IntVar(value=1)
        self.ma_short_var = tk.IntVar(value=DEFAULT_MA_SHORT)
        self.ma_intermediate_var = tk.IntVar(value=DEFAULT_MA_INTERMEDIATE)
        self.ma_medium_var = tk.IntVar(value=DEFAULT_MA_MEDIUM)
        self.ma_long_var = tk.IntVar(value=DEFAULT_MA_LONG)

        ttk.Label(params_frame, text="Min Market Cap (M):").grid(row=0, column=0, sticky=tk.W, padx=5, pady=5)
        ttk.Entry(params_frame, textvariable=self.min_cap_var, width=12).grid(row=0, column=1, sticky=tk.W, padx=5, pady=5)
        ttk.Label(params_frame, text="Max Market Cap (M):").grid(row=0, column=2, sticky=tk.W, padx=5, pady=5)
        ttk.Entry(params_frame, textvariable=self.max_cap_var, width=12).grid(row=0, column=3, sticky=tk.W, padx=5, pady=5)
        ttk.Label(params_frame, text="Volume Multiplier:").grid(row=1, column=0, sticky=tk.W, padx=5, pady=5)
        ttk.Entry(params_frame, textvariable=self.volume_mult_var, width=12).grid(row=1, column=1, sticky=tk.W, padx=5, pady=5)
        ttk.Label(params_frame, text="Price Avg Weeks:").grid(row=1, column=2, sticky=tk.W, padx=5, pady=5)
        ttk.Entry(params_frame, textvariable=self.price_avg_weeks_var, width=12).grid(row=1, column=3, sticky=tk.W, padx=5, pady=5)

        ttk.Label(params_frame, text="Lookback Weeks:").grid(row=2, column=0, sticky=tk.W, padx=5, pady=5)
        ttk.Entry(params_frame, textvariable=self.lookback_weeks_var, width=12).grid(row=2, column=1, sticky=tk.W, padx=5, pady=5)

        ma_frame = ttk.LabelFrame(controls_frame, text="Moving Averages (Weeks)", padding=10)
        ma_frame.pack(fill=tk.X, pady=5)
        ma_frame.columnconfigure(1, weight=1); ma_frame.columnconfigure(3, weight=1)
        ttk.Label(ma_frame, text="Short:").grid(row=0, column=0, sticky=tk.W, padx=5, pady=5)
        ttk.Entry(ma_frame, textvariable=self.ma_short_var, width=10).grid(row=0, column=1, sticky=tk.W, padx=5, pady=5)
        ttk.Label(ma_frame, text="Intermediate:").grid(row=0, column=2, sticky=tk.W, padx=5, pady=5)
        ttk.Entry(ma_frame, textvariable=self.ma_intermediate_var, width=10).grid(row=0, column=3, sticky=tk.W, padx=5, pady=5)
        ttk.Label(ma_frame, text="Medium:").grid(row=1, column=0, sticky=tk.W, padx=5, pady=5)
        ttk.Entry(ma_frame, textvariable=self.ma_medium_var, width=10).grid(row=1, column=1, sticky=tk.W, padx=5, pady=5)
        ttk.Label(ma_frame, text="Long:").grid(row=1, column=2, sticky=tk.W, padx=5, pady=5)
        ttk.Entry(ma_frame, textvariable=self.ma_long_var, width=10).grid(row=1, column=3, sticky=tk.W, padx=5, pady=5)

        action_frame = ttk.Frame(controls_frame)
        action_frame.pack(fill=tk.X, pady=20)
        action_frame.columnconfigure(0, weight=1)
        action_frame.columnconfigure(1, weight=1)
        action_frame.columnconfigure(2, weight=1)
        action_frame.columnconfigure(3, weight=1)

        self.run_filter_button = ttk.Button(action_frame, text="Apply Filter", command=self.start_filter, style="Accent.TButton")
        self.run_filter_button.grid(row=0, column=1, padx=5, sticky=tk.EW)

        self.load_button = ttk.Button(action_frame, text="Load Filter", command=self.load_filter_settings)
        self.load_button.grid(row=0, column=0, padx=5, sticky=tk.EW)

        self.save_button = ttk.Button(action_frame, text="Save Filter", command=self.save_filter_settings)
        self.save_button.grid(row=0, column=2, padx=5, sticky=tk.EW)
        
        self.show_log_button = ttk.Button(action_frame, text="Show Log", command=self.show_filter_log)
        self.show_log_button.grid(row=0, column=3, padx=5, sticky=tk.EW)
        
        results_frame.rowconfigure(0, weight=1); results_frame.columnconfigure(0, weight=1)
        self.columns = ("Ticker", "Date", "Close", "Market Cap", "AvgVol", "VolRatio")
        self.tree = ttk.Treeview(results_frame, columns=self.columns, show="headings")
        
        header_texts = {
            "Market Cap": "Mkt Cap", 
            "AvgVol": "Avg Vol", 
            "VolRatio": "Vol Ratio"
        }
        col_widths = {"Ticker": 100, "Date": 100, "Close": 80, "Market Cap": 100, "AvgVol": 110, "VolRatio": 90}

        for col in self.columns: 
            self.tree.heading(col, text=header_texts.get(col, col))
            self.tree.column(col, width=col_widths.get(col, 80), anchor=tk.CENTER)

        self.tree.tag_configure("hyperlink", foreground="#007bff", font=('TkDefaultFont', 10, 'underline'))
        self.tree.bind("<Button-1>", self.on_tree_click)
        vsb = ttk.Scrollbar(results_frame, orient="vertical", command=self.tree.yview)
        hsb = ttk.Scrollbar(results_frame, orient="horizontal", command=self.tree.xview)
        self.tree.configure(yscrollcommand=vsb.set, xscrollcommand=hsb.set)
        self.tree.grid(row=0, column=0, sticky="nsew"); vsb.grid(row=0, column=1, sticky="ns"); hsb.grid(row=1, column=0, sticky="ew")

        status_frame = ttk.Frame(results_frame); status_frame.grid(row=2, column=0, columnspan=2, sticky="ew", pady=(10, 0))
        ttk.Label(status_frame, textvariable=self.status_var, anchor=tk.W).pack(fill=tk.X)

    def save_filter_settings(self):
        settings = {
            'min_market_cap': self.min_cap_var.get(),
            'max_market_cap': self.max_cap_var.get(),
            'volume_multiplier': self.volume_mult_var.get(),
            'price_avg_weeks': self.price_avg_weeks_var.get(),
            'ma_short': self.ma_short_var.get(),
            'ma_intermediate': self.ma_intermediate_var.get(),
            'ma_medium': self.ma_medium_var.get(),
            'ma_long': self.ma_long_var.get(),
        }
        filename = filedialog.asksaveasfilename(
            title="Save Filter Settings",
            defaultextension=".json",
            filetypes=[("JSON files", "*.json")]
        )
        if filename:
            try:
                with open(filename, 'w', encoding='utf-8') as f:
                    json.dump(settings, f, indent=4)
                self.status_var.set(f"Filter settings saved to {filename}")
            except Exception as e:
                messagebox.showerror("Save Error", f"Failed to save settings file: {e}")

    def load_filter_settings(self):
        filename = filedialog.askopenfilename(
            title="Load Filter Settings",
            filetypes=[("JSON files", "*.json")]
        )
        if filename:
            try:
                with open(filename, 'r', encoding='utf-8') as f:
                    settings = json.load(f)
                
                self.min_cap_var.set(settings.get('min_market_cap', 0.0))
                self.max_cap_var.set(settings.get('max_market_cap', 0.0))
                self.volume_mult_var.set(settings.get('volume_multiplier', DEFAULT_VOLUME_MULTIPLIER))
                self.price_avg_weeks_var.set(settings.get('price_avg_weeks', DEFAULT_PRICE_AVG_WEEKS))
                self.lookback_weeks_var.set(settings.get('lookback_weeks', 1))
                self.ma_short_var.set(settings.get('ma_short', DEFAULT_MA_SHORT))
                self.ma_intermediate_var.set(settings.get('ma_intermediate', DEFAULT_MA_INTERMEDIATE))
                self.ma_medium_var.set(settings.get('ma_medium', DEFAULT_MA_MEDIUM))
                self.ma_long_var.set(settings.get('ma_long', DEFAULT_MA_LONG))

                self.status_var.set(f"Loaded filter settings from {filename}")
            except Exception as e:
                messagebox.showerror("Load Error", f"Failed to load or parse settings file: {e}")

    def load_data_on_startup(self):
        try: self.load_data(DEFAULT_DATA_FILE, silent=True)
        except Exception: self.status_var.set(f"Ready. Load '{DEFAULT_DATA_FILE}' or fetch data.")

    def load_data_file(self):
        filename = filedialog.askopenfilename(title="Select Stock Data File", filetypes=(("JSON files", "*.json"), ("All files", "*.* sviluppo")))
        if filename: self.load_data(filename)

    def load_data(self, filename, silent=False):
        try:
            self.status_var.set(f"Loading data from {filename}...")
            self.root.update_idletasks()
            with open(filename, 'r', encoding='utf-8') as f: data = json.load(f)
            self.stock_data = data.get('stocks', {})
            self.data_file_var.set(filename)
            fetch_date = data.get('metadata', {}).get('fetch_date_utc', 'N/A')
            if fetch_date != 'N/A': fetch_date = datetime.fromisoformat(fetch_date).strftime('%Y-%m-%d %H:%M UTC')
            self.status_var.set(f"Loaded {len(self.stock_data)} stocks. Data from: {fetch_date}. Ready to filter.")
            self.tree.delete(*self.tree.get_children())
        except FileNotFoundError:
            if not silent: messagebox.showerror("File Not Found", f"The file '{filename}' was not found.")
            self.status_var.set("File not found. Please load or fetch data.")
        except Exception as e:
            if not silent: messagebox.showerror("Load Error", f"Failed to load or parse file: {e}")
            self.status_var.set("Error loading file.")

    def on_tree_click(self, event):
        if self.tree.identify_region(event.x, event.y) == "cell" and self.tree.identify_column(event.x) == "#1":
            item_id = self.tree.identify_row(event.y)
            if item_id:
                raw_ticker = self.tree.item(item_id, 'values')[0]
                # TradingView uses a different format for some exchanges, e.g., ASX:TICKER for .AX
                if raw_ticker.endswith(".AX"):
                    tv_symbol = f"ASX:{raw_ticker[:-3]}"
                else:
                    tv_symbol = raw_ticker # Works for US stocks
                url = f"https://www.tradingview.com/chart/?symbol={tv_symbol}"
                webbrowser.open_new_tab(url)

    def start_filter(self):
        if not self.stock_data: messagebox.showwarning("No Data", "Please load a stock data file first."); return
        if self.filter_thread and self.filter_thread.is_alive(): messagebox.showwarning("In Progress", "A filter is already running."); return

        try:
            config = {
                'volume_multiplier': self.volume_mult_var.get(), 
                'price_avg_weeks': self.price_avg_weeks_var.get(), 
                'min_market_cap': self.min_cap_var.get(), 
                'max_market_cap': self.max_cap_var.get(), 
                'avg_volume_weeks': DEFAULT_AVG_VOLUME_WEEKS, 
                'lookback_weeks': self.lookback_weeks_var.get(),
                'ma_periods': {"short": self.ma_short_var.get(), "intermediate": self.ma_intermediate_var.get(), "medium": self.ma_medium_var.get(), "long": self.ma_long_var.get()}
            }
            self.run_filter_button.config(state=tk.DISABLED)
            self.status_var.set("Status: Filtering...")
            self.tree.delete(*self.tree.get_children())
            while not self.results_queue.empty(): self.results_queue.get_nowait()
            while not self.progress_queue.empty(): self.progress_queue.get_nowait()
            self.filter_thread = threading.Thread(target=run_filter_thread, args=(config, self.stock_data, self.results_queue, self.progress_queue), daemon=True).start()
        except ValueError: messagebox.showerror("Input Error", "Please enter valid numbers."); self.run_filter_button.config(state=tk.NORMAL)

    def check_queues(self):
        try:
            while True:
                line = self.log_queue.get_nowait()
                if line is None: 
                    self.run_fetch_button.config(state=tk.NORMAL)
                    self.load_data(self.output_file_var.get(), silent=True)
                    self.notebook.select(self.filter_tab)
                    break
                self.log_text.config(state='normal'); self.log_text.insert(tk.END, line); self.log_text.see(tk.END); self.log_text.config(state='disabled')
        except queue.Empty: pass

        try:
            while True:
                message = self.progress_queue.get_nowait()
                if message == "DONE": self.run_filter_button.config(state=tk.NORMAL)
                self.status_var.set(message)
        except queue.Empty: pass

        try:
            while True: self.add_result_to_treeview(self.results_queue.get_nowait())
        except queue.Empty: pass

        try:
            while True:
                log_line = self.filter_log_queue.get_nowait()
                if self.filter_log_window and self.filter_log_window.winfo_exists():
                    self.filter_log_text.config(state='normal')
                    self.filter_log_text.insert(tk.END, log_line + '\n')
                    self.filter_log_text.see(tk.END)
                    self.filter_log_text.config(state='disabled')
        except queue.Empty: pass

        self.root.after(100, self.check_queues)

    def add_result_to_treeview(self, item):
        values = (item['ticker'], item['date'], f"{item['close_price']:.2f}", self._format_market_cap(item.get('market_cap')), f"{item['avg_volume']:,.0f}", f"{item['volume_ratio']:.2f}x")
        self.tree.insert("", tk.END, values=values, tags=("hyperlink",))

    def _format_market_cap(self, mc):
        if mc is None: return "N/A"
        if mc >= 1_000_000_000: return f"{mc / 1_000_000_000:.2f}B"
        if mc >= 1_000_000: return f"{mc / 1_000_000:.2f}M"
        return f"{mc / 1_000:.0f}"

if __name__ == "__main__":
    root = tk.Tk()
    app = MoneymakerProAlphaApp(root)
    root.mainloop()
