import json
from typing import List
from bs4 import BeautifulSoup
import tkinter as tk
from tkinter import ttk
from tkcalendar import DateEntry
import requests
import pandas as pd
import yfinance as yf
from datetime import datetime, timedelta
import numpy as np
from scipy.interpolate import interp1d
import warnings
warnings.filterwarnings("ignore", category=RuntimeWarning)

class SimpleEarningsApp:
    def __init__(self, root):
        self.root = root
        self.root.title("Earnings Scanner")
        self.df = pd.read_csv('NasdaqAndNYSETradedStocks.csv')

        frame = ttk.Frame(self.root, padding=10)
        frame.pack(fill="x", padx=10, pady=10)

        ttk.Label(frame, text="Earnings Date:").pack(side="left", padx=(0, 5))
        self.date_entry = DateEntry(frame, width=12, date_pattern='yyyy-MM-dd')
        self.date_entry.pack(side="left", padx=(0, 5))

        scan_btn = ttk.Button(frame, text="Scan Earnings", command=self.on_scan_earnings)
        scan_btn.pack(side="left")

        result_frame = ttk.Frame(self.root, padding=10)
        result_frame.pack(fill="both", expand=True, padx=10, pady=10)

        self.tree = ttk.Treeview(result_frame, columns=("Ticker", "Avg Volume", "IV30/RV30", "TS Slope", "Expected Move"), show="headings")
        self.tree.heading("Ticker", text="Ticker")
        self.tree.heading("Avg Volume", text="Avg Volume")
        self.tree.heading("IV30/RV30", text="IV30/RV30")
        self.tree.heading("TS Slope", text="TS Slope")
        self.tree.heading("Expected Move", text="Expected Move")
        self.tree.pack(fill="both", expand=True)

        self.status_label = ttk.Label(self.root, text="Query Complete", foreground="green")
        self.status_label.pack(side="bottom", fill="x", padx=10, pady=10)

        threshold_frame = ttk.Frame(self.root, padding=10)
        threshold_frame.pack(fill="x", padx=10, pady=10)

        ttk.Label(threshold_frame, text="Avg Volume Threshold:").pack(side="left", padx=(0, 5))
        self.avg_volume_entry = ttk.Entry(threshold_frame, width=10)
        self.avg_volume_entry.insert(0, '1500000')
        self.avg_volume_entry.pack(side="left", padx=(0, 5))

        ttk.Label(threshold_frame, text="IV30/RV30 Threshold:").pack(side="left", padx=(0, 5))
        self.iv30_rv30_entry = ttk.Entry(threshold_frame, width=10)
        self.iv30_rv30_entry.insert(0, '1.25')
        self.iv30_rv30_entry.pack(side="left", padx=(0, 5))

        ttk.Label(threshold_frame, text="TS Slope Threshold:").pack(side="left", padx=(0, 5))
        self.ts_slope_entry = ttk.Entry(threshold_frame, width=10)
        self.ts_slope_entry.insert(0, '-0.00406')
        self.ts_slope_entry.pack(side="left", padx=(0, 5))

    def add_stock_to_tree(self, stock_info, passes_threshold):
        tag = "green" if passes_threshold else "red"
        self.tree.insert("", "end", values=(
            stock_info['ticker'],
            f"{stock_info['avg_volume']:,}",
            f"{stock_info['iv30_rv30']:.2f}",
            f"{stock_info['ts_slope_0_45']:.5f}",
            f"{stock_info['expected_move']}"
        ), tags=(tag,))
        
        self.tree.tag_configure("green", background="lightgreen")
        self.tree.tag_configure("red", background="lightcoral")


    def on_scan_earnings(self):
        selected_date = self.date_entry.get()
        self.status_label.config(text="Scanning Earnings", foreground="red")
        self.root.update()
        self.scan_earnings_callback(selected_date)

    def tradedOnNYSEOrNasdaq(self, stock):
        if stock in self.df['Ticker'].values:
            return True
        return False

    def passesThresholds(self, stockInformation):
        avg_volume_threshold = float(self.avg_volume_entry.get())
        iv30_rv30_threshold = float(self.iv30_rv30_entry.get())
        ts_slope_threshold = float(self.ts_slope_entry.get())

        return (stockInformation['avg_volume'] >= avg_volume_threshold) and (stockInformation['iv30_rv30'] >= iv30_rv30_threshold) and (stockInformation['ts_slope_0_45'] <= ts_slope_threshold)

    def scan_earnings_callback(self, date_str):
        print(f"Scan Earnings triggered for date: {date_str}")
        allStocksWithEarnings = self.fetch_earnings_data(date_str)
        filteredStocks = []
        for stock in allStocksWithEarnings:
            if(self.tradedOnNYSEOrNasdaq(stock)):
                filteredStocks.append(stock)
                self.tree.delete(*self.tree.get_children())
        for stock in filteredStocks:
            computedData = self.compute_recommendation(stock)
            if isinstance(computedData, dict):
                computedData['ticker'] = stock
                self.add_stock_to_tree(computedData, self.passesThresholds(computedData))
        self.status_label.config(text="Query Complete", foreground="green")


    def fetch_earnings_data(self, date: str) -> List[str]:
        url = "https://www.investing.com/earnings-calendar/Service/getCalendarFilteredData"
        headers = {
            'User-Agent': 'Mozilla/5.0',
            'X-Requested-With': 'XMLHttpRequest',
            'Content-Type': 'application/x-www-form-urlencoded',
            'Referer': 'https://www.investing.com/earnings-calendar/'
        }
        payload = {
            'country[]': '5',  # Country code for the United States
            'dateFrom': date,
            'dateTo': date,
            'currentTab': 'custom',
            'limit_from': 0
        }

        try:
            response = requests.post(url, headers=headers, data=payload)
            response.raise_for_status()

            data = response.json()
            soup = BeautifulSoup(data['data'], 'html.parser')
            rows = soup.find_all('tr')

            earnings_stocks = []

            for row in rows:
                company_name_span = row.find('span', class_='earnCalCompanyName')
                if not company_name_span:
                    continue

                try:
                    ticker = row.find('a', class_='bold').text.strip().upper()
                    earnings_stocks.append(ticker)
                except Exception as e:
                    print(f"Error parsing row: {e}")
                    continue

            return earnings_stocks

        except requests.RequestException as e:
            print(f"HTTP Request failed: {e}")
            return []
        except json.JSONDecodeError as e:
            print(f"JSON decoding failed: {e}")
            return []
    
    def filter_dates(self, dates):
        today = datetime.today().date()
        cutoff_date = today + timedelta(days=45)
        
        sorted_dates = sorted(datetime.strptime(date, "%Y-%m-%d").date() for date in dates)

        arr = []
        for i, date in enumerate(sorted_dates):
            if date >= cutoff_date:
                arr = [d.strftime("%Y-%m-%d") for d in sorted_dates[:i+1]]  
                break
        
        if len(arr) > 0:
            if arr[0] == today.strftime("%Y-%m-%d"):
                return arr[1:]
            return arr

        raise ValueError("No date 45 days or more in the future found.")

    def yang_zhang(self, price_data, window=30, trading_periods=252, return_last_only=True):
        log_ho = (price_data['High'] / price_data['Open']).apply(np.log)
        log_lo = (price_data['Low'] / price_data['Open']).apply(np.log)
        log_co = (price_data['Close'] / price_data['Open']).apply(np.log)
        
        log_oc = (price_data['Open'] / price_data['Close'].shift(1)).apply(np.log)
        log_oc_sq = log_oc**2
        
        log_cc = (price_data['Close'] / price_data['Close'].shift(1)).apply(np.log)
        log_cc_sq = log_cc**2
        
        rs = log_ho * (log_ho - log_co) + log_lo * (log_lo - log_co)
        
        close_vol = log_cc_sq.rolling(
            window=window,
            center=False
        ).sum() * (1.0 / (window - 1.0))

        open_vol = log_oc_sq.rolling(
            window=window,
            center=False
        ).sum() * (1.0 / (window - 1.0))

        window_rs = rs.rolling(
            window=window,
            center=False
        ).sum() * (1.0 / (window - 1.0))

        k = 0.34 / (1.34 + ((window + 1) / (window - 1)) )
        result = (open_vol + k * close_vol + (1 - k) * window_rs).apply(np.sqrt) * np.sqrt(trading_periods)

        if return_last_only:
            return result.iloc[-1]
        else:
            return result.dropna()
        

    def build_term_structure(self, days, ivs):
        days = np.array(days)
        ivs = np.array(ivs)

        sort_idx = days.argsort()
        days = days[sort_idx]
        ivs = ivs[sort_idx]


        spline = interp1d(days, ivs, kind='linear', fill_value="extrapolate")

        def term_spline(dte):
            if dte < days[0]:  
                return ivs[0]
            elif dte > days[-1]:
                return ivs[-1]
            else:  
                return float(spline(dte))

        return term_spline

    def get_current_price(self, ticker):
        todays_data = ticker.history(period='1d')
        return todays_data['Close'].iloc[0]

    
    def compute_recommendation(self, ticker):
        try:
            ticker = ticker.strip().upper()
            if not ticker:
                return "No stock symbol provided."
            
            try:
                stock = yf.Ticker(ticker)
                if len(stock.options) == 0:
                    raise KeyError()
            except KeyError:
                return f"Error: No options found for stock symbol '{ticker}'."
            
            exp_dates = list(stock.options)
            try:
                exp_dates = self.filter_dates(exp_dates)
            except:
                return "Error: Not enough option data."
            
            options_chains = {}
            for exp_date in exp_dates:
                options_chains[exp_date] = stock.option_chain(exp_date)
            
            try:
                underlying_price = self.get_current_price(stock)
                if underlying_price is None:
                    raise ValueError("No market price found.")
            except Exception:
                return "Error: Unable to retrieve underlying stock price."
            
            atm_iv = {}
            straddle = None 
            i = 0
            for exp_date, chain in options_chains.items():
                calls = chain.calls
                puts = chain.puts

                if calls.empty or puts.empty:
                    continue

                call_diffs = (calls['strike'] - underlying_price).abs()
                call_idx = call_diffs.idxmin()
                call_iv = calls.loc[call_idx, 'impliedVolatility']

                put_diffs = (puts['strike'] - underlying_price).abs()
                put_idx = put_diffs.idxmin()
                put_iv = puts.loc[put_idx, 'impliedVolatility']

                atm_iv_value = (call_iv + put_iv) / 2.0
                atm_iv[exp_date] = atm_iv_value

                if i == 0:
                    call_bid = calls.loc[call_idx, 'bid']
                    call_ask = calls.loc[call_idx, 'ask']
                    put_bid = puts.loc[put_idx, 'bid']
                    put_ask = puts.loc[put_idx, 'ask']
                    
                    if call_bid is not None and call_ask is not None:
                        call_mid = (call_bid + call_ask) / 2.0
                    else:
                        call_mid = None

                    if put_bid is not None and put_ask is not None:
                        put_mid = (put_bid + put_ask) / 2.0
                    else:
                        put_mid = None

                    if call_mid is not None and put_mid is not None:
                        straddle = (call_mid + put_mid)

                i += 1
            
            if not atm_iv:
                return "Error: Could not determine ATM IV for any expiration dates."
            
            today = datetime.today().date()
            dtes = []
            ivs = []
            for exp_date, iv in atm_iv.items():
                exp_date_obj = datetime.strptime(exp_date, "%Y-%m-%d").date()
                days_to_expiry = (exp_date_obj - today).days
                dtes.append(days_to_expiry)
                ivs.append(iv)
            
            term_spline = self.build_term_structure(dtes, ivs)
            
            ts_slope_0_45 = (term_spline(45) - term_spline(dtes[0])) / (45-dtes[0])
            
            price_history = stock.history(period='3mo')
            iv30_rv30 = term_spline(30) / self.yang_zhang(price_history)

            avg_volume = price_history['Volume'].rolling(30).mean().dropna().iloc[-1]

            expected_move = str(round(straddle / underlying_price * 100,2)) + "%" if straddle else None

            return {'avg_volume': avg_volume, 'iv30_rv30': iv30_rv30, 'ts_slope_0_45': ts_slope_0_45, 'expected_move': expected_move} #Check that they are in our desired range (see video)
        except Exception as e:
            raise Exception(f'Error occured processing')
            
def main():
    root = tk.Tk()
    app = SimpleEarningsApp(root)
    root.mainloop()

main()