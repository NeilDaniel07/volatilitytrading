import time
from datetime import datetime, timedelta
from typing import List, Dict, Tuple, Optional
import pandas as pd
import requests
import config as config

class TradingDataCollector:
    BASE_STOCK = "https://data.alpaca.markets/v2/stocks"
    BASE_OPTIONS = "https://data.alpaca.markets/v1beta1/options"

    def __init__(self, screener_df, date):
        if "Ticker" not in screener_df.columns:
            raise ValueError("Input DataFrame must contain a 'Ticker' column.")

        self.df = screener_df.reset_index(drop=True)
        self.date = date
        self.rate_limit_delay = 0.25
        self.max_retries = 8
        self.max_wait_time = 60
        self.wiggle = 10
        self.hdr = config.header

    def run(self):
        rows: List[Dict] = []

        for tk in self.df["Ticker"]:
            try:
                snap = self.collect_ticker_information(tk)
                if snap:
                    rows.append(snap)
            except Exception as e:
                print(f"[{tk}] skipped - {e}")

            time.sleep(self.rate_limit_delay)

        if not rows:
            return self.df

        add_df = pd.DataFrame(rows)
        merged = self.df.merge(add_df, how="left", left_on="Ticker", right_on="ticker").drop(columns=["ticker"])

        required = ["Stock Price", "Front Expiry", "Back Expiry", "Strike", "Front Symbol", "Back Symbol"]
        merged = merged.dropna(subset=required)

        self.df = merged
        return self.df
    
    def collect_ticker_information(self, ticker):
        price = self.latest_trade_price(ticker)
        
        if price is None:
            return None

        front_exp, back_exp, front_options, back_options = self.get_expiry_dates(ticker, price)
       
        if not front_exp or not back_exp:
            return None

        if not front_options or not back_options:
            return None

        result = self.at_the_money_common_strike(front_options, back_options, price)
        
        if result is None:
            return None
        
        strike, front_sym, back_sym = result

        return {
            "ticker": ticker,
            "Stock Price": price,
            "Front Expiry": front_exp,
            "Back Expiry": back_exp,
            "Strike": strike,
            "Front Symbol": front_sym,
            "Back Symbol": back_sym,
        }

    def latest_trade_price(self, ticker):
        url = f"{self.BASE_STOCK}/{ticker}/trades/latest"
        data = self.getURLData(url)
        if not data or "trade" not in data:
            return None
        return data["trade"]["p"]

    def get_expiry_dates(self, ticker, price):
        today = self.date.date()
        initialOffset = (4 - today.weekday() + 7) % 7
        firstFriday = today + timedelta(days=initialOffset)

        frontDate = None
        frontOptions = None

        for i in range(5):
            candidate = firstFriday + timedelta(weeks=i)
            candStr = candidate.strftime("%Y-%m-%d")
            options = self.gather_options(ticker, candStr, price)
            if options:
                frontDate, frontOptions = candidate, options
                break

        if frontDate is None:
            return None, None, None, None
        
        backDate = None
        backOptions = None

        offsets = [28, 27] #Optimize later

        for offset in offsets:
            candidate = frontDate + timedelta(days=offset)
            candStr = candidate.strftime("%Y-%m-%d")
            options = self.gather_options(ticker, candStr, price)
            if options:
                backDate, backOptions = candidate, options
                break

        if backDate is None:
            return None, None, None, None

        return (frontDate.strftime("%Y-%m-%d"), backDate.strftime("%Y-%m-%d"), frontOptions, backOptions)

    def gather_options(self, ticker, expiry, price):
        lo = max(price - self.wiggle, 0)
        hi = price + self.wiggle
        url = (f"{self.BASE_OPTIONS}/snapshots/{ticker}?limit=100&type=call&feed=indicative&expiration_date={expiry}&strike_price_gte={lo}&strike_price_lte={hi}")
        
        js = self.getURLData(url)
       
        snaps = js.get("snapshots", {}) if js else {}
        return list(snaps.keys())

    def at_the_money_common_strike(self, front_syms, back_syms, spot):

        def get_price(sym):
            return int(sym[-8:]) / 1000

        front_map = {get_price(s): s for s in front_syms}
        back_map  = {get_price(s): s for s in back_syms}

        common = sorted(set(front_map) & set(back_map), key=lambda k: abs(k - spot))

        if not common:
            return None

        k = common[0]
        return k, front_map[k], back_map[k] #Returned as a tuple

    def getURLData(self, url):
        for attempt in range(self.max_retries):
            try:
                r = requests.get(url, headers=self.hdr, timeout=10)

                if r.status_code == 404:
                    return None

                if r.status_code == 429:
                    wait = min(self.max_wait_time, self.rate_limit_delay * (2 ** attempt))
                    print(f"[429 Error] waiting {wait} seconds - {url}")
                    time.sleep(wait)
                    continue

                r.raise_for_status()
                return r.json()

            except requests.RequestException as e:
                if attempt == self.max_retries - 1:
                    print(f"[ERROR] {url} - {e}")
                    return None
                time.sleep(self.rate_limit_delay * (2 ** attempt))

        return None

def main():
    screener_df = pd.read_csv("EarningsScanning.csv")
    snapshot = TradingDataCollector(screener_df, datetime.now())
    enriched_df = snapshot.run()
    enriched_df.to_csv("alpaca_snapshot.csv", index=False)
    print(enriched_df.head())

main()