import json, time, requests
from datetime import datetime
from pathlib import Path
from typing import List, Dict
import pandas as pd
from dateutil.tz import gettz
import paperconfig 

class CalendarOpener:
    PAPER_DOMAIN = "https://paper-api.alpaca.markets"
    QUOTES = "https://data.alpaca.markets/v1beta1/options/quotes/latest?symbols={sym}&feed=indicative"

    def __init__(self, enriched_df):
        self.df = enriched_df.copy()
        self.rate_delay = 0.25
        self.max_retries = 8
        self.max_wait = 60
        self.hdr = paperconfig.header

        self.df.sort_values("TS Slope", inplace=True)

        account = self.request("GET", f"{self.PAPER_DOMAIN}/v2/account")
        self.orig_capital = float(account["options_buying_power"])
        self.capital_left = self.orig_capital

        #Define an ouput CSV file
    
    def run(self):
        for _, row in self.df.iterrows():
            if self.capital_left < 10: 
                break

            self.execute_trade(row)
            time.sleep(30)
            time.sleep(self.rate_delay)
        
        #Save the resultant CSV file
        print(f"\nRemaining capital: ${self.capital_left:,.2f}")

    def execute_trade(self, row):
        ticker = row["Ticker"]
        frontSymbol = row["Front Symbol"]
        backSymbol = row["Back Symbol"]

        frontBid = self.get_quote_data(frontSymbol, "bp")
        backAsk = self.get_quote_data(backSymbol, "ap")

        if frontBid is None or backAsk is None:
            return

        debitPerContract = (backAsk - frontBid) * 100 #A contact consists of 100 shares, hence we multiply by 100.
        
        if debitPerContract <= 0:
            return

        target = 0.15 * self.orig_capital
        maximum = 0.20 * self.orig_capital

        idealContracts = max(1, round(target / debitPerContract))
        finalNumberContracts = min(idealContracts, int(maximum // debitPerContract), int(self.capital_left // debitPerContract))

        if finalNumberContracts == 0:
            return
        
        order = {
            "order_class": "mleg",
            "qty": str(finalNumberContracts),
            "type": "limit",
            "limit_price" : f"{debitPerContract/100:.2f}",
            "time_in_force": "day",
            "legs": [
                {
                    "symbol": frontSymbol,
                    "ratio_qty": "1",
                    "side": "sell",
                    "position_intent": "sell_to_open"
                },
                {
                    "symbol": backSymbol,
                    "ratio_qty": "1",
                    "side": "buy",
                    "position_intent": "buy_to_open"
                }
            ]
        }

        try:
            resp = self.request("POST", f"{self.PAPER_DOMAIN}/v2/orders", json=order)
            order_id = resp["id"]
            filled = False
            max_poll = 10
            while max_poll > 0:
                od = self.request("GET", f"{self.PAPER_DOMAIN}/v2/orders/{order_id}")
                if od["status"] == "filled":
                    filled = True
                    break
                time.sleep(1)
                max_poll -= 1

            debit = 0.0

            if filled:
                for leg in od["legs"]:
                    price = float(leg.get("filled_avg_price", 0))
                    qty = int(leg.get("filled_qty", 0))
                    if qty == 0:
                        continue
                    if leg["side"] == "buy":
                        debit += price * qty * 100
                    else:
                        debit -= price * qty * 100
            else:
                print("Order Not Yet Fulfilled. Defaulting To Estimated Debit.")
                debit = finalNumberContracts * debitPerContract

            self.capital_left -= debit
            print(f"{ticker} Position Opened of Amount ${debit:,.2f}")

        except Exception as e:
            print(f"{ticker} Order Failed - {e}")

    def get_quote_data(self, symbol, field):
        url = self.QUOTES.format(sym=symbol)
        for attempt in range(self.max_retries):
            try:
                r = requests.get(url, headers=self.hdr, timeout=10)

                if r.status_code == 404:
                    return None

                if r.status_code == 429:
                    wait = min(self.max_wait, self.rate_delay * (2 ** attempt))
                    print(f"[429 Error] waiting {wait} seconds - {url}")
                    time.sleep(wait)
                    continue
                
                r.raise_for_status()
                js = r.json()
                return js["quotes"][symbol][field]
            
            except Exception as e:
                if attempt == self.max_retries - 1:
                    print(f"[ERROR] {url} - {e}")
                    return None
                time.sleep(self.rate_delay * (2 ** attempt))

        return None
            
    def request(self, method, url, **kw):
        for attempt in range(self.max_retries):
            try:
                r = requests.request(method, url, headers=self.hdr, timeout=10, **kw)

                if r.status_code == 404:
                    return None

                if r.status_code == 429:
                    wait = min(self.max_wait, self.rate_delay * (2 ** attempt))
                    print(f"[429 Error] waiting {wait} seconds - {url}")
                    time.sleep(wait)
                    continue

                r.raise_for_status()
                return r.json()

            except requests.RequestException as e:
                if attempt == self.max_retries - 1:
                    print(f"[ERROR] {url} - {e}")
                    return None
                time.sleep(self.rate_delay * (2 ** attempt))

        return None
    
def main():
    df = pd.read_csv("alpaca_snapshot.csv")
    opener = CalendarOpener(df)
    opener.run()

main()
