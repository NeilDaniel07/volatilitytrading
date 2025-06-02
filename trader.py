import time
import pandas as pd
import requests
import config

class LongCalendarTrader:
    QUOTES = "https://data.alpaca.markets/v1beta1/options/quotes/latest?symbols={sym}&feed=indicative"

    def __init__(self, df, capital):
        self.df = df.copy()
        self.orig_capital = float(capital)
        self.capital_left = float(capital)
        self.rate_delay = 0.25
        self.max_retries = 8
        self.max_wait = 60
        self.hdr = config.header

        self.df.sort_values("TS Slope", inplace=True)

    def run(self):
        for _, row in self.df.iterrows():
            if self.capital_left < 10: 
                break

            result = self.execute_trade(row)
            if result:
                ticker, debit = result
                self.capital_left -= debit
                print(f"{ticker} Position Opened of Amount ${debit:,.2f}")

            time.sleep(self.rate_delay)

        print(f"\nRemaining capital: ${self.capital_left:,.2f}")

    def execute_trade(self, row):
        ticker = row["Ticker"]
        frontSymbol = row["Front Symbol"]
        backSymbol = row["Back Symbol"]

        frontBid = self.get_quote_data(frontSymbol, "bp")
        backAsk = self.get_quote_data(backSymbol, "ap")

        if frontBid is None or backAsk is None:
            return None

        debitPerContract = (backAsk - frontBid) * 100 #A contact consists of 100 shares, hence we multiply by 100.
        
        if debitPerContract <= 0: #NAVIN, please confirm this logic.
            return None

        target = 0.15 * self.orig_capital
        maximum = 0.20 * self.orig_capital

        idealContracts = max(1, round(target / debitPerContract))
        finalNumberContracts = min(idealContracts, int(maximum // debitPerContract), int(self.capital_left // debitPerContract))

        if finalNumberContracts == 0:
            return None
        
        totalCost = finalNumberContracts * debitPerContract
        return ticker, totalCost

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

def main():
    enriched = pd.read_csv("alpaca_snapshot.csv")
    trader = LongCalendarTrader(enriched, capital=10000)
    trader.run()

main()