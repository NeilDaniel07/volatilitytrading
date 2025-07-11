import time, requests
import pandas as pd
import paperconfig

class CalendarCloser:
    PAPER_DOMAIN = "https://paper-api.alpaca.markets"
    QUOTES = "https://data.alpaca.markets/v1beta1/options/quotes/latest?symbols={sym}&feed=indicative"

    def __init__(self,reconciled_df):
        self.df = reconciled_df.copy()
        self.rate_delay = 0.25
        self.max_retries = 8
        self.max_wait = 60
        self.hdr = paperconfig.header

    def run(self):
        for _, row in self.df.iterrows():
            self.close_position(row)
            time.sleep(self.rate_delay)

    def close_position(self, row):
        front = row["Front Symbol"]
        back = row["Back Symbol"]
        frontQuantity = int(row["Front Qty"])
        backQuantity = int(row["Back Qty"])

        minQuantity = min(frontQuantity, backQuantity)

        if minQuantity > 0:
            self.close_spread(front, back, minQuantity)

        frontExcess = frontQuantity - minQuantity
        backExcess = backQuantity - minQuantity

        if frontExcess > 0:
            self.close_single_leg(front, frontExcess, "buy", "ap")
 
        if backExcess > 0:
            self.close_single_leg(back, backExcess, "sell", "bp") 
    
    def close_spread(self, front, back, quantity):
        frontQuote = self.get_quote_data(front, "ap")
        backQuote = self.get_quote_data(back, "bp")

        if frontQuote is None and backQuote is None:
            print(f"Missing Quote Data")
            return
        
        if frontQuote is None:
            print(f"[{front}] Ask Missing - Closing The Back Leg")
            self.close_single_leg(back, quantity, "sell", "bp")
            return

        if backQuote is None:
            print(f"[{back}] Bid missing - Closing The Front Leg")
            self.close_single_leg(front, quantity, "buy", "ap")
            return

        debit = frontQuote - backQuote
        
        order = {
            "order_class": "mleg",
            "qty": str(quantity),
            "type": "limit",
            "limit_price": f"{debit:.2f}",
            "time_in_force": "day",
            "legs": [
                {
                    "symbol": back,
                    "ratio_qty": "1",
                    "side": "sell",
                    "position_intent": "sell_to_close"
                },
                {
                    "symbol": front,
                    "ratio_qty": "1",
                    "side": "buy",
                    "position_intent": "buy_to_close"
                }
            ]
        }
        
        try:
            self.submit_order(order)
            return

        except Exception as e:
            print(f"Order Failed: {e}")
            return
    
    def close_single_leg(self, symbol, qty, side, priceSide):
        quote = self.get_quote_data(symbol, priceSide)
        if quote is None:
            print(f"Quote Unavailable: {symbol} Skipped.")
            return
        
        order = {
            "symbol": symbol,
            "qty": str(qty),
            "side": side,           
            "type": "limit",
            "limit_price": f"{quote:.2f}",
            "time_in_force": "day",
        }
        
        try:
            self.submit_order(order)
            return

        except Exception as e:
            print(f"Order Failed: {e}")
            return
        
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

    def submit_order(self, body):
        url = f"{self.PAPER_DOMAIN}/v2/orders"
        return self.request("POST", url, json=body)

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

# def main():
#     rec_df = pd.read_csv("FilteredOrders.csv")
#     closer = CalendarCloser(rec_df)
#     closer.run()

# main()