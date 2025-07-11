import time, requests
from typing import List, Dict
import pandas as pd
import paperconfig

class CalendarOpenReconciler:
    PAPER_DOMAIN = "https://paper-api.alpaca.markets"
    OUTPUT_COLS = [
        "Order ID",
        "Front Qty",
        "Back Qty",
        "Front Symbol",
        "Back Symbol",
        "Limit Price",
    ]

    def __init__(self, input_df):
        self.df = input_df
        self.rate_delay = 0.25
        self.max_retries = 8
        self.max_wait = 60
        self.hdr = paperconfig.header
        self.cleanedRows: List[Dict] = []

    def extract_fills(self, orderJSON, frontSymbol, backSymbol):
        frontQuantity = 0
        backQuantity = 0
        frontPrice = 0.0
        backPrice =  0.0
        for leg in orderJSON.get("legs", []):
            if leg["symbol"] == frontSymbol:
                frontQuantity = int(leg["filled_qty"])
                frontPrice  = float(leg.get("filled_avg_price", 0))
            elif leg["symbol"] == backSymbol:
                backQuantity = int(leg["filled_qty"])
                backPrice  = float(leg.get("filled_avg_price", 0))
        return frontQuantity, backQuantity, frontPrice, backPrice
    
    def get_quote_data(self, symbol, field):
        url = f"https://data.alpaca.markets/v1beta1/options/quotes/latest?symbols={symbol}&feed=indicative"
        js = self.request("GET", url)
        if js:
            return js["quotes"][symbol][field]
        return None

    def run(self):
        for _, row in self.df.iterrows():
            updated = self.process_row(row)
            if updated is not None:
                self.cleanedRows.append(updated)
            
            time.sleep(self.rate_delay)

        toReturn = pd.DataFrame(self.cleanedRows, columns=self.OUTPUT_COLS)

        return toReturn

    def process_row(self, row):
        order_id = row["Order ID"]
        frontSymbol = row["Front Symbol"]
        backSymbol = row["Back Symbol"]

        orderData = self.get_order(order_id)
        if orderData is None:
            print(f"{order_id} Not Found")
            return None

        status = orderData["status"]

        if status in ("canceled", "expired"):
            return None

        if status == "partially_filled":
            self.cancel_order(order_id)
            time.sleep(2) #Allow time for the order information to update
            orderData = self.get_order(order_id) or orderData
        
        front_fill, back_fill, frontPrice, backPrice = self.extract_fills(orderData, frontSymbol, backSymbol)

        if front_fill == 0 and back_fill == 0:
            print(f"{order_id} Completely Unfilled")
            return None
            
        minQuantity = min(front_fill, back_fill)

        if front_fill > minQuantity:
            extra = front_fill - minQuantity
            currentAsk = self.get_quote_data(frontSymbol, "ap")
            if currentAsk is not None and currentAsk <= (0.75 * frontPrice):
                successful = self.dumpExcess(frontSymbol, extra, side="buy")
                if successful:
                    front_fill = minQuantity
        elif back_fill > minQuantity:
            extra = back_fill - minQuantity
            currentBid = self.get_quote_data(backSymbol, "bp")
            if currentBid is not None and currentBid >= (0.75 * backPrice):
                successful = self.dumpExcess(backSymbol, extra, side="sell")
                if successful:
                    back_fill = minQuantity
            
        return {
            "Order ID":     order_id,
            "Front Qty":    front_fill,
            "Back Qty":     back_fill,
            "Front Symbol": frontSymbol,
            "Back Symbol":  backSymbol,
            "Limit Price":  row["Limit Price"],
        }

    def dumpExcess(self, symbol, qty, side):
        body = {
            "symbol": symbol,
            "qty": str(qty),
            "side": side,
            "type": "market",
            "time_in_force": "day",
        }
        try:
            resp = self.request("POST", f"{self.PAPER_DOMAIN}/v2/orders", json=body)
            if resp is None:
                print("No Response For Flattening Order Request")
                return False
            return True
        except Exception as e:
            print(f"Removing {qty} Excess Contracts of Ticker {symbol} Options Failed: {e}")
            return False

    def get_order(self, order_id):
        url = f"{self.PAPER_DOMAIN}/v2/orders/{order_id}"
        return self.request("GET", url)

    def cancel_order(self, order_id):
        url = f"{self.PAPER_DOMAIN}/v2/orders/{order_id}"
        self.request("DELETE", url)

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

                if not r.content or r.status_code == 204:
                    return {} if method == "DELETE" else None

                return r.json()

            except requests.RequestException as e:
                if attempt == self.max_retries - 1:
                    print(f"[ERROR] {url} - {e}")
                    return None
                time.sleep(self.rate_delay * (2 ** attempt))

        return None

# def main():
#     df = pd.read_csv("PlacedOrders.csv")
#     recon = CalendarOpenReconciler(df)
#     result_df = recon.run()
#     result_df.to_csv("FilteredOrders.csv", index=False)
#     print(result_df)

# main()