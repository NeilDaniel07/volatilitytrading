import time, requests
from typing import List, Dict, Optional
import pandas as pd
import paperconfig

class CalendarOpenReconciler:
    PAPER_DOMAIN = "https://paper-api.alpaca.markets"

    def __init__(self, input_df):
        self.df = input_df
        self.rate_delay = 0.25
        self.max_retries = 8
        self.max_wait = 60
        self.hdr = paperconfig.header
        self.cleanedRows: List[Dict] = []

    def run(self):
        for _, row in self.df.iterrows():
            updated = self.process_row(row)
            if updated is not None:
                self.cleanedRows.append(updated)
            
            time.sleep(self.rate_delay)

        toReturn = pd.DataFrame(self.cleanedRows, columns=["Order ID", "Quantity", "Front Symbol", "Back Symbol", "Limit Price", "Filled"])
        return toReturn

    def process_row(self, row):
        if row["Filled"] == "Yes":
            return row.to_dict()

        order_id = row["Order ID"]
        orderData = self.get_order(order_id)
        if orderData is None:
            print(f"{order_id} Not Found")
            return None

        status = orderData["status"]

        if status == "filled":
            row["Filled"] = "Yes"
            return row.to_dict()

        if status in ("canceled", "expired"):
            return None

        if status == "partially_filled":
            self.cancel_order(order_id)
            updatedOrderData = self.get_order(order_id)
            if updatedOrderData is None:
                return None

            fill_counts = [int(leg["filled_qty"]) for leg in updatedOrderData["legs"]]
            qty_filled  = min(fill_counts)
            if qty_filled == 0:
                print(f"{order_id} Completely Unfilled.")
                return None

            row["Quantity"] = qty_filled
            return row.to_dict()

        try:
            self.cancel_order(order_id)
        except Exception as e:
            print(f"{order_id} Failed To Cancel: {e}")
        return None

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
                    return {}
            
                return r.json()

            except requests.RequestException as e:
                if attempt == self.max_retries - 1:
                    print(f"[ERROR] {url} - {e}")
                    return None
                time.sleep(self.rate_delay * (2 ** attempt))

        return None

def main():
    df = pd.read_csv("PlacedOrders.csv")
    recon = CalendarOpenReconciler(df)
    result_df = recon.run()
    result_df.to_csv("FilteredOrders.csv", index=False)
    print(result_df)

main()