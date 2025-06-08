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
        "Filled"
    ]


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

        toReturn = pd.DataFrame(self.cleanedRows, columns=self.OUTPUT_COLS)

        return toReturn

    def process_row(self, row):
        if row["Filled"] == "Yes":
            qty = int(row["Quantity"])
            row_dict = {
                "Order ID":     row["Order ID"],
                "Front Qty":    qty,
                "Back Qty":     qty,
                "Front Symbol": row["Front Symbol"],
                "Back Symbol":  row["Back Symbol"],
                "Limit Price":  row["Limit Price"],
                "Filled":       "Yes"
            }
            return row_dict

        order_id = row["Order ID"]
        orderData = self.get_order(order_id)
        if orderData is None:
            print(f"{order_id} Not Found")
            return None

        status = orderData["status"]

        if status == "filled":
            qty = int(row["Quantity"])
            return {
                "Order ID":     row["Order ID"],
                "Front Qty":    qty,
                "Back Qty":     qty,
                "Front Symbol": row["Front Symbol"],
                "Back Symbol":  row["Back Symbol"],
                "Limit Price":  row["Limit Price"],
                "Filled":       "Yes"
            }

        if status in ("canceled", "expired"):
            return None

        if status == "partially_filled":
            self.cancel_order(order_id)
            time.sleep(2) #Allow time for the order information to update
            updatedOrderData = self.get_order(order_id)
            if updatedOrderData is None:
                return None

            front_fill = 0
            back_fill = 0
            for leg in updatedOrderData["legs"]:
                if leg["symbol"] == row["Front Symbol"]:
                    front_fill = int(leg["filled_qty"])
                elif leg["symbol"] == row["Back Symbol"]:
                    back_fill = int(leg["filled_qty"])

            if front_fill == 0 and back_fill == 0:
                print(f"{order_id} Completely Unfilled")
                return None

            return {
                "Order ID":     row["Order ID"],
                "Front Qty":    front_fill,
                "Back Qty":     back_fill,
                "Front Symbol": row["Front Symbol"],
                "Back Symbol":  row["Back Symbol"],
                "Limit Price":  row["Limit Price"],
                "Filled":       "Yes"
            }

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
                    return {} if method == "DELETE" else None

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