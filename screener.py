import json
from typing import List
from bs4 import BeautifulSoup
import tkinter as tk
from tkinter import ttk
from tkcalendar import DateEntry
import requests

class SimpleEarningsApp:
    def __init__(self, root):
        self.root = root
        self.root.title("Earnings Scanner")

        frame = ttk.Frame(self.root, padding=10)
        frame.pack(fill="x", padx=10, pady=10)

        ttk.Label(frame, text="Earnings Date:").pack(side="left", padx=(0, 5))
        self.date_entry = DateEntry(frame, width=12, date_pattern='yyyy-MM-dd')
        self.date_entry.pack(side="left", padx=(0, 5))

        scan_btn = ttk.Button(frame, text="Scan Earnings", command=self.on_scan_earnings)
        scan_btn.pack(side="left")

    def on_scan_earnings(self):
        selected_date = self.date_entry.get()
        self.scan_earnings_callback(selected_date)

    def scan_earnings_callback(self, date_str):
        print(f"Scan Earnings triggered for date: {date_str}")
        allStocksWithEarnings = self.fetch_earnings_data(date_str)
        print(allStocksWithEarnings)
        #WE WILL NOW CHECK TO ENSURE THAT THEY ARE IN THE NYSE OR NASDAQ
    
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
            response.raise_for_status()  # Raise an exception for HTTP errors

            data = response.json()
            soup = BeautifulSoup(data['data'], 'html.parser')
            rows = soup.find_all('tr')

            earnings_stocks = []

            for row in rows:
                company_name_span = row.find('span', class_='earnCalCompanyName')
                if not company_name_span:
                    continue

                try:
                    ticker = row.find('a', class_='bold').text.strip()
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

def main():
    root = tk.Tk()
    app = SimpleEarningsApp(root)
    root.mainloop()

main()