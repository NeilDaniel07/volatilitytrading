import os
import json
import time
from tradesizing import TradingDataCollector
from alpaca.data.historical import StockHistoricalDataClient, OptionHistoricalDataClient
from alpaca.data.requests import StockBarsRequest
from alpaca.data.requests import OptionLatestQuoteRequest
from alpaca.data.timeframe import TimeFrame
from alpaca.common.exceptions import APIError
from datetime import date, timedelta
from dotenv import load_dotenv




ticker = "AAPL"


class DataPlot():
    QUOTES = 'https://data.alpaca.markets/v1beta1/options/quotes/latest?symbols={sym}&feed=indicative'

    def __init__(self):
        load_dotenv("keys.env")
        self.api_key = os.getenv("APCA_API_KEY_ID")
        self.secret_key = os.getenv("APCA_API_SECRET_KEY")
        self.stock_client = StockHistoricalDataClient(self.api_key, self.secret_key)
        self.options_client = OptionHistoricalDataClient(self.api_key, self.secret_key)
        self.date = date.today()
        self.header = {
            "APCA_API_KEY_ID": self.api_key,
            "APCA_API_SECRET_KEY": self.secret_key,
        }
        self.rate_limit_delay = 0.25
        self.max_retries = 8
        self.max_wait_time = 60

        self.datapoints = {}
    def get_30day_average(self, ticker: str):
        #print(f"\n{ticker}")
        end_date = self.date
        start_date = end_date - timedelta(days=30)
        volume_request = StockBarsRequest(
                            symbol_or_symbols=[ticker],
                            timeframe=TimeFrame.Day,
                            start=start_date,
                            end=end_date
        )
        bars = self.stock_client.get_stock_bars(volume_request)
        
        if ticker not in bars.data:
            print(f"[ERROR]: {ticker} is not accessible")
            print(bars)
            return 1

        data = bars[ticker]
        total_volume = 0
        num_days = 0
        for day in data:
            total_volume += day.volume
            num_days+=1
        #print(f"total_volume: {total_volume}, days: {num_days}, average_30_day_volume: {total_volume//num_days}")
        self.datapoints[ticker+"_front"] = [total_volume//num_days]
        self.datapoints[ticker+"_back"]  = [total_volume//num_days]
        return 0

    def get_bid_ask_percentage(self, ticker: str):
        ts = TradingDataCollector(self.date)
        price = ts.latest_trade_price(ticker)
        packed_expiries = ts.get_expiry_dates(ticker, price= price)
        f_date, b_date, f_opts, b_opts = packed_expiries
        #print(f_date, b_date)
        if f_date == None or b_date == None:
            print(f"[ERROR]: dates could not be found for {ticker}")
            return 1
        valid_options = ts.at_the_money_common_strike(f_opts, b_opts, price)
        if valid_options is None:
            print("[ERROR]: empty valid options")
            return 1

        strike, f_sym, b_sym = valid_options   
        #print(f_sym, b_sym)
        url = self.QUOTES.format(sym=f_sym)
        for attempt in range(self.max_retries):
            try:
                request = OptionLatestQuoteRequest(symbol_or_symbols=[f_sym])
                quote = (self.options_client.get_option_latest_quote(request))[f_sym]
                attempt = self.max_retries
                bid_ask_percentage = ((quote.ask_price-quote.bid_price)/quote.ask_price)*100
                self.datapoints[(ticker + "_front")].append(bid_ask_percentage)
                #print(bid_ask_percentage)
                break
            except APIError as e:
                if attempt == self.max_retries - 1:
                    print(f"[ERROR]: {url} - {e}")
                    return None
                elif e.status_code == 404:
                    return None
                elif e.status_code == 429:
                    wait = min(self.max_wait_time, self.rate_limit_delay * (2 ** attempt))
                    print(f"[429 Error]: waiting {wait} seconds - {url}")
                    time.sleep(wait)
                    continue

        for attempt in range(self.max_retries):
            try:
                request = OptionLatestQuoteRequest(symbol_or_symbols=[b_sym])
                quote = (self.options_client.get_option_latest_quote(request))[b_sym]
                attempt = self.max_retries
                bid_ask_percentage = ((quote.ask_price-quote.bid_price)/quote.ask_price)*100
                self.datapoints[(ticker + "_back")].append(bid_ask_percentage)
                #print(bid_ask_percentage)
                break
            except APIError as e:
                if attempt == self.max_retries - 1:
                    print(f"[ERROR]: {url} - {e}")
                    return 1
                elif e.status_code == 404:
                    return 1
                elif e.status_code == 429:
                    wait = min(self.max_wait_time, self.rate_limit_delay * (2 ** attempt))
                    print(f"[429 Error]: waiting {wait} seconds - {url}")
                    time.sleep(wait)
                    continue
        return 0
    def getDatapoints(self):
        return self.datapoints


dp = DataPlot()
file = open("./Legacy/NasdaqAndNYSETradedStockscopy.csv", "r")
for line in file:
    ticker = line.strip("\n")
    print(ticker)
    ret = dp.get_30day_average(ticker)
    if ret != 1:
        inner_ret = dp.get_bid_ask_percentage(ticker)
        if inner_ret != 1:
            print(f"{ticker} worked!")
            datapoints = dp.getDatapoints()
            with open("./data/output.json", 'w') as fp:
                json.dump(datapoints, fp, indent=2)
