import pandas as pd
import requests
import time
from datetime import datetime, timedelta
import config
from typing import Optional, Tuple
import json
import calendar
import numpy as np

class AlpacaAPIManager:
    def __init__(self):
        self.rate_limit_delay = 0.1  # Adjust based on Alpaca's rate limits
        self.max_retries = 10  # Increased max retries for rate limit handling
        self.max_wait_time = 60  # Maximum wait time between retries in seconds
        self.sortedDict = {}
    def find_next_third_friday(self):
        # Get today's date
        today = datetime.now()
        # Start from the current month or next month if we've already passed the 3rd Friday
        year, month = today.year, today.month
        
        while True:
            month_days = calendar.monthcalendar(year, month)           # get all days in the current month            
            fridays = [week[4] for week in month_days if week[4] != 0] # count the Fridays (where Friday is index 4 in weekday())
            # Check if we have at least 3 Fridays
            if len(fridays) >= 3:
                third_friday = datetime(year, month, fridays[2])
                # If the third Friday is in the future, return it
                if third_friday > today:
                    days_until = (third_friday - today).days + 1
                    return days_until
            # Move to the next month
            month += 1
            if month > 12:
                month = 1
                year += 1
    def request_info(self, ticker_symbol: str) -> Optional[Tuple[float, str, str, int]]:
        """
        Retrieve stock and options information with robust error handling and rate limit management
        
        Args:
            ticker_symbol (str): Stock ticker symbol
        
        Returns:
            Tuple of (stock_price, front_option, back_option, active_capital)
        """
        for attempt in range(self.max_retries):
            try:
                # Fetch latest trade price
                url = f"https://data.alpaca.markets/v2/stocks/{ticker_symbol}/trades/latest"
                response = requests.get(url, headers=config.header)
                print(response.status_code)

                # Check for rate limit error
                if response.status_code == 429:
                    # Calculate wait time with exponential backoff
                    wait_time = min(self.max_wait_time, self.rate_limit_delay * (2 ** attempt))
                    print(f"Rate limit reached for {ticker_symbol}. Waiting {wait_time} seconds...")
                    time.sleep(wait_time)
                    continue  # Retry the request
                
                response.raise_for_status()  # Raise exception for other bad status codes
                trade_data = response.json()
                stock_price: float = trade_data['trade']['p']

                # Optional: Log tickers with options
                """
                if has_options:
                    with open("./alpacaFIXED/logs/available_log.txt", "a") as file:
                        file.write(ticker_symbol+"\n")   
                    with open("./alpacaFIXED/logs/all_logs.txt", "a") as file:
                        file.write(ticker_symbol+" has option chain \n")     
                else:
                    with open("./alpacaFIXED/logs/all_logs.txt", "a") as file:
                        file.write(ticker_symbol+" NO OPTIONS CHAIN \n")
                """
                ### Calc Front and Back Options Dates
                today = datetime.now()
                weeklyDF = pd.read_csv("./alpacaFIXED/weeklyOpt.csv")
                if ticker_symbol in weeklyDF["Ticker"]:
                    days_until_friday = (4 - today.weekday() + 7) % 7
                else:
                    days_until_friday = self.find_next_third_friday()
                
                frontDate = (today + timedelta(days=days_until_friday))
                backDate = (frontDate + timedelta(days=28))
                print(f"first date: {frontDate}\nback date: {backDate}")

                fbool = True
                bbool = True

                for elem in weeklyDF["Date"][:33]:
                    if frontDate.strftime("%Y-%m-%d") == elem and fbool:
                        frontDate -= timedelta(days=1)
                        fbool = False
                    if backDate.strftime("%Y-%m-%d") == elem and bbool:
                        backDate -= timedelta(days=1)
                        bbool = False

                frontDate = frontDate.strftime("%Y-%m-%d")
                backDate = backDate.strftime("%Y-%m-%d")

                print(f"first date: {frontDate}\nback date: {backDate}")

                ### Fetch front options contracts
                options_url = f"https://data.alpaca.markets/v1beta1/options/snapshots/{ticker_symbol}?limit=100&type=call&feed=indicative&expiration_date={frontDate}&strike_price_gte={stock_price-10.0}&strike_price_lte={stock_price+10}"
                front_options_response = requests.get(options_url, headers=config.header)
                options_url = f"https://data.alpaca.markets/v1beta1/options/snapshots/{ticker_symbol}?limit=100&type=call&feed=indicative&expiration_date={backDate}&strike_price_gte={stock_price-10.0}&strike_price_lte={stock_price+10}"
                back_options_response = requests.get(options_url, headers=config.header)

                # Check for rate limit error in options request
                if front_options_response.status_code == 429 or back_options_response.status_code == 429:
                    wait_time = min(self.max_wait_time, self.rate_limit_delay * (2 ** attempt))
                    print(f"Rate limit reached for {ticker_symbol} options. Waiting {wait_time} seconds...")
                    time.sleep(wait_time)
                    continue  # Retry the request
                
                
                front_options_response.raise_for_status()
                back_options_response.raise_for_status()
                
                front_opt_data = front_options_response.json()
                back_opt_data = back_options_response.json()

                front_has_contracts = len(front_opt_data.get("snapshots", [])) > 0
                back_has_contracts = len(back_opt_data.get("snapshots", [])) > 0
                if(not front_has_contracts or not back_has_contracts):
                    print("no contracts")
                    return None
                else:
                    #print(json.dumps(front_opt_data, indent=2))
                    print("------------------------")
                    #print(json.dumps(back_opt_data, indent=2))

                ### Determine Strike Price
                front_symbol_list = []
                for element in front_opt_data["snapshots"]:
                    front_symbol_list.append(element)
                front_symbol_list.sort()
                print(front_symbol_list[int(len(front_opt_data["snapshots"])/2)-1])

                back_symbol_list = []
                for element in back_opt_data["snapshots"]:
                    back_symbol_list.append(element)
                back_symbol_list.sort()
                print(back_symbol_list[int(len(back_opt_data["snapshots"])/2)-1])

                front_strike = int(front_symbol_list[int(len(front_opt_data["snapshots"])/2)-1][-8:])/1000
                back_strike = int(back_symbol_list[int(len(back_opt_data["snapshots"])/2)-1][-8:])/1000
                print(f"front_strike: {front_strike} + back_strike: {back_strike}")
                strike_price = back_strike
                print(f"actual price: {stock_price}, strike price: {strike_price}")

                options_url = f"https://data.alpaca.markets/v1beta1/options/snapshots/{ticker_symbol}?limit=100&type=call&feed=indicative&expiration_date={frontDate}&strike_price_lte={strike_price}&strike_price_gte={strike_price}"
                print(options_url)
                response = requests.get(options_url, headers=config.header)
                print(response.status_code)

                front_option = response.json()["snapshots"]
                for elem in front_option:
                    front_option = elem

                options_url = f"https://data.alpaca.markets/v1beta1/options/snapshots/{ticker_symbol}?limit=100&type=call&feed=indicative&expiration_date={backDate}&strike_price_lte={strike_price}&strike_price_gte={strike_price}"
                print(options_url)
                response = requests.get(options_url, headers=config.header)
                print(response.status_code)

                back_option = response.json()["snapshots"]
                for elem in back_option:
                    back_option = elem

                print(f"{front_option} + {back_option}")

                ### Account Details:
                acct_url = "https://paper-api.alpaca.markets/v2/account"
                response = requests.get(acct_url, headers=config.header)
                acct = response.json()

                active_capital = acct["options_buying_power"]
                
                time.sleep(self.rate_limit_delay)
                t:tuple = (stock_price, front_option, back_option, active_capital)
                return t

            except requests.RequestException as e:
                with open("./alpacaFIXED/logs/all_logs.txt", "a") as file:
                    file.write(f"Error fetching data for {ticker_symbol}: {e}\n")  
                
                # Check if the error is specifically a 429 rate limit error
                if isinstance(e, requests.exceptions.HTTPError) and e.response.status_code == 429:
                    wait_time = min(self.max_wait_time, self.rate_limit_delay * (2 ** attempt))
                    print(f"Rate limit error for {ticker_symbol}. Waiting {wait_time} seconds...")
                    time.sleep(wait_time)
                else:
                    time.sleep(self.rate_limit_delay * (attempt + 1))  # Exponential backoff
        
        with open("./alpaca/logs/all_logs.txt", "a") as file:
            file.write(f"Failed to retrieve data for {ticker_symbol}\n")  
        return None 
    
    def howmanyshitterscaniget(self, acctBalance: int, frontopt: str, backopt: str, ticker: str):
        fronturl = f"https://data.alpaca.markets/v1beta1/options/quotes/latest?symbols={frontopt}&feed=indicative"
        backurl  = f"https://data.alpaca.markets/v1beta1/options/quotes/latest?symbols={backopt}&feed=indicative"

        front_data = requests.get(fronturl, headers=config.header).json()
        back_data  = requests.get(backurl, headers=config.header).json()
        print(json.dumps(front_data, indent=2))
        print(json.dumps(back_data , indent=2))

        bid_ask_front = np.array([front_data["quotes"][frontopt]["bp"], front_data["quotes"][frontopt]["ap"]])
        bid_ask_back  = np.array([back_data["quotes"][backopt]["bp"], back_data["quotes"][backopt]["ap"]])
        bid_ask_spread_front = np.abs(bid_ask_front[1:] - bid_ask_front[:-1]) / ((bid_ask_front[1:] + bid_ask_front[:-1]) / 2) * 100
        bid_ask_spread_back = np.abs(bid_ask_back[1:] - bid_ask_back[:-1]) / ((bid_ask_back[1:] + bid_ask_back[:-1]) / 2) * 100
        print(f"bid ask spread for front leg: {round(float(bid_ask_spread_front), 4)}% + bid ask spread for back leg: {round(float(bid_ask_spread_back), 4)}%")

        front_leg_cost = -1*front_data["quotes"][frontopt]["bp"]
        back_leg_cost = back_data["quotes"][backopt]["ap"]
        total_cost = front_leg_cost + back_leg_cost
        print(f"cost per calander contract: {total_cost}")

        quantity = int((int(acctBalance)*.2)/(total_cost*100))
        print(f"this many to buy: {quantity}")

        self.sortedDict[ticker] = {
                                    "q": quantity,
                                    "frontopt": frontopt,
                                    "backopt": backopt,
                                  }
        return self.sortedDict

def main():
    
    obj: AlpacaAPIManager = AlpacaAPIManager()
    df = pd.read_csv("./alpacaFIXED/data.csv")
    print(df.shape[0])
    retrivedDict = {}
    for i in range(df[:5].shape[0]):
        ticker = df["Ticker"][i]
        retrivedDict[ticker] = obj.request_info(ticker)
        obj.howmanyshitterscaniget(retrivedDict[ticker][3], retrivedDict[ticker][1], retrivedDict[ticker][2], ticker)
        time.sleep(2)
    print(json.dumps(obj.sortedDict,indent=2))

main()