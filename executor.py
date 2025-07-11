import schedule, time, pytz, datetime as dt
import pandas_market_calendars as mcal
import pandas as pd
from screener import Screener
from tradesizing import TradingDataCollector
from calendaropener import CalendarOpener
from reconciliation import CalendarOpenReconciler
from calendarcloser import CalendarCloser
from pathlib import Path

EASTERN = pytz.timezone("US/Eastern")
NYSE = mcal.get_calendar("XNYS")

VOL_THRESHOLD = 1500000
IVRV_THRESHOLD = 1.25
TS_SLOPE_THRESHOLD = -0.00406

DATA_DIR = Path("/data")

RAW_SCREENER_CSV = DATA_DIR / "EarningsScanning.csv"
SIZEDTRADES_CSV  = DATA_DIR / "SizedTrades.csv"
PLACED_CSV = DATA_DIR / "PlacedOrders.csv"
FILTERED_CSV = DATA_DIR / "FilteredOrders.csv"

def is_market_day(d=None):
    if d is None:
        d = dt.date.today()
    return not NYSE.schedule(d, d).empty

def job_closer():
    print("[09:45] - Position Closing Script Executing ...")
    try:
        df = pd.read_csv(FILTERED_CSV)
    except FileNotFoundError:
        print("No Available Position Data To Close")
        return

    CalendarCloser(df).run()
    print("Closing Script Complete")

def job_screener():
    print("[3:30] - Screening Script Executing ...")
    scan_date = dt.date.today().strftime("%Y-%m-%d")
    app = Screener(scan_date, VOL_THRESHOLD, IVRV_THRESHOLD, TS_SLOPE_THRESHOLD)
    app.outputDF.to_csv(RAW_SCREENER_CSV, index=False)
    print(f"Screener Produced {len(app.outputDF)} Rows")
    schedule.run_pending()

def job_trade_sizer():
    print("[3:30] - Trade Sizing Script Executing ...")
    df = pd.read_csv(RAW_SCREENER_CSV)
    enriched = TradingDataCollector(df, dt.datetime.now()).run()
    enriched.to_csv(SIZEDTRADES_CSV, index=False)
    print("Trade Sizing Script Completed")

def job_opener():
    print("[3:40] - Position Opener Script Executing ...")
    df = pd.read_csv(SIZEDTRADES_CSV)
    orders_df = CalendarOpener(df).run()
    orders_df.to_csv(PLACED_CSV, index=False)
    print(f"Opener Placed {len(orders_df)} Complex Orders")

def job_reconciler():
    print("[3:50] -  Reconcilation Script Executing ...")
    df = pd.read_csv(PLACED_CSV)
    filt = CalendarOpenReconciler(df).run()
    filt.to_csv(FILTERED_CSV, index=False)
    print("Reconcilation Script Completed")

def schedule_today():
    schedule.every().day.at("09:45", EASTERN).do(job_closer)
    schedule.every().day.at("15:30", EASTERN).do(job_screener)
    schedule.every().day.at("15:30", EASTERN).do(job_trade_sizer)
    schedule.every().day.at("15:40", EASTERN).do(job_opener)
    schedule.every().day.at("15:50", EASTERN).do(job_reconciler)

def main():
    if not is_market_day():
        print("Market Closed Today. Resting Until Next Trading Day.")
        return

    schedule_today()
    print("Executor Running …")

    while True:
        schedule.run_pending()
        time.sleep(15)

main()