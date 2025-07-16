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

STOP_PIPELINE = False

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
        d = dt.datetime.now(EASTERN).date()
    return not NYSE.schedule(d, d).empty

def job_closer():
    print("[09:45] - Position Closing Script Executing ...")
    global STOP_PIPELINE
    if STOP_PIPELINE:
        STOP_PIPELINE = False
        return False
    try:
        df = pd.read_csv(FILTERED_CSV)
    except FileNotFoundError:
        print("No Available Position Data To Close")
        return False
    CalendarCloser(df).run()
    print("Closing Script Complete")
    STOP_PIPELINE = False
    return True

def job_screener_and_sizer():
    print("[3:30] - Screening and Sizing Scripts Executing ...")
    global STOP_PIPELINE
    if STOP_PIPELINE:
        print("Pipeline Stopped For Today ... Skipping This Step")
        return False
    scan_date = dt.datetime.now(EASTERN).date().strftime("%Y-%m-%d")
    app = Screener(scan_date, VOL_THRESHOLD, IVRV_THRESHOLD, TS_SLOPE_THRESHOLD)
    app.outputDF.to_csv(RAW_SCREENER_CSV, index=False)
    print(f"Screener Produced {len(app.outputDF)} Rows")
    try:
        df = pd.read_csv(RAW_SCREENER_CSV)
    except FileNotFoundError:
        print("No Available Screening Data")
        STOP_PIPELINE = True
        return False
    enriched = TradingDataCollector(df, dt.datetime.now()).run()
    enriched.to_csv(SIZEDTRADES_CSV, index=False)
    print("Trade Screening and Sizing Scripts Completed")
    return True

def job_opener():
    print("[3:40] - Position Opener Script Executing ...")
    global STOP_PIPELINE
    if STOP_PIPELINE:
        print("Pipeline Stopped For Today ... Skipping This Step")
        return False
    try:
        df = pd.read_csv(SIZEDTRADES_CSV)
    except FileNotFoundError:
        print("No Available Sized Trades Data")
        STOP_PIPELINE = True
        return False
    orders_df = CalendarOpener(df).run()
    orders_df.to_csv(PLACED_CSV, index=False)
    print(f"Opener Placed {len(orders_df)} Complex Orders")
    return True

def job_reconciler():
    print("[3:50] - Reconciliation Script Executing ...")
    global STOP_PIPELINE
    if STOP_PIPELINE:
        print("Pipeline Stopped For Today ... Skipping This Step")
        return False
    try:
        df = pd.read_csv(PLACED_CSV)
    except FileNotFoundError:
        print("No Available Trading Data")
        STOP_PIPELINE = True
        return False
    filt = CalendarOpenReconciler(df).run()
    filt.to_csv(FILTERED_CSV, index=False)
    print("Reconciliation Script Completed")
    return True

def schedule_today():
    schedule.every().day.at("09:45", EASTERN).do(job_closer)
    schedule.every().day.at("15:30", EASTERN).do(job_screener_and_sizer)
    schedule.every().day.at("15:40", EASTERN).do(job_opener)
    schedule.every().day.at("15:50", EASTERN).do(job_reconciler)

def sleep_until_next_midnight():
    now = dt.datetime.now(EASTERN)
    tomorrow = (now + dt.timedelta(days=1)).date()
    nxt_midnight = EASTERN.localize(dt.datetime.combine(tomorrow, dt.time.min))
    seconds = (nxt_midnight - now).total_seconds()
    time.sleep(max(1, seconds))

def main():
    DATA_DIR.mkdir(parents=True, exist_ok=True)
    while True:
        et_now = dt.datetime.now(EASTERN)
        et_today = et_now.date()

        if is_market_day(et_today):
            print(f"Market Is Open ... Executor Active")
            schedule.clear()
            schedule_today()
            
            while dt.datetime.now(EASTERN).date() == et_today:
                schedule.run_pending()
                time.sleep(15)
        else:
            print(f"Market Is Closed")
            sleep_until_next_midnight()

main()
