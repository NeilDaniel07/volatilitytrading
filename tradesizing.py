import time
from datetime import datetime, timedelta
from typing import List, Dict, Tuple, Optional
import pandas as pd
import requests
import config

class AlpacaOptionSnapshot:
    BASE_STOCK   = "https://data.alpaca.markets/v2/stocks"
    BASE_OPTIONS = "https://data.alpaca.markets/v1beta1/options"

    def __init__(self,
                 screener_df: pd.DataFrame,
                 as_of: datetime,
                 *,
                 rate_limit_delay: float = 0.25,
                 max_retries: int = 8,
                 max_wait_time: int = 60,
                 price_wiggle: float = 10.0):

        if "Ticker" not in screener_df.columns:
            raise ValueError("Input DataFrame must contain a 'Ticker' column.")

        self.df               = screener_df.reset_index(drop=True)
        self.as_of            = as_of
        self.rate_limit_delay = rate_limit_delay
        self.max_retries      = max_retries
        self.max_wait_time    = max_wait_time
        self.wiggle           = price_wiggle

        self.hdr = config.header

    # ------------------------------------------------------------------
    # public driver
    # ------------------------------------------------------------------
    def run(self) -> pd.DataFrame:
        rows: List[Dict] = []

        for tk in self.df["Ticker"]:
            try:
                snap = self._collect_ticker_snapshot(tk)
                if snap:
                    rows.append(snap)
            except Exception as e:
                print(f"[{tk}] skipped – {e}")

            time.sleep(self.rate_limit_delay)   # gentle pacing

        return pd.DataFrame(rows)

    # ------------------------------------------------------------------
    # orchestrator for one ticker
    # ------------------------------------------------------------------
    def _collect_ticker_snapshot(self, ticker: str) -> Optional[Dict]:
        price = self._latest_trade_price(ticker)
        if price is None:
            return None

        front_exp, back_exp = self._front_back_expiries(ticker)
        if not front_exp or not back_exp:
            return None

        front_syms = self._option_snapshots(ticker, front_exp, price)
        back_syms  = self._option_snapshots(ticker, back_exp,  price)
        if not front_syms or not back_syms:
            return None

        res = self._atm_common_strike(front_syms, back_syms, price)
        if res is None:
            return None
        strike, front_sym, back_sym = res

        return {
            "ticker":        ticker,
            "stock_price":   price,
            "front_expiry":  front_exp,
            "back_expiry":   back_exp,
            "strike":        strike,
            "front_symbol":  front_sym,
            "back_symbol":   back_sym,
        }

    # ------------------------------------------------------------------
    # helpers
    # ------------------------------------------------------------------
    def _latest_trade_price(self, ticker: str) -> Optional[float]:
        url = f"{self.BASE_STOCK}/{ticker}/trades/latest"
        data = self._rate_limited_get(url)
        if not data or "trade" not in data:
            return None
        return data["trade"]["p"]

    # --------------------------------------------------------------
    # front & back expiries (API → fallback calendar rule)
    # --------------------------------------------------------------
    def _front_back_expiries(self, ticker: str) -> Tuple[str, str]:
        url = f"{self.BASE_OPTIONS}/expirations/{ticker}"
        meta = self._rate_limited_get(url)

        # --- path A: endpoint available ---
        if meta and "expirations" in meta and meta["expirations"]:
            expiries = sorted(meta["expirations"])
            today_str = self.as_of.strftime("%Y-%m-%d")

            front = next((d for d in expiries if d >= today_str), "")
            if not front:
                return "", ""

            target = datetime.strptime(front, "%Y-%m-%d") + timedelta(days=30)
            back = min(expiries,
                       key=lambda d: abs(datetime.strptime(d, "%Y-%m-%d") - target))
            return front, back

        # --- path B: fallback calendar rule ---
        today = self.as_of.date()

        # nearest Friday ≥ today
        days_ahead = (4 - today.weekday() + 7) % 7
        front_dt = today if days_ahead == 0 else today + timedelta(days=days_ahead)

        # back ≈ +28 days, roll to next Friday
        back_dt = front_dt + timedelta(days=28)
        back_days_ahead = (4 - back_dt.weekday() + 7) % 7
        if back_days_ahead != 0:
            back_dt += timedelta(days=back_days_ahead)

        return front_dt.strftime("%Y-%m-%d"), back_dt.strftime("%Y-%m-%d")

    # --------------------------------------------------------------
    # option snapshots (return OCC symbols)
    # --------------------------------------------------------------
    def _option_snapshots(self,
                          ticker: str,
                          expiry: str,
                          price: float) -> List[str]:
        lo = max(price - self.wiggle, 0)
        hi = price + self.wiggle
        url = (f"{self.BASE_OPTIONS}/snapshots/{ticker}"
               f"?limit=100&type=call&feed=indicative"
               f"&expiration_date={expiry}"
               f"&strike_price_gte={lo}&strike_price_lte={hi}")

        js = self._rate_limited_get(url)
        snaps = js.get("snapshots", {}) if js else {}
        return list(snaps.keys())

    # --------------------------------------------------------------
    # ATM strike common to both expiries
    # --------------------------------------------------------------
    def _atm_common_strike(self,
                           front_syms: List[str],
                           back_syms:  List[str],
                           spot: float) -> Optional[Tuple[int, str, str]]:

        def strike_from_symbol(sym: str) -> float:
            return int(sym[-8:]) / 1000

        front_map = {strike_from_symbol(s): s for s in front_syms}
        back_map  = {strike_from_symbol(s): s for s in back_syms}

        common = sorted(set(front_map) & set(back_map),
                        key=lambda k: abs(k - spot))

        if not common:
            return None

        k = common[0]
        return k, front_map[k], back_map[k]

    # --------------------------------------------------------------
    # GET with retry & 429 / 404 handling
    # --------------------------------------------------------------
    def _rate_limited_get(self, url: str) -> Optional[Dict]:
        for attempt in range(self.max_retries):
            try:
                r = requests.get(url, headers=self.hdr, timeout=10)

                # 404 ⇒ endpoint not available for plan
                if r.status_code == 404:
                    return None

                # 429 ⇒ rate-limit, exponential back-off
                if r.status_code == 429:
                    wait = min(self.max_wait_time,
                               self.rate_limit_delay * (2 ** attempt))
                    print(f"[429] waiting {wait}s – {url}")
                    time.sleep(wait)
                    continue

                r.raise_for_status()
                return r.json()

            except requests.RequestException as e:
                if attempt == self.max_retries - 1:
                    print(f"[ERROR] {url} – {e}")
                    return None
                time.sleep(self.rate_limit_delay * (2 ** attempt))

        return None


if __name__ == "__main__":
    screener_df = pd.read_csv("EarningsScanning.csv")  # your screener output
    snapshot = AlpacaOptionSnapshot(screener_df, datetime.now())
    out_df = snapshot.run()
    out_df.to_csv("alpaca_snapshot.csv", index=False)
    print(out_df.head())
