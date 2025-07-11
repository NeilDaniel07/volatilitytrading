from __future__ import annotations
import os

ALPACA_KEY: str | None = os.getenv("APCA_API_KEY_ID")
ALPACA_SECRET_KEY: str | None = os.getenv("APCA_API_SECRET_KEY")

if not ALPACA_KEY or not ALPACA_SECRET_KEY:
    raise RuntimeError("Count not retrieve Alpaca Key Information")

header = {
    "APCA-API-KEY-ID": ALPACA_KEY,
    "APCA-API-SECRET-KEY": ALPACA_SECRET_KEY,
}
