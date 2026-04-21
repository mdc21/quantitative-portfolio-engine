import sys
import os
sys.path.append(os.path.abspath(os.path.join(os.path.dirname(__file__), '..')))

import yfinance as yf
from core.data_loader import session
from core.universe import _evaluate_fundamentals
import pandas as pd

def diagnostic_fetch():
    print("--- 🔬 yfinance Diagnostic Probe ---")
    tickers = ["AAPL", "TCS.NS", "RELIANCE.NS"]
    
    print(f"\n1. Testing yf.download with session for {tickers}...")
    try:
        data = yf.download(tickers, period="5d", session=session, progress=False)
        if not data.empty:
            print(f"✅ Success! Fetched {len(data)} rows.")
            print(data["Close"].tail(2))
        else:
            print("❌ Failure: yf.download returned empty DataFrame.")
    except Exception as e:
        print(f"❌ Error during yf.download: {e}")

    print(f"\n2. Testing _evaluate_fundamentals (Ticker.info) with jitter...")
    for t in tickers:
        print(f"   Fetching info for {t}...")
        result = _evaluate_fundamentals((t, "Large"))
        if result["DataSource"] == "Live":
            print(f"   ✅ {t}: Live data fetched successfully (Sector: {result['Sector']})")
        else:
            print(f"   ⚠️ {t}: Fell back to {result['DataSource']}")

if __name__ == "__main__":
    diagnostic_fetch()
