import pandas as pd
import numpy as np
from core.universe import apply_fundamental_filters
from core.factors import compute_factor_scores
import core.universe

class MockTicker:
    def __init__(self, info):
        self.info = info

def run_trace():
    # 1. Mock Data (Same as conftest.py / diagnostic)
    mock_data = {
        "RELIANCE.NS": {
            "returnOnEquity": 0.15, "debtToEquity": 40.0, "sector": "Energy", "operatingMargins": 0.12, 
            "pegRatio": 1.8, "heldPercent": 0.50, "operatingCashflow": 1e6, "netIncomeToCommon": 1e6,
            "earningsGrowth": 0.12, "revenueGrowth": 0.10
        },
        "TCS.NS": {
            "returnOnEquity": 0.35, "debtToEquity": 5.0, "sector": "Technology", "operatingMargins": 0.25,
            "pegRatio": 3.0, "heldPercent": 0.72, "operatingCashflow": 2e6, "netIncomeToCommon": 1.8e6,
            "earningsGrowth": 0.15, "revenueGrowth": 0.08
        },
        "HDFCBANK.NS": {
            "returnOnEquity": 0.18, "debtToEquity": 80.0, "sector": "Financial Services", "operatingMargins": 0.40,
            "pegRatio": 0.9, "heldPercent": 0.25, "operatingCashflow": 300000, "netIncomeToCommon": 800000,
            "earningsGrowth": 0.20, "revenueGrowth": 0.18
        },
        "INFY.NS": {
            "returnOnEquity": 0.28, "debtToEquity": 0.0, "sector": "Technology", "operatingMargins": 0.22,
            "pegRatio": 1.5, "heldPercent": 0.15, "operatingCashflow": 800000, "netIncomeToCommon": 1e6,
            "earningsGrowth": 0.10, "revenueGrowth": 0.12
        }
    }

    def mock_ticker_init(ticker, session=None):
        return MockTicker(mock_data.get(ticker, {}))
    core.universe.yf.Ticker = mock_ticker_init
    
    universe_dict = {t: "Large" for t in mock_data.keys()}
    tickers, sector_map, cap_map, df_fund = apply_fundamental_filters(universe_dict, top_percentile=1.0)
    
    # 2. Mock Prices (Synthetic for diagnostic)
    # Let's see if HDFC or RELIANCE has poor momentum
    # conftest.py generates random walk. Let's force some trends.
    dates = pd.date_range(end='2026-04-19', periods=126, freq='B')
    price_data = {}
    for t in tickers:
        # RELIANCE: Downward trend (Sell)
        if t == "RELIANCE.NS":
            returns = np.random.normal(-0.002, 0.01, len(dates)) # -0.2% drift
        # HDFCBANK: Flat/Volatile (Sell)
        elif t == "HDFCBANK.NS":
            returns = np.random.normal(0.0, 0.02, len(dates)) # 0.0% drift, high vol
        # TCS/INFY: Upward trend (Buy)
        else:
            returns = np.random.normal(0.002, 0.01, len(dates)) # +0.2% drift
            
        price_data[t] = 100 * (1 + returns).cumprod()
    
    df_prices = pd.DataFrame(price_data, index=dates)
    
    # 3. Factor Scoring
    config = {"momentum_lookback_days": 90, "volatility_lookback_days": 60}
    scores = compute_factor_scores(df_prices, config)
    
    print("\n--- Pipeline Audit Trace ---")
    audit = df_fund[["Stock", "Fundamental_Score"]].set_index("Stock")
    audit["Factor_Score"] = scores
    audit["Blended_Score"] = (audit["Fundamental_Score"] + audit["Factor_Score"]) / 2
    
    print(audit.sort_values("Blended_Score", ascending=False).to_string())

if __name__ == "__main__":
    run_trace()
