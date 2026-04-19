import pandas as pd
import numpy as np
from core.universe import apply_fundamental_filters
from core.factors import compute_factor_scores
from core.momentum import select_top_momentum, apply_sector_caps
from core.optimizer import optimize_weights
from core.execution import generate_trade_list
import core.universe

class MockTicker:
    def __init__(self, info):
        self.info = info

def run_deep_trace():
    mock_data = {
        "RELIANCE.NS": {
            "returnOnEquity": 0.15, "debtToEquity": 40.0, "sector": "Energy", "operatingMargins": 0.12, 
            "pegRatio": 1.8, "heldPercent": 0.50, "operatingCashflow": 1e6, "netIncomeToCommon": 1e6,
            "earningsGrowth": 0.12, "revenueGrowth": 0.10, "marketCap": 1e13
        },
        "TCS.NS": {
            "returnOnEquity": 0.35, "debtToEquity": 5.0, "sector": "Technology", "operatingMargins": 0.25,
            "pegRatio": 3.0, "heldPercent": 0.72, "operatingCashflow": 2e6, "netIncomeToCommon": 1.8e6,
            "earningsGrowth": 0.15, "revenueGrowth": 0.08, "marketCap": 1e13
        },
        "HDFCBANK.NS": {
            "returnOnEquity": 0.18, "debtToEquity": 80.0, "sector": "Financial Services", "operatingMargins": 0.40,
            "pegRatio": 0.9, "heldPercent": 0.25, "operatingCashflow": 1e6, "netIncomeToCommon": 1e6,
            "earningsGrowth": 0.20, "revenueGrowth": 0.18, "marketCap": 1e13
        },
        "INFY.NS": {
            "returnOnEquity": 0.28, "debtToEquity": 0.0, "sector": "Technology", "operatingMargins": 0.22,
            "pegRatio": 1.5, "heldPercent": 0.15, "operatingCashflow": 800000, "netIncomeToCommon": 1e6,
            "earningsGrowth": 0.10, "revenueGrowth": 0.12, "marketCap": 1e13
        }
    }

    def mock_ticker_init(ticker, session=None):
        return MockTicker(mock_data.get(ticker, {}))
    core.universe.yf.Ticker = mock_ticker_init
    
    universe_dict = {t: "Large" for t in mock_data.keys()}
    
    # Simulation Case: Default Dashboard Settings
    # 1. Fundamental Cutoff (Top 30%)
    investable_tickers, sector_map, cap_map, scoring_df = apply_fundamental_filters(universe_dict, top_percentile=0.3)
    print(f"Investable Tickers (Top 30%): {investable_tickers}")
    
    # 2. Mock Prices with 100% Momentum (User set 100%)
    dates = pd.date_range(end='2026-04-19', periods=126, freq='B')
    price_data = {}
    for t in mock_data.keys():
        returns = np.random.normal(0.001, 0.01, len(dates)) # Positive drift for all
        price_data[t] = 100 * (1 + returns).cumprod()
    df_prices = pd.DataFrame(price_data, index=dates)
    
    # 3. Momentum Stage
    scores = compute_factor_scores(df_prices[investable_tickers], {"momentum_lookback_days": 90, "volatility_lookback_days": 60})
    selected = select_top_momentum(scores, top_percent=1.0) # User said 100%
    print(f"Selected Tickers (100% Momentum): {selected}")
    
    # 4. Optimization
    regime = {"optimization_mode": "Markowitz"}
    limits = {"cap_large": 1.0, "cap_mid": 0.0, "cap_small": 0.0, "category_caps": {"Others": 0.1}}
    weights = optimize_weights(df_prices, selected, regime, sector_map, cap_map, limits)
    print(f"Final Weights: {weights}")
    
    # 5. Trade Generation
    dummy_portfolio = [{"Ticker": "RELIANCE", "qty_longterm": 50}]
    df_trades = generate_trade_list(weights, dummy_portfolio, df_prices, fresh_capital=0, assessed_tickers=scoring_df["Stock"].tolist())
    
    print("\n--- Trade Recommendations Output ---")
    if not df_trades.empty:
        print(df_trades[["Stock", "Action", "Group", "Shares", "Target Weight"]].to_string(index=False))
    else:
        print("Empty Trade List.")

if __name__ == "__main__":
    run_deep_trace()
