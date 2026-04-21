import sys
import os
sys.path.append(os.path.abspath(os.path.join(os.path.dirname(__file__), '..')))

import pandas as pd
import numpy as np
import yfinance as yf
from curl_cffi import requests
from core.universe import fetch_broad_universe, apply_fundamental_filters
from core.factors import compute_factor_scores
from core.momentum import select_top_momentum, apply_sector_caps
from core.optimizer import optimize_weights

# Global session to mimic browser and bypass Yahoo Finance 401/Invalid Crumb errors
session = requests.Session(impersonate="chrome120")
session.headers.update({
    "Accept": "*/*",
    "Accept-Language": "en-US,en;q=0.9",
    "Origin": "https://finance.yahoo.com",
    "Referer": "https://finance.yahoo.com",
    "User-Agent": "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/120.0.0.0 Safari/537.36"
})

print("Initiating 10-Year Walk-Forward Backtester...")
print("⚠️ Note: Subject to Survivorship Bias. Point-in-time fundamentals unavailable.\n")

# 1. Base Universe
universe_dict = fetch_broad_universe("multi_cap")
tickers, sector_map, cap_map, _ = apply_fundamental_filters(universe_dict)

if not tickers:
    print("⚠️ Fundamental scrape failed due to Yahoo network block. Engaging aggressive fallback standard universe instead!")
    tickers = list(universe_dict.keys())
    sector_map = {t: "Others" for t in tickers}
    cap_map = universe_dict

print(f"\n--- Backtest Starting Universe ({len(tickers)} Assets) ---")
print(", ".join(tickers))
print("--------------------------------------------------\n")

print(f"Fetching 10 years of data for {len(tickers)} assets... (This may take roughly ~15 seconds via YF)")
data = yf.download(tickers, period="10y", progress=False, session=session)["Close"]
prices = data.ffill()

# Drop stocks that didn't exist for at least 50% of the period
prices = prices.dropna(axis=1, thresh=int(len(prices) * 0.5))
tickers = list(prices.columns)

if len(prices) < 252:
    print("❌ Fatal Network Error: Failed to fetch at least 1 year of historical market data. Please verify your connection to Yahoo Finance.")
    import sys
    sys.exit(1)

# 2. Walk-Forward Loop Config
LOOKBACK_DAYS = 252 # 1 Year
REBALANCE_DAYS = 60 # Quarterly
PORTFOLIO_VALUE = 100000.0

portfolio_history = []
dates = []

# Global Regime Tracker
def proxy_regime(prices_history):
    market_proxy = prices_history.pct_change().dropna().mean(axis=1)
    recent_vol = market_proxy.tail(20).std()
    historical_vol = market_proxy.std()
    recent_trend = market_proxy.tail(20).mean()
    
    if recent_vol > (historical_vol * 1.3) or recent_trend < -0.0005:
        return {"rate_trend": "falling", "inflation": "high", "optimization_mode": "HRP"}
    return {"rate_trend": "rising", "inflation": "moderate", "optimization_mode": "Markowitz"}

print("\nExecuting Rolling Simulation...")
current_weights = {}
previous_holdings = set()

for t in range(LOOKBACK_DAYS, len(prices), REBALANCE_DAYS):
    t_date = prices.index[t]
    print(f"\n[{t_date.date()}] Rebalancing...")
    
    # 1 Year Trailing History
    hist_prices = prices.iloc[t - LOOKBACK_DAYS : t]
    
    # Regime
    regime = proxy_regime(hist_prices)
    print(f"  -> Engine: {regime['optimization_mode']}")
    
    # Scores
    scores = compute_factor_scores(hist_prices, {"momentum_lookback_days": 90, "volatility_lookback_days": 60})
    
    # Select
    selected_raw = select_top_momentum(scores, top_percent=0.5)
    selected = apply_sector_caps(selected_raw, sector_map, max_per_sector=3)
    
    if not selected:
        print("  -> No stocks selected! Moving to CASH.")
        current_weights = {"CASH": 1.0}
    else:
        limits = {
            "cap_large": 0.70, "cap_mid": 0.20, "cap_small": 0.10,
            "category_caps": {"Financials": 0.30, "Technology": 0.20, "Industrials_Infra": 0.20, 
                              "Consumer_FMCG": 0.15, "Pharma_Healthcare": 0.15, "Others": 0.50}
        }
        
        # Optimize
        try:
            current_weights = optimize_weights(hist_prices, selected, regime, sector_map, cap_map, limits)
        except Exception as e:
            print(f"  -> Optimizer failed: {e}. Defaulting to Equal Weight.")
            current_weights = {s: 1.0/len(selected) for s in selected}
            
    # Track turnover log
    current_holdings = set([s for s in current_weights.keys() if current_weights[s] > 0.005 and s != "CASH"])
    added = current_holdings - previous_holdings
    dropped = previous_holdings - current_holdings
    
    if added:
        print(f"  -> [+] Added: {', '.join(added)}")
    if dropped:
        print(f"  -> [-] Dropped: {', '.join(dropped)}")
        
    previous_holdings = current_holdings

    # Calculate performance for the next 60 days using these weights
    end_t = min(t + REBALANCE_DAYS, len(prices))
    future_prices = prices.iloc[t:end_t]
    
    # Forward Returns
    period_returns = future_prices.pct_change().fillna(0.0)
    
    for i in range(len(period_returns)):
        day_date = period_returns.index[i]
        day_ret = 0.0
        
        for stock, weight in current_weights.items():
            if stock != "CASH" and stock in period_returns.columns:
                day_ret += period_returns[stock].iloc[i] * weight
                
        # Apply performance
        PORTFOLIO_VALUE *= (1 + day_ret)
        portfolio_history.append(PORTFOLIO_VALUE)
        dates.append(day_date)

# 3. Final Metrics
print("\n🔥 Backtest Complete!")
print(f"Final Value: ₹{PORTFOLIO_VALUE:,.2f}")

years = len(prices) / 252.0
cagr = ((PORTFOLIO_VALUE / 100000.0) ** (1 / years)) - 1

df_curve = pd.Series(portfolio_history, index=dates)
drawdown = (df_curve / df_curve.cummax()) - 1
max_dd = drawdown.min()

print(f"CAGR: {cagr * 100:.2f}%")
print(f"Max Drawdown: {max_dd * 100:.2f}%")
print(f"Calmar Ratio: {cagr / abs(max_dd) if max_dd != 0 else 'N/A'}")
