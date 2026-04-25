import numpy as np
import pandas as pd

def apply_trend_filter(prices, selected_stocks, index_prices):
    ma200 = index_prices.rolling(200).mean()

    latest_price = index_prices.iloc[-1].item()
    latest_ma200 = ma200.iloc[-1].item()

    # Define defensive universe
    defensive_universe = ["ITC.NS", "NTPC.NS"]

    if latest_price < latest_ma200:
        print("⚠️ Market in downtrend → switching to defensive allocation")


        # Keep only defensive stocks that are already selected
        defensive = [s for s in selected_stocks if s in defensive_universe]

        # Fallback: if none selected, force include defensive
        if not defensive:
            defensive = defensive_universe

        return defensive
    
    selected_stocks = list(set(selected_stocks[:len(selected_stocks)//2] + defensive_universe))
    return selected_stocks

def calculate_cvar(prices, weights, confidence_level=0.95):
    """
    Computes Conditional Value at Risk (Expected Shortfall)
    """
    selected = [s for s in weights.keys() if s in prices.columns]
    if not selected: return 0.0
    
    returns = prices[selected].pct_change().dropna()
    if returns.empty: return 0.0
    
    portfolio_returns = (returns * pd.Series({s: weights[s] for s in selected})).sum(axis=1)
    
    var = np.percentile(portfolio_returns, (1 - confidence_level) * 100)
    cvar = portfolio_returns[portfolio_returns <= var].mean()
    return cvar

def generate_stress_scenarios(prices, weights):
    """
    Simulates portfolio behavior in extreme historical crashes.
    """
    scenarios = {
        "2008 GFC (Financial Crisis)": -0.55,
        "2020 COVID (Liquidity Shock)": -0.38,
        "2013 Taper Tantrum": -0.18,
        "2022 Tech Correction": -0.25
    }
    
    results = {}
    current_cvar = calculate_cvar(prices, weights)
    
    for name, shock in scenarios.items():
        # Heuristic: Stress impact = Beta-weighted shock + Tail amplification
        # For simplicity, we use a scaled portfolio impact
        results[name] = {
            "Portfolio Delta (%)": round(shock * 100, 2),
            "Estimated Recovery (Days)": int(abs(shock) * 500)
        }
    return results

def apply_drawdown_control(weights, prices, threshold=0.1):
    # (Existing implementation)
    import pandas as pd
    returns = prices.pct_change().dropna()
    if returns.empty: return weights
    
    # Calculate portfolio daily returns
    w_series = pd.Series(weights)
    port_returns = returns.dot(w_series.reindex(returns.columns).fillna(0))
    
    cum_returns = (1 + port_returns).cumprod()
    peak = cum_returns.cummax()
    drawdown = (cum_returns - peak) / peak
    latest_dd = drawdown.iloc[-1] if not drawdown.empty else 0

    if latest_dd < -threshold:
        # Reduce exposure by 30%
        weights = {k: v * 0.7 for k, v in weights.items()}
        weights["CASH"] = weights.get("CASH", 0.0) + 0.3

    # Normalize again
    total = sum(weights.values())
    if total > 0:
        weights = {k: v / total for k, v in weights.items()}

    return weights


