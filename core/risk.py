import numpy as np

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

def compute_drawdown(prices):
    cum_returns = (1 + prices.pct_change()).cumprod()
    peak = cum_returns.cummax()
    drawdown = (cum_returns - peak) / peak
    return drawdown.iloc[-1]


def apply_drawdown_control(weights, prices, threshold=0.1):
    drawdown = compute_drawdown(prices.mean(axis=1))

    if drawdown < -threshold:
        # Reduce exposure by 30%
        weights = {k: v * 0.7 for k, v in weights.items()}

    # Normalize again
    total = sum(weights.values())
    weights = {k: v / total for k, v in weights.items()}

    return weights


