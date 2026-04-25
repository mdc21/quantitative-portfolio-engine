import numpy as np
import pandas as pd

def compute_momentum(prices, lookback=90):
    """
    Computes a blended multi-timeframe momentum with adaptive fallbacks for new listings.
    Blends: 30-day (20%), 90-day (50%), 180-day (30%)
    """
    if prices.empty or len(prices) < 2:
        return pd.Series(dtype=float)
        
    # Multi-timeframe pct_change
    # We use min_periods=1 to support stocks with shorter history
    mom_30 = prices.pct_change(min(30, len(prices)-1)).iloc[-1].rank(pct=True)
    mom_90 = prices.pct_change(min(90, len(prices)-1)).iloc[-1].rank(pct=True)
    mom_180 = prices.pct_change(min(180, len(prices)-1)).iloc[-1].rank(pct=True)
    
    # Fill gaps: if 180 is missing, use 90. If 90 is missing, use 30.
    mom_180 = mom_180.fillna(mom_90)
    mom_90 = mom_90.fillna(mom_30)
    mom_180 = mom_180.fillna(mom_90) # Final pass
        
    blended_mom = (mom_30 * 0.2) + (mom_90 * 0.5) + (mom_180 * 0.3)
    return blended_mom

def compute_volatility(prices, lookback=60):
    if prices.empty or len(prices) < 2:
        return pd.Series(dtype=float)
    # Use min_periods to support newer stocks
    return prices.pct_change().rolling(window=lookback, min_periods=5).std().iloc[-1]

def compute_factor_scores(prices, config):
    if prices.empty:
        return pd.DataFrame()
        
    # mom is already ranked natively in the new blended function
    mom_rank = compute_momentum(prices, config.get("momentum_lookback_days", 90))
    vol = compute_volatility(prices, config.get("volatility_lookback_days", 60))

    # Normalize Volatility
    vol_rank = vol.rank(pct=True)

    # Composite score: high momentum, low volatility (Normalized 0-1)
    # We use (1 - vol_rank) because we want low volatility for high quality
    score = (mom_rank + (1 - vol_rank)) / 2

    # Assemble Detailed DataFrame
    results = pd.DataFrame({
        "Stock": mom_rank.index,
        "Momentum_Rank": mom_rank.values,
        "Vol_Rank": vol_rank.values,
        "Stability_Rank": (1 - vol_rank.values),
        "Composite_Score": score.values
    }).set_index("Stock")

    return results.sort_values(by="Composite_Score", ascending=False)
