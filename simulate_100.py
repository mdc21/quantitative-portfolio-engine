import pandas as pd
import numpy as np
import core.universe
from core.universe import apply_fundamental_filters
from core.data_loader import fetch_prices
from core.factors import compute_factor_scores
from core.momentum import select_top_momentum, apply_sector_caps
from core.optimizer import optimize_weights

def simulate_100_percent():
    print("🚀 Simulating 100% Quality Cutoff Pipeline...")
    
    # 1. Universe with Fallback (Manual trigger)
    broad_universe = core.universe.fetch_broad_universe()
    tickers, sector_map, cap_map, scoring_df = apply_fundamental_filters(broad_universe, top_percentile=1.0)
    print(f"Survivors: {len(tickers)}")
    
    # 2. Prices (Synthetic Fallback)
    prices = fetch_prices(tickers)
    print(f"Price Matrix Shape: {prices.shape}")
    
    # 3. Factors
    buy_list_prices = prices[[t for t in tickers if t in prices.columns]]
    print(f"Factor Input Shape: {buy_list_prices.shape}")
    
    scores = compute_factor_scores(buy_list_prices, {"momentum_lookback_days": 90, "volatility_lookback_days": 60})
    selected = select_top_momentum(scores, top_percent=1.0)
    print(f"Selected for Optimization: {len(selected)}")
    
    # 4. Optimization (The likely crash point)
    regime = {"optimization_mode": "Markowitz"}
    limits = {
        "cap_large": 0.7, "cap_mid": 0.2, "cap_small": 0.1,
        "category_caps": {"Others": 0.1, "Technology": 0.2, "Financials": 0.3}
    }
    
    try:
        weights = optimize_weights(prices, selected, regime, sector_map, cap_map, limits)
        print(f"Optimization Successful. Weights: {len(weights)}")
    except Exception as e:
        import traceback
        print(f"❌ Optimization Failed!")
        traceback.print_exc()

if __name__ == "__main__":
    simulate_100_percent()
