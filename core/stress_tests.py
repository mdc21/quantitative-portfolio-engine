import pandas as pd
import numpy as np
from core.logger import logger

def run_stress_scenarios(prices, weights):
    """
    Simulates portfolio performance under extreme market conditions.
    """
    if not weights or "CASH" in weights and len(weights) == 1:
        return {}

    invested_tickers = [t for t in weights.keys() if t != "CASH"]
    if not invested_tickers:
        return {}
        
    # Get recent returns for baseline volatility
    returns = prices[invested_tickers].pct_change().tail(30).dropna(how='all')
    if returns.empty:
        return {}

    scenarios = {
        "Market Crash (-20%)": {"Equity": -0.20, "Metal": -0.10, "Cash": 0.0},
        "Tech Rout (-15%)": {"Technology": -0.15, "Other": -0.05},
        "Interest Rate Spike (+2%)": {"Financials": 0.05, "Industrials_Infra": -0.10, "Other": -0.02},
        "Commodity Supercycle": {"Metal": 0.15, "Equity": -0.02}
    }

    stress_results = {}
    
    # We need a ticker to sector/underlying mapping here, but since we are in a scratch script,
    # we'll use a simplified version. In the real app, we pass these maps.
    
    for name, impact in scenarios.items():
        impact_multiplier = pd.Series(0.0, index=invested_tickers)
        
        for t in invested_tickers:
            # Simplified classification for stress test
            if "BEES" in t or "AMFI" in t:
                if "GOLD" in t or "SILVER" in t:
                    impact_multiplier[t] = impact.get("Metal", impact.get("Other", 0.0))
                else:
                    impact_multiplier[t] = impact.get("Equity", impact.get("Other", 0.0))
            else:
                impact_multiplier[t] = impact.get("Equity", impact.get("Other", 0.0))
        
        # Calculate weighted impact
        portfolio_impact = sum(weights[t] * impact_multiplier[t] for t in invested_tickers)
        stress_results[name] = portfolio_impact

    return stress_results
