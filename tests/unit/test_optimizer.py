import pytest
import pandas as pd
from core.optimizer import optimize_weights, apply_sector_weight_constraints, apply_cap_size_constraints

def test_optimize_weights_markowitz(mock_prices, mock_sector_map, mock_cap_map):
    regime = {"optimization_mode": "Markowitz"}
    selected = ["RELIANCE.NS", "TCS.NS", "HDFCBANK.NS"]
    prices = mock_prices[selected]
    
    limits = {
        "cap_large": 0.70,
        "cap_mid": 0.20,
        "cap_small": 0.10,
        "category_caps": {"Energy": 0.30, "Technology": 0.30, "Financial Services": 0.30}
    }
    
    weights = optimize_weights(prices, selected, regime, mock_sector_map, mock_cap_map, limits)
    
    assert isinstance(weights, dict)
    # The raw optimizer should attempt full allocation (1.0)
    assert abs(sum(weights.values()) - 1.0) < 0.001
    assert all(w >= 0 for w in weights.values())

def test_apply_sector_weight_constraints(mock_sector_map):
    weights = {"TCS.NS": 0.40, "INFY.NS": 0.40, "RELIANCE.NS": 0.20}
    regime = {"rate_trend": "stable"}
    
    # Tech cap is 0.20 in optimizer.py
    constrained = apply_sector_weight_constraints(weights, mock_sector_map, regime)
    
    tech_weight = constrained.get("TCS.NS", 0) + constrained.get("INFY.NS", 0)
    assert tech_weight <= 0.2001
    assert constrained["CASH"] > 0

def test_apply_cap_size_constraints(mock_cap_map):
    weights = {"RELIANCE.NS": 0.90}
    cap_large_limit = 0.70
    
    constrained = apply_cap_size_constraints(weights, mock_cap_map, cap_large_limit, 0.20, 0.10)
    
    assert constrained["RELIANCE.NS"] <= 0.7001
    assert constrained["CASH"] >= 0.20
