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

def test_apply_cap_size_constraints(mock_cap_map, mock_sector_map):
    weights = {"RELIANCE.NS": 0.90, "TCS.NS": 0.10}
    cap_large_limit = 0.70
    regime = {"rate_trend": "flat"}
    
    # RELIANCE is "Others" (10% max). Large Cap alloc within Others is 10% * 0.70 = 7%
    constrained = apply_cap_size_constraints(weights, mock_cap_map, mock_sector_map, regime, cap_large_limit, 0.20, 0.10)
    
    assert constrained["RELIANCE.NS"] <= 0.071
    assert constrained["CASH"] >= 0.80  # Vast overflow because Sector limit severely truncates the 90% allocation

def test_clean_weights_enforces_floor():
    from core.optimizer import clean_weights
    # Case: Multiple dust positions
    weights = {
        "RELIANCE.NS": 0.50,
        "TCS.NS": 0.49,
        "DUST.NS": 0.005,  # 0.5% (Below 1% floor)
        "CRUMB.NS": 0.005   # 0.5% (Below 1% floor)
    }
    
    cleaned = clean_weights(weights, min_weight=0.01)
    
    # RELIANCE and TCS should remain, DUST and CRUMB should move to CASH
    assert "DUST.NS" not in cleaned
    assert "CRUMB.NS" not in cleaned
    assert cleaned["CASH"] == pytest.approx(0.01)
    assert cleaned["RELIANCE.NS"] == 0.50
    assert abs(sum(cleaned.values()) - 1.0) < 0.0001

def test_apply_asset_class_constraints():
    from core.optimizer import apply_asset_class_constraints
    
    weights = {
        "RELIANCE.NS": 0.50,
        "GOLDBEES.NS": 0.30,
        "NIFTYBEES.NS": 0.20
    }
    
    asset_map = {"RELIANCE.NS": "Equity", "GOLDBEES.NS": "ETF", "NIFTYBEES.NS": "ETF"}
    under_map = {"RELIANCE.NS": "Equity", "GOLDBEES.NS": "Metal", "NIFTYBEES.NS": "Equity"}
    
    constrained = apply_asset_class_constraints(weights, asset_map, under_map, equity_target=0.60, metal_cap=0.05)
    
    # RELIANCE target is 50%, has 60% room, so it stays 50%
    assert constrained["RELIANCE.NS"] == 0.50
    # NIFTYBEES processed first (0.20), passive has 40% room, gets full 0.20
    # GOLDBEES processed next (0.30), metal cap is 5% global, so it gets truncated to 0.05
    # Total assigned = 0.50 + 0.20 + 0.05 = 0.75
    # Cash = 0.25 (which is the 0.25 cut from Goldbees)
    
    # Wait, sorted order: RELIANCE(0.50) -> GOLDBEES(0.30) -> NIFTYBEES(0.20)
    # GOLDBEES gets processed first among passives.
    # GOLDBEES asks for 0.30. Metal cap=0.05, Passive room=0.40. It gets min(0.30, 0.40, 0.05) = 0.05.
    # NIFTYBEES asks for 0.20. Metal_cap=inf, Passive room=0.40 - 0.05 = 0.35. It gets min(0.20, 0.35, inf) = 0.20.
    assert constrained["GOLDBEES.NS"] == 0.05
    assert constrained["NIFTYBEES.NS"] == 0.20
    assert constrained["CASH"] == 0.25
