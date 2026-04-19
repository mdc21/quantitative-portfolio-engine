import pytest
import pandas as pd
import numpy as np
from core.tactical import calculate_rsi, compute_tactical_audit

def test_rsi_calculation():
    # Create a simple upward trending price series
    prices = pd.Series([100, 101, 102, 103, 104, 105, 106, 107, 108, 109, 110, 111, 112, 113, 114, 115])
    rsi = calculate_rsi(prices, period=14)
    
    assert len(rsi) == len(prices)
    assert rsi.iloc[-1] > 50  # Upward trend should have RSI > 50
    assert not np.isnan(rsi.iloc[-1])

def test_tactical_audit_uptrend():
    # Moderate uptrend with noise to keep RSI around 50-60
    np.random.seed(42)
    dates = pd.date_range(start="2023-01-01", periods=250)
    noise = np.random.normal(0, 0.5, 250)
    prices = pd.Series(100 + np.linspace(0, 10, 250) + noise, index=dates)
    
    audit = compute_tactical_audit(prices)
    assert audit["Trend"] == "Strong Uptrend"
    # RSI should be moderate (< 75), so Grade should be A or B
    assert "A" in audit["Grade"] or "B" in audit["Grade"]
    assert audit["Execution"] == "Staggered"

def test_tactical_audit_overbought():
    # Parabolic move to create overbought RSI
    prices = pd.Series([100]*100 + [110, 120, 130, 150, 200, 300], index=pd.date_range(start="2023-01-01", periods=106))
    
    audit = compute_tactical_audit(prices)
    assert audit["RSI"] > 70
    assert "C" in audit["Grade"] or "Bulk" == audit["Execution"]

def test_tactical_audit_downtrend():
    # Strong downtrend: Price < SMA50 < SMA200
    dates = pd.date_range(start="2023-01-01", periods=250)
    prices = pd.Series(np.linspace(200, 100, 250), index=dates)
    
    audit = compute_tactical_audit(prices)
    assert audit["Trend"] == "Strong Downtrend"
    assert "D" in audit["Grade"]
    assert audit["Execution"] == "Bulk"
