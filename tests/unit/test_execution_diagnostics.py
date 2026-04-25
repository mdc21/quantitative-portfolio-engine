import pytest
import pandas as pd
from core.execution import generate_trade_list

def test_generate_trade_list_skips_insufficient_capital():
    # Capital: 1000 INR
    # Stock Price: 1200 INR (1 share costs more than total capital)
    weights = {"TITAN.NS": 1.0}
    holdings = []
    prices = pd.DataFrame({"TITAN.NS": [1200, 1200]})
    fresh_capital = 1000.0
    
    df_trades, skipped_report = generate_trade_list(weights, holdings, prices, fresh_capital)
    
    assert df_trades.empty
    assert len(skipped_report) == 1
    assert "Insufficient Capital" in skipped_report[0]["Reason"]
    assert skipped_report[0]["Stock"] == "TITAN.NS"

def test_generate_trade_list_skips_missing_price():
    # Stock in weights but missing from prices DataFrame
    weights = {"RELIANCE.NS": 0.5, "TCS.NS": 0.5}
    holdings = []
    prices = pd.DataFrame({"RELIANCE.NS": [2500, 2500]}) # TCS.NS missing
    fresh_capital = 100000.0
    
    df_trades, skipped_report = generate_trade_list(weights, holdings, prices, fresh_capital)
    
    # Reliance should be bought
    assert not df_trades.empty
    assert "RELIANCE.NS" in df_trades["Stock"].values
    
    # TCS.NS should be in skipped_report
    assert any("TCS.NS" == s["Stock"] for s in skipped_report)
    assert any("Missing" in s["Reason"] for s in skipped_report)

def test_generate_trade_list_handles_ffill_prices():
    # Last price is NaN, should use ffill logic if integrated (or fail gracefully)
    import numpy as np
    weights = {"RELIANCE.NS": 1.0}
    holdings = []
    # DataFrame with trailing NaN
    prices = pd.DataFrame({"RELIANCE.NS": [2500, np.nan]})
    
    # Note: data_loader.py handles ffill, but generate_trade_list receives the DF.
    # If the DF still has NaN at the end, it should skip.
    df_trades, skipped_report = generate_trade_list(weights, holdings, prices, 10000.0)
    
    assert df_trades.empty
    assert any("Missing" in s["Reason"] for s in skipped_report)
