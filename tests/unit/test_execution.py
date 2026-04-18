import pytest
import pandas as pd
from core.execution import calculate_portfolio_value, generate_trade_list

@pytest.fixture
def dummy_prices():
    return {
        "RELIANCE.NS": 2000.0,
        "TCS.NS": 3000.0,
        "INFY.NS": 1500.0
    }

def test_calculate_portfolio_value(dummy_prices):
    holdings = [
        {"Ticker": "RELIANCE.NS", "Qty_LongTerm": 10, "Qty_ShortTerm": 5}, # 15 * 2000 = 30000
        {"Ticker": "TCS.NS", "Qty_LongTerm": 0, "Qty_ShortTerm": 10}       # 10 * 3000 = 30000
    ]
    
    val = calculate_portfolio_value(holdings, dummy_prices, fresh_capital=10000.0)
    assert val == 70000.0

def test_generate_trade_list_buy_and_sell(dummy_prices):
    holdings = [
        {"Ticker": "RELIANCE.NS", "Qty_LongTerm": 10, "Qty_ShortTerm": 0}, # Worth 20000
        {"Ticker": "INFY.NS", "Qty_LongTerm": 0, "Qty_ShortTerm": 20}      # Worth 30000 (Short term)
    ]
    
    # Portfolio total = 50000. No fresh capital.
    # Targets: RELIANCE (80%), TCS (20%), INFY (0%)
    # Target Values: RELIANCE (40000), TCS (10000), INFY (0)
    # Target Quantities: RELIANCE (20), TCS (3, since 10000/3000=3.33 -> 3)
    
    targets = {
        "RELIANCE.NS": 0.80,
        "TCS.NS": 0.20,
        "INFY.NS": 0.00
    }
    
    df_trades = generate_trade_list(targets, holdings, dummy_prices, fresh_capital=0.0)
    
    assert not df_trades.empty
    
    # Check Reliance Buy
    rel_trade = df_trades[df_trades["Stock"] == "RELIANCE.NS"].iloc[0]
    assert rel_trade["Action"] == "BUY"
    assert rel_trade["Shares"] == 10
    
    # Check TCS Buy
    tcs_trade = df_trades[df_trades["Stock"] == "TCS.NS"].iloc[0]
    assert tcs_trade["Action"] == "BUY"
    assert tcs_trade["Shares"] == 3
    
    # Check INFY Sell + Tax Warning
    infy_trade = df_trades[df_trades["Stock"] == "INFY.NS"].iloc[0]
    assert infy_trade["Action"] == "SELL"
    assert infy_trade["Shares"] == 20
    assert "STCG" in infy_trade["Tax Indicator"]
