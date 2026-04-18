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
    assert rel_trade["Action"] == "🟢 BUY"
    assert rel_trade["Shares"] == 10
    
    # Check TCS Buy
    tcs_trade = df_trades[df_trades["Stock"] == "TCS.NS"].iloc[0]
    assert tcs_trade["Action"] == "🟢 BUY"
    assert tcs_trade["Shares"] == 3
    
    # Check INFY Sell + Tax Warning
    infy_trade = df_trades[df_trades["Stock"] == "INFY.NS"].iloc[0]
    assert infy_trade["Action"] == "🔴 SELL"
    assert infy_trade["Shares"] == 20
    assert "STCG" in infy_trade["Tax Indicator"]

def test_generate_trade_list_messy_csv_parsing(dummy_prices):
    """
    Tests the engine's ability to handle raw spaces, no .NS suffixes, and lowercase keys.
    """
    messy_holdings = [
        {" Ticker  ": "RELIANCE", "qty_longterm": 10, " Qty_ShortTerm ": 5}, # Expected: RELIANCE.NS, 15 qty
        {"ticker": "tcs", "Qty_LongTerm": 10, "QTY_SHORTTERM": 0}            # Expected: TCS.NS, 10 qty
    ]
    
    # Value = (15 * 2000) + (10 * 3000) = 60000. 
    # Target 50% / 50% -> 30000 each.
    # RELIANCE target: 30000/2000 = 15 shares. Current 15. Action: HOLD/None.
    # TCS target: 30000/3000 = 10 shares. Current 10. Action: HOLD/None.
    
    targets = {
        "RELIANCE.NS": 0.50,
        "TCS.NS": 0.50
    }
    
    val = calculate_portfolio_value(messy_holdings, dummy_prices)
    assert val == 60000.0, "Failed to parse messy formats into correct valuation"
    
    df_trades = generate_trade_list(targets, messy_holdings, dummy_prices)
    
    # Since they perfectly match, there should be no trades needed!
    assert df_trades.empty, "Generated dummy trades when perfect alignment existed"

def test_generate_trade_list_unresolved_ticker(dummy_prices):
    """
    Tests that significantly deformed tickers failing heuristic resolution output 'Not Available' instead of a pure sell.
    """
    messy_holdings = [
        {"stock_symbol": "OBSCUREBROKERNAME", "isin_name": "INE12345678", "qty_longterm": 100, "qty_shortterm": 0}
    ]
    
    # Target some valid stock
    targets = {
        "RELIANCE.NS": 1.0
    }
    
    df_trades = generate_trade_list(targets, messy_holdings, dummy_prices, fresh_capital=10000.0)
    
    # We expect a row for the obscure broker name
    assert not df_trades.empty
    assert "Group" in df_trades.columns
    
    obscure_trade = df_trades[df_trades["Action"] == "⚪ N/A"].iloc[0]
    assert "OBSCUREBROKERNAME" in obscure_trade["Stock"]
    assert obscure_trade["Shares"] == 100

def test_generate_trade_list_empty_holdings(dummy_prices):
    """
    Verifies that the engine defaults to full fresh capital allocation when no holdings are provided.
    """
    empty_holdings = []
    targets = {
        "RELIANCE.NS": 0.50,
        "TCS.NS": 0.50
    }
    
    # Capital: 100000. Targets: 50% each (50000).
    # RELIANCE: 50000 / 2000 = 25 shares.
    # TCS: 50000 / 3000 = 16.66 -> 16 shares.
    
    df_trades = generate_trade_list(targets, empty_holdings, dummy_prices, fresh_capital=100000.0)
    
    assert len(df_trades) == 2
    assert all(df_trades["Action"] == "🟢 BUY")
    assert all(df_trades["Group"] == "Buy Orders")
    
    rel_trade = df_trades[df_trades["Stock"] == "RELIANCE.NS"].iloc[0]
    assert rel_trade["Shares"] == 25
    
    tcs_trade = df_trades[df_trades["Stock"] == "TCS.NS"].iloc[0]
    assert tcs_trade["Shares"] == 16

def test_generate_trade_list_grouping_logic(dummy_prices):
    """
    Verifies the specific labeling for Strategic Exits, Rebalance Trims, and Buy Orders.
    """
    holdings = [
        {"Ticker": "RELIANCE.NS", "Qty_LongTerm": 100, "Qty_ShortTerm": 0}, # Overweight
        {"Ticker": "INFY.NS", "Qty_LongTerm": 50, "Qty_ShortTerm": 0}        # Rejected / Outside Universe
    ]
    
    # Total Value: (100*2000) + (50*1500) = 200000 + 75000 = 275000.
    # New Targets: 
    # RELIANCE: 10% (Worth 27500 -> 13.75 -> 13 shares). SELL 87.
    # TCS: 90% (Worth 247500 -> 82.5 -> 82 shares). BUY 82.
    # INFY: 0% (SELL 50).
    
    targets = {
        "RELIANCE.NS": 0.10,
        "TCS.NS": 0.90
    }
    
    df_trades = generate_trade_list(targets, holdings, dummy_prices)
    
    # RELIANCE should be a "Rebalance Trim" as target weight is > 0.01%
    rel_trade = df_trades[df_trades["Stock"] == "RELIANCE.NS"].iloc[0]
    assert rel_trade["Group"] == "Rebalance Trims"
    assert rel_trade["Action"] == "🔴 SELL"
    
    # INFY should be "Unresolved Assets" since it's a dummy ticker that failed confident resolution
    infy_trade = df_trades[df_trades["Stock"].str.contains("INFY.NS")].iloc[0]
    assert infy_trade["Group"] == "Unresolved Assets"
    assert infy_trade["Action"] == "⚪ N/A"
    
    # TCS should be "Buy Orders"
    tcs_trade = df_trades[df_trades["Stock"] == "TCS.NS"].iloc[0]
    assert tcs_trade["Group"] == "Buy Orders"
    assert tcs_trade["Action"] == "🟢 BUY"
