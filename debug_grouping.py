import pandas as pd
from core.execution import generate_trade_list

# Setup dummy data
target_weights = {
    "TCS.NS": 0.5,      # Rebalance/Buy test
    "RELIANCE.NS": 0.0  # Liquidation test
}

# Current holdings
# 1. TCS - Own 10, Target is higher -> BUY
# 2. RELIANCE - Own 10, Target is 0 -> Strategic Exit
# 3. MARICO - Not in targets -> Strategic Exit
holdings = [
    {"stock_symbol": "TCS", "qty_longterm": 10},
    {"stock_symbol": "RELIANCE", "qty_longterm": 10},
    {"stock_symbol": "MARICO", "qty_longterm": 10}
]

# Prices
prices = {
    "TCS.NS": 4000.0,
    "RELIANCE.NS": 2500.0,
    "MARICO.NS": 600.0
}

# Assessed universe
all_assessed = ["TCS.NS", "RELIANCE.NS", "MARICO.NS"]

df = generate_trade_list(target_weights, holdings, prices, fresh_capital=100000.0, assessed_tickers=all_assessed)

print(df[["Stock", "Action", "Group", "Shares"]])
