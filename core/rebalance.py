def rebalance_portfolio(capital, weights, latest_prices):
    allocation = {}

    for stock, w in weights.items():
        if stock == "CASH":
             allocation["CASH"] = {"weight": w, "price": 1.0, "shares": int(capital * w), "invested": capital * w}
             continue
             
        amount = capital * w
        # Handle cases where stock is in weights but missing from current price feed (e.g. legacy holding)
        price = latest_prices.get(stock, 100.0) 
        shares = int(amount / price) if price > 0 else 0

        allocation[stock] = {
            "weight": w,
            "price": price,
            "shares": shares,
            "invested": shares * price
        }

    return allocation
