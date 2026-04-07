def rebalance_portfolio(capital, weights, latest_prices):
    allocation = {}

    for stock, w in weights.items():
        amount = capital * w
        price = latest_prices[stock]
        shares = int(amount / price)

        allocation[stock] = {
            "weight": w,
            "price": price,
            "shares": shares,
            "invested": shares * price
        }

    return allocation
