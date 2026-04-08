import pandas as pd

def load_macro_data():
    # You can replace with API later
    repo = pd.read_csv("data/repo_rate.csv", parse_dates=["Date"])
    cpi = pd.read_csv("data/cpi.csv", parse_dates=["Date"])

    repo.set_index("Date", inplace=True)
    cpi.set_index("Date", inplace=True)

    return repo, cpi


def compute_macro_regime(repo, cpi, prices=None):
    repo = repo.resample("D").ffill()   # 👈 critical fix

    repo_trend = repo["Rate"].diff().iloc[-1]
    inflation = cpi["CPI"].iloc[-1]

    regime = {
        "rate_trend": "rising" if repo_trend > 0 else "falling",
        "inflation": "high" if inflation > 6 else "moderate",
        "optimization_mode": "Markowitz" # Default assumption
    }
    
    # 📌 Mathematical Regime Detection
    if prices is not None and not prices.empty:
        # Proxy the market index natively using equal weight across active universe
        market_proxy = prices.pct_change().dropna().mean(axis=1)
        
        recent_vol = market_proxy.tail(20).std()
        historical_vol = market_proxy.std()
        
        recent_trend = market_proxy.tail(20).mean()
        
        # If the market is abnormally volatile or breaking down structurally, trigger the HRP Shield
        if recent_vol > (historical_vol * 1.3) or recent_trend < -0.0005:
            regime["optimization_mode"] = "HRP"
            print("🚨 High Volatility Detected: Switching to HRP Engine.")
        else:
            regime["optimization_mode"] = "Markowitz"

    return regime