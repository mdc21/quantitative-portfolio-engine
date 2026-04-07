import pandas as pd

def load_macro_data():
    # You can replace with API later
    repo = pd.read_csv("data/repo_rate.csv", parse_dates=["Date"])
    cpi = pd.read_csv("data/cpi.csv", parse_dates=["Date"])

    repo.set_index("Date", inplace=True)
    cpi.set_index("Date", inplace=True)

    return repo, cpi


def compute_macro_regime(repo, cpi):
    repo = repo.resample("D").ffill()   # 👈 critical fix

    repo_trend = repo["Rate"].diff().iloc[-1]
    inflation = cpi["CPI"].iloc[-1]

    regime = {
        "rate_trend": "rising" if repo_trend > 0 else "falling",
        "inflation": "high" if inflation > 6 else "moderate"
    }
    print("Latest Repo Rate:", repo["Rate"].iloc[-1])
    return regime