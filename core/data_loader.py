import yfinance as yf
import pandas as pd

def fetch_prices(tickers, period="6mo"):
    fetch_list = list(tickers)
    if "^NSEI" not in fetch_list:
        fetch_list.append("^NSEI")
        
    data = yf.download(fetch_list, period=period, interval="1d")["Close"]
    data = data.dropna(axis=1, how='all')
    data = data.ffill().bfill()
    return data
