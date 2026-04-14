import yfinance as yf
import pandas as pd
from curl_cffi import requests

# Global session to mimic browser and bypass Yahoo Finance 401/Invalid Crumb errors
# This is critical for cloud environments like Streamlit Cloud
session = requests.Session(impersonate="chrome110")

def fetch_prices(tickers, period="6mo"):
    fetch_list = list(tickers)
    if "^NSEI" not in fetch_list:
        fetch_list.append("^NSEI")
        
    data = yf.download(fetch_list, period=period, interval="1d", session=session)["Close"]
    data = data.dropna(axis=1, how='all')
    data = data.ffill().bfill()
    return data

