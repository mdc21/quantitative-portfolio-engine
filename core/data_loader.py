import yfinance as yf
import pandas as pd
import numpy as np
from curl_cffi import requests

# Global session to mimic browser and bypass Yahoo Finance 401/Invalid Crumb errors
# This is critical for cloud environments like Streamlit Cloud
session = requests.Session(impersonate="chrome110")

def _is_network_available(timeout=3):
    """
    Fast-fail connectivity probe. Returns True only if Yahoo Finance 
    responds within `timeout` seconds. Prevents 130-ticker download 
    from hanging for 30+ minutes in restricted environments.
    """
    try:
        import urllib.request
        urllib.request.urlopen("https://query1.finance.yahoo.com", timeout=timeout)
        return True
    except Exception:
        return False

def _generate_synthetic_prices(fetch_list):
    """Generate realistic random-walk price data for simulation mode."""
    from core.logger import logger
    logger.info(f"🛡️ Simulation Mode: Generating synthetic prices for {len(fetch_list)} tickers.")
    dates = pd.date_range(end=pd.Timestamp.now().normalize(), periods=126, freq='B')
    synth_prices = {}
    for t in fetch_list:
        np.random.seed(hash(t) % 2**31)  # Stable per-ticker seed
        base = np.random.uniform(200, 5000)
        returns = np.random.normal(0.0005, 0.015, len(dates))
        synth_prices[t] = base * (1 + returns).cumprod()
    return pd.DataFrame(synth_prices, index=dates)

def fetch_prices(tickers, period="6mo"):
    from core.logger import logger
    fetch_list = list(tickers)
    if "^NSEI" not in fetch_list:
        fetch_list.append("^NSEI")
    
    # 🚀 Fast-fail: Check network BEFORE attempting a massive download
    if not _is_network_available():
        logger.warning("Network probe failed (3s timeout). Skipping yf.download entirely.")
        return _generate_synthetic_prices(fetch_list)
    
    data = pd.DataFrame()
    try:
        data = yf.download(fetch_list, period=period, interval="1d", session=session, progress=False)["Close"]
        data = data.dropna(axis=1, how='all')
        data = data.ffill().bfill()
    except Exception as e:
        logger.warning(f"Live market data fetch failed: {e}. Falling back to synthetic simulation data.")
        
    # 🛡️ Robust Fallback: Generate Synthetic Simulation Data if download returned empty
    if data.empty:
        return _generate_synthetic_prices(fetch_list)
        
    return data

