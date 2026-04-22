import yfinance as yf
import pandas as pd
import numpy as np
from curl_cffi import requests
from core.nse_api import NSEAPI

# Global session to mimic browser and bypass Yahoo Finance 401/Invalid Crumb errors
# Hardened with secondary headers to avoid bot detection
session = requests.Session(impersonate="chrome120")
session.headers.update({
    "Accept": "*/*",
    "Accept-Language": "en-US,en;q=0.9",
    "Origin": "https://finance.yahoo.com",
    "Referer": "https://finance.yahoo.com",
    "User-Agent": "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/120.0.0.0 Safari/537.36"
})

def _is_network_available(timeout=5):
    """
    Fast-fail connectivity probe using the same curl_cffi session as yfinance.
    This ensures consistency: if fundamentals can be fetched, prices can too.
    """
    try:
        response = session.get("https://query1.finance.yahoo.com", timeout=timeout)
        return response.status_code in [200, 301, 302, 403]  # Any response = network works
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
    data = pd.DataFrame(synth_prices, index=dates)
    data.attrs["data_source"] = "Synthetic Simulation"
    return data

def fetch_prices(tickers, period="6mo"):
    from core.logger import logger
    import time
    import random
    
    fetch_list = list(tickers)
    if "^NSEI" not in fetch_list:
        fetch_list.append("^NSEI")
    
    logger.info(f"📡 Attempting live price download for {len(fetch_list)} tickers...")
    
    data = pd.DataFrame()
    max_retries = 3
    for attempt in range(max_retries):
        try:
            data = yf.download(fetch_list, period=period, interval="1d", session=session, progress=False)["Close"]
            if not data.empty:
                break
            logger.warning(f"Attempt {attempt+1}: Download returned empty data. Retrying...")
        except Exception as e:
            if "401" in str(e) or "Unauthorized" in str(e):
                wait_time = (2 ** attempt) + random.uniform(0.1, 1.0)
                logger.warning(f"⚠️ 401 Unauthorized detected. Backing off for {wait_time:.2f}s (Attempt {attempt+1}/{max_retries})")
                time.sleep(wait_time)
            else:
                logger.warning(f"Live market data fetch failed: {e}.")
                break
        
    # Secondary Fallback: NSE India API
    if data.empty:
        logger.warning("⚠️ Yahoo Finance failed completely. Falling back to official NSE API for historical data...")
        nse_client = NSEAPI()
        
        nse_data_frames = {}
        for ticker in fetch_list:
            if ticker == "^NSEI": continue # Skip index for now, complex NSE mapping
            
            series = nse_client.fetch_historical_prices(ticker, months_back=6)
            if not series.empty:
                nse_data_frames[ticker] = series
                
        if nse_data_frames:
            data = pd.DataFrame(nse_data_frames)
            data = data.ffill().bfill()
            data.attrs["data_source"] = "NSE India API (Fallback)"
            return data

    # 🛡️ Tertiary Fallback: Generate Synthetic Simulation Data if download returned empty
    if data.empty:
        logger.error("❌ Live market data sources exhausted or blocked. Falling back to Synthetic Simulation.")
        return _generate_synthetic_prices(fetch_list)
    
    data.attrs["data_source"] = "Yahoo Finance (Live)"
    return data

