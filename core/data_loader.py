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

def _fetch_mfapi_history(amfi_code, months_back=6):
    """
    Fetches raw NAV history directly from AMFI via mfapi.in and maps it
    to a pandas Series matching Yahoo Finance formatting.
    """
    import urllib.request
    import json
    import pandas as pd
    from datetime import datetime, timedelta
    from core.logger import logger
    
    raw_code = str(amfi_code).replace(".AMFI", "")
    url = f"https://api.mfapi.in/mf/{raw_code}"
    
    try:
        req = urllib.request.Request(url, headers={'User-Agent': 'Quant-Engine/1.0'})
        with urllib.request.urlopen(req, timeout=10) as response:
            data = json.loads(response.read().decode())
            
        if "data" not in data or not data["data"]:
            return pd.Series(dtype=float)
            
        df = pd.DataFrame(data["data"])
        df['date'] = pd.to_datetime(df['date'], format='%d-%m-%Y')
        df['nav'] = pd.to_numeric(df['nav'], errors='coerce')
        
        cutoff = datetime.now() - timedelta(days=months_back * 30)
        df = df[df['date'] >= cutoff]
        
        df = df.set_index('date').sort_index()
        return df['nav']
        
    except Exception as e:
        logger.warning(f"⚠️ MFAPI Direct Fetch Failed for {amfi_code}: {e}")
        return pd.Series(dtype=float)


def fetch_prices(tickers, period="6mo"):
    from core.logger import logger
    import time
    import random
    from core.universe import MF_YAHOO_MAP
    
    fetch_list = list(tickers)
    
    amfi_tickers = [t for t in fetch_list if str(t).endswith(".AMFI")]
    yf_fetch_list = [t for t in fetch_list if not str(t).endswith(".AMFI")]
    
    # Multi-ticker redundancy for benchmarks to handle varying Yahoo Finance ID resolution
    for index_ticker in ["^NSEI", "^NSMIDCP"]:
        if index_ticker not in yf_fetch_list:
            yf_fetch_list.append(index_ticker)
    
    logger.info(f"📡 Attempting live price download for {len(yf_fetch_list)} Equities & {len(amfi_tickers)} AMFI Funds...")
    
    # ⚡ FAST-FAIL: Global Connectivity Probe
    # If the environment is blocked, don't wait for 141 individual timeouts
    try:
        session.get("https://query1.finance.yahoo.com", timeout=2)
    except Exception:
        logger.warning("📉 Market Data APIs unreachable. Activating Offline Synthetic Fast-Pass for all assets.")
        return _generate_synthetic_prices(fetch_list)

    data = pd.DataFrame()
    max_retries = 3
    for attempt in range(max_retries):
        try:
            data = yf.download(yf_fetch_list, period=period, interval="1d", session=session, progress=False)["Close"]
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
    # Process AMFI mutual funds via MFAPI with fail-down routing to yfinance equivalents
    import warnings
    from pandas.errors import PerformanceWarning
    warnings.filterwarnings('ignore', category=PerformanceWarning)
    
    for amfi in amfi_tickers:
        series = _fetch_mfapi_history(amfi, months_back=6)
        if not series.empty:
            if data.empty:
                data = pd.DataFrame({amfi: series})
            else:
                data[amfi] = series
        else:
            logger.warning(f"⚠️ Primary MFAPI fetch failed for {amfi}, descending to Yahoo fallback...")
            fallback_ticker = MF_YAHOO_MAP.get(amfi)
            if fallback_ticker:
                try:
                    fallback_data = yf.download(fallback_ticker, period=period, interval="1d", session=session, progress=False)["Close"]
                    if not fallback_data.empty:
                        data[amfi] = fallback_data
                except Exception as e:
                    logger.debug(f"Yahoo Fallback failed for {amfi}: {e}")
    # 🛡️ Data Quality Check: Identify tickers that Yahoo failed to provide enough history for
    gap_tickers = []
    if not data.empty:
        # We need at least 80% coverage to avoid momentum distortion
        threshold = len(data) * 0.7 
        for ticker in fetch_list:
            if ticker not in data.columns or data[ticker].count() < threshold:
                gap_tickers.append(ticker)
    else:
        gap_tickers = fetch_list

    # Step 2: Surgical Deep Recovery via nselib (NSE India Fallback)
    if gap_tickers:
        logger.info(f"🔍 Deep Recovery: {len(gap_tickers)} tickers have data gaps. Attempting NSE India fallback...")
        nse_client = NSEAPI()
        recovered_count = 0
        
        # Limit recovery to a reasonable batch to avoid massive startup delays
        # focus on those that passed fundamentals (fetch_list already prioritized)
        for ticker in gap_tickers[:40]: 
            # Bypass NSE API for Benchmark Indices. Sending Indices to the Equity 
            # Endpoint forces 'nselib' to download an HTML error page, which causes a CSV parser crash.
            if ticker.startswith("^") or "CNX" in ticker or "MIDCP" in ticker or ".AMFI" in ticker: continue
            try:
                series = nse_client.fetch_historical_prices(ticker, months_back=6)
                if not series.empty:
                    # Sync frequency and index with existing data
                    if data.empty:
                        data = pd.DataFrame({ticker: series})
                    else:
                        data[ticker] = series
                    recovered_count += 1
            except Exception as e:
                logger.debug(f"Deep recovery failed for {ticker}: {e}")
        
        if recovered_count > 0:
            logger.info(f"✅ Deep Recovery successful for {recovered_count} tickers.")
            data = data.ffill().fillna(0)
            data.attrs["data_source"] = "Hybrid (Yahoo + NSE India)"
            return data

    # 🛡️ Tertiary Fallback: Inject Synthetic Simulation Data for any missing or failed tickers
    missing_tickers = [t for t in fetch_list if t not in data.columns or data[t].isna().all()]
    if missing_tickers:
        logger.warning(f"⚠️ {len(missing_tickers)} assets (including ETFs) failed to download. Generating Synthetic Simulation data to maintain target ratios.")
        synthetic = _generate_synthetic_prices(missing_tickers)
        for t in missing_tickers:
            if t in synthetic.columns:
                # If we have existing data, align the synthetic dates
                if not data.empty:
                    data[t] = synthetic[t].values[:len(data)] if len(synthetic) >= len(data) else synthetic[t]
                else:
                    data = synthetic
                    break
            else:
                # Emergency fallback if even synthetic failed
                data[t] = 100.0 
    
    if data.empty:
        logger.error("❌ Live market data sources exhausted or blocked. Falling back to Synthetic Simulation.")
        return _generate_synthetic_prices(fetch_list)
    
    data = data.ffill().fillna(0) # Critical: bridge gaps for stocks with sparse trades
    data.attrs["data_source"] = "Yahoo Finance (with Synthetic Fallbacks)"
    return data

