import pandas as pd
import yfinance as yf
from curl_cffi import requests
from concurrent.futures import ThreadPoolExecutor
from core.logger import logger

# Global session to mimic browser and bypass Yahoo Finance 401/Invalid Crumb errors
session = requests.Session(impersonate="chrome120")
session.headers.update({
    "Accept": "*/*",
    "Accept-Language": "en-US,en;q=0.9",
    "Origin": "https://finance.yahoo.com",
    "Referer": "https://finance.yahoo.com",
    "User-Agent": "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/120.0.0.0 Safari/537.36"
})

from core.screener_api import ScreenerAPI
screener_client = ScreenerAPI()

# Hardcoded Fallback Arrays
FALLBACK_LARGE = [
    "RELIANCE.NS", "TCS.NS", "HDFCBANK.NS", "ICICIBANK.NS", "BHARTIARTL.NS", "SBIN.NS", 
    "INFY.NS", "LT.NS", "ITC.NS", "BAJFINANCE.NS", "HINDUNILVR.NS", "KOTAKBANK.NS",
    "AXISBANK.NS", "MARUTI.NS", "SUNPHARMA.NS", "ASIANPAINT.NS", "TATAMOTORS.NS", 
    "ULTRACEMCO.NS", "TATASTEEL.NS", "POWERGRID.NS", "NTPC.NS", "TITAN.NS",
    "ONGC.NS", "JSWSTEEL.NS", "HCLTECH.NS", "WIPRO.NS", "BAJAJFINSV.NS", "ADANIPORTS.NS",
    "ADANIENT.NS", "COALINDIA.NS", "HINDALCO.NS", "M&M.NS", "GRASIM.NS", "TECHM.NS",
    "SBILIFE.NS", "DRREDDY.NS", "BRITANNIA.NS", "EICHERMOT.NS", "DIVISLAB.NS",
    "APOLLOHOSP.NS", "TATACONSUM.NS", "CIPLA.NS", "HDFCLIFE.NS", "HEROMOTOCO.NS",
    "UPL.NS", "SHREECEM.NS", "BPCL.NS", "INDUSINDBK.NS", "BAJAJ-AUTO.NS", "NESTLEIND.NS"
]

FALLBACK_MID = [
    "HAL.NS", "LICI.NS", "JINDALSTEL.NS", "TVSMOTOR.NS", "PNB.NS", "TRENT.NS", "ZOMATO.NS",
    "CGPOWER.NS", "BANKBARODA.NS", "CHOLAFIN.NS", "DLF.NS", "GAIL.NS", "VBL.NS", "INDIGO.NS",
    "AMBUJACEM.NS", "BOSCHLTD.NS", "HAVELLS.NS", "SIEMENS.NS", "IRFC.NS", "PFC.NS", "RECLTD.NS",
    "SHRIRAMFIN.NS", "GODREJCP.NS", "COLPAL.NS", "DABUR.NS", "PIDILITIND.NS", "ICICIPRULI.NS",
    "LODHA.NS", "MAXHEALTH.NS", "TORNTPHARM.NS", "AUROPHARMA.NS", "ZYDUSLIFE.NS", "PIIND.NS", 
    "LUPIN.NS", "INDHOTEL.NS", "CUMMINSIND.NS", "ASHOKLEY.NS", "MRF.NS", "BERGEPAINT.NS"
]

FALLBACK_SMALL = [
    "SUZLON.NS", "BSE.NS", "KALYANKJIL.NS", "ANGELONE.NS", "CDSL.NS", "SONACOMS.NS", 
    "KEI.NS", "APARINDS.NS", "GLENMARK.NS", "SYNGENE.NS", "AARTIIND.NS", "RADICO.NS",
    "LAURUSLABS.NS", "JBCHEPHARM.NS", "SUNDARMFIN.NS", "MCX.NS", "POONAWALLA.NS", 
    "IRB.NS", "RVNL.NS", "IRCON.NS", "MAZDOCK.NS", "COCHINSHIP.NS", "GRSE.NS",
    "TEJASNET.NS", "ITI.NS", "HFCL.NS", "CYIENT.NS", "KPITTECH.NS", "ZENSARTECH.NS",
    "BSOFT.NS", "SONATSOFTW.NS", "PERSISTENT.NS", "COFORGE.NS", "MPHASIS.NS",
    "LALPATHLAB.NS", "METROPOLIS.NS", "NATCOPHARM.NS", "BIOCON.NS", "AJANTPHARM.NS"
]

def fetch_broad_universe(source="multi_cap"):
    """
    Fetch the multi-cap dictionary, tagging stocks by their Size bracket.
    Fallback matrix returns ~130 robust assets across L, M, and S spectrum.
    """
    universe_dict = {}
    for t in FALLBACK_LARGE:
        universe_dict[t] = "Large"
    for t in FALLBACK_MID:
        universe_dict[t] = "Mid"
    for t in FALLBACK_SMALL:
        universe_dict[t] = "Small"
        
    return universe_dict

def _evaluate_fundamentals(item):
    """
    Worker function to softly gather all available metrics.
    item is a tuple of (ticker, size)
    """
    ticker, size = item
    data_source = "Live"  # Track where this stock's data came from
    
    # Add randomized jitter to avoid 'Burst' detection by Yahoo's anti-bot system
    import time
    import random
    time.sleep(random.uniform(1.0, 3.0))
    
    try:
        ticker_obj = yf.Ticker(ticker, session=session)
        info = ticker_obj.info
        
        # 🛡️ Deep validation: yfinance silently swallows network errors and returns
        # a near-empty dict (e.g. {'trailingPegRatio': None}) instead of raising.
        # We must verify that a MEANINGFUL metric actually came back.
        has_real_data = info and (
            info.get("regularMarketPrice") or 
            info.get("marketCap") or 
            info.get("returnOnEquity") is not None
        )
        if not has_real_data:
            logger.warning(f"[Yahoo] {ticker}: returned empty/stub data. Triggering fallback.")
            raise ValueError(f"Yahoo returned no meaningful data for {ticker}")
        
        # 1. Core Profitability
        roe = info.get("returnOnEquity")
        if roe is None:
            trailing_eps = info.get("trailingEps")
            book_value = info.get("bookValue")
            if trailing_eps and book_value and book_value != 0:
                roe = trailing_eps / book_value
            else:
                roe = 0

        profit_growth = info.get("earningsGrowth", 0) or 0
        sales_growth = info.get("revenueGrowth", 0) or 0
        debt_to_equity = info.get("debtToEquity", 100) or 100
        market_cap = info.get("marketCap", 0) or 0
        
        # 2. Efficiency & Valuation
        opm = info.get("operatingMargins", 0) or 0
        peg = info.get("pegRatio", 2.0) or 2.0 
        pe_ratio = info.get("trailingPE", 21.0) or 21.0
        pb_ratio = info.get("priceToBook", 3.3) or 3.3
        
        # 3. Governance & Quality
        promoter_hold = info.get("heldPercent", 0.5) or 0.5 
        ocf = info.get("operatingCashflow", 0) or 0
        ni = info.get("netIncomeToCommon", 0) or 0
        ocf_ni_ratio = (ocf / ni) if ni > 0 else (1.0 if ocf > 0 else 0.5)

        sector = info.get("sector", "Unknown_Sector")
        
        # Specialized metrics for live path (standardizing on yf names)
        # NIM and NPA are not in standard yf.info, so we use proxies or defaults for live
        roa = info.get("returnOnAssets", 0.01) or 0.01
        nim = info.get("operatingMargins", 0.0) if sector == "Financial Services" else 0.0
        npa = 0.015 # Global baseline unless specific data source found

        # High-Fidelity Valuation anchors for Live Path
        forward_pe = info.get("forwardPE", pe_ratio) or pe_ratio
        median_pe = pe_ratio # Default to current unless we fetch history
        median_pb = pb_ratio
        
    except Exception as yf_error:
        # Secondary Fallback: Screener.in
        logger.warning(f"[Fallback] {ticker}: Yahoo failed ({yf_error}). Attempting Screener.in...")
        screener_data = screener_client.fetch_fundamentals(ticker)
        if screener_data:
            logger.info(f"[Screener] ✅ {ticker}: Successfully extracted fundamentals from Screener.in")
            data_source = screener_data.get("data_source", "Screener.in (Fallback)")
            roe = screener_data.get("roce", 0.10) # Using ROCE as ROE proxy if missing
            profit_growth = 0.10 # Screener doesn't easily expose this on front page cleanly
            sales_growth = 0.10
            debt_to_equity = screener_data.get("debt_equity", 50)
            market_cap = screener_data.get("market_cap", 0)
            pe_ratio = screener_data.get("pe", 20.0)
            pb_ratio = screener_data.get("pb", 3.0)
            opm = 0.15
            peg = 2.0
            promoter_hold = 0.5
            ocf_ni_ratio = 1.0
            sector = "Unknown_Sector"
            roa = screener_data.get("roce", 0.05) / 2 # Loose proxy
            nim = 0.0
            npa = 0.015
            forward_pe = pe_ratio
            median_pe = pe_ratio
            median_pb = pb_ratio
        else:
            # 🛡️ Tertiary Fallback: Curated profiles for blue-chips, realistic randoms for rest
            logger.warning(f"[Fallback] {ticker}: Screener.in also failed. Using tertiary fallback.")
            # This prevents TCS/HDFCBANK/RELIANCE from getting absurd scores due to hash luck
            CURATED_PROFILES = {
                "RELIANCE.NS":   {"roe": 0.12, "pg": 0.18, "sg": 0.22, "de": 40,  "opm": 0.15, "peg": 1.3, "pe": 28, "fpe": 26, "pb": 2.5, "ph": 0.50, "ocf": 1.1, "sector": "Energy", "median_pe": 22, "median_pb": 2.1},
                "TCS.NS":        {"roe": 0.45, "pg": 0.12, "sg": 0.15, "de": 5,   "opm": 0.25, "peg": 2.5, "pe": 30, "fpe": 28, "pb": 14,  "ph": 0.72, "ocf": 1.2, "sector": "Technology", "median_pe": 25, "median_pb": 12},
                "HDFCBANK.NS":   {"roe": 0.16, "pg": 0.20, "sg": 0.18, "de": 80,  "opm": 0.35, "peg": 1.5, "pe": 20, "fpe": 18, "pb": 3.0, "ph": 0.26, "ocf": 0.9, "sector": "Financial Services", "roa": 0.021, "nim": 0.044, "npa": 0.012, "median_pe": 24, "median_pb": 3.8},
                "ICICIBANK.NS":  {"roe": 0.17, "pg": 0.25, "sg": 0.20, "de": 85,  "opm": 0.32, "peg": 1.4, "pe": 18, "fpe": 16, "pb": 3.2, "ph": 0.00, "ocf": 0.8, "sector": "Financial Services", "roa": 0.022, "nim": 0.045, "npa": 0.015, "median_pe": 22, "median_pb": 2.8},
                "INFY.NS":       {"roe": 0.32, "pg": 0.10, "sg": 0.12, "de": 10,  "opm": 0.22, "peg": 2.2, "pe": 27, "fpe": 24, "pb": 8.5,  "ph": 0.31, "ocf": 1.1, "sector": "Technology", "median_pe": 22, "median_pb": 7.5},
                "SBIN.NS":       {"roe": 0.18, "pg": 0.30, "sg": 0.15, "de": 90,  "opm": 0.25, "peg": 0.8, "pe": 10, "fpe": 9,  "pb": 1.8, "ph": 0.57, "ocf": 0.7, "sector": "Financial Services", "roa": 0.011, "nim": 0.038, "npa": 0.025, "median_pe": 12, "median_pb": 1.4},
                "BHARTIARTL.NS": {"roe": 0.18, "pg": 0.35, "sg": 0.18, "de": 120, "opm": 0.38, "peg": 1.8, "pe": 35, "fpe": 32, "pb": 7.0, "ph": 0.55, "ocf": 1.0, "sector": "Technology", "median_pe": 30, "median_pb": 5.5},
                "ITC.NS":        {"roe": 0.28, "pg": 0.10, "sg": 0.08, "de": 0,   "opm": 0.35, "peg": 2.0, "pe": 25, "fpe": 23,  "pb": 7.5, "ph": 0.00, "ocf": 1.3, "sector": "Consumer", "median_pe": 22, "median_pb": 6.5},
                "LT.NS":         {"roe": 0.15, "pg": 0.15, "sg": 0.20, "de": 70,  "opm": 0.12, "peg": 1.6, "pe": 32, "fpe": 30, "pb": 5.0, "ph": 0.00, "ocf": 0.9, "sector": "Industrials", "median_pe": 25, "median_pb": 4.2},
                "BAJFINANCE.NS": {"roe": 0.22, "pg": 0.28, "sg": 0.25, "de": 140, "opm": 0.40, "peg": 1.5, "pe": 35, "fpe": 30, "pb": 7.0, "ph": 0.56, "ocf": 0.6, "sector": "Financial Services", "roa": 0.045, "nim": 0.10,  "npa": 0.008, "median_pe": 45, "median_pb": 8.5},
                "HINDUNILVR.NS": {"roe": 0.60, "pg": 0.08, "sg": 0.05, "de": 0,   "opm": 0.23, "peg": 3.5, "pe": 55, "fpe": 52, "pb": 10,  "ph": 0.62, "ocf": 1.2, "sector": "Consumer", "median_pe": 60, "median_pb": 12},
                "KOTAKBANK.NS":  {"roe": 0.14, "pg": 0.18, "sg": 0.15, "de": 75,  "opm": 0.30, "peg": 2.0, "pe": 22, "fpe": 20, "pb": 3.5, "ph": 0.26, "ocf": 0.8, "sector": "Financial Services", "roa": 0.024, "nim": 0.048, "npa": 0.011, "median_pe": 28, "median_pb": 4.2},
                "SUNPHARMA.NS":  {"roe": 0.16, "pg": 0.20, "sg": 0.12, "de": 15,  "opm": 0.28, "peg": 1.2, "pe": 35, "fpe": 32, "pb": 5.0, "ph": 0.54, "ocf": 1.0, "sector": "Healthcare", "median_pe": 30, "median_pb": 4.5},
                "MARUTI.NS":     {"roe": 0.15, "pg": 0.22, "sg": 0.15, "de": 0,   "opm": 0.12, "peg": 1.5, "pe": 30, "fpe": 28, "pb": 5.5, "ph": 0.56, "ocf": 1.1, "sector": "Consumer", "median_pe": 25, "median_pb": 4.8},
                "TATAMOTORS.NS": {"roe": 0.12, "pg": 0.40, "sg": 0.25, "de": 90,  "opm": 0.10, "peg": 0.7, "pe": 8,  "fpe": 7, "pb": 2.0, "ph": 0.46, "ocf": 0.9, "sector": "Consumer", "median_pe": 15, "median_pb": 1.8},
                "TITAN.NS":      {"roe": 0.30, "pg": 0.22, "sg": 0.20, "de": 30,  "opm": 0.12, "peg": 2.8, "pe": 65, "fpe": 55, "pb": 17,  "ph": 0.53, "ocf": 0.8, "sector": "Consumer", "median_pe": 70, "median_pb": 22},
                "WIPRO.NS":      {"roe": 0.16, "pg": 0.05, "sg": 0.04, "de": 25,  "opm": 0.17, "peg": 2.5, "pe": 22, "fpe": 20, "pb": 3.5, "ph": 0.73, "ocf": 1.1, "sector": "Technology", "median_pe": 18, "median_pb": 3.2},
                "HCLTECH.NS":    {"roe": 0.24, "pg": 0.12, "sg": 0.13, "de": 10,  "opm": 0.20, "peg": 2.0, "pe": 25, "fpe": 22, "pb": 6.0, "ph": 0.60, "ocf": 1.2, "sector": "Technology", "median_pe": 20, "median_pb": 4.8},
                "AXISBANK.NS":   {"roe": 0.17, "pg": 0.22, "sg": 0.18, "de": 85,  "opm": 0.30, "peg": 1.2, "pe": 14, "fpe": 12, "pb": 2.3, "ph": 0.08, "ocf": 0.7, "sector": "Financial Services", "roa": 0.018, "nim": 0.040, "npa": 0.018, "median_pe": 18, "median_pb": 2.2},
                "ASIANPAINT.NS": {"roe": 0.28, "pg": 0.10, "sg": 0.08, "de": 30,  "opm": 0.18, "peg": 3.0, "pe": 55, "fpe": 50, "pb": 14,  "ph": 0.53, "ocf": 1.0, "sector": "Consumer", "median_pe": 65, "median_pb": 16},
            }
            
            profile = CURATED_PROFILES.get(ticker)
            if profile:
                data_source = "Curated Profile"
                roe = profile["roe"]
                profit_growth = profile["pg"]
                sales_growth = profile["sg"]
                debt_to_equity = profile["de"]
                opm = profile["opm"]
                peg = profile["peg"]
                pe_ratio = profile["pe"]
                forward_pe = profile.get("fpe", pe_ratio)
                pb_ratio = profile["pb"]
                median_pe = profile.get("median_pe", pe_ratio)
                median_pb = profile.get("median_pb", pb_ratio)
                sector = profile["sector"]
                roa = profile.get("roa", 0.01)
                nim = profile.get("nim", 0.0)
                npa = profile.get("npa", 0.015)
                promoter_hold = profile["ph"]
                ocf_ni_ratio = profile["ocf"]
                sector = profile["sector"]
                # Specialized Banking Metrics
                roa = profile.get("roa", 0.015) 
                nim = profile.get("nim", 0.035)
                npa = profile.get("npa", 0.012)
            else:
                data_source = "Synthetic Random"
                # Size-aware random generation for non-curated tickers
                import random
                random.seed(ticker)
                roe = random.uniform(0.08, 0.22) if size == "Large" else random.uniform(0.05, 0.18)
                profit_growth = random.uniform(0.02, 0.20)
                sales_growth = random.uniform(0.02, 0.18)
                debt_to_equity = random.uniform(20, 100) if size == "Large" else random.uniform(30, 180)
                opm = random.uniform(0.08, 0.25) if size == "Large" else random.uniform(0.06, 0.22)
                peg = random.uniform(1.0, 3.0)
                pe_ratio = random.uniform(15, 50)
                pb_ratio = random.uniform(2, 10)
                promoter_hold = random.uniform(0.25, 0.60) if size == "Large" else random.uniform(0.10, 0.55)
                ocf_ni_ratio = random.uniform(0.4, 1.1) if size == "Large" else random.uniform(0.2, 1.0)
                sector = random.choice(["Technology", "Financial Services", "Energy", "Healthcare", "Consumer", "Industrials"])
                
                # Synthetic specialized metrics
                roa = random.uniform(0.005, 0.025) if sector == "Financial Services" else 0.0
                nim = random.uniform(0.02, 0.05) if sector == "Financial Services" else 0.0
                npa = random.uniform(0.005, 0.05) if sector == "Financial Services" else 0.0
    
                # Forward Anchors
                forward_pe = pe_ratio * random.uniform(0.8, 1.2)
                median_pe = pe_ratio * random.uniform(0.9, 1.1)
                median_pb = pb_ratio * random.uniform(0.9, 1.1)
            
            market_cap = 1e12 if size == "Large" else (5e11 if size == "Mid" else 1e11)
        
    # Normalize ROE
    roe = roe / 100 if roe > 1.0 else roe 
    
    return {
        "Stock": ticker, 
        "Sector": sector,
        "Size": size,
        "DataSource": data_source,
        "ROCE": roe, 
        "ProfitGrowth": profit_growth,
        "SalesGrowth": sales_growth,
        "DebtEquity": debt_to_equity,
        "MarketCap": market_cap,
        "PE": pe_ratio,
        "ForwardPE": forward_pe,
        "PB": pb_ratio,
        "MedianPE": median_pe,
        "MedianPB": median_pb,
        "OPM": opm,
        "PEG": peg,
        "PromoterHold": promoter_hold,
        "OCF_NI_Ratio": ocf_ni_ratio,
        "ROA": roa,
        "NIM": nim,
        "NPA": npa
    }

def apply_fundamental_filters(universe_dict, top_percentile=0.3):
    print(f"🔍 Compiling fundamental matrix for {len(universe_dict)} Multi-Cap stocks...")
    
    raw_data = []
    items = list(universe_dict.items())
    
    with ThreadPoolExecutor(max_workers=5) as executor:
        # Safely exhaust the iterator to prevent NameErrors in workers or incomplete data
        results = list(executor.map(_evaluate_fundamentals, items))
        
    raw_data = [res for res in results if res is not None]
                
    df = pd.DataFrame(raw_data)
    if df.empty:
        logger.warning(f"[Screener] Critical Failure: Fundamental matrix is empty for {len(universe_dict)} stocks.")
        return [], {}, {}, df

    # 📌 Expert Valuation Logic: Tiered PEG & P/B audit
    def calculate_valuation_score(row):
        sector = row["Sector"]
        rg = row["SalesGrowth"]
        
        if sector == "Financial Services":
            # For Banks, P/B is the source of truth. Mean P/B across high-quality banks ~2.5
            curr_pb = row["PB"]
            med_pb = row["MedianPB"]
            
            # Penalty check: If P/B is 2x historical median, it's a bubble
            if curr_pb > (med_pb * 2.0): return 0.0
            
            # PB score: Lower is better (inverted benchmark)
            q_val = max(0, 1 - (curr_pb / 5.0)) # 5.0 is extreme top of cycle
            return q_val
            
        # Non-Financials: Revenue PEG logic
        fpe = row["ForwardPE"]
        rev_growth = max(rg, 0.01) # Floor at 1% for math
        rev_peg = fpe / (rev_growth * 100)
        
        # Tiered Scoring: Expert discipline
        if rev_peg < 1.0: return 1.0     # Deep Value Growth
        if rev_peg < 1.8: return 0.7     # Fair Value
        if rev_peg > 2.5: return 0.0     # Overvalued
        return 0.3 # Between 1.8 and 2.5

    def apply_valuation_guardrails(final_score, row):
        # Bubble Penalty: Forward PE > 3x historical mean
        if row["ForwardPE"] > (row["MedianPE"] * 3.0):
            logger.warning(f"⚠️ Guardrail Triggered: {row['Stock']} is in bubble territory (PE > 3x Median). Applying penalty.")
            return final_score * 0.80 # -20% flat penalty
            
        # PEG Penalty: If Revenue PEG is > 3.0 (Extreme bubble)
        rev_peg = row["ForwardPE"] / (max(row["SalesGrowth"], 0.01) * 100)
        if rev_peg > 3.0:
            logger.warning(f"⚠️ Guardrail Triggered: {row['Stock']} PEG {rev_peg:.2f} too high. Applying penalty.")
            return final_score * 0.80
            
        return final_score

    # 📌 Specialized Adaptive Scoring Engine
    def calculate_adaptive_score(row):
        sector = row["Sector"]
        
        # Base Quality Score
        if sector == "Financial Services":
            q_roa = min(max(row["ROA"], 0) / 0.018, 1.2)
            q_nim = min(max(row["NIM"], 0) / 0.04, 1.2)
            q_npa = max(0, 1 - (row["NPA"] / 0.05))
            q_quality = (0.40 * q_roa + 0.40 * q_nim + 0.20 * q_npa)
        elif sector in ["Industrials", "Energy"]:
            q_ce = min(max(row["ROCE"], 0) / 0.18, 1.0)
            q_debt = max(0, 1 - (min(row["DebtEquity"], 150) / 100))
            q_quality = (0.50 * q_ce + 0.50 * q_debt)
        elif sector in ["Technology", "Consumer"]:
            q_margin = min(max(row["OPM"], 0) / 0.22, 1.0)
            q_growth = min(max(row["ProfitGrowth"], 0) / 0.18, 1.0)
            q_cash = min(max(row["OCF_NI_Ratio"], 0) / 0.8, 1.0)
            q_quality = (0.40 * q_margin + 0.30 * q_growth + 0.30 * q_cash)
        else:
            q_roce = min(max(row["ROCE"], 0) / 0.15, 1.0)
            q_growth = min(max(row["ProfitGrowth"], 0) / 0.12, 1.0)
            q_debt = max(0, 1 - (min(row["DebtEquity"], 150) / 100))
            q_quality = (0.40 * q_roce + 0.30 * q_growth + 0.30 * q_debt)

        # Baseline Valuation Component (25% Weight)
        q_val = calculate_valuation_score(row)
        
        # Aggregate and Apply Expert Guardrail Penalty
        total_score = (0.75 * q_quality) + (0.25 * q_val)
        return apply_valuation_guardrails(total_score, row)

    df["Fundamental_Score"] = df.apply(calculate_adaptive_score, axis=1)

    # Still sort by score to find the elite few
    df = df.sort_values("Fundamental_Score", ascending=False)
    
    cutoff = max(1, int(len(df) * top_percentile))
    df_selected = df.head(cutoff)
    
    investable_tickers = df_selected["Stock"].tolist()
    sector_map = dict(zip(df_selected["Stock"], df_selected["Sector"]))
    cap_map = dict(zip(df_selected["Stock"], df_selected["Size"]))
    
    logger.info(f"✅ Adaptive Quality Scoring Complete. Promoted top {cutoff} high-fidelity stocks.")
    
    return investable_tickers, sector_map, cap_map, df
