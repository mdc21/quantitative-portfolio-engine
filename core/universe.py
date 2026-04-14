import pandas as pd
import yfinance as yf
from curl_cffi import requests
from concurrent.futures import ThreadPoolExecutor

# Global session to mimic browser and bypass Yahoo Finance 401/Invalid Crumb errors
session = requests.Session(impersonate="chrome110")

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
    try:
        info = yf.Ticker(ticker, session=session).info
        
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
        pe_ratio = info.get("trailingPE")
        if pe_ratio is None:
            pe_ratio = 21.0  # Indian standard fallback
            
        pb_ratio = info.get("priceToBook")
        if pb_ratio is None:
            pb_ratio = 3.3  # Historical Indian Nifty PB benchmark
            
        sector = info.get("sector", "Unknown_Sector")
        
        roe = roe / 100 if roe > 1.0 else roe 
        
        return {
            "Stock": ticker, 
            "Sector": sector,
            "Size": size,
            "ROCE": roe, 
            "ProfitGrowth": profit_growth,
            "SalesGrowth": sales_growth,
            "DebtEquity": debt_to_equity,
            "MarketCap": market_cap,
            "PE": pe_ratio,
            "PB": pb_ratio
        }
    except Exception as e:
        return None

def apply_fundamental_filters(universe_dict, top_percentile=0.3):
    print(f"🔍 Compiling fundamental matrix for {len(universe_dict)} Multi-Cap stocks...")
    
    raw_data = []
    items = list(universe_dict.items())
    
    with ThreadPoolExecutor(max_workers=10) as executor:
        results = executor.map(_evaluate_fundamentals, items)
        
        for res in results:
            if res is not None:
                raw_data.append(res)
                
    df = pd.DataFrame(raw_data)
    if df.empty:
        return [], {}, {}, df
        
    df["ROCE_rank"] = df["ROCE"].rank(pct=True)
    df["Sales_rank"] = df["SalesGrowth"].rank(pct=True)
    df["Profit_rank"] = df["ProfitGrowth"].rank(pct=True)
    df["Debt_rank"] = df["DebtEquity"].rank(pct=True)
    
    # 📌 Perform Segmented Rank for Market Cap (Small caps rank against Small caps!)
    df["MarketCap_rank"] = df.groupby("Size")["MarketCap"].rank(pct=True)

    df["Fundamental_Score"] = (
        0.3 * df["ROCE_rank"] +
        0.25 * df["Profit_rank"] +
        0.2 * df["Sales_rank"] +
        0.25 * (1 - df["Debt_rank"]) # Penalize high debt relatively
    )

    df = df.sort_values("Fundamental_Score", ascending=False)
    
    cutoff = max(1, int(len(df) * top_percentile))
    df_selected = df.head(cutoff)
    
    investable_tickers = df_selected["Stock"].tolist()
    sector_map = dict(zip(df_selected["Stock"], df_selected["Sector"]))
    cap_map = dict(zip(df_selected["Stock"], df_selected["Size"]))
    
    print(f"✅ Fundamental Scoring Complete. Promoted top {cutoff} Multi-Cap stocks.")
    
    return investable_tickers, sector_map, cap_map, df
