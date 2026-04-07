import pandas as pd
import yfinance as yf
from concurrent.futures import ThreadPoolExecutor

# Hardcoded Fallback
FALLBACK_TICKERS = [
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

def fetch_broad_universe(source="nifty50"):
    """
    Fetch a list of base symbols to serve as the broad universe.
    Currently attempts to dynamically pull Nifty 50 from Wikipedia, 
    with a fallback out-of-the-box static list.
    """
    if source == "nifty50":
        try:
            url = "https://en.wikipedia.org/wiki/NIFTY_50"
            tables = pd.read_html(url)
            df = tables[1]  # Typically the constituents table
            tickers = df['Symbol'].astype(str) + ".NS"
            return list(set(tickers))
        except Exception as e:
            print(f"⚠️ Scraping Wikipedia failed ({e}), using fallback universe.")
            return FALLBACK_TICKERS
    
    return FALLBACK_TICKERS

def _evaluate_fundamentals(ticker):
    """
    Worker function to softly gather all available metrics.
    """
    try:
        info = yf.Ticker(ticker).info
        
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

def apply_fundamental_filters(broad_universe, top_percentile=0.3):
    print(f"🔍 Compiling fundamental matrix for {len(broad_universe)} stocks...")
    
    raw_data = []
    
    with ThreadPoolExecutor(max_workers=10) as executor:
        results = executor.map(_evaluate_fundamentals, broad_universe)
        
        for res in results:
            if res is not None:
                raw_data.append(res)
                
    df = pd.DataFrame(raw_data)
    if df.empty:
        return [], {}, df
        
    df["ROCE_rank"] = df["ROCE"].rank(pct=True)
    df["Sales_rank"] = df["SalesGrowth"].rank(pct=True)
    df["Profit_rank"] = df["ProfitGrowth"].rank(pct=True)
    df["Debt_rank"] = df["DebtEquity"].rank(pct=True)
    df["MarketCap_rank"] = df["MarketCap"].rank(pct=True)

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
    
    print(f"✅ Fundamental Scoring Complete. Promoted top {cutoff} stocks.")
    
    return investable_tickers, sector_map, df
