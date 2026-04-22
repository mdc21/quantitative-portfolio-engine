import math
import logging
from core.ticker_mapper import resolve_ticker

logger = logging.getLogger("QuantEngine")

def extract_portfolio_row(row_dict, latest_prices=None):
    """
    Heuristically extracts ticker, ISIN, quantity, and buy price from a raw CSV row dictionary.
    Handles multiple common broker column names.
    
    Returns: {
        'ticker': str (mapped),
        'qty': float,
        'avg_buy_price': float,
        'original_ticker': str,
        'current_price': float (from latest_prices)
    }
    """
    # Normalize keys to lowercase for matching
    cl = {str(k).strip().lower(): v for k, v in row_dict.items()}
    
    # 1. Ticker / Symbol
    rt = str(cl.get('stock_symbol', cl.get('ticker', cl.get('symbol', cl.get('stock', cl.get('entity', '')))))).strip().upper()
    ri = str(cl.get('isin_name', cl.get('isin_code', cl.get('isin', '')))).strip().upper()
    
    if not rt:
        return None
        
    resolved_t, _ = resolve_ticker(rt, isin=ri)
    
    # 2. Quantity (Heuristic)
    # Check for specific LT/ST split first
    if 'qty_longterm' in cl or 'qty_shortterm' in cl:
        qty = float(cl.get('qty_longterm', 0) or 0) + float(cl.get('qty_shortterm', 0) or 0)
    else:
        # Check general quantity columns
        qty = float(
            cl.get('quantity', 
            cl.get('qty', 
            cl.get('available qty', 
            cl.get('net qty', 
            cl.get('shares', 0))))) or 0
        )
        
    # 3. Buy Price
    buy_p = float(
        cl.get('avg_buy_price', 
        cl.get('buy_price', 
        cl.get('average_price', 
        cl.get('avg_cost', 
        cl.get('avg price', 0))))) or 0
    )
    
    # 4. Current Price lookup
    curr_p = 0.0
    if latest_prices is not None:
        try:
            if resolved_t == "CASH":
                curr_p = 1.0
            elif resolved_t in latest_prices:
                val = latest_prices[resolved_t]
                curr_p = float(val) if not math.isnan(val) else 0.0
        except:
            pass
            
    return {
        'ticker': resolved_t,
        'qty': qty,
        'avg_buy_price': buy_p,
        'original_ticker': rt,
        'current_price': curr_p
    }

def get_portfolio_summary(holdings_list, latest_prices=None):
    """
    Aggregates a raw holdings list into a mapped ticker-to-value summary.
    """
    summary = {}
    total_val = 0.0
    matched_count = 0
    unmatched = []
    nan_prices = []
    
    for row in holdings_list:
        data = extract_portfolio_row(row, latest_prices)
        if not data:
            continue
            
        ticker = data['ticker']
        qty = data['qty']
        p = data['current_price']
        
        # Check if ticker actually exists in price feed
        if latest_prices is not None and ticker != "CASH" and ticker not in latest_prices:
            unmatched.append(data['original_ticker'])
            continue
            
        # Check for NaN or missing price
        if latest_prices is not None and ticker != "CASH" and p <= 0:
            nan_prices.append(ticker)
            
        val = qty * p
        summary[ticker] = summary.get(ticker, 0.0) + val
        total_val += val
        matched_count += 1
        
    return {
        'weights': {s: v/total_val for s, v in summary.items()} if total_val > 0 else {},
        'values': summary,
        'total_value': total_val,
        'matched_count': matched_count,
        'unmatched_tickers': unmatched,
        'nan_price_tickers': list(set(nan_prices))
    }
