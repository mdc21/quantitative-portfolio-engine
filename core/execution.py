import pandas as pd
import math
import logging
from core.ticker_mapper import resolve_ticker
from core.logger import logger

def calculate_portfolio_value(holdings_list, live_prices, fresh_capital=0.0):
    """
    Calculates the total absolute value of the portfolio.
    holdings_list is expected to be a list of dicts: [{'Ticker': 'TCS', 'Qty_LongTerm': 10, 'Qty_ShortTerm': 5}]
    """
    current_value = 0.0
    logger.info(f"Calculating portfolio value for {len(holdings_list)} holdings.")
    for hl in holdings_list:
        # Robust dictionary key extraction (handles case & spaces)
        clean_hl = {str(k).strip().lower(): v for k, v in hl.items()}
        
        raw_ticker = str(clean_hl.get('stock_symbol', clean_hl.get('ticker', ''))).strip().upper()
        raw_isin = str(clean_hl.get('isin_name', clean_hl.get('isin_code', ''))).strip().upper()
        if not raw_ticker:
            continue
            
        ticker, is_confident = resolve_ticker(raw_ticker, isin=raw_isin)
        logger.debug(f"[Calculations] Extracted: Ticker='{raw_ticker}', ISIN='{raw_isin}' -> Resolved: {ticker} (Confident: {is_confident})")
            
        qty = float(clean_hl.get('qty_longterm', 0) or 0) + float(clean_hl.get('qty_shortterm', 0) or 0)
        
        # Fallback if price is missing or ticker format is weird
        price = 0.0
        try:
            raw_p = live_prices.get(ticker, 0.0) if hasattr(live_prices, 'get') else (live_prices.loc[ticker] if ticker in live_prices else 0.0)
            if isinstance(raw_p, pd.Series):
                 price = float(raw_p.iloc[0])
            else:
                 price = float(raw_p)
            if math.isnan(price): price = 0.0
            
            if price > 0:
                logger.debug(f"[Value] Price found for {ticker}: {price}")
            else:
                logger.warning(f"[Value] No price found for resolved ticker {ticker}")
        except Exception as e:
            logger.error(f"[Value] Error fetching price for {ticker}: {e}")
            pass
            
        current_value += (float(qty) * price)

    return current_value + float(fresh_capital)

def generate_trade_list(target_weights, holdings_list, live_prices, fresh_capital=0.0, assessed_tickers=None):
    """
    Translates abstract percentage weights into hard integer share quantities.
    """
    total_capital = calculate_portfolio_value(holdings_list, live_prices, fresh_capital)
    if assessed_tickers is None:
        assessed_tickers = []
    
    if total_capital <= 0:
        return pd.DataFrame()
        
    trades = []
    
    current_map = {}
    logger.info("Building Trade List Map from current holdings...")
    for hl in holdings_list:
        clean_hl = {str(k).strip().lower(): v for k, v in hl.items()}
        raw_ticker = str(clean_hl.get('stock_symbol', clean_hl.get('ticker', ''))).strip().upper()
        raw_isin = str(clean_hl.get('isin_name', clean_hl.get('isin_code', ''))).strip().upper()
        if not raw_ticker: continue
        
        ticker, is_confident = resolve_ticker(raw_ticker, isin=raw_isin)
        logger.info(f"[Trade.Gen] Extracted: Ticker='{raw_ticker}', ISIN='{raw_isin}' | Resolved -> {ticker} (Confident: {is_confident})")
            
        current_map[ticker] = {
            'Qty': float(clean_hl.get('qty_longterm', 0) or 0) + float(clean_hl.get('qty_shortterm', 0) or 0),
            'Qty_ShortTerm': float(clean_hl.get('qty_shortterm', 0) or 0),
            'Original_Ticker': raw_ticker,
            'Original_ISIN': raw_isin,
            'Is_Confident': is_confident
        }

    # Extract single exact series if live_prices is a DataFrame (Yahoo multi-ticker download issue)
    latest_prices = live_prices.iloc[-1] if isinstance(live_prices, pd.DataFrame) else live_prices

    # Evaluate target allocations
    evaluated_tickers = set()
    for ticker, target_weight in target_weights.items():
        if ticker == "CASH":
            continue
            
        evaluated_tickers.add(ticker)
        current_data = current_map.get(ticker, {'Qty': 0.0, 'Qty_ShortTerm': 0.0})
        current_qty = current_data['Qty']
        
        # Extract scalar price
        price = 0.0
        try:
            raw_p = latest_prices.get(ticker, 0.0)
            if isinstance(raw_p, pd.Series):
                 price = float(raw_p.iloc[0])
            else:
                 price = float(raw_p)
            if math.isnan(price): price = 0.0
            
            if price > 0:
                logger.debug(f"[Trade.Target] Price match for {ticker}: {price}")
            else:
                logger.warning(f"[Trade.Target] No price match for {ticker}")
        except Exception as e:
            logger.error(f"[Trade.Target] Error matching price for {ticker}: {e}")
            pass
            
        if price <= 0:
            continue
            
        target_value = total_capital * target_weight
        target_qty = math.floor(target_value / price) # Always floor to prevent over-allocation of capital
        
        delta_qty = target_qty - current_qty
        
        if delta_qty != 0:
            action = "BUY" if delta_qty > 0 else "SELL"
            
            # Tax Shield Logic
            stcg_warning = ""
            if action == "SELL" and current_data['Qty_ShortTerm'] > 0:
                stcg_warning = "⚠️ STCG RISK (Hold < 1Yr)"
                
            trades.append({
                "Stock": ticker,
                "ISIN": current_data.get('Original_ISIN', ''),
                "Action": action,
                "Shares": abs(int(delta_qty)),
                "Current Price": round(price, 2),
                "Est. Value": round(abs(delta_qty) * price, 2),
                "Target Weight": f"{target_weight * 100:.2f}%",
                "Tax Indicator": stcg_warning
            })
            
    # Also evaluate stocks currently held that dropped out of the Target Weights entirely (Target Weight = 0%)
    for hl in holdings_list:
        clean_hl = {str(k).strip().lower(): v for k, v in hl.items()}
        raw_ticker = str(clean_hl.get('stock_symbol', clean_hl.get('ticker', ''))).strip().upper()
        raw_isin = str(clean_hl.get('isin_name', clean_hl.get('isin_code', ''))).strip().upper()
        if not raw_ticker: continue
        
        ticker, is_confident = resolve_ticker(raw_ticker, isin=raw_isin)
        logger.info(f"[Trade.Liquidation] Extracted: Ticker='{raw_ticker}', ISIN='{raw_isin}' | Resolved -> {ticker} (Confident: {is_confident})")
        
        if ticker in evaluated_tickers:
            continue
            
        current_qty = float(clean_hl.get('qty_longterm', 0) or 0) + float(clean_hl.get('qty_shortterm', 0) or 0)
        stcg_warning = "⚠️ STCG RISK (Hold < 1Yr)" if float(clean_hl.get('qty_shortterm', 0) or 0) > 0 else ""
        
        if not is_confident:
            if current_qty > 0:
                # Check if it was in the scanned universe
                label = raw_ticker + " (Unresolved)"
                if raw_isin:
                    if any(t.startswith(raw_ticker) for t in assessed_tickers):
                         label = raw_ticker + " (Outside Active Universe)"
                    else:
                         label = raw_ticker + " (Unresolved / Not Assessed)"

                trades.append({
                    "Stock": label,
                    "ISIN": raw_isin,
                    "Action": "Not Available",
                    "Shares": int(current_qty),
                    "Current Price": 0.0,
                    "Est. Value": 0.0,
                    "Target Weight": "Unknown",
                    "Tax Indicator": ""
                })
            # Prevent re-evaluating the same raw ticker later
            evaluated_tickers.add(ticker)
            continue
            
        if ticker not in target_weights or target_weights.get(ticker) == 0.0:
            if current_qty > 0:
                
                price = 0.0
                try:
                    raw_p = latest_prices.get(ticker, 0.0)
                    if isinstance(raw_p, pd.Series):
                         price = float(raw_p.iloc[0])
                    else:
                         price = float(raw_p)
                    if math.isnan(price): price = 0.0
                    
                    if price > 0:
                        logger.debug(f"[Trade.Sell] Price match for {ticker}: {price}")
                    else:
                        logger.warning(f"[Trade.Sell] No price match for {ticker}")
                except Exception as e:
                    logger.error(f"[Trade.Sell] Error matching price for {ticker}: {e}")
                    pass
                    
                # Labeling: Rejected vs Outside Universe
                display_name = ticker
                if ticker in assessed_tickers:
                    display_name = f"{ticker} (Rejected by Rules)"
                else:
                    display_name = f"{ticker} (Outside Active Universe)"
                    
                trades.append({
                    "Stock": display_name,
                    "ISIN": raw_isin,
                    "Action": "SELL",
                    "Shares": int(current_qty),
                    "Current Price": round(price, 2),
                    "Est. Value": round(current_qty * price, 2),
                    "Target Weight": "0.00%",
                    "Tax Indicator": stcg_warning
                })

    # Sort so BUYs are together, SELLs together
    df_trades = pd.DataFrame(trades)
    if not df_trades.empty:
        df_trades = df_trades.sort_values(by=["Action", "Est. Value"], ascending=[True, False])

    return df_trades
