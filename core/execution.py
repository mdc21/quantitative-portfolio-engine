import pandas as pd
import math

def calculate_portfolio_value(holdings_list, live_prices, fresh_capital=0.0):
    """
    Calculates the total absolute value of the portfolio.
    holdings_list is expected to be a list of dicts: [{'Ticker': 'TCS', 'Qty_LongTerm': 10, 'Qty_ShortTerm': 5}]
    """
    current_value = 0.0
    for hl in holdings_list:
        # Robust dictionary key extraction (handles case & spaces)
        clean_hl = {str(k).strip().lower(): v for k, v in hl.items()}
        
        ticker = str(clean_hl.get('ticker', '')).strip().upper()
        if not ticker:
            continue
            
        # Auto-append .NS for Indian markets if user forgot in CSV
        if not ticker.endswith(".NS") and not ticker == "CASH":
            ticker = ticker + ".NS"
            
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
        except Exception:
            pass
            
        current_value += (float(qty) * price)

    return current_value + float(fresh_capital)

def generate_trade_list(target_weights, holdings_list, live_prices, fresh_capital=0.0):
    """
    Translates abstract percentage weights into hard integer share quantities.
    """
    total_capital = calculate_portfolio_value(holdings_list, live_prices, fresh_capital)
    
    if total_capital <= 0:
        return pd.DataFrame()
        
    trades = []
    
    # Map out current holdings for O(1) lookup
    current_map = {}
    for hl in holdings_list:
        clean_hl = {str(k).strip().lower(): v for k, v in hl.items()}
        ticker = str(clean_hl.get('ticker', '')).strip().upper()
        if not ticker: continue
        if not ticker.endswith(".NS") and not ticker == "CASH": ticker = ticker + ".NS"
            
        current_map[ticker] = {
            'Qty': float(clean_hl.get('qty_longterm', 0) or 0) + float(clean_hl.get('qty_shortterm', 0) or 0),
            'Qty_ShortTerm': float(clean_hl.get('qty_shortterm', 0) or 0)
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
        except Exception:
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
        ticker = str(clean_hl.get('ticker', '')).strip().upper()
        if not ticker: continue
        if not ticker.endswith(".NS") and not ticker == "CASH": ticker = ticker + ".NS"
        
        if ticker in evaluated_tickers:
            continue
            
        if ticker not in target_weights or target_weights.get(ticker) == 0.0:
            current_qty = float(clean_hl.get('qty_longterm', 0) or 0) + float(clean_hl.get('qty_shortterm', 0) or 0)
            if current_qty > 0:
                
                price = 0.0
                try:
                    raw_p = latest_prices.get(ticker, 0.0)
                    if isinstance(raw_p, pd.Series):
                         price = float(raw_p.iloc[0])
                    else:
                         price = float(raw_p)
                    if math.isnan(price): price = 0.0
                except Exception:
                    pass
                    
                stcg_warning = "⚠️ STCG RISK (Hold < 1Yr)" if float(clean_hl.get('qty_shortterm', 0) or 0) > 0 else ""
                
                trades.append({
                    "Stock": ticker,
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
