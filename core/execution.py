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

def calculate_likely_tax(current_price, avg_buy_price, qty_to_sell, qty_longterm, qty_shortterm):
    """
    Calculates estimated capital gains tax (LTCG 12.5% | STCG 20%) assuming 
    Tax-Optimized FIFO (selling Long Term shares first).
    """
    if avg_buy_price <= 0 or qty_to_sell <= 0:
        return 0, ""

    gain_per_share = current_price - avg_buy_price
    if gain_per_share <= 0:
        return 0, "📉 No Tax (Loss)"

    # Sell Long Term first
    lt_shares_sold = min(qty_to_sell, qty_longterm)
    st_shares_sold = max(0, qty_to_sell - lt_shares_sold)

    lt_tax = lt_shares_sold * gain_per_share * 0.125
    st_tax = st_shares_sold * gain_per_share * 0.20
    
    total_tax = round(lt_tax + st_tax)
    
    if total_tax > 0:
        return total_tax, f"₹{total_tax:,} Est. Tax"
    return 0, "No Tax"

def generate_trade_list(target_weights, holdings_list, live_prices, fresh_capital=0.0, assessed_tickers=None, tactical_audits=None):
    """
    Translates abstract percentage weights into hard integer share quantities.
    Incorporates tactical execution notes for staggered entries/exits.
    """
    total_capital = calculate_portfolio_value(holdings_list, live_prices, fresh_capital)
    if assessed_tickers is None:
        assessed_tickers = []
    if tactical_audits is None:
        tactical_audits = {}
    
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
            'Qty_LongTerm': float(clean_hl.get('qty_longterm', 0) or 0),
            'Qty_ShortTerm': float(clean_hl.get('qty_shortterm', 0) or 0),
            'Avg_Buy_Price': float(clean_hl.get('avg_buy_price', 0) or 0),
            'Original_Ticker': raw_ticker,
            'Original_ISIN': raw_isin,
            'Is_Confident': is_confident
        }

    # Extract single exact series if live_prices is a DataFrame
    latest_prices = live_prices.iloc[-1] if isinstance(live_prices, pd.DataFrame) else live_prices

    # Evaluate target allocations
    evaluated_tickers = set()
    for ticker, target_weight in target_weights.items():
        if ticker == "CASH":
            continue
            
        evaluated_tickers.add(ticker)
        current_data = current_map.get(ticker, {'Qty': 0.0, 'Qty_ShortTerm': 0.0, 'Qty_LongTerm': 0.0, 'Avg_Buy_Price': 0.0, 'Is_Confident': True})
        current_qty = current_data['Qty']
        
        # Extract scalar price
        price = 0.0
        try:
            raw_p = latest_prices.get(ticker, 0.0)
            if hasattr(raw_p, 'iloc'):
                 price = float(raw_p.iloc[0])
            else:
                 price = float(raw_p)
            if math.isnan(price): price = 0.0
        except Exception:
            pass
            
        if price <= 0:
            continue
            
        target_value = total_capital * target_weight
        target_qty = math.floor(target_value / price)
        
        delta_qty = target_qty - current_qty
        
        if delta_qty != 0:
            action = "BUY" if delta_qty > 0 else "SELL"
            
            tax_label = ""
            if action == "SELL":
                if current_data['Avg_Buy_Price'] > 0:
                    _, tax_label = calculate_likely_tax(price, current_data['Avg_Buy_Price'], abs(delta_qty), current_data['Qty_LongTerm'], current_data['Qty_ShortTerm'])
                else:
                    tax_label = "⚠️ No Buy Price"
            
            # Tactical Audit
            audit = tactical_audits.get(ticker, {})
            mode = audit.get("Execution", "Bulk")
            note = audit.get("Note", "")
            grade = audit.get("Grade", "B")
            
            action_label = "🟢 BUY" if action == "BUY" else "🔴 SELL"
            if action == "BUY":
                group_label = "Buy Orders"
                exec_label = f"{grade}"
            else:
                group_label = "Strategic Exits" if target_weight < 0.0001 else "Rebalance Trims"
                exec_label = f"Tactical {mode}" if mode == "Staggered" else "Bulk Order"
            
            trades.append({
                "Stock": ticker,
                "ISIN": current_data.get('Original_ISIN', ''),
                "Action": action_label,
                "Group": group_label,
                "Shares": abs(int(delta_qty)),
                "Current Price": round(price, 2),
                "Est. Value": round(abs(delta_qty) * price, 2),
                "Execution": exec_label,
                "Tactical Note": note,
                "Target Weight": f"{target_weight * 100:.2f}%",
                "Tax Indicator": tax_label
            })
            
    # Also evaluate stocks currently held that dropped out (Target Weight = 0%)
    for ticker, data in current_map.items():
        if ticker in evaluated_tickers or ticker == "CASH":
            continue
            
        current_qty = data['Qty']
        if current_qty <= 0:
            continue

        price = 0.0
        try:
            raw_p = latest_prices.get(ticker, 0.0)
            if hasattr(raw_p, 'iloc'):
                 price = float(raw_p.iloc[0])
            else:
                 price = float(raw_p)
            if math.isnan(price): price = 0.0
        except:
            pass

        # Handle Unresolved tickers separately (Action: N/A)
        if not data.get('Is_Confident', True):
            tax_l = ""
            if data['Avg_Buy_Price'] > 0:
                _, tax_l = calculate_likely_tax(price, data['Avg_Buy_Price'], current_qty, data['Qty_LongTerm'], data['Qty_ShortTerm'])
            
            display_name = data.get('Original_Ticker', ticker)
            trades.append({
                "Stock": display_name,
                "ISIN": data.get('Original_ISIN', ''),
                "Action": "⚪ N/A",
                "Group": "Unresolved Assets",
                "Shares": int(current_qty),
                "Current Price": round(price, 2),
                "Est. Value": round(current_qty * price, 2),
                "Execution": "Review Required",
                "Tactical Note": "Ticker failed confident resolution. Manual mapping needed.",
                "Target Weight": "Unknown",
                "Tax Indicator": tax_l
            })
            continue

        # Tax calculation for strategic exits
        tax_label = ""
        if data['Avg_Buy_Price'] > 0:
            _, tax_label = calculate_likely_tax(price, data['Avg_Buy_Price'], current_qty, data['Qty_LongTerm'], data['Qty_ShortTerm'])
        else:
            tax_label = "⚠️ No Buy Price"
            
        audit = tactical_audits.get(ticker, {})
        mode = audit.get("Execution", "Bulk")
        note = audit.get("Note", "")
        
        trades.append({
            "Stock": ticker,
            "ISIN": data.get('Original_ISIN', ''),
            "Action": "🔴 SELL",
            "Group": "Strategic Exits",
            "Shares": int(current_qty),
            "Current Price": round(price, 2),
            "Est. Value": round(current_qty * price, 2),
            "Execution": f"Tactical {mode}" if mode == "Staggered" else "Bulk Order",
            "Tactical Note": note,
            "Target Weight": "0.00%",
            "Tax Indicator": tax_label
        })

    df_trades = pd.DataFrame(trades)
    if not df_trades.empty:
        df_trades = df_trades.sort_values(by=["Action", "Est. Value"], ascending=[True, False])

    return df_trades
