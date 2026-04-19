import pandas as pd
import numpy as np
from core.logger import logger

def calculate_rsi(prices, period=14):
    """
    Standard Relative Strength Index (RSI) calculation using Wilder's smoothing.
    """
    if len(prices) < period + 1:
        return 50.0  # Neutral fallback
    
    delta = prices.diff()
    gain = delta.where(delta > 0, 0)
    loss = -delta.where(delta < 0, 0)
    
    avg_gain = gain.rolling(window=period).mean()
    avg_loss = loss.rolling(window=period).mean()
    
    # Apply Wilder's smoothing
    for i in range(period, len(prices)):
        avg_gain.iloc[i] = (avg_gain.iloc[i-1] * (period - 1) + gain.iloc[i]) / period
        avg_loss.iloc[i] = (avg_loss.iloc[i-1] * (period - 1) + loss.iloc[i]) / period
        
    rs = avg_gain / avg_loss
    rsi = 100 - (100 / (1 + rs))
    return rsi

def compute_tactical_audit(prices):
    """
    Performs a technical audit of an asset to determine entry/exit quality.
    - RSI Status (Overbought/Oversold)
    - Trend Status (Price vs SMA50/200)
    - Staggered Suggestion Logic
    """
    if prices.empty or len(prices) < 20:
        return {
            "RSI": 50.0,
            "Trend": "Neutral",
            "Grade": "B",
            "Execution": "Bulk",
            "Note": "Insufficient Data"
        }
    
    # 1. Technical Indicators
    rsi_series = calculate_rsi(prices)
    current_rsi = float(rsi_series.iloc[-1])
    # Handle NaNs
    if np.isnan(current_rsi): current_rsi = 50.0
    
    sma50 = float(prices.rolling(window=min(50, len(prices))).mean().iloc[-1])
    sma200 = float(prices.rolling(window=min(200, len(prices))).mean().iloc[-1])
    current_price = float(prices.iloc[-1])
    
    # 2. Trend Classification
    if current_price > sma50 > sma200:
        trend = "Strong Uptrend"
    elif current_price > sma50:
        trend = "Improving"
    elif current_price < sma50 < sma200:
        trend = "Strong Downtrend"
    else:
        trend = "Weakening"
        
    # 3. Tactical Grading (Entry Focus)
    if current_rsi > 75:
        grade = "C (Extended / Wait)"
    elif trend == "Strong Downtrend":
        grade = "D (Avoid)"
    elif trend == "Strong Uptrend" and current_rsi < 65:
        grade = "A (Strong Entry)"
    elif current_rsi < 35:
        grade = "A (Oversold Bounce)"
    else:
        grade = "B (Neutral)"
        
    # 4. Staggered Execution Logic (Exit Focus)
    # If we need to sell, should we stagger or bulk?
    if current_rsi > 80 or current_price < sma50:
        execution = "Bulk"
        note = "Structural breakdown or parabolic top. Exit immediately."
    elif trend in ["Strong Uptrend", "Improving"] and current_rsi < 70:
        execution = "Staggered"
        note = "Trend still healthy. Exit 50% now, trail 50% on SL or T1."
    else:
        execution = "Bulk"
        note = "Standard rebalance exit."
        
    return {
        "RSI": round(current_rsi, 1),
        "Trend": trend,
        "Grade": grade,
        "Execution": execution,
        "Note": note
    }

def get_bulk_tactical_audit(all_prices):
    """
    Computes tactical audits for a list of tickers in parallel (vectorized).
    """
    audit_results = {}
    for ticker in all_prices.columns:
        if ticker == "^NSEI" or ticker == "CASH":
            continue
        audit_results[ticker] = compute_tactical_audit(all_prices[ticker])
    return audit_results
