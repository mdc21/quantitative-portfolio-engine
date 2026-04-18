import pandas as pd
import os
import logging
from core.universe import FALLBACK_LARGE, FALLBACK_MID, FALLBACK_SMALL
from core.logger import logger

UNIVERSES = FALLBACK_LARGE + FALLBACK_MID + FALLBACK_SMALL

ISIN_MAP = {}
try:
    _csv_path = os.path.join(os.path.dirname(__file__), '../data/isin_master.csv')
    if os.path.exists(_csv_path):
        _df = pd.read_csv(_csv_path, usecols=["SYMBOL", "ISIN NO"])
        ISIN_MAP = dict(zip(_df["ISIN NO"], _df["SYMBOL"] + ".NS"))
except Exception as e:
    logger.warning(f"Could not load ISIN Master CSV: {e}")

# A heuristic dictionary for the most heavily mangled common broker acronyms
MANUAL_BROKER_MAP = {
    "TATMOT": "TATAMOTORS.NS",
    "ASIPAI": "ASIANPAINT.NS",
    "HDFBAN": "HDFCBANK.NS",
    "ITCHOT": "INDHOTEL.NS",
    "INDHOT": "INDHOTEL.NS",
    "INDEN": "INDIGO.NS",
    "JKPAP": "JKPAPER.NS",
    "INFTEC": "INFY.NS",
    "DRREDD": "DRREDDY.NS",
    "TATCAP": "TATAINVEST.NS",
    "STAHEA": "STARHEALTH.NS",
    "RELCON": "RELIANCE.NS",
    "RELIND": "RELIANCE.NS",
    "HINLEV": "HINDUNILVR.NS",
    "KOTMAH": "KOTAKBANK.NS",
    "SIEMEN": "SIEMENS.NS",
    "TATTEC": "TATATECH.NS",
    "BAFINS": "BAJAJFINSV.NS",
    "TATCOM": "TATACOMM.NS",
    "CHOINV": "CHOLAFIN.NS",
    "IDFBAN": "IDFCFIRSTB.NS",
    "TATPOW": "TATAPOWER.NS",
    "MARLIM": "MARUTI.NS",
    "APOHOS": "APOLLOHOSP.NS",
    "JKLAKS": "JKLAKSHMI.NS",
    "TECMAH": "TECHM.NS",
    "CANBAN": "CANBK.NS",
    "TATSTE": "TATASTEEL.NS",
    "GLACON": "GLAXO.NS",
    "HDFSTA": "HDFCAMC.NS",
    "EICMOT": "EICHERMOT.NS",
    "GLOHEA": "MEDANTA.NS",
    "HINDAL": "HINDALCO.NS",
    "ASHLEY": "ASHOKLEY.NS",
    "STABAN": "SBIN.NS",
    "NARHRU": "NH.NS",
    "AXIBAN": "AXISBANK.NS",
    "SBILIF": "SBILIFE.NS",
    "FEDBAN": "FEDERALBNK.NS",
    "HINAER": "HAL.NS",
    "LARTOU": "LT.NS",
    "GESHIP": "GESHIP.NS",
    "POWGRI": "POWERGRID.NS",
    "BHAAIR": "BHARTIARTL.NS",
    "MAHMAH": "M&M.NS",
    "HCLTEC": "HCLTECH.NS",
    "ICIBAN": "ICICIBANK.NS",
    "TITIND": "TITAN.NS",
    "PERSYS": "PERSISTENT.NS",
    "ADICAP": "ABCAPITAL.NS",
    "ABBPOW": "ABB.NS",
    "TCS": "TCS.NS",
    "ITC": "ITC.NS",
    "HAL": "HAL.NS",
    "ZOMATO": "ZOMATO.NS"
}

def resolve_ticker(raw_ticker, isin=None):
    """
    Attempts to map a broker-specific mangled ticker into a valid Yahoo Finance NSE ticker.
    Prioritizes ISIN lookup if available.
    Returns: (mapped_ticker, is_confident_boolean)
    """
    if isin:
        clean_isin = str(isin).strip().upper()
        if clean_isin in ISIN_MAP:
            return ISIN_MAP[clean_isin], True
            
    clean = str(raw_ticker).strip().upper()
    if not clean:
        return raw_ticker, False
        
    # 1. Check strict manual map
    if clean in MANUAL_BROKER_MAP:
        return MANUAL_BROKER_MAP[clean], True
        
    # 2. Check complete direct match in Universe (user entered perfect ticker without .NS)
    for u in UNIVERSES:
        if u.split('.')[0] == clean:
            return u, True
            
    # 3. Fuzzy Prefix Match (e.g., RELIAN matches RELIANCE.NS)
    # Must be at least 4 characters to confidently guess
    if len(clean) >= 4:
        for u in UNIVERSES:
            base = u.split('.')[0]
            if base.startswith(clean):
                return u, True
                
    # If all heuristic resolution fails, it is unresolvable
    return raw_ticker, False
