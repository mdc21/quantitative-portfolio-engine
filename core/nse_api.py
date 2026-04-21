import time
import random
from datetime import datetime, timedelta
import pandas as pd
from core.logger import logger

class NSEAPI:
    def __init__(self):
        # We now rely on the nselib library to handle session state and cookie handshakes
        self.lib_available = False
        try:
            from nselib import capital_market
            self._cap = capital_market
            self.lib_available = True
        except ImportError:
            logger.warning("nselib is not installed. Please add it to requirements.txt.")

    def _clean_ticker(self, ticker):
        return str(ticker).replace(".NS", "").replace(".BO", "").strip().upper()

    def fetch_historical_prices(self, ticker, months_back=6):
        """
        Fetches historical daily adjusted closing prices using the nselib python library.
        Returns a pandas Series matching the yfinance format.
        """
        if not self.lib_available:
            return pd.Series(dtype=float)
            
        clean_tick = self._clean_ticker(ticker)
        
        # Jitter to avoid instant IP ban from NSE India rate limiters
        time.sleep(random.uniform(2.0, 4.0)) 
        
        today = datetime.now()
        start_date = today - timedelta(days=30 * months_back)
        
        # nselib expects format 'dd-mm-yyyy'
        from_str = start_date.strftime("%d-%m-%Y")
        to_str = today.strftime("%d-%m-%Y")
        
        try:
            logger.info(f"[NSELib] Fetching {clean_tick} from {from_str} to {to_str}...")
            # Note: nselib may require chunking if the date range is > 1 year.
            # 6 months is typically safe for this endpoint.
            df = self._cap.price_volume_and_deliverable_position_data(
                symbol=clean_tick, 
                from_date=from_str, 
                to_date=to_str
            )
            
            if df is not None and not df.empty:
                # nselib returns 'Date' (as string sometimes) and 'ClosePrice'
                if 'Date' in df.columns and 'ClosePrice' in df.columns:
                    # Clean the data types
                    df['Date'] = pd.to_datetime(df['Date'], format="%d-%b-%Y", errors='coerce')
                    df['ClosePrice'] = pd.to_numeric(df['ClosePrice'].astype(str).str.replace(',', ''), errors='coerce')
                    
                    df = df.dropna(subset=['Date', 'ClosePrice'])
                    df = df.set_index('Date')
                    series = df['ClosePrice'].sort_index()
                    return series
                else:
                    logger.debug(f"[NSELib] Unexpected columns returned for {clean_tick}: {df.columns}")
            else:
                logger.debug(f"[NSELib] Empty dataset returned for {clean_tick}.")
                
        except Exception as e:
            logger.debug(f"[NSELib] Exception fetching {clean_tick}: {e}")
            
        return pd.Series(dtype=float)
