import pandas as pd
import yfinance as yf
import logging
import sys

yf.set_tz_cache_location("/tmp/")
yf.enable_debug_mode = False
# Suppress the massive Yahoo stdout
logger = logging.getLogger('yfinance')
logger.disabled = True
logger.propagate = False
import warnings
warnings.filterwarnings("ignore")

df = pd.read_csv('data/test_portfolio_1.csv', sep=None, engine='python')
tickers = df.iloc[:, 0].dropna().astype(str).str.strip().str.upper().tolist()
tickers = [t for t in tickers if t.lower() != 'ticker']

formatted_tickers = [f"{t}.NS" if not t.endswith('.NS') else t for t in tickers]

data = yf.download(formatted_tickers, period="5d", progress=False)['Close']
failed = [t for t in formatted_tickers if t not in data.columns or data[t].dropna().empty]

print(f"TOTAL_CHECKED: {len(formatted_tickers)}")
print(f"TOTAL_FAILED: {len(failed)}")
# Only print a max of 5 so we don't truncate
print(f"FAILED_SAMPLE: {failed[:10]}...")
