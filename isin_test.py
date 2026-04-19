import yfinance as yf
ticker = yf.Ticker("INE419U01012")
try:
    print(ticker.info)
except Exception as e:
    print(f"Failed to fetch by ISIN directly: {e}")
