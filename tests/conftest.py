import pytest
import pandas as pd
import numpy as np
from datetime import datetime, timedelta

@pytest.fixture
def mock_tickers():
    return ["RELIANCE.NS", "TCS.NS", "HDFCBANK.NS", "INFY.NS"]

@pytest.fixture
def mock_prices(mock_tickers):
    """Generates 6 months of synthetic price data."""
    dates = pd.date_range(end=datetime.now(), periods=126, freq='B')
    data = {}
    for ticker in mock_tickers:
        # Generate random walk with drift
        returns = np.random.normal(0.0005, 0.01, len(dates))
        price = 100 * (1 + returns).cumprod()
        data[ticker] = price
    
    # Add benchmark
    bench_returns = np.random.normal(0.0003, 0.008, len(dates))
    data["^NSEI"] = 18000 * (1 + bench_returns).cumprod()
    
    df = pd.DataFrame(data, index=dates)
    return df

@pytest.fixture
def mock_fundamental_data():
    """Returns synthetic data for yf.Ticker.info mocking."""
    return {
        "RELIANCE.NS": {
            "returnOnEquity": 0.15,
            "earningsGrowth": 0.12,
            "revenueGrowth": 0.10,
            "debtToEquity": 40.0,
            "marketCap": 15000000000000,
            "trailingPE": 25.0,
            "priceToBook": 2.5,
            "sector": "Energy",
            "operatingMargins": 0.12,
            "pegRatio": 1.8,
            "heldPercent": 0.50,
            "operatingCashflow": 1000000,
            "netIncomeToCommon": 1000000
        },
        "TCS.NS": {
            "returnOnEquity": 0.35,
            "earningsGrowth": 0.15,
            "revenueGrowth": 0.08,
            "debtToEquity": 5.0,
            "marketCap": 12000000000000,
            "trailingPE": 30.0,
            "priceToBook": 12.0,
            "sector": "Technology",
            "operatingMargins": 0.25,
            "pegRatio": 2.1,
            "heldPercent": 0.72,
            "operatingCashflow": 2000000,
            "netIncomeToCommon": 1800000
        },
        "HDFCBANK.NS": {
            "returnOnEquity": 0.18,
            "earningsGrowth": 0.20,
            "revenueGrowth": 0.18,
            "debtToEquity": 80.0,
            "marketCap": 10000000000000,
            "trailingPE": 18.0,
            "priceToBook": 3.0,
            "sector": "Financial Services",
            "operatingMargins": 0.40,
            "pegRatio": 0.9,
            "heldPercent": 0.25,
            "operatingCashflow": 500000,
            "netIncomeToCommon": 800000
        },
        "INFY.NS": {
            "returnOnEquity": 0.28,
            "earningsGrowth": 0.10,
            "revenueGrowth": 0.12,
            "debtToEquity": 0.0,
            "marketCap": 6000000000000,
            "trailingPE": 24.0,
            "priceToBook": 8.0,
            "sector": "Technology",
            "operatingMargins": 0.22,
            "pegRatio": 1.5,
            "heldPercent": 0.15,
            "operatingCashflow": 800000,
            "netIncomeToCommon": 1000000
        }
    }

@pytest.fixture
def mock_cap_map():
    return {
        "RELIANCE.NS": "Large",
        "TCS.NS": "Large",
        "HDFCBANK.NS": "Large",
        "INFY.NS": "Large"
    }

@pytest.fixture
def mock_sector_map():
    return {
        "RELIANCE.NS": "Energy",
        "TCS.NS": "Technology",
        "HDFCBANK.NS": "Financial Services",
        "INFY.NS": "Technology"
    }
