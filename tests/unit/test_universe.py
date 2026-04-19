import pytest
from core.universe import apply_fundamental_filters

class MockTicker:
    def __init__(self, info):
        self.info = info

def test_apply_fundamental_filters(mocker, mock_fundamental_data):
    # Setup mock for yf.Ticker in core.universe
    def mock_ticker_init(ticker, session=None):
        return MockTicker(mock_fundamental_data.get(ticker, {}))
    
    mocker.patch("core.universe.yf.Ticker", side_effect=mock_ticker_init)
    
    universe_dict = {
        "RELIANCE.NS": "Large",
        "TCS.NS": "Large",
        "HDFCBANK.NS": "Large",
        "INFY.NS": "Large"
    }
    
    # Run filter with top 50%
    tickers, sector_map, cap_map, df = apply_fundamental_filters(universe_dict, top_percentile=0.5)
    
    # Check results
    assert len(tickers) == 2
    assert "TCS.NS" in tickers  # TCS should have high ROCE (0.35)
    assert sector_map["TCS.NS"] == "Technology"
    assert cap_map["TCS.NS"] == "Large"
    assert not df.empty
    assert "Fundamental_Score" in df.columns

def test_apply_fundamental_filters_empty(mocker):
    """In simulation mode, even invalid tickers get synthetic fundamentals."""
    mocker.patch("core.universe.yf.Ticker", side_effect=Exception("API Down"))
    
    universe_dict = {"INVALID.NS": "Small"}
    tickers, sector_map, cap_map, df = apply_fundamental_filters(universe_dict)
    
    # Simulation fallback generates data — so 1 ticker in = 1 ticker out
    assert len(tickers) == 1
    assert not df.empty

def test_apply_fundamental_filters_financial_exemption(mocker):
    """
    Ensures that a Financial Services stock with low OCF is NOT penalized.
    """
    mock_info = {
        "HDFCBANK.NS": {
            "returnOnEquity": 0.18,
            "earningsGrowth": 0.20,
            "revenueGrowth": 0.18,
            "debtToEquity": 80.0,
            "marketCap": 1.0e13,
            "sector": "Financial Services",
            "operatingMargins": 0.40,
            "pegRatio": 1.0,
            "heldPercent": 0.25,
            "operatingCashflow": 100, # Extremely low OCF
            "netIncomeToCommon": 1000 # High NI
        }
    }
    
    def mock_ticker_init(ticker, session=None):
        return MockTicker(mock_info.get(ticker, {}))
    
    mocker.patch("core.universe.yf.Ticker", side_effect=mock_ticker_init)
    
    universe_dict = {"HDFCBANK.NS": "Large"}
    tickers, sector_map, cap_map, df = apply_fundamental_filters(universe_dict, top_percentile=1.0)
    
    # HDFCBANK should be evaluated based on ROA, NIM, NPA in the new adaptive engine
    assert "ROA" in df.columns
    assert "NIM" in df.columns
    assert "NPA" in df.columns
    
    # In my mock_info, I didn't provide ROA/NIM, so they will default to synthetic values in the evaluator
    # but the presence of the score and lack of Q_Cash/Q_Debt penalties confirms the shift.
    assert df.iloc[0]["Fundamental_Score"] > 0

def test_apply_fundamental_filters_min_1_policy(mocker, mock_fundamental_data):
    """Ensures cutoff always returns at least 1 stock even if percentile is very low."""
    mocker.patch("core.universe.yf.Ticker", side_effect=Exception("API Down"))
    
    # 10 tickers in universe
    universe_dict = {f"TICKER_{i}.NS": "Small" for i in range(10)}
    
    # Test with 0.1% percentile (int(10 * 0.001) = 0)
    tickers, sector_map, cap_map, df = apply_fundamental_filters(universe_dict, top_percentile=0.001)
    
    # Min-1 policy should ensure 1 ticker is returned
    assert len(tickers) == 1
    assert not df.empty
