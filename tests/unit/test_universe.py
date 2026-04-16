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
    mocker.patch("core.universe.yf.Ticker", side_effect=Exception("API Down"))
    
    universe_dict = {"INVALID.NS": "Small"}
    tickers, sector_map, cap_map, df = apply_fundamental_filters(universe_dict)
    
    assert len(tickers) == 0
    assert df.empty
