import pytest
import pandas as pd
from streamlit.testing.v1 import AppTest
from datetime import datetime

# We mock at the module level because AppTest spawns its own script execution
# Using a common fixture to avoid Yahoo Finance hits during UI tests

class MockTicker:
    def __init__(self, info):
        self.info = info

def test_dashboard_render(mocker, mock_fundamental_data, mock_prices):
    """
    Simulates a headless run of the dashboard and verifies core elements exist.
    """
    # 1. Mock the engine calls used in dashboard.py
    def mock_ticker_init(ticker, session=None):
        return MockTicker(mock_fundamental_data.get(ticker, {}))
    
    mock_repo = pd.DataFrame({"Rate": [6.5, 6.5]}, index=[datetime.now() - pd.Timedelta(days=1), datetime.now()])
    mock_cpi = pd.DataFrame({"CPI": [5.0, 5.0]}, index=[datetime.now() - pd.Timedelta(days=30), datetime.now()])
    
    mocker.patch("core.universe.yf.Ticker", side_effect=mock_ticker_init)
    mocker.patch("core.data_loader.fetch_prices", return_value=mock_prices)
    mocker.patch("core.macro.load_macro_data", return_value=(mock_repo, mock_cpi))
    
    # 2. Load the App
    at = AppTest.from_file("apps/dashboard.py")
    
    # 3. Increase timeout slightly for compilation
    at.run(timeout=30)
    
    # 4. Assert Title and Main Components
    assert at.title[0].value == "⚙️ Strategy Parameters" # Sidebar title
    assert at.markdown[1].value == "## 📊 Quant Portfolio Dashboard"
    
    # 5. Verify Metrics (KPIs)
    # The metrics we added: Analyzed Universe, Selected Stocks, Portfolio P/E, etc.
    metrics = at.get("metric")
    assert len(metrics) >= 4
    
    # Test if "Analyzed Universe" is present
    universe_metric = [m for m in metrics if m.label == "Analyzed Universe"]
    assert len(universe_metric) > 0
    assert "Stocks" in universe_metric[0].value
    
    # 6. Verify Tabs
    tabs = at.get("tab")
    assert len(tabs) == 5
    assert tabs[0].label == "🚀 Portfolio Allocation"

def test_dashboard_slider_interaction(mocker, mock_fundamental_data, mock_prices):
    """
    Verifies that changing a slider triggers a re-run and state update.
    """
    mock_repo = pd.DataFrame({"Rate": [6.5, 6.5]}, index=[datetime.now() - pd.Timedelta(days=1), datetime.now()])
    mock_cpi = pd.DataFrame({"CPI": [5.0, 5.0]}, index=[datetime.now() - pd.Timedelta(days=30), datetime.now()])
    
    mocker.patch("core.universe.yf.Ticker", return_value=MockTicker(mock_fundamental_data["RELIANCE.NS"]))
    mocker.patch("core.data_loader.fetch_prices", return_value=mock_prices)
    mocker.patch("core.macro.load_macro_data", return_value=(mock_repo, mock_cpi))
    
    at = AppTest.from_file("apps/dashboard.py")
    at.run()
    
    # Locate Slider (Momentum Lookback (Days))
    # We can find it by its label
    momentum_slider = at.sidebar.slider[0]
    assert momentum_slider.label == "Momentum Lookback (Days)"
    
    # Change value and run
    momentum_slider.set_value(180).run()
    
    # Verify the slider state persisted
    assert at.sidebar.slider[0].value == 180
    
    # Success message for caps should be visible if we didn't touch those
    assert "Portfolio Caps optimally scaled to 100%." in at.sidebar.success[0].value
    assert at.sidebar.success[0].icon == "✅"
