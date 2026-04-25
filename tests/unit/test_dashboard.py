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
    
    # 🛡️ SPEED BOOST: Mock the heavy optimization and filter steps for UI testing
    mocker.patch("core.universe.apply_fundamental_filters", return_value=(["RELIANCE.NS", "TCS.NS"], {"RELIANCE.NS": "Energy", "TCS.NS": "Tech"}, {"RELIANCE.NS": "Large", "TCS.NS": "Large"}, pd.DataFrame([{"Stock": "RELIANCE.NS", "Score": 90, "DataSource": "Live"}, {"Stock": "TCS.NS", "Score": 85, "DataSource": "Live"}])))
    mocker.patch("apps.dashboard.optimize_weights", return_value={"RELIANCE.NS": 0.5, "TCS.NS": 0.5, "CASH": 0.0})

    # 2. Load the App
    at = AppTest.from_file("apps/dashboard.py")
    
    # 3. Increase timeout for compilation
    at.run(timeout=60)
    
    # 4. Assert Title and Main Components
    assert at.title[0].value == "⚙️ Strategy Parameters" # Sidebar title
    # Verify the CSS style block is injected (first markdown)
    assert "<style>" in at.markdown[0].value
    
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
    assert len(tabs) == 6
    assert tabs[0].label == "📥 Data Ingestion"

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
    
    # Locate Slider by label (index shifted due to Fundamental Quality Cutoff at [0])
    sliders = at.sidebar.slider
    momentum_slider = next(s for s in sliders if s.label == "Momentum Lookback (Days)")
    assert momentum_slider.label == "Momentum Lookback (Days)"
    
    # Change value and run
    momentum_slider.set_value(180).run()
    
    # Verify the slider state persisted
    updated_slider = next(s for s in at.sidebar.slider if s.label == "Momentum Lookback (Days)")
    assert updated_slider.value == 180
    
    # Success message for caps should be visible if we didn't touch those
    success_msgs = [s.value for s in at.sidebar.success]
    assert any("Portfolio Caps optimally scaled to 100%." in msg for msg in success_msgs)

def test_dashboard_tab1_comparison_render(mocker, mock_fundamental_data, mock_prices):
    """
    Verifies that the new Portfolio Allocation tab contains the dual-view analysis subheader.
    """
    mock_repo = pd.DataFrame({"Rate": [6.5, 6.5]}, index=[datetime.now() - pd.Timedelta(days=1), datetime.now()])
    mock_cpi = pd.DataFrame({"CPI": [5.0, 5.0]}, index=[datetime.now() - pd.Timedelta(days=30), datetime.now()])
    
    mocker.patch("core.universe.yf.Ticker", return_value=MockTicker(mock_fundamental_data["RELIANCE.NS"]))
    mocker.patch("core.data_loader.fetch_prices", return_value=mock_prices)
    mocker.patch("core.macro.load_macro_data", return_value=(mock_repo, mock_cpi))
    
    at = AppTest.from_file("apps/dashboard.py")
    
    # 1. Preset the state to skip ingestion and go direct to allocation
    at.session_state['is_allocated'] = True
    at.run()
    
    # 2. Check tab 1 labels
    # We look for the newly added subheader "📊 Dual-View Portfolio Analysis"
    subheaders = [s.value for s in at.get("subheader")]
    assert "📊 Dual-View Portfolio Analysis" in subheaders
    assert "📋 Portfolio Comparison Matrix" in subheaders
