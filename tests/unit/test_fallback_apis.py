import pytest
import pandas as pd
from datetime import datetime

# Import our new API scrapers
from core.screener_api import ScreenerAPI
from core.nse_api import NSEAPI

class MockResponse:
    def __init__(self, json_data=None, text_data=None, status_code=200):
        self.json_data = json_data
        self.text = text_data
        self.status_code = status_code

    def json(self):
        return self.json_data

# --- MOCK DATA --- 

MOCK_SCREENER_HTML = """
<html><body>
<ul id="top-ratios">
    <li><span class="name">Stock P/E</span><span class="number">25.4</span></li>
    <li><span class="name">ROCE</span><span class="number">15.2 %</span></li>
    <li><span class="name">Current Price</span><span class="number">₹ 1,500.50</span></li>
    <li><span class="name">Market Cap</span><span class="number">₹ 15,000 Cr.</span></li>
</ul>
</body></html>
"""

MOCK_NSE_JSON = {
    "data": [
        {"CH_TIMESTAMP": "01-Apr-2024", "CH_CLOSING_PRICE": 1500.5},
        {"CH_TIMESTAMP": "02-Apr-2024", "CH_CLOSING_PRICE": 1520.0}
    ]
}

def test_screener_api_parsing(mocker):
    """
    Simultaneously test that logic cleans the ticker correctly
    and successfully extracts values from the HTML DOM.
    """
    screener = ScreenerAPI()
    
    # Mock the internet request so it works even offline
    mocker.patch.object(screener.session, 'get', return_value=MockResponse(text_data=MOCK_SCREENER_HTML))
    
    # Action
    results = screener.fetch_fundamentals("TCS.NS")  # Pass Yahoo ticker
    
    # Assertions
    assert results is not None
    assert results["pe"] == 25.4
    assert results["roce"] == 0.152  # Assert it converted % to decimal
    assert results["current_price"] == 1500.5  # Assert it stripped commas and ₹
    assert results["market_cap"] == 150000000000.0  # Assert it converted Crores
    assert results["data_source"] == "Screener.in (Fallback)"


def test_nse_api_parsing(mocker):
    """
    Test that NSE data correctly transforms the nselib DataFrame into a 
    Pandas TimeSeries format exactly like yfinance.
    """
    nse = NSEAPI()
    
    # Manually enable the flag since nselib isn't installed locally
    nse.lib_available = True
    
    # Create the mock DataFrame that nselib would return
    mock_df = pd.DataFrame(MOCK_NSE_JSON["data"])
    mock_df = mock_df.rename(columns={"CH_TIMESTAMP": "Date", "CH_CLOSING_PRICE": "ClosePrice"})
    
    # Mock the nselib object dynamically
    mock_cap = mocker.MagicMock()
    mock_cap.price_volume_and_deliverable_position_data.return_value = mock_df
    nse._cap = mock_cap
    
    # Action
    series = nse.fetch_historical_prices("RELIANCE.NS", months_back=1)
    
    # Assertions
    assert not series.empty
    assert isinstance(series, pd.Series)
    assert len(series) == 2
    
    # Verify the index was correctly parsed into Datetime and sorted
    assert list(series.values) == [1500.5, 1520.0]
    
    # Verify the datetime was parsed as expected (assuming format string matched)
    # The MOCK dates are "01-Apr-2024", which when parsed with "%d-%b-%Y" works correctly.
    first_date = series.index[0]
    assert first_date == datetime.strptime("01-Apr-2024", "%d-%b-%Y")
