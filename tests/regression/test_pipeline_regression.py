import pytest
import pandas as pd
from core.universe import apply_fundamental_filters
from core.factors import compute_factor_scores
from core.momentum import select_top_momentum, apply_sector_caps
from core.optimizer import optimize_weights

class MockTicker:
    def __init__(self, info):
        self.info = info

def test_full_pipeline_regression(mocker, mock_fundamental_data, mock_prices):
    """
    Ensures that a full run of the core pipeline with synthetic data 
    produces a valid portfolio and doesn't crash.
    """
    # 1. Mock Fundamental API
    def mock_ticker_init(ticker, session=None):
        return MockTicker(mock_fundamental_data.get(ticker, {}))
    mocker.patch("core.universe.yf.Ticker", side_effect=mock_ticker_init)
    
    # 2. Setup Universe
    universe_dict = {t: "Large" for t in mock_fundamental_data.keys()}
    
    # 3. Step 1: Fundamental Filters
    tickers, sector_map, cap_map, scoring_df = apply_fundamental_filters(universe_dict, top_percentile=1.0)
    assert len(tickers) == 4
    
    # 4. Step 2: Factor Scores
    prices = mock_prices[tickers]
    config = {"momentum_lookback_days": 90, "volatility_lookback_days": 60}
    scores = compute_factor_scores(prices, config)
    assert len(scores) == 4
    
    # 5. Step 3: Momentum Selection
    selected = select_top_momentum(scores, top_percent=0.75)
    assert len(selected) > 0
    
    # 6. Step 4: Optimizer
    regime = {"optimization_mode": "Markowitz"}
    limits = {
        "cap_large": 1.0, "cap_mid": 0.0, "cap_small": 0.0,
        "category_caps": {"Energy": 0.5, "Technology": 0.5, "Financial Services": 0.5}
    }
    
    weights = optimize_weights(prices, selected, regime, sector_map, cap_map, limits)
    
    # 7. Final Assertions
    sum_weights = sum(weights.values())
    assert abs(sum_weights - 1.0) < 0.01, f"Total allocation constraint failed. Sum={sum_weights}"
    
    # --- PHASE 6: EXECUTION & RETAIL CSV INTEGRATION ---
    from core.execution import generate_trade_list
    
    dummy_portfolio = [
        {"Ticker": "RELIANCE", "Qty_LongTerm": 50, "Qty_ShortTerm": 0},
        {"ticker": "tcs", "qty_longterm": 0, "qty_shortterm": 15}
    ]
    
    df_trades = generate_trade_list(weights, dummy_portfolio, mock_prices, fresh_capital=100000.0)
    
    # We should have a valid dataframe (not empty) meaning the engine successfully mapped the dummy portfolio
    # against the newly optimized Markowitz weights
    assert not df_trades.empty, "Execution mapping failed to produce a trade list during pipeline regression."
    assert "Action" in df_trades.columns
    assert "Group" in df_trades.columns
    assert "Tax Indicator" in df_trades.columns
    
    # Verify that groupings are happening (at least one Strategic Exit or Buy should exist in this mock)
    available_groups = df_trades["Group"].unique()
    assert any(g in ["Buy Orders", "Strategic Exits", "Rebalance Trims"] for g in available_groups)
    assert any(w > 0 for stock, w in weights.items() if stock != "CASH")
