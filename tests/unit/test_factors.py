import pytest
import pandas as pd
from core.factors import compute_factor_scores

def test_compute_factor_scores(mock_prices):
    config = {
        "momentum_lookback_days": 90,
        "volatility_lookback_days": 60
    }
    
    # Remove benchmark for score computation
    prices = mock_prices.drop(columns=["^NSEI"])
    
    scores = compute_factor_scores(prices, config)
    
    assert isinstance(scores, pd.Series)
    assert not scores.empty
    assert all(scores >= 0)
    assert all(scores <= 1.0)
    # Ensure all stocks have a score
    assert set(scores.index) == set(prices.columns)

def test_compute_factor_scores_zero_momentum(mock_prices):
    # Create flat prices
    flat_prices = mock_prices.copy()
    for col in flat_prices.columns:
        flat_prices[col] = 100.0
    
    config = {
        "momentum_lookback_days": 90,
        "volatility_lookback_days": 60
    }
    
    prices = flat_prices.drop(columns=["^NSEI"])
    scores = compute_factor_scores(prices, config)
    
    # Scores should be low or uniform
    assert scores.std() < 0.001 or scores.isnull().all()
