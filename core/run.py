import yaml
from core.data_loader import fetch_prices
from core.factors import compute_factor_scores
from core.momentum import select_top_momentum
from core.optimizer import optimize_weights
from core.rebalance import rebalance_portfolio
from core.risk import apply_trend_filter, apply_drawdown_control 
import yfinance as yf
from core.macro import load_macro_data, compute_macro_regime
from core.optimizer import apply_macro_overlay, apply_sector_weight_constraints 
from core.logger import logger

# Load config
with open("config/portfolio_config.yaml") as f:
    config = yaml.safe_load(f)

from core.universe import fetch_broad_universe, apply_fundamental_filters
from core.state import load_portfolio_state, save_portfolio_state
from core.optimizer import apply_turnover_control, apply_cap_size_constraints
from core.momentum import apply_sector_caps

universe_dict = fetch_broad_universe("multi_cap")
tickers, sector_map, cap_map, scoring_df = apply_fundamental_filters(universe_dict)

capital = config["capital"]

index_data = yf.download("^NSEI", period="6mo")
if index_data.empty:
    print("Failed to download market data from Yahoo Finance! Continuing without benchmark visualization dependencies...")
    index_prices = None
else:
    index_prices = index_data["Close"]

try:
    logger.info("Initiating Headless Execution Pipeline...")

    repo, cpi = load_macro_data()
    regime = compute_macro_regime(repo, cpi)
    logger.info(f"Macro Regime Detected - Inflation: {regime['inflation']}, Trend: {regime['rate_trend']}")

    # 1. Broad Universe
    logger.info("Fetching Broad Universe...")
    universe_tickers = fetch_broad_universe("multi_cap")

    # 2. Fundamental Sandbox
    logger.info("Running Fundamental Matrix...")
    tickers, sector_map, cap_map, scoring_df = apply_fundamental_filters(universe_tickers)

    if not tickers:
        logger.error("Zero fundamental assets survived logic trap.")
        exit()

    # 3. Fetch Prices
    logger.info(f"Sourcing pricing metrics for {len(tickers)} core assets...")
    from core.data_loader import fetch_prices
    prices = fetch_prices(tickers, period="6mo")
    
    if "^NSEI" in prices.columns:
        prices = prices.drop(columns=["^NSEI"])

    # 4. Momentum Factors
    logger.info("Crunching Momentum structures...")
    scores = compute_factor_scores(prices, config)

    # 5. Selection
    selected_raw = select_top_momentum(scores, config["top_percentile"])
    # 6. Sector Overlay
    selected = apply_sector_caps(selected_raw, sector_map, config["max_per_sector"])

    if not selected:
        logger.warning("No assets remained following Momentum & Sector Caps.")
        exit()

    # 7. Weights
    logger.info("Generating Final Math Constraints...")
    regime = compute_macro_regime(repo, cpi, prices=prices)
    
    limits = {
        "cap_large": 0.70,
        "cap_mid": 0.20,
        "cap_small": 0.10,
        "category_caps": {
            "Financials": 0.30 if regime.get("rate_trend") != "rising" else 0.20,
            "Technology": 0.20,
            "Industrials_Infra": 0.20,
            "Consumer_FMCG": 0.15,
            "Pharma_Healthcare": 0.15,
            "Chemicals": 0.12, 
            "PSU_Utilities": 0.10,
            "Others": 0.10
        }
    }
    
    weights = optimize_weights(prices, selected, regime, sector_map, cap_map, limits)
    weights = apply_sector_weight_constraints(weights, sector_map, regime)
    weights = apply_cap_size_constraints(weights, cap_map)
    weights = apply_macro_overlay(weights, regime)
    weights = apply_drawdown_control(weights, prices[selected])

    logger.info(f"Pipeline Cleared. Final Ratios: {weights}")

except Exception as e:
    logger.error("Critical Fault inside Headless Pipeline Router.", exc_info=True)
    print("Execution aborted due to runtime errors. Check logs/quant_system.log for details.")

# Step 4.5: Turnover control & State persistence
old_state = load_portfolio_state()
if "^NSEI" in old_state:
    del old_state["^NSEI"]
    
weights = apply_turnover_control(old_state, weights, max_turnover=0.3)
save_portfolio_state(weights, retention_days=60)
# Step 5: Rebalance
latest_prices = prices.iloc[-1]
portfolio = rebalance_portfolio(capital, weights, latest_prices)

# Output
print("\nFinal Portfolio Allocation:\n")
for stock, data in portfolio.items():
    print(stock, data)
