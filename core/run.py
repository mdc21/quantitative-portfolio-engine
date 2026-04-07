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
from core.optimizer import apply_turnover_control
from core.momentum import apply_sector_caps

broad_universe = fetch_broad_universe("nifty50")
tickers, sector_map, scoring_df = apply_fundamental_filters(broad_universe)

capital = config["capital"]

index_data = yf.download("^NSEI", period="6mo")
if index_data.empty:
    print("Failed to download market data from Yahoo Finance! Exiting...")
    exit()

index_prices = index_data["Close"]

try:
    logger.info("Initiating Headless Execution Pipeline...")

    repo, cpi = load_macro_data()
    regime = compute_macro_regime(repo, cpi)
    logger.info(f"Macro Regime Detected - Inflation: {regime['inflation']}, Trend: {regime['rate_trend']}")

    # 1. Broad Universe
    logger.info("Fetching Broad Universe...")
    universe_tickers = fetch_broad_universe("nifty50")

    # 2. Fundamental Sandbox
    logger.info("Running Fundamental Matrix...")
    tickers, sector_map, scoring_df = apply_fundamental_filters(universe_tickers)

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
    weights = optimize_weights(prices, selected)
    weights = apply_sector_weight_constraints(weights, sector_map, regime)
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
