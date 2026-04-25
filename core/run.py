import yaml
import pandas as pd
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
from curl_cffi import requests

# Global session to mimic browser and bypass Yahoo Finance 401/Invalid Crumb errors
session = requests.Session(impersonate="chrome120")
session.headers.update({
    "Accept": "*/*",
    "Accept-Language": "en-US,en;q=0.9",
    "Origin": "https://finance.yahoo.com",
    "Referer": "https://finance.yahoo.com",
    "User-Agent": "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/120.0.0.0 Safari/537.36"
})

# Load config
with open("config/portfolio_config.yaml") as f:
    config = yaml.safe_load(f)

from core.universe import fetch_broad_universe, apply_fundamental_filters
from core.state import load_portfolio_state, save_portfolio_state
from core.optimizer import apply_turnover_control, apply_cap_size_constraints
from core.momentum import apply_sector_caps

universe_tickers = fetch_broad_universe("multi_cap")
tickers, sector_map, cap_map, asset_map, underlying_map, region_map, scoring_df = apply_fundamental_filters(universe_tickers)

capital = config["capital"]

index_prices = None
try:
    # Quick probe to see if we even have internet for the benchmark
    session.get("https://query1.finance.yahoo.com", timeout=1)
    index_data = yf.download("^NSEI", period="6mo", session=session, progress=False)
    if index_data is not None and not index_data.empty:
        index_prices = index_data["Close"]
except Exception:
    logger.warning("📊 Benchmark indices unreachable. Proceeding with internal math engine.")

try:
    logger.info("Initiating Headless Execution Pipeline...")

    repo, cpi = load_macro_data()
    regime = compute_macro_regime(repo, cpi)
    logger.info(f"Macro Regime Detected - Inflation: {regime['inflation']}, Trend: {regime['rate_trend']}")

    # 1. Broad Universe
    logger.info("Fetching Broad Universe...")
    universe_tickers = fetch_broad_universe("multi_cap")

    # 2. Fundamental Sandbox is already run above to populate maps
    # We just ensure tickers are locally scoped
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
    selected_raw = select_top_momentum(scores, config.get("top_momentum_percentile", 0.5))
    # 6. Sector Overlay
    selected = apply_sector_caps(selected_raw, sector_map, config.get("max_per_sector", 0.2))

    # 🛡️ Prevent Passive vehicles (ETFs/MFs) from losing the Momentum contest
    for tick in prices.columns:
        if asset_map.get(tick) in ["ETF", "MutualFund"] and tick not in selected:
            selected.append(tick)

    if not selected:
        logger.warning("No assets remained following Momentum & Sector Caps.")
        exit()

    # 7. Weights
    logger.info("Generating Final Math Constraints...")
    regime = compute_macro_regime(repo, cpi, prices=prices)
    regime["equity_target"] = config.get("equity_target", 0.60)
    
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
    
    weights = optimize_weights(prices, selected, regime, asset_map, sector_map, cap_map, limits)
    weights = apply_sector_weight_constraints(weights, sector_map, regime)
    weights = apply_cap_size_constraints(weights, cap_map, sector_map, regime)
    from core.optimizer import apply_asset_class_constraints
    weights = apply_asset_class_constraints(weights, asset_map, underlying_map, region_map, equity_target=0.60)
    weights = apply_macro_overlay(weights, regime)
    weights = apply_drawdown_control(weights, prices[selected])

    logger.info(f"Pipeline Cleared. Final Ratios: {weights}")

    # Step 4.5: Turnover control & State persistence
    old_state = load_portfolio_state()
    if "^NSEI" in old_state:
        del old_state["^NSEI"]
        
    weights = apply_turnover_control(old_state, weights, max_turnover=1.0)
    save_portfolio_state(weights, retention_days=60)
    # Step 5: Rebalance
    latest_prices = prices.iloc[-1]
    portfolio = rebalance_portfolio(capital, weights, latest_prices)

except Exception as e:
    logger.error("Critical Fault inside Headless Pipeline Router.", exc_info=True)
    print("Execution aborted due to runtime errors. Check logs/quant_system.log for details.")
    exit(1)

# Output
print("\nFinal Portfolio Allocation:\n")
for stock, data in portfolio.items():
    print(stock, data)
