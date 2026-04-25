import sys
import os
sys.path.append(os.path.abspath(os.path.join(os.path.dirname(__file__), '..')))

import streamlit as st
import pandas as pd
import numpy as np
import plotly.express as px
from core.data_loader import fetch_prices
from core.factors import compute_factor_scores
from core.momentum import select_top_momentum, apply_sector_caps
from core.optimizer import optimize_weights, apply_macro_overlay, apply_turnover_control, apply_sector_weight_constraints, apply_cap_size_constraints, apply_asset_class_constraints
from core.macro import load_macro_data, compute_macro_regime
from core.universe import fetch_broad_universe, apply_fundamental_filters
from core.ticker_mapper import resolve_ticker
from core.state import load_portfolio_state, save_portfolio_state
from core.portfolio_parser import extract_portfolio_row, get_portfolio_summary
from core.tactical import get_bulk_tactical_audit
from core.logger import logger

st.set_page_config(page_title="Quant Dashboard", layout="wide", page_icon="📈")

# --- SESSION STATE INITIALIZATION ---
if 'holdings_list' not in st.session_state:
    st.session_state['holdings_list'] = []
if 'fresh_capital' not in st.session_state:
    st.session_state['fresh_capital'] = 0.0
if 'is_allocated' not in st.session_state:
    st.session_state['is_allocated'] = False

# --- UI WHITESPACE REDUCTION ---
st.markdown("""
    <style>
        .block-container {
            padding-top: 1rem;
            padding-bottom: 0rem;
        }
    </style>
""", unsafe_allow_html=True)

# --- CACHING DATA FETCH ---
# This makes it reactive and lightning fast when touching sliders!
@st.cache_data(ttl=3600)
def get_cached_prices(tickers):
    return fetch_prices(tickers)

@st.cache_data(ttl=3600)
def get_cached_macro():
    return load_macro_data()

# --- DASHBOARD AESTHETICS ---
st.markdown("""
<div style="background: linear-gradient(135deg, #1e3a8a 0%, #3b82f6 100%); padding: 25px; border-radius: 12px; margin-bottom: 25px; border: 1px solid rgba(255,255,255,0.1); box-shadow: 0 8px 32px 0 rgba(0, 0, 0, 0.37);">
    <h1 style="color: white; margin: 0; font-family: 'Inter', sans-serif;">📊 Quant Portfolio Dashboard</h1>
    <p style="color: rgba(255,255,255,0.8); margin: 5px 0 0 0; font-size: 1.1rem;">Institutional-Grade Multi-Cap Quantitative Analytics</p>
</div>
""", unsafe_allow_html=True)

try:
    # --- SIDEBAR & INTERACTIVITY ---
    st.sidebar.title("⚙️ Strategy Parameters")
    
    if st.sidebar.button("🗑️ Clear Analytics Cache"):
        st.cache_data.clear()
        st.rerun()
    
    st.sidebar.header("Strategy Tuning")
    fund_cutoff = st.sidebar.slider("Fundamental Quality Cutoff (%)", 0.1, 1.0, 0.3, help="Controls the percentage of the universe that passes the initial quality audit (ROCE, Cash Quality, Promoter Skin).")
    mom_lookback = st.sidebar.slider("Momentum Lookback (Days)", 30, 252, 90, help="Number of trading days to track for historical price momentum. Usually 90-180 days.")
    vol_lookback = st.sidebar.slider("Volatility Lookback (Days)", 30, 252, 60, help="Amount of history used to compute standard deviation. Lower values react faster to market crashes.")
    top_pct_filter = st.sidebar.slider("Momentum Retention Cutoff (%)", 0.1, 1.0, 0.5, help="Percentage of fundamental survivors to execute Momentum buying on. Example: 0.5 means buy top 50%.")
    max_assets = st.sidebar.slider("Max Assets in Portfolio", 5, 50, 25, help="Concentration Limit: Forces the strategy to only pick the very best N stocks. Reduces 'dust' positions and trading churn.")
    max_turnover = st.sidebar.slider("Max Turnover Damper (%)", 0.05, 1.0, 0.30, help="Smooths giant target rebalances to prevent huge trading fees & slippage. 0.30 means weights move 30% per iteration.")

    st.sidebar.header("Asset Allocation Setup")
    equity_target = st.sidebar.slider("Target Equity Split (%)", 0.1, 1.0, 0.60, help="Controls the ratio of Equities versus Passive assets (ETFs & Mutual Funds). 0.60 means 60% standard stocks, 40% Passive.")
    metals_cap = st.sidebar.slider("Max Metals/Commodity Allocation (%)", 0.0, 0.20, 0.05, help="Global Absolute Cap: Ensures that no matter what the 60:40 rules say, Metal/Commodity ETFs never exceed this total percentage of the portfolio.")

    # --- UNIVERSE DISCOVERY ---
    @st.cache_data(ttl=86400)
    def get_institutional_universe(top_pct):
        logger.info(f"Triggered Institutional Universe Cache Refresh with Cutoff: {top_pct}")
        broad_universe = fetch_broad_universe("nifty50")
        return apply_fundamental_filters(broad_universe, top_percentile=top_pct)

    with st.spinner("🤖 Evaluating Fundamental Screener (Daily Cache)..."):
        investable_tickers, sector_map, cap_map, asset_map, underlying_map, region_map, scoring_df = get_institutional_universe(fund_cutoff)
        all_assessed_tickers = scoring_df["Stock"].tolist() if not scoring_df.empty else investable_tickers

    # Fetch - Expand fetch to include user's owner tickers for comparison chart
    owner_tickers = []
    if st.session_state['holdings_list']:
        logger.info(f"Resolving {len(st.session_state['holdings_list'])} owner tickers for comparison...")
        for hl in st.session_state['holdings_list']:
            c_hl = {str(k).strip().lower(): v for k, v in hl.items()}
            r_t = str(c_hl.get('stock_symbol', c_hl.get('ticker', ''))).strip().upper()
            r_i = str(c_hl.get('isin_name', c_hl.get('isin_code', ''))).strip().upper()
            if r_t:
                res_t, _ = resolve_ticker(r_t, isin=r_i)
                owner_tickers.append(res_t)
    
    unique_fetch_list = list(set(all_assessed_tickers + owner_tickers))
    
    with st.spinner(f"Fetching market data for {len(unique_fetch_list)} stocks (Strategy + Owner Portfolio)..."):
        prices = get_cached_prices(unique_fetch_list)
        
    benchmark_map = {
        "Nifty 50": ["^NSEI"],
        "Next 50": ["^NSMIDCP"],
        "Midcap 100": ["^NSMIDCP"], 
        "Smallcap 100": ["^CNXSMALLCAP"]
    }
    
    benchmarks = {}
    if not prices.empty:
        for label, tickers in benchmark_map.items():
            for ticker in tickers:
                if ticker in prices.columns and not prices[ticker].isnull().all():
                    benchmarks[label] = prices[ticker]
                    break
    
    nifty_prices = benchmarks.get("Nifty 50")

    # --- DATA QUALITY WARNINGS ---
    # Warn users when analysis is based on non-live data
    if not scoring_df.empty and "DataSource" in scoring_df.columns:
        live_count = (scoring_df["DataSource"] == "Live").sum()
        curated_count = (scoring_df["DataSource"] == "Curated Profile").sum()
        synthetic_count = (scoring_df["DataSource"] == "Synthetic Random").sum()
        total = len(scoring_df)
        
        if live_count == total:
            st.sidebar.success(f"📡 **Fundamental Data:** Live ({total} stocks via Yahoo Finance)")
        elif live_count > 0:
            # Mostly live with a few fallbacks — this is normal operation
            screener_count = (scoring_df["DataSource"] == "Screener.in (Fallback)").sum()
            fallback_count = curated_count + synthetic_count + screener_count
            st.sidebar.success(
                f"📡 **Fundamental Data:** Live ({live_count}/{total} stocks via Yahoo Finance)\n\n"
                f"*{fallback_count} stocks used fallback data (curated/screener/synthetic).*"
            )
        elif curated_count > 0 or synthetic_count > 0:
            st.sidebar.warning(
                f"⚠️ **Fundamental Data: Simulation Mode**\n\n"
                f"Yahoo Finance is unreachable. Fundamentals are estimated:\n"
                f"- **{curated_count}** stocks using curated blue-chip profiles\n"
                f"- **{synthetic_count}** stocks using synthetic random data\n\n"
                f"*Recommendations may differ from live market conditions.*"
            )
    
    price_source = prices.attrs.get("data_source", "Unknown")
    if price_source == "Synthetic Simulation":
        st.sidebar.warning(
            "⚠️ **Price Data: Simulation Mode**\n\n"
            "Market prices are synthetically generated (random walk). "
            "Momentum rankings and return charts are illustrative only."
        )
    elif price_source == "Yahoo Finance (Live)":
        st.sidebar.success("📡 **Price Data:** Live (Yahoo Finance)")
        
    st.sidebar.header("Multi-Cap Bounds")
    cap_large = st.sidebar.slider("Max Large Cap Limit (%)", 0.0, 1.0, 0.70, help="Maximum portfolio % constrained to Nifty 50 constituents.")
    cap_mid = st.sidebar.slider("Max Mid Cap Limit (%)", 0.0, 1.0, 0.20, help="Maximum portfolio % constrained to Nifty Next 50 constituents.")
    cap_small = st.sidebar.slider("Max Small Cap Limit (%)", 0.0, 1.0, 0.10, help="Maximum portfolio % constrained to Nifty 250 Smallcap constituents.")

    # Validation Hook to ensure logic equates near 100%
    cap_total = cap_large + cap_mid + cap_small
    if abs(cap_total - 1.0) > 0.01:
        st.sidebar.warning(f"⚠️ Cap Limits total {cap_total*100:.0f}%. Adjust sliders to exactly 100% to ensure no default capital is forced into the CASH barrier.")
    else:
        st.sidebar.success("✅ Portfolio Caps optimally scaled to 100%.")

    # --- TACTICAL ENGINE STATUS ---
    # Moved diagnostics to end of script to ensure access to computed variables

    # Compute factors dynamically ONLY for the fundamentally approved survivors (Top 30%)
    # This ensures we don't accidentally buy a low-quality stock just because it has high momentum.
    # Compute factors dynamically ONLY for the fundamentally approved survivors
    # This ensures we don't accidentally buy a low-quality stock just because it has high momentum.
    buy_list_tickers = [t for t in investable_tickers if t in prices.columns]
    buy_list_prices = prices[buy_list_tickers]
    
    if buy_list_prices.empty:
        if scoring_df.empty or not investable_tickers:
            st.warning("⚠️ **Strategy Gap:** No stocks passed the current Institutional Quality criteria.")
            st.info("💡 **Resolution:** Try moving the **Fundamental Quality Cutoff (%)** slider in the sidebar to a higher value to broaden the screening funnel.")
        else:
            st.error("📉 **Data Gap:** Fundamental survivors were found, but no matching market price data was available for them. "
                     "Check your network connectivity or confirm if Simulation Mode is generating enough history.")
        st.stop()

    scores = compute_factor_scores(buy_list_prices, {
        "momentum_lookback_days": mom_lookback,
        "volatility_lookback_days": vol_lookback
    })

    if scores.empty:
        st.error(f"📉 **Lookback Error:** All {len(buy_list_tickers)} stocks were skipped during scoring.")
        st.info(f"💡 **Why?** Your lookback settings require **{max(mom_lookback, vol_lookback)} days** of history. "
                 "The current data provider (Yahoo or Simulation) provided less than this for these tickers. "
                 "Try reducing the **Momentum Lookback** slider in the sidebar to 30 days.")
        st.stop()

    # Macro Integration
    repo, cpi = load_macro_data()
    regime = compute_macro_regime(repo, cpi, prices=prices)
    
    st.sidebar.markdown("---")
    if regime["optimization_mode"] == "Markowitz":
        st.sidebar.success(f"🧠 **Active Engine:** Markowitz (Aggressive)")
    else:
        st.sidebar.error(f"🛡️ **Active Engine:** HRP (Defensive Volatility Shield)")

    # Filter & Sector Constraints
    selected_raw = select_top_momentum(scores, top_percent=top_pct_filter)
    # Apply Hard Concentration Limit
    selected_raw = selected_raw[:max_assets]
    selected = apply_sector_caps(selected_raw, sector_map, max_per_sector=3)

    # 🛡️ Holding Protection: Force-include user-held stocks that passed fundamentals
    # Prevents "Strategic Exit" recommendations on blue-chips purely due to weak 
    # synthetic momentum data. Only protects stocks that cleared the quality audit
    # AND have price data available (prevents KeyError in optimizer).
    protected_count = 0
    price_columns = set(buy_list_prices.columns)
    for ot in owner_tickers:
        if ot in investable_tickers and ot not in selected and ot in price_columns:
            selected.append(ot)
            protected_count += 1
            logger.info(f"[HoldingProtection] Force-included {ot} (fundamentally qualified, user-held, price available)")
        elif ot in investable_tickers and ot not in selected:
            logger.warning(f"[HoldingProtection] Cannot protect {ot} — no price data available")
    if protected_count > 0:
        logger.info(f"[HoldingProtection] Protected {protected_count} user-held blue-chips from synthetic momentum dropout")

    # 🛡️ Asset Class Protection: Force-include ETFs and Mutual Funds
    # Because ETFs track averages, their absolute momentum is mathematically lower
    # than the hottest individual stocks. If we don't protect them, the momentum 
    # sorter deletes them all, causing the 60:40 optimizer to have 0% Passive assets.
    passive_count = 0
    for tick in buy_list_tickers:
        if asset_map.get(tick) in ["ETF", "MutualFund"] and tick not in selected:
            selected.append(tick)
            passive_count += 1
    if passive_count > 0:
         logger.info(f"[AssetProtection] Force-included {passive_count} Passive Trackers into solver to enable 60/40 Equity bounds.")

    if not selected:
        logger.warning("Momentum Engine returned exactly 0 assets after constraint trimming.")
        st.error("No stocks met the criteria to proceed to Portfolio Allocation.")
        st.stop()

    # --- PORTFOLIO OPTIMIZATION ---
    limits = {
        "cap_large": cap_large,
        "cap_mid": cap_mid,
        "cap_small": cap_small,
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

    # --- INJECT UI OVERRIDES INTO REGIME ---
    regime["equity_target"] = equity_target
    regime["metals_cap"] = metals_cap
    
    try:
        with st.spinner(f"⚖️ Optimization Stage 2: {regime['optimization_mode']} Allocations..."):
            import importlib
            import core.optimizer
            importlib.reload(core.optimizer)
            raw_weights = optimize_weights(buy_list_prices, selected, regime, asset_map, sector_map, cap_map, limits)
    except Exception as opt_err:
        logger.error(f"Optimization Failure: {opt_err}", exc_info=True)
        st.error(f"⚠️ **Portfolio Optimization Fault:** The {regime['optimization_mode']} solver encountered a mathematical singularity or timeout with {len(selected)} assets.")
        st.info("💡 **Resolution:** Try reducing the **Fundamental Quality Cutoff** or **Momentum Retention** to decrease universe complexity.")
        st.stop()
    
    # Run the safety nets to force overflow into CASH (Critical for HRP which lacks native bounds)
    raw_weights = apply_asset_class_constraints(raw_weights, asset_map, underlying_map, region_map, equity_target, metals_cap)
    if raw_weights is None:
        st.error("📉 **Asset Class Constraint Failure:** Mathematical engine returned no valid weights.")
        st.stop()
        
    raw_weights = apply_sector_weight_constraints(raw_weights, sector_map, regime)
    if raw_weights is None:
        st.error("📉 **Sector Constraint Failure:** Mathematical engine returned no valid weights.")
        st.stop()

    raw_weights = apply_cap_size_constraints(raw_weights, cap_map, sector_map, regime, cap_large, cap_mid, cap_small)
    if raw_weights is None:
        st.write("DEBUG: Optimization Trace")
        st.write(f"Weights Type: {type(raw_weights)}")
        st.error("📉 **Cap Size Constraint Failure:** Mathematical engine returned no valid weights.")
        st.stop()

    raw_weights = apply_macro_overlay(raw_weights, regime)
    if raw_weights is None:
        st.error("📉 **Macro Overlay Failure:** Mathematical engine returned no valid weights.")
        st.stop()

    # 🛡️ Weight Floor for Protected Holdings
    # Force-included stocks often get near-zero weights from the optimizer because
    # their synthetic momentum is weak. This makes the trade engine sell ALL shares.
    # Fix: guarantee protected stocks get at least equal-weight allocation.
    if protected_count > 0:
        protected_tickers = [ot for ot in owner_tickers if ot in investable_tickers and ot in raw_weights]
        if protected_tickers:
            n_stocks = len([s for s, w in raw_weights.items() if w > 0.0001 and s != "CASH"])
            min_weight = max(1.0 / max(n_stocks, 1), 0.01)  # At least equal-weight, min 1%
            
            deficit = 0.0
            for pt in protected_tickers:
                current_w = raw_weights.get(pt, 0)
                if current_w < min_weight:
                    deficit += (min_weight - current_w)
                    raw_weights[pt] = min_weight
            logger.info(f"[HoldingProtection] Raised {pt} weight from {current_w:.4f} to {min_weight:.4f}")
            
            # Redistribute deficit proportionally from non-protected, non-CASH stocks
            if deficit > 0:
                non_protected = {s: w for s, w in raw_weights.items() 
                                if s not in protected_tickers and s != "CASH" and w > 0.001}
                total_non_prot = sum(non_protected.values())
                if total_non_prot > 0:
                    for s in non_protected:
                        raw_weights[s] -= deficit * (non_protected[s] / total_non_prot)
                        raw_weights[s] = max(raw_weights[s], 0)
    
    # 3. Final CASH Adjustment (Total must be 1.0)
    total_non_cash = sum([w for s, w in raw_weights.items() if s != "CASH"])
    raw_weights["CASH"] = max(0, 1.0 - total_non_cash)

    # --- ADVANCED RISK ENGINE ---
    from core.optimizer import calculate_cvar
    from core.stress_tests import run_stress_scenarios
    
    portfolio_cvar = calculate_cvar(buy_list_prices, {k: v for k, v in raw_weights.items() if k != "CASH"})
    stress_results = run_stress_scenarios(buy_list_prices, raw_weights)


    # --- TACTICAL EXECUTION AUDIT ---
    with st.spinner("🔍 Stage 3: Running Tactical Technical Audits..."):
        # Extract tickers from current holdings for auditing
        owner_holdings_tickers = [str(k.get('stock_symbol', k.get('ticker', ''))).upper() for k in st.session_state.get('holdings_list', [])]
        # Filter audit list to only include tickers that actually have price data downloaded
        audit_list = [t for t in set(selected + [k for k in owner_holdings_tickers if k]) if t in prices.columns]
        from core.tactical import get_bulk_tactical_audit
        st.session_state['tactical_audits'] = get_bulk_tactical_audit(prices[audit_list]) if not prices.empty else {}

    # Apply Churn Control (Turnover limit)
    old_state = load_portfolio_state()
    # Forcibly purge legacy Index bugs from yesterday's JSON
    if "^NSEI" in old_state: del old_state["^NSEI"]
        
    # 🛡️ Ensure we are using the LATEST optimizer logic for the dampening step
    import importlib
    import core.optimizer
    importlib.reload(core.optimizer)
    from core.optimizer import apply_turnover_control, clean_weights
    
    weights = apply_turnover_control(old_state, raw_weights, max_turnover)
    
    # 🛡️ Weight Cleaning: Consolidate dust positions (<1%) AFTER turnover smoothing
    weights = clean_weights(weights, min_weight=0.01)

    # Save State
    save_portfolio_state(weights)

    # --- PORTFOLIO P/E CALCULATION ---
    portfolio_pe = 0.0
    portfolio_pb = 0.0
    equity_weight = sum([w for s, w in weights.items() if s != "CASH"])
    if equity_weight > 0:
        for stock, weight in weights.items():
            if stock != "CASH":
                row = scoring_df[scoring_df["Stock"] == stock]
                if not row.empty:
                    stock_pe = row["PE"].values[0] if "PE" in row.columns else 21.0
                    stock_pb = row["PB"].values[0] if "PB" in row.columns else 3.3
                else:
                    stock_pe = 21.0  # Legacy stocks from turnover control
                    stock_pb = 3.3
                    
                portfolio_pe += (weight / equity_weight) * stock_pe
                portfolio_pb += (weight / equity_weight) * stock_pb
    else:
        portfolio_pe = 21.0
        portfolio_pb = 3.3

    # KPI Row
    cols = st.columns(6)
    cols[0].metric(label="Analyzed Universe", value=f"{len(scoring_df)} Stocks", delta="Live")
    cols[1].metric(label="Selected Stocks", value=f"{len(selected)} Stocks", delta=f"Top {int(top_pct_filter*100)}%")

    # P/E Metric (Inverse colored since lower PE is functionally safer/better relative value)
    pe_delta = portfolio_pe - 21.0
    cols[2].metric(label="Portfolio P/E", value=f"{portfolio_pe:.1f}x", delta=f"{pe_delta:+.1f}x vs Nifty", delta_color="inverse")

    pb_delta = portfolio_pb - 3.3
    cols[3].metric(label="Portfolio P/B", value=f"{portfolio_pb:.1f}x", delta=f"{pb_delta:+.1f}x vs Nifty", delta_color="inverse")

    cols[4].metric(label="Inflation Regime", value=regime["inflation"].title(), delta="CPI Factor", delta_color="off")
    cols[5].metric(label="Interest Rate Trend", value=regime["rate_trend"].title(), delta="Repo Factor", delta_color="off")


    # Tabs Layout
    # Tabs Layout
    tab0, tab1, tab2, tab3, tab4, tab5, tab6 = st.tabs(["📥 Data Ingestion", "🚀 Portfolio Allocation", "📈 Price Action", "🔥 Factor Heatmap", "📊 Scoreboard", "🛒 Shopping List", "🛡️ Risk Engine"])

    with tab0:
        st.subheader("📥 Step 1: Portfolio Data Ingestion")
        st.markdown("""
        Upload your current portfolio holdings to calculate trade deltas, or simply enter a **Fresh Capital** amount to generate a brand new portfolio allocation.
        """)
        
        col_ing_1, col_ing_2 = st.columns(2)
        with col_ing_1:
            portfolio_file = st.file_uploader("Upload Current Holdings (CSV)", type=["csv"], help="Expected columns: stock_symbol, isin_name, qty_longterm, qty_shortterm, avg_buy_price")
            if portfolio_file is not None:
                try:
                    import io
                    raw_content = portfolio_file.getvalue().decode("utf-8")
                    try:
                        df_holdings = pd.read_csv(io.StringIO(raw_content))
                    except Exception:
                        df_holdings = pd.read_csv(io.StringIO(raw_content), sep='\t')
                    if len(df_holdings.columns) == 1:
                         df_holdings = pd.read_csv(io.StringIO(raw_content), sep='\t')
                    
                    df_holdings = df_holdings.fillna('')
                    st.session_state['holdings_list'] = df_holdings.to_dict('records')
                    st.success(f"✅ Successfully identified {len(st.session_state['holdings_list'])} records from {portfolio_file.name}")
                    st.dataframe(df_holdings.head(5), hide_index=True)
                except Exception as e:
                    st.error(f"File parse error: {e}")
            else:
                # Don't wipe holdings on re-run — file_uploader returns None after first read
                if 'holdings_list' not in st.session_state:
                    st.session_state['holdings_list'] = []

        with col_ing_2:
            st.number_input("Fresh Capital to Deploy (₹)", min_value=0.0, key='fresh_capital', help="Enter the amount you wish to invest in the strategy. This is used to calculate share quantities.")
            st.info("💡 **Tax Tip:** Including an `avg_buy_price` column in your upload enables the execution engine to calculate precise STCG/LTCG liabilities for all trade recommendations.")

        st.markdown("---")
        if st.button("🚀 Review Portfolio & Allocate Investment", use_container_width=True):
            st.session_state['is_allocated'] = True
            st.balloons()
            st.success("Strategy computed! Switch to the other tabs to view your optimized results.")

    holdings_list = st.session_state.get('holdings_list', [])
    fresh_capital = float(st.session_state.get('fresh_capital', 0.0))

    with tab1:
        if not st.session_state['is_allocated']:
            st.warning("⚠️ **Analysis Pending**: Please go to the **📥 Data Ingestion** tab to review your holdings and click 'Review & Allocate' first.")
        else:
            with st.expander("💡 Explainability: How was this portfolio selected?"):
                final_stock_count = len([s for s, w in weights.items() if w > 0.001 and s != "CASH"])
                cash_w = weights.get("CASH", 0.0) * 100
                st.markdown(f"""
                **The quantitative engine acts as a ruthless filter, removing weak assets at every mathematical layer:**
                - 🏢 **Starting Universe**: `{len(scoring_df)}` stocks analyzed for structural financial health.
                - 🥇 **Fundamental Screen**: `{len(investable_tickers)}` stocks survived by ranking in the top tier for ROCE, Profit Growth, and low Debt.
                - 📈 **Momentum Cutoff**: `{len(selected_raw)}` stocks retained for exhibiting confirming multi-timeframe price momentum.
                - 🛡️ **Sector Caps**: Trimmed down to `{len(selected)}` finalists to prevent extreme cluster correlation.
                - ⚖️ **Optimization ({regime['optimization_mode']})**: Finalized `{final_stock_count}` precise asset allocations. `{cash_w:.1f}%` of the portfolio was systematically routed to `CASH` to prevent breaching maximum mathematical limits.
                """)

        if not st.session_state['is_allocated']:
            st.info("📥 Waiting for Data Ingestion...")
        else:
            # --- PORTFOLIO COMPARISON LOGIC ---
            from core.execution import calculate_portfolio_value
            from core.ticker_mapper import resolve_ticker
            
            # 1. Map Existing Weights using centralized parser (calculated once for all tabs)
            latest_prices = prices.iloc[-1] if isinstance(prices, pd.DataFrame) else prices
            summary = get_portfolio_summary(st.session_state['holdings_list'], latest_prices)
            st.session_state['portfolio_summary'] = summary
            
            existing_values = summary['values']
            total_existing_value = summary['total_value']
            existing_weights = summary['weights']
            
            logger.info(f"[Portfolio] Mapped {summary['matched_count']}/{len(st.session_state['holdings_list'])} holdings. Total Value: ₹{total_existing_value:,.2f}. Unmatched: {len(summary['unmatched_tickers'])}")
            
            if summary['nan_price_tickers']:
                st.warning(f"⚠️ **Price feed missing** for {len(summary['nan_price_tickers'])} holdings (e.g., {summary['nan_price_tickers'][:3]}). These are currently valued at ₹0.")
            
            # 2. Build Comparison DataFrame
            all_assets = sorted(list(set(list(weights.keys()) + list(existing_weights.keys()))))
            comparison_rows = []
            for asset in all_assets:
                tw = weights.get(asset, 0.0)
                ew = existing_weights.get(asset, 0.0)
                
                # Use a small epsilon to avoid float rounding issues
                if tw > 1e-6 or ew > 1e-6:
                    comparison_rows.append({
                        "Stock": asset,
                        "Sector": "Cash / Buffer" if asset == "CASH" else sector_map.get(asset, "Other"),
                        "Current (%)": round(ew * 100, 2),
                        "Target (%)": round(tw * 100, 2),
                        "Delta (%)": round((tw - ew) * 100, 2)
                    })
            
            df_comparison = pd.DataFrame(comparison_rows)
            if not df_comparison.empty:
                df_comparison = df_comparison.sort_values(by="Target (%)", ascending=False)

            # --- UI RENDERING ---
            st.subheader("📊 Dual-View Portfolio Analysis")
            view_mode = st.radio("Group Breakdown By:", ["Sector", "Asset"], horizontal=True)
            
            chart_col1, chart_col2 = st.columns(2)
            
            # Helper to generate pie data based on view mode
            def get_pie_data(weight_dict):
                df = pd.DataFrame(weight_dict.items(), columns=["Stock", "Weight(%)"])
                df["Weight(%)"] = (df["Weight(%)"] * 100).round(2)
                df["Sector"] = df["Stock"].apply(lambda x: "Cash / Safety Buffer" if x == "CASH" else sector_map.get(x, "Other"))
                if view_mode == "Sector":
                    return df.groupby("Sector", as_index=False)["Weight(%)"].sum(), "Sector", px.colors.qualitative.Vivid
                else:
                    return df[df["Weight(%)"] > 0], "Stock", px.colors.qualitative.Pastel

            with chart_col1:
                st.caption("Current Portfolio Exposure")
                df_curr, name_col, colors = get_pie_data(existing_weights)
                if not df_curr.empty:
                    fig_curr = px.pie(df_curr, values="Weight(%)", names=name_col, hole=0.4, color_discrete_sequence=colors)
                    fig_curr.update_layout(margin=dict(t=0, b=0, l=0, r=0), height=300, showlegend=False)
                    st.plotly_chart(fig_curr, use_container_width=True)
                else:
                    st.info("No existing holdings detected.")

            with chart_col2:
                st.caption("Quant Target Exposure")
                df_tgt, name_col, colors = get_pie_data(weights)
                if not df_tgt.empty:
                    fig_tgt = px.pie(df_tgt, values="Weight(%)", names=name_col, hole=0.4, color_discrete_sequence=colors)
                    fig_tgt.update_layout(margin=dict(t=0, b=0, l=0, r=0), height=300, showlegend=False)
                    st.plotly_chart(fig_tgt, use_container_width=True)
                else:
                    st.error("Target allocation could not be generated.")

            st.markdown("---")
            st.subheader("📋 Portfolio Comparison Matrix")
            st.dataframe(df_comparison, hide_index=True, use_container_width=True)

    with tab2:
        if not st.session_state['is_allocated']:
            st.info("📈 Waiting for Strategy Allocation...")
        else:
            # 1. Base Strategy Curve (Hardened)
            # Use ffill to bridge small gaps and fillna(0) in dot product to prevent NaN blowout
            daily_returns = prices[selected].ffill().pct_change().fillna(0)
            w_array = np.array([weights.get(s, 0.0) for s in daily_returns.columns])
            port_returns = daily_returns.dot(w_array)
            port_curve = 100 * (1 + port_returns).cumprod()
            port_curve = pd.concat([pd.Series({prices.index[0]: 100.0}), port_curve])
            
            df_curve = pd.DataFrame({"Quant Strategy": port_curve})
            color_map = {"Quant Strategy": "#00FF00"}
            
            # 2. Add Live Benchmarks
            for label, b_prices in benchmarks.items():
                b_curve = (b_prices / b_prices.iloc[0]) * 100
                df_curve[f"{label} Benchmark"] = b_curve
                
            color_map["Nifty 50 Benchmark"] = "#FF4444"
            color_map["Next 50 Benchmark"] = "#9370DB" # Purple
            color_map["Midcap 100 Benchmark"] = "#FFA500" # Orange
            color_map["Smallcap 100 Benchmark"] = "#4169E1" # Royal Blue
            
            # 3. Add Your Portfolio Curve (Hardened)
            owner_curve = None
            o_summary = st.session_state.get('portfolio_summary', {})
            o_weights = o_summary.get('weights', {})
            
            if o_weights:
                o_tickers = [t for t in o_weights.keys() if t in prices.columns]
                if o_tickers:
                    # Apply NaN hardening to Your Portfolio as well
                    o_returns_df = prices[o_tickers].ffill().pct_change().fillna(0)
                    o_w_sum = sum(o_weights[t] for t in o_tickers)
                    if o_w_sum > 1e-9:
                        o_w_array = np.array([o_weights[t] / o_w_sum for t in o_tickers])
                        o_port_returns = o_returns_df.dot(o_w_array)
                        owner_curve = 100 * (1 + o_port_returns).cumprod()
                        owner_curve = pd.concat([pd.Series({prices.index[0]: 100.0}), owner_curve])
            
            if owner_curve is not None:
                df_curve["Your Portfolio"] = owner_curve
                color_map["Your Portfolio"] = "#FFD700" # GOLD
            
            # 4. Peer/Sector Aggregates (Synthetic Fallback only if live benchmarks missing)
            all_returns = prices.pct_change()
            if "Midcap 100 Benchmark" not in df_curve:
                mid_cols = [c for c in all_returns.columns if cap_map.get(c) == "Mid"]
                if len(mid_cols) > 0:
                    mid_returns = all_returns[mid_cols].mean(axis=1)
                    mid_curve = pd.concat([pd.Series({prices.index[0]: 100.0}), 100 * (1 + mid_returns).cumprod()])
                    df_curve["Synthetic Midcap"] = mid_curve
                    color_map["Synthetic Midcap"] = "#808080"
                
            if "Smallcap 100 Benchmark" not in df_curve:
                small_cols = [c for c in all_returns.columns if cap_map.get(c) == "Small"]
                if len(small_cols) > 0:
                    small_returns = all_returns[small_cols].mean(axis=1)
                    small_curve = pd.concat([pd.Series({prices.index[0]: 100.0}), 100 * (1 + small_returns).cumprod()])
                    df_curve["Synthetic Smallcap"] = small_curve
                    color_map["Synthetic Smallcap"] = "#C0C0C0"

            fig2 = px.line(df_curve, title="Strategy Edge vs Benchmark & Your Portfolio (Base 100)",
                           color_discrete_map=color_map)
            fig2.update_traces(line=dict(width=3))
            st.plotly_chart(fig2, width='stretch')

    with tab3:
        if not st.session_state['is_allocated']:
            st.info("🔥 Waiting for Strategy Allocation...")
        else:
            st.subheader("Factor Quadrant Analysis: Momentum vs. Stability")
            st.markdown("Visualizing the 'Golden Zone'—where winning performance meets institutional-grade stability.")
            
            # Prepare data for 2D scatter
            df_plot = scores.reset_index()
            
            # Defensive Check: ensure new columns exist (handles stale cache)
            if "Momentum_Rank" not in df_plot.columns:
                st.warning("🔄 **Factor Data Refresh Required:** The dashboard is using a cached version of the previous factor engine. Please change any strategy parameter (like Lookback) to force a refresh of the 2D Quadrant view.")
                st.stop()
            
            # Add Quadrant Labels
            def get_quadrant(row):
                m = row.get("Momentum_Rank", 0)
                s = row.get("Stability_Rank", 0)
                if m >= 0.5 and s >= 0.5: return "Golden Zone"
                if m >= 0.5 and s < 0.5: return "Speculative"
                if m < 0.5 and s >= 0.5: return "Laggards"
                return "Risk Trap"
            
            df_plot["Quadrant"] = df_plot.apply(get_quadrant, axis=1)
            df_plot["Composite (%)"] = (df_plot["Composite_Score"] * 100).round(2)
            
            # 🛡️ Safety Check: Prevent Plotly from crashing on NaN or negative sizes
            df_plot["Plot_Size"] = df_plot["Composite_Score"].fillna(0.01).clip(lower=0.01)
            
            if df_plot.empty:
                st.warning("⚠️ **Heatmap Gap:** No factor data available to plot.")
            else:
                # Remove any stocks that still have NaN ranks to prevent Plotly from crashing
                df_plot = df_plot.dropna(subset=["Momentum_Rank", "Stability_Rank"])
                
                fig3 = px.scatter(
                    df_plot, 
                    x="Momentum_Rank", 
                    y="Stability_Rank",
                    color="Composite_Score",
                    size="Plot_Size",
                    hover_name="Stock",
                    color_continuous_scale="Viridis",
                    labels={"Momentum_Rank": "Momentum Strength (Percentile)", "Stability_Rank": "Trend Stability (1 - Volatility)"},
                    range_x=[0, 1.05],
                    range_y=[0, 1.05],
                    category_orders={"Quadrant": ["Golden Zone", "Speculative", "Laggards", "Risk Trap"]}
                )
                
                # High-Fidelity Quadrant Lines
                fig3.add_hline(y=0.5, line_dash="dot", line_color="rgba(255,255,255,0.3)")
                fig3.add_vline(x=0.5, line_dash="dot", line_color="rgba(255,255,255,0.3)")
                
                # Quadrant Annotations
                annotations = [
                    dict(x=0.75, y=0.95, text="GOLDEN ZONE", showarrow=False, font=dict(color="#10b981", size=14)),
                    dict(x=0.75, y=0.05, text="SPECULATIVE", showarrow=False, font=dict(color="#fbbf24", size=14)),
                    dict(x=0.25, y=0.95, text="LAGGARDS", showarrow=False, font=dict(color="#94a3b8", size=14)),
                    dict(x=0.25, y=0.05, text="RISK TRAP", showarrow=False, font=dict(color="#ef4444", size=14))
                ]
                for annot in annotations:
                    fig3.add_annotation(annot)

                fig3.update_layout(
                    coloraxis_showscale=False,
                    margin=dict(t=30, b=30, l=30, r=30),
                    height=500
                )
                
                st.plotly_chart(fig3, use_container_width=True)
            
            with st.expander("💡 How to read this chart?"):
                st.markdown("""
                - **Top Right (Golden Zone)**: Stocks with powerful uptrends and low volatility. Institutions love these.
                - **Bottom Right (Speculative)**: High octane momentum but extremely 'noisy'. High chance of sudden reversals.
                - **Top Left (Laggards)**: Stable but no growth. Good for capital preservation, poor for alpha.
                - **Bottom Left (Risk Trap)**: Low momentum and high volatility. These are usually structural wealth destroyers.
                """)

    with tab4:
        if not st.session_state['is_allocated']:
            st.info("📊 Waiting for Strategy Allocation...")
        else:
            st.subheader("Fundamental Scoreboard (Top 30% Advancing)")
            st.markdown("Displaying the continuous scoring matrix. High performers are ranked via multi-factor blending.")
        
        if not scoring_df.empty:
            # Format for display
            display_df = scoring_df.copy()
            display_df["Score"] = display_df["Fundamental_Score"] * 100
            display_df["ROCE"] = display_df["ROCE"] * 100
            display_df["ProfitGrowth"] = display_df["ProfitGrowth"] * 100
            display_df["SalesGrowth"] = display_df["SalesGrowth"] * 100
            # Specialized adaptive metrics (Defensive for cache transitions)
            for col in ["ROA", "NIM", "ForwardPE", "PB"]:
                if col not in display_df.columns:
                    display_df[col] = 0.0
            
            display_df["ROA"] = display_df["ROA"] * 100
            display_df["NIM"] = display_df["NIM"] * 100
            display_df["Rev PEG"] = display_df["ForwardPE"] / (display_df["SalesGrowth"].replace(0, 0.01) * 100)
            
            # Select key columns including specialized adaptive and valuation metrics
            display_df = display_df[["Stock", "Size", "Sector", "Score", "ROCE", "ROA", "Rev PEG", "ForwardPE", "PB", "SalesGrowth", "DebtEquity"]]
            
            # --- INJECT TACTICAL GRADES ---
            if 'tactical_audits' in st.session_state and st.session_state['tactical_audits']:
                tactical_audits = st.session_state['tactical_audits']
                display_df["Grade"] = display_df["Stock"].map(lambda x: tactical_audits.get(x, {}).get("Grade", "B (Neutral)"))
                display_df["Trend"] = display_df["Stock"].map(lambda x: tactical_audits.get(x, {}).get("Trend", "Neutral"))
                # Reorder to put Grade/Trend early
                cols = ["Stock", "Grade", "Trend", "Score", "Sector", "Size", "Rev PEG", "ForwardPE", "PB", "ROCE", "DebtEquity"]
                display_df = display_df[[c for c in cols if c in display_df.columns]]
            
            st.dataframe(
                display_df, 
                hide_index=True,
                column_config={
                    "Score": st.column_config.NumberColumn("Score", format="%.2f"),
                    "Grade": st.column_config.TextColumn("Tactical", help="Technical Entry Grade: A (Strong) to D (Avoid)"),
                    "Trend": st.column_config.TextColumn("Trend", help="Price vs SMA50/200 Status"),
                    "ROCE": st.column_config.NumberColumn("ROCE", format="%.2f%%"),
                    "ROA": st.column_config.NumberColumn("ROA", format="%.2f%%"),
                    "Rev PEG": st.column_config.NumberColumn("Rev PEG", format="%.2f"),
                    "ForwardPE": st.column_config.NumberColumn("Fwd PE", format="%.1f"),
                    "PB": st.column_config.NumberColumn("P/B", format="%.2f"),
                    "SalesGrowth": st.column_config.NumberColumn("Sales Gr.", format="%.2f%%"),
                    "DebtEquity": st.column_config.NumberColumn("D/E", format="%.2f")
                }
            )
        else:
            logger.warning("Empty fundamental matrix detected upon visualization render.")
            st.error("No fundamental data compiled.")
            
    with tab5:
        from core.ticker_mapper import ISIN_MAP
        st.subheader(f"🛒 Execution Engine: Tax-Aware Shopping List (ISIN DB: {len(ISIN_MAP)} entries)")
        if not st.session_state['is_allocated']:
            st.info("💡 **Analyze Results**: Upload data in the Ingestion tab and click 'Review & Allocate' to see your shopping list.")
        else:
            with st.spinner("Calculating trade deltas..."):
                import importlib
                import core.execution
                importlib.reload(core.execution)
                from core.execution import generate_trade_list
            df_trades, skipped_report = generate_trade_list(
                    weights, 
                    st.session_state['holdings_list'], 
                    prices, 
                    float(fresh_capital),
                    all_assessed_tickers,
                    tactical_audits=st.session_state.get('tactical_audits', {})
                )
                
            if skipped_report:
                with st.expander(f"⚠️ **Data Gaps Detected** ({len(skipped_report)} stocks skipped)"):
                    st.warning("The following stocks were included in the target portfolio but were skipped for trade generation:")
                    st.table(pd.DataFrame(skipped_report))

            if df_trades.empty:
                st.warning("⚠️ **Execution Engine Idle:** No actionable trades generated.")
                if fresh_capital <= 0 and not st.session_state['holdings_list']:
                    st.info("💡 **Resolution:** Please go to the **Data Ingestion** tab and either upload a portfolio OR enter a **Fresh Capital** amount to see trade recommendations.")
                elif fresh_capital > 0:
                    # Check if prices are just too high for the capital
                    highest_min_buy = 0
                    if weights:
                        for s, w in weights.items():
                            if s != "CASH" and w > 0.05:
                                p = 0
                                try:
                                    raw_p = prices[s].iloc[-1] if s in prices.columns else 0
                                    p = float(raw_p)
                                except: pass
                                if p > highest_min_buy: highest_min_buy = p
                    
                    if highest_min_buy > fresh_capital:
                        st.error(f"📉 **Capital Insufficiency:** Your entered capital (₹{fresh_capital:,.2f}) is lower than the price of a single share of your top picks (e.g. ₹{highest_min_buy:,.2f}).")
                        st.info("💡 **Resolution:** Increase your **Fresh Capital** to at least the price of one share of your chosen stocks.")
                    else:
                        st.info("💡 **Resolution:** Your target portfolio weights might already match your current holdings, or the minimum trade size wasn't met.")
            else:
                def highlight_tax(s):
                    return ['color: white; background-color: #ff4b4b; font-weight: bold' if "⚠️" in str(v) else '' for v in s]
                    
                st.markdown("Trades are intelligently grouped by their strategic purpose below. Click a section to expand details.")
                
                # Definitive sorting of categories
                category_priority = ["Buy Orders", "Rebalance Trims", "Strategic Exits", "Unresolved Assets"]
                
                for group_name in category_priority:
                    group_df = df_trades[df_trades["Group"] == group_name]
                    if group_df.empty:
                        continue
                    
                    # Summary metrics for header
                    total_value = group_df["Est. Value"].sum()
                    asset_count = len(group_df)
                    
                    expander_label = f"**{group_name}** ({asset_count} Assets | ₹{total_value:,.2f})"
                    
                    with st.expander(expander_label, expanded=(group_name == "Buy Orders")):
                        # Configure columns specifically for this group view
                        st.dataframe(
                            group_df.style.apply(highlight_tax, subset=['Tax Indicator']),
                            use_container_width=True,
                            hide_index=True,
                            column_config={
                                "Group": None, # Hide the grouping column as it is redundant
                                "Action": st.column_config.TextColumn(
                                    "Action",
                                    help="Trade direction",
                                ),
                                "Shares": st.column_config.NumberColumn(
                                    "Shares (Qty)",
                                    help="Quantity to trade",
                                    format="%d"
                                ),
                                "Current Price": st.column_config.NumberColumn(
                                    "Market Price (₹)",
                                    format="₹ %.2f"
                                ),
                                "Est. Value": st.column_config.NumberColumn(
                                    "Trade Value (₹)",
                                    format="₹ %.2f"
                                ),
                                "Stock": st.column_config.TextColumn(
                                    "Asset / Security",
                                    width="large"
                                ),
                                "Target Weight": st.column_config.TextColumn(
                                    "Target %"
                                ),
                                "Execution": st.column_config.TextColumn(
                                    "Plan",
                                    help="Execution strategy: Bulk (Immediate) vs. Staggered (Trend Follow)"
                                ),
                                "Tactical Note": st.column_config.TextColumn(
                                    "Institutional Note",
                                    width="large"
                                )
                            }
                        )

    with tab6:
        if not st.session_state['is_allocated']:
            st.info("🛡️ Waiting for Portfolio Statistics...")
        else:
            st.subheader("🛡️ Institutional Risk Engine")
            
            risk_col1, risk_col2 = st.columns(2)
            
            with risk_col1:
                # Use a small epsilon to avoid divide by zero if prices are empty
                if not buy_list_prices.empty:
                    st.metric(label="Portfolio 95% CVaR", value=f"{portfolio_cvar*100:.2f}%", 
                            help="Conditional Value at Risk (Expected Shortfall). If the 5% worst-case event happens, this is the expected average loss.")
                    st.progress(min(max(abs(portfolio_cvar) * 5, 0.0), 1.0))
                    if abs(portfolio_cvar) < 0.03:
                        st.success("✅ Risk Profile: Low (Defensive)")
                    elif abs(portfolio_cvar) < 0.05:
                        st.warning("⚠️ Risk Profile: Moderate")
                    else:
                        st.error("🚨 Risk Profile: Aggressive (High Tail Risk)")
                else:
                    st.metric(label="Portfolio 95% CVaR", value="N/A")

            with risk_col2:
                # Gauge-like indicator for diversifying assets
                p_count = len([s for s in weights if asset_map.get(s) != "Equity" and s != "CASH"])
                st.metric(label="Diversification Assets", value=p_count, help="Count of non-equity assets (ETFs, Gold, Liquid) for crash protection.")
            
            st.markdown("---")
            st.subheader("🌋 Stress Scenario Simulations")
            st.caption("Projected portfolio impact under absolute black-swan market conditions.")
            
            if stress_results:
                stress_cols = st.columns(len(stress_results))
                for i, (name, impact_val) in enumerate(stress_results.items()):
                    stress_cols[i % len(stress_cols)].metric(label=name, value=f"{impact_val*100:.2f}%", 
                                          delta=f"{impact_val*100:.1f}%", delta_color="inverse")
            else:
                st.info("Not enough historical volatility data to run stress simulations.")

    logger.info("Streamlit Application rendered successfully.")

except Exception as e:
    logger.error("Structural Runtime Fault Encountered in Dashboard GUI Pipeline.", exc_info=True)
    st.error("⚠️ System encountered a structural fault. Please review `logs/quant_system.log` for precise technical details.")
    st.stop()
# --- FINAL: RENDER SIDEBAR DIAGNOSTICS ---
if st.session_state.get('is_allocated', False):
    st.sidebar.markdown("---")
    st.sidebar.subheader("🛡️ Engine Diagnostics")
    try:
        # Use session state or local variables to pull final counts
        d_uni = len(scoring_df) if 'scoring_df' in locals() else 0
        d_sco = len(scores) if 'scores' in locals() else 0
        d_sel = len(selected) if 'selected' in locals() else 0
        d_hld = len([w for w in weights.values() if w > 0.001 and w != 'CASH']) if 'weights' in locals() else 0
        
        # Financial Buffer Audit
        from core.execution import calculate_portfolio_value
        fresh_cap = float(st.session_state.get('fresh_capital', 0.0))
        h_list = st.session_state.get('holdings_list', [])
        fin_buffer = calculate_portfolio_value(h_list, prices, fresh_cap)
        
        st.sidebar.code(f"""
Universe Discovery: {d_uni}
Factor Scoring   : {d_sco}
Strategy Segment : {d_sel}
Target Positions : {d_hld}
Allocated Capital: ₹{fin_buffer:,.0f}
        """)
        
        if d_hld > 0:
            st.sidebar.success(f"✅ Engine active with {d_hld} targets.")
        else:
            st.sidebar.warning("⚠️ Engine idle: 0 targets.")
            
    except Exception as diag_err:
        logger.debug(f"Sidebar diagnostics deferred until next run: {diag_err}")
