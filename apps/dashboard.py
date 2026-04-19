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
from core.optimizer import optimize_weights, apply_macro_overlay, apply_turnover_control, apply_sector_weight_constraints, apply_cap_size_constraints
from core.macro import load_macro_data, compute_macro_regime
from core.universe import fetch_broad_universe, apply_fundamental_filters
from core.ticker_mapper import resolve_ticker
from core.state import load_portfolio_state, save_portfolio_state
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

try:
    # --- SIDEBAR & INTERACTIVITY ---
    st.sidebar.title("⚙️ Strategy Parameters")
    
    st.sidebar.header("Strategy Tuning")
    fund_cutoff = st.sidebar.slider("Fundamental Quality Cutoff (%)", 0.1, 1.0, 0.3, help="Controls the percentage of the universe that passes the initial quality audit (ROCE, Cash Quality, Promoter Skin).")
    mom_lookback = st.sidebar.slider("Momentum Lookback (Days)", 30, 252, 90, help="Number of trading days to track for historical price momentum. Usually 90-180 days.")
    vol_lookback = st.sidebar.slider("Volatility Lookback (Days)", 30, 252, 60, help="Amount of history used to compute standard deviation. Lower values react faster to market crashes.")
    top_pct_filter = st.sidebar.slider("Momentum Retention Cutoff (%)", 0.1, 1.0, 0.5, help="Percentage of fundamental survivors to execute Momentum buying on. Example: 0.5 means buy top 50%.")
    max_turnover = st.sidebar.slider("Max Turnover Damper (%)", 0.05, 1.0, 0.30, help="Smooths giant target rebalances to prevent huge trading fees & slippage. 0.30 means weights move 30% per iteration.")

    # --- UNIVERSE DISCOVERY ---
    @st.cache_data(ttl=86400)
    def get_investable_universe(top_pct):
        logger.info(f"Triggered Universe Cache Refresh with Cutoff: {top_pct}")
        broad_universe = fetch_broad_universe("nifty50")
        return apply_fundamental_filters(broad_universe, top_percentile=top_pct)

    with st.spinner("🤖 Evaluating Fundamental Screener (Daily Cache)..."):
        investable_tickers, sector_map, cap_map, scoring_df = get_investable_universe(fund_cutoff)
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
        
    nifty_prices = None
    if "^NSEI" in prices.columns:
        nifty_prices = prices["^NSEI"]
        prices = prices.drop(columns=["^NSEI"])

    # --- DATA QUALITY WARNINGS ---
    # Warn users when analysis is based on non-live data
    if not scoring_df.empty and "DataSource" in scoring_df.columns:
        live_count = (scoring_df["DataSource"] == "Live").sum()
        curated_count = (scoring_df["DataSource"] == "Curated Profile").sum()
        synthetic_count = (scoring_df["DataSource"] == "Synthetic Random").sum()
        total = len(scoring_df)
        
        if live_count == total:
            st.sidebar.success(f"📡 **Fundamental Data:** Live ({total} stocks via Yahoo Finance)")
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

    # Compute factors dynamically ONLY for the fundamentally approved survivors (Top 30%)
    # This ensures we don't accidentally buy a low-quality stock just because it has high momentum.
    buy_list_prices = prices[[t for t in investable_tickers if t in prices.columns]]
    
    if buy_list_prices.empty:
        if scoring_df.empty or investable_tickers == []:
            st.warning("⚠️ **Strategy Gap:** No stocks passed the current Institutional Quality criteria.")
            st.info("💡 **Resolution:** Try moving the **Fundamental Quality Cutoff (%)** slider to a higher value to broaden the screening funnel.")
        else:
            st.error("⚠️ **Data Connectivity Issue:** Stocks passed the quality check, but no historical price data was found.")
            st.info("💡 **Resolution:** The system is currently operating in network-restricted mode. Check `logs/quant_system.log` to confirm if Simulation Mode is active.")
        st.stop()

    scores = compute_factor_scores(buy_list_prices, {
        "momentum_lookback_days": mom_lookback,
        "volatility_lookback_days": vol_lookback
    })

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

    try:
        with st.spinner(f"⚖️ Optimization Stage 2: {regime['optimization_mode']} Allocations..."):
            raw_weights = optimize_weights(buy_list_prices, selected, regime, sector_map, cap_map, limits)
    except Exception as opt_err:
        logger.error(f"Optimization Failure: {opt_err}", exc_info=True)
        st.error(f"⚠️ **Portfolio Optimization Fault:** The {regime['optimization_mode']} solver encountered a mathematical singularity or timeout with {len(selected)} assets.")
        st.info("💡 **Resolution:** Try reducing the **Fundamental Quality Cutoff** or **Momentum Retention** to decrease universe complexity.")
        st.stop()
    
    # Run the safety nets to force overflow into CASH (Critical for HRP which lacks native bounds)
    raw_weights = apply_sector_weight_constraints(raw_weights, sector_map, regime)
    raw_weights = apply_cap_size_constraints(raw_weights, cap_map, cap_large, cap_mid, cap_small)
    raw_weights = apply_macro_overlay(raw_weights, regime)

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
                        raw_weights[s] = max(raw_weights[s], 0)  # Floor at 0

    # Apply Churn Control
    old_state = load_portfolio_state()
    # Forcibly purge legacy Index bugs from yesterday's JSON
    if "^NSEI" in old_state:
        del old_state["^NSEI"]
        
    weights = apply_turnover_control(old_state, raw_weights, max_turnover)

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
    tab0, tab1, tab2, tab3, tab4, tab5 = st.tabs(["📥 Data Ingestion", "🚀 Portfolio Allocation", "📈 Price Action", "🔥 Factor Heatmap", "📊 Scoreboard", "🛒 Shopping List"])

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
            st.session_state['fresh_capital'] = st.number_input("Fresh Capital to Deploy (₹)", min_value=0.0, value=st.session_state.get('fresh_capital', 0.0), step=1000.0)
            st.info("💡 **Tax Tip:** Including an `avg_buy_price` column in your upload enables the execution engine to calculate precise STCG/LTCG liabilities for all trade recommendations.")

        st.markdown("---")
        if st.button("🚀 Review Portfolio & Allocate Investment", use_container_width=True):
            st.session_state['is_allocated'] = True
            st.balloons()
            st.success("Strategy computed! Switch to the other tabs to view your optimized results.")

    holdings_list = st.session_state['holdings_list']
    fresh_capital = st.session_state['fresh_capital']

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
            
            # 1. Map Existing Weights
            existing_values = {}
            total_existing_value = 0.0
            latest_prices = prices.iloc[-1] if isinstance(prices, pd.DataFrame) else prices
            
            for hl in st.session_state['holdings_list']:
                cl = {str(k).strip().lower(): v for k, v in hl.items()}
                rt = str(cl.get('stock_symbol', cl.get('ticker', ''))).strip().upper()
                ri = str(cl.get('isin_name', cl.get('isin_code', ''))).strip().upper()
                if rt:
                    rest, _ = resolve_ticker(rt, isin=ri)
                    if rest in prices.columns or rest == "CASH":
                        qty = float(cl.get('qty_longterm', 0) or 0) + float(cl.get('qty_shortterm', 0) or 0)
                        p = latest_prices[rest] if rest != "CASH" else 1.0
                        val = qty * p
                        existing_values[rest] = existing_values.get(rest, 0.0) + val
                        total_existing_value += val
            
            existing_weights = {s: v / total_existing_value for s, v in existing_values.items()} if total_existing_value > 0 else {}
            
            # 2. Build Comparison DataFrame
            all_assets = sorted(list(set(list(weights.keys()) + list(existing_weights.keys()))))
            comparison_rows = []
            for asset in all_assets:
                tw = weights.get(asset, 0.0)
                ew = existing_weights.get(asset, 0.0)
                
                if tw > 0.0001 or ew > 0.0001:
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
            st.caption("Comparative breakdown of current vs. target weights (Filtered for >0.01% weight)")
            st.dataframe(df_comparison, hide_index=True, use_container_width=True)

    with tab2:
        if not st.session_state['is_allocated']:
            st.info("📈 Waiting for Strategy Allocation...")
        elif nifty_prices is not None:
            st.subheader("Historical Alpha Generation (6-Month)")
            nifty_curve = (nifty_prices / nifty_prices.iloc[0]) * 100
            
            # Synthesize Base BenchMARKS and the OWNER PORTFOLIO
            all_returns = prices.pct_change().dropna()
            
            # --- QUANT STRATEGY CURVE ---
            daily_returns = prices[selected].pct_change().dropna()
            w_array = np.array([weights.get(s, 0.0) for s in daily_returns.columns])
            port_returns = daily_returns.dot(w_array)
            port_curve = 100 * (1 + port_returns).cumprod()
            port_curve = pd.concat([pd.Series({prices.index[0]: 100.0}), port_curve])
            
            # --- OWNER PORTFOLIO CURVE CALCULATION ---
            owner_curve = None
            if st.session_state['holdings_list']:
                # Resolve weights based on current market value
                owner_raw_values = {}
                latest_p = prices.iloc[-1]
                for hl in st.session_state['holdings_list']:
                    cl = {str(k).strip().lower(): v for k, v in hl.items()}
                    rt = str(cl.get('stock_symbol', cl.get('ticker', ''))).strip().upper()
                    ri = str(cl.get('isin_name', cl.get('isin_code', ''))).strip().upper()
                    if rt:
                        rest, _ = resolve_ticker(rt, isin=ri)
                        if rest in prices.columns:
                            qty = float(cl.get('qty_longterm', 0) or 0) + float(cl.get('qty_shortterm', 0) or 0)
                            price = latest_p[rest]
                            owner_raw_values[rest] = owner_raw_values.get(rest, 0.0) + (qty * price)
                
                total_o_val = sum(owner_raw_values.values())
                if total_o_val > 0:
                    o_weights = {t: v / total_o_val for t, v in owner_raw_values.items()}
                    # Filter returns to only those tickers we own
                    o_tickers = list(o_weights.keys())
                    o_returns_df = all_returns[o_tickers]
                    o_w_array = np.array([o_weights[t] for t in o_tickers])
                    
                    o_port_returns = o_returns_df.dot(o_w_array)
                    owner_curve = 100 * (1 + o_port_returns).cumprod()
                    owner_curve = pd.concat([pd.Series({prices.index[0]: 100.0}), owner_curve])
            
            df_curve = pd.DataFrame({
                "Quant Strategy": port_curve,
                "Nifty 50 Benchmark": nifty_curve
            })
            
            color_map = {
                "Quant Strategy": "#00FF00", 
                "Nifty 50 Benchmark": "#FF4444"
            }

            if owner_curve is not None:
                df_curve["Owner Portfolio"] = owner_curve
                color_map["Owner Portfolio"] = "#FFD700" # GOLD
            
            mid_cols = [c for c in all_returns.columns if cap_map.get(c) == "Mid"]
            small_cols = [c for c in all_returns.columns if cap_map.get(c) == "Small"]
            
            if len(mid_cols) > 0:
                mid_returns = all_returns[mid_cols].mean(axis=1)
                mid_curve = pd.concat([pd.Series({prices.index[0]: 100.0}), 100 * (1 + mid_returns).cumprod()])
                df_curve["Synthetic Midcap (Next50)"] = mid_curve
                color_map["Synthetic Midcap (Next50)"] = "#FFA500" # Orange
                
            if len(small_cols) > 0:
                small_returns = all_returns[small_cols].mean(axis=1)
                small_curve = pd.concat([pd.Series({prices.index[0]: 100.0}), 100 * (1 + small_returns).cumprod()])
                df_curve["Synthetic Smallcap (250)"] = small_curve
                color_map["Synthetic Smallcap (250)"] = "#4169E1" # Royal Blue
            
            fig2 = px.line(df_curve, title="Strategy Edge vs Benchmark & Your Portfolio (Base 100)",
                           color_discrete_map=color_map)
            fig2.update_traces(line=dict(width=3))
            st.plotly_chart(fig2, width='stretch')
            
        else:
            st.error("Nifty 50 anchor missing from data extraction.")

    with tab3:
        if not st.session_state['is_allocated']:
            st.info("🔥 Waiting for Strategy Allocation...")
        else:
            st.subheader("Momentum vs Risk Ranking")
            st.markdown("Visualizing the final composite momentum factor structure.")
        
        df_scores = scores.reset_index()
        df_scores.columns = ["Stock", "Composite Score"]
        df_scores["Composite Score"] = df_scores["Composite Score"] * 100
        
        fig3 = px.bar(df_scores, x="Stock", y="Composite Score", color="Composite Score", 
                      color_continuous_scale="Viridis", text="Composite Score")
        fig3.update_traces(texttemplate='%{text:.2f}', textposition='outside')
        fig3.update_layout(xaxis_tickangle=-45)
        st.plotly_chart(fig3, width='stretch')

    with tab4:
        if not st.session_state['is_allocated']:
            st.info("📊 Waiting for Strategy Allocation...")
        else:
            st.subheader("Fundamental Scoreboard (Top 30% Advancing)")
            st.markdown("Displaying the continuous scoring matrix. High performers are ranked via multi-factor blending.")
        
        if not scoring_df.empty:
            # Format for display
            display_df = scoring_df.copy()
            display_df["Fundamental_Score"] = display_df["Fundamental_Score"] * 100
            display_df["ROCE"] = display_df["ROCE"] * 100
            display_df["ProfitGrowth"] = display_df["ProfitGrowth"] * 100
            display_df["SalesGrowth"] = display_df["SalesGrowth"] * 100
            
            # Select key columns including new Size tier
            display_df = display_df[["Stock", "Size", "Sector", "Fundamental_Score", "ROCE", "ProfitGrowth", "SalesGrowth", "DebtEquity"]]
            
            st.dataframe(
                display_df, 
                hide_index=True,
                column_config={
                    "Fundamental_Score": st.column_config.NumberColumn("Score", format="%.2f"),
                    "ROCE": st.column_config.NumberColumn("ROCE", format="%.2f%%"),
                    "ProfitGrowth": st.column_config.NumberColumn("Profit Gr.", format="%.2f%%"),
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
                df_trades = generate_trade_list(
                    weights, 
                    holdings_list, 
                    prices, 
                    fresh_capital,
                    assessed_tickers=all_assessed_tickers
                )
                
            if df_trades.empty:
                st.warning("No actionable trades generated based on current capital and targets.")
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
                                )
                            }
                        )

    logger.info("Streamlit Application rendered successfully.")

except Exception as e:
    logger.error("Structural Runtime Fault Encountered in Dashboard GUI Pipeline.", exc_info=True)
    st.error("⚠️ System encountered a structural fault. Please review `logs/quant_system.log` for precise technical details.")
    st.stop()
