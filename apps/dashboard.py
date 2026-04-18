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
from core.execution import generate_trade_list
from core.state import load_portfolio_state, save_portfolio_state
from core.logger import logger

st.set_page_config(page_title="Quant Dashboard", layout="wide", page_icon="📈")

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

# --- SIDEBAR & INTERACTIVITY ---
st.sidebar.title("⚙️ Strategy Parameters")


try:
    logger.info("Executing Streamlit Pipeline - Rebuilding Target Matrix...")
    
    st.markdown("## 📊 Quant Portfolio Dashboard")
    st.caption("A reactive, multi-factor quantitative portfolio builder using Risk-Adjusted Momentum.")

    # --- UNIVERSE DISCOVERY ---
    @st.cache_data(ttl=86400)
    def get_investable_universe():
        logger.info("Triggered Universe Cache Refresh.")
        broad_universe = fetch_broad_universe("nifty50")
        return apply_fundamental_filters(broad_universe)

    with st.spinner("🤖 Evaluating Fundamental Screener (Daily Cache)..."):
        tickers, sector_map, cap_map, scoring_df = get_investable_universe()

    # Fetch
    with st.spinner("Fetching market data (Cached)..."):
        prices = get_cached_prices(tickers)
        
    nifty_prices = None
    if "^NSEI" in prices.columns:
        nifty_prices = prices["^NSEI"]
        prices = prices.drop(columns=["^NSEI"])
        
    # Sidebar
    st.sidebar.header("Retail Execution (Optional)")
    portfolio_file = st.sidebar.file_uploader("Upload Current Holdings (CSV)", type=["csv"], help="Expected columns: stock_symbol, isin_name, qty_longterm, qty_shortterm")
    fresh_capital = st.sidebar.number_input("Fresh Capital to Deploy", min_value=0.0, value=0.0, step=1000.0)
    
    holdings_list = []
    if portfolio_file is not None:
        try:
            df_holdings = pd.read_csv(portfolio_file, sep=None, engine='python')
            holdings_list = df_holdings.to_dict('records')
            st.sidebar.success(f"Loaded {len(holdings_list)} legacy positions.")
        except Exception as e:
            st.sidebar.error("File parse error. Check strictly for columns: stock_symbol, isin_name, qty_longterm, qty_shortterm")

    st.sidebar.markdown("---")
    st.sidebar.header("Strategy Tuning")
    mom_lookback = st.sidebar.slider("Momentum Lookback (Days)", 30, 252, 90, help="Number of trading days to track for historical price momentum. Usually 90-180 days.")
    vol_lookback = st.sidebar.slider("Volatility Lookback (Days)", 30, 252, 60, help="Amount of history used to compute standard deviation. Lower values react faster to market crashes.")
    top_pct_filter = st.sidebar.slider("Momentum Retention Cutoff (%)", 0.1, 1.0, 0.5, help="Percentage of fundamental survivors to execute Momentum buying on. Example: 0.5 means buy top 50%.")
    max_turnover = st.sidebar.slider("Max Turnover Damper (%)", 0.05, 1.0, 0.30, help="Smooths giant target rebalances to prevent huge trading fees & slippage. 0.30 means weights move 30% per iteration.")
    
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

    # Compute factors dynamically
    scores = compute_factor_scores(prices, {
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

    if not selected:
        logger.warning("Momentum Engine returned exactly 0 assets after constraint trimming.")
        st.error("No stocks met the criteria to proceed to Portfolio Allocation.")
        st.stop()

    # Optimize
    logger.info(f"Optimizing via {regime['optimization_mode']} Allocations...")
    
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
    
    raw_weights = optimize_weights(prices, selected, regime, sector_map, cap_map, limits)
    
    # Run the safety nets to force overflow into CASH (Critical for HRP which lacks native bounds)
    raw_weights = apply_sector_weight_constraints(raw_weights, sector_map, regime)
    raw_weights = apply_cap_size_constraints(raw_weights, cap_map, cap_large, cap_mid, cap_small)
    raw_weights = apply_macro_overlay(raw_weights, regime)

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
    tab1, tab2, tab3, tab4, tab5 = st.tabs(["🚀 Portfolio Allocation", "📈 Price Action", "🔥 Factor Heatmap", "📊 Scoreboard", "🛒 Shopping List"])

    with tab1:
        with st.expander("💡 Explainability: How was this portfolio selected?"):
            final_stock_count = len([s for s, w in weights.items() if w > 0.001 and s != "CASH"])
            cash_w = weights.get("CASH", 0.0) * 100
            st.markdown(f"""
            **The quantitative engine acts as a ruthless filter, removing weak assets at every mathematical layer:**
            - 🏢 **Starting Universe**: `{len(scoring_df)}` stocks analyzed for structural financial health.
            - 🥇 **Fundamental Screen**: `{len(tickers)}` stocks survived by ranking in the top tier for ROCE, Profit Growth, and low Debt.
            - 📈 **Momentum Cutoff**: `{len(selected_raw)}` stocks retained for exhibiting confirming multi-timeframe price momentum.
            - 🛡️ **Sector Caps**: Trimmed down to `{len(selected)}` finalists to prevent extreme cluster correlation.
            - ⚖️ **Optimization ({regime['optimization_mode']})**: Finalized `{final_stock_count}` precise asset allocations. `{cash_w:.1f}%` of the portfolio was systematically routed to `CASH` to prevent breaching maximum mathematical limits.
            """)

        col1, col2 = st.columns([1, 2])
        
        with col1:
            st.subheader("Target Allocations")
            df_weights = pd.DataFrame(weights.items(), columns=["Stock", "Weight(%)"])
            df_weights["Weight(%)"] = (df_weights["Weight(%)"] * 100).round(2)
            df_weights["Sector"] = df_weights["Stock"].apply(lambda x: "Cash / Safety Buffer" if x == "CASH" else sector_map.get(x, "Other"))
            
            # Sort heavily allocated stocks to the top
            df_weights = df_weights.sort_values(by="Weight(%)", ascending=False)
            
            # Enforce a tight height bound to trigger the native Streamlit vertical scrollbar
            st.dataframe(df_weights[["Stock", "Sector", "Weight(%)"]], hide_index=True, height=400)
            
        with col2:
            st.subheader("Exposure Analysis")
            view_mode = st.radio("Breakdown By:", ["Sector", "Asset"], horizontal=True, label_visibility="collapsed")
            
            if view_mode == "Asset":
                fig = px.pie(df_weights, values="Weight(%)", names="Stock", hole=0.4, 
                             color_discrete_sequence=px.colors.qualitative.Pastel)
            else:
                df_sectors = df_weights.groupby("Sector", as_index=False)["Weight(%)"].sum()
                fig = px.pie(df_sectors, values="Weight(%)", names="Sector", hole=0.4, 
                             color_discrete_sequence=px.colors.qualitative.Vivid)
                             
            fig.update_traces(textposition='inside', textinfo='percent+label')
            fig.update_layout(margin=dict(t=10, b=20, l=10, r=10), height=380)
            st.plotly_chart(fig, use_container_width=True)

    with tab2:
        st.subheader("Historical Alpha Generation (6-Month)")
        
        if nifty_prices is not None:
            nifty_curve = (nifty_prices / nifty_prices.iloc[0]) * 100
            
            # Synthesize the exact portfolio curve from live weights
            daily_returns = prices[selected].pct_change().dropna()
            w_array = np.array([weights.get(s, 0.0) for s in daily_returns.columns])
            
            port_returns = daily_returns.dot(w_array)
            port_curve = 100 * (1 + port_returns).cumprod()
            
            # Align origin
            port_curve = pd.concat([pd.Series({prices.index[0]: 100.0}), port_curve])
            
            df_curve = pd.DataFrame({
                "Quant Strategy": port_curve,
                "Nifty 50 Benchmark": nifty_curve
            })
            
            color_map = {
                "Quant Strategy": "#00FF00", 
                "Nifty 50 Benchmark": "#FF4444"
            }
            
            # Synthesize Base Benchmarks for Mid and Small Caps (Equal Weighted Component Drift)
            all_returns = prices.pct_change().dropna()
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
            
            fig2 = px.line(df_curve, title="Strategy Edge vs Multi-Cap Benchmarks (Base 100)",
                           color_discrete_map=color_map)
            fig2.update_traces(line=dict(width=3))
            st.plotly_chart(fig2, width='stretch')
            
        else:
            st.error("Nifty 50 anchor missing from data extraction.")

    with tab3:
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
        st.subheader("🛒 Execution Engine: Tax-Aware Shopping List")
        if not holdings_list and fresh_capital <= 0:
            st.info("💡 **Activate Retail Execution**: Upload your existing portfolio CSV or input Fresh Capital in the sidebar to generate a deterministic integer-share shopping list.")
        else:
            with st.spinner("Calculating trade deltas..."):
                df_trades = generate_trade_list(weights, holdings_list, prices, fresh_capital)
                
            if df_trades.empty:
                st.warning("No actionable trades generated based on current capital and targets.")
            else:
                def highlight_tax(s):
                    return ['color: white; background-color: #ff4b4b; font-weight: bold' if "⚠️" in str(v) else '' for v in s]
                    
                st.markdown("This list calculates exactly how many shares you need to buy or sell to reach the mathematical target, accounting for the cash you've added today.")
                st.dataframe(
                    df_trades.style.apply(highlight_tax, subset=['Tax Indicator']), 
                    use_container_width=True,
                    hide_index=True
                )

    logger.info("Streamlit Application rendered successfully.")

except Exception as e:
    logger.error("Structural Runtime Fault Encountered in Dashboard GUI Pipeline.", exc_info=True)
    st.error("⚠️ System encountered a structural fault. Please review `logs/quant_system.log` for precise technical details.")
    st.stop()
