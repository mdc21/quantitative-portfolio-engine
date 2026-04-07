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
from core.optimizer import optimize_weights, apply_macro_overlay, apply_turnover_control, apply_sector_weight_constraints
from core.macro import load_macro_data, compute_macro_regime
from core.universe import fetch_broad_universe, apply_fundamental_filters
from core.state import load_portfolio_state, save_portfolio_state
from core.logger import logger

st.set_page_config(page_title="Quant Dashboard", layout="wide", page_icon="📈")

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

st.sidebar.markdown("### Component Lookbacks")
mom_lookback = st.sidebar.slider("Momentum Lookback (Days)", 30, 200, 90, 10)
try:
    logger.info("Executing Streamlit Pipeline - Rebuilding Target Matrix...")
    
    st.title("📊 Quant Portfolio Dashboard")
    st.markdown("A reactive, multi-factor quantitative portfolio builder using Risk-Adjusted Momentum.")

    # --- UNIVERSE DISCOVERY ---
    @st.cache_data(ttl=86400)
    def get_investable_universe():
        logger.info("Triggered Universe Cache Refresh.")
        broad_universe = fetch_broad_universe("nifty50")
        return apply_fundamental_filters(broad_universe)

    with st.spinner("🤖 Evaluating Fundamental Screener (Daily Cache)..."):
        tickers, sector_map, scoring_df = get_investable_universe()

    # Fetch
    with st.spinner("Fetching market data (Cached)..."):
        prices = get_cached_prices(tickers)
        
    nifty_prices = None
    if "^NSEI" in prices.columns:
        nifty_prices = prices["^NSEI"]
        prices = prices.drop(columns=["^NSEI"])
        
    # Sidebar
    st.sidebar.header("Strategy Tuning")
    mom_lookback = st.sidebar.slider("Momentum Lookback (Days)", 30, 252, 90)
    vol_lookback = st.sidebar.slider("Volatility Lookback (Days)", 30, 252, 60)
    top_pct_filter = st.sidebar.slider("Momentum Retention Cutoff (%)", 0.1, 1.0, 0.5)
    max_turnover = st.sidebar.slider("Max Turnover Damper (%)", 0.05, 1.0, 0.30)

    # Compute factors dynamically
    scores = compute_factor_scores(prices, {
        "momentum_lookback_days": mom_lookback,
        "volatility_lookback_days": vol_lookback
    })

    # Macro Integration
    repo, cpi = load_macro_data()
    regime = compute_macro_regime(repo, cpi)

    # Filter & Sector Constraints
    selected_raw = select_top_momentum(scores, top_percent=top_pct_filter)
    selected = apply_sector_caps(selected_raw, sector_map, max_per_sector=3)

    if not selected:
        logger.warning("Momentum Engine returned exactly 0 assets after constraint trimming.")
        st.error("No stocks met the criteria to proceed to Portfolio Allocation.")
        st.stop()

    # Optimize
    logger.info("Optimizing Inverse Volatility Allocations...")
    raw_weights = optimize_weights(prices, selected)
    raw_weights = apply_sector_weight_constraints(raw_weights, sector_map, regime)
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
    st.markdown("---")
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
    st.markdown("---")


    # Tabs Layout
    tab1, tab2, tab3, tab4 = st.tabs(["🚀 Portfolio Allocation", "📈 Price Action", "🔥 Factor Heatmap", "📊 Fundamental Scoreboard"])

    with tab1:
        col1, col2 = st.columns([1, 2])
        
        with col1:
            st.subheader("Target Allocations")
            df_weights = pd.DataFrame(weights.items(), columns=["Stock", "Weight(%)"])
            df_weights["Weight(%)"] = (df_weights["Weight(%)"] * 100).round(2)
            st.dataframe(df_weights, hide_index=True)
            
        with col2:
            fig = px.pie(df_weights, values="Weight(%)", names="Stock", hole=0.4, 
                         title="Capital Distribution", color_discrete_sequence=px.colors.qualitative.Pastel)
            fig.update_traces(textposition='inside', textinfo='percent+label')
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
                "Nifty 50": nifty_curve
            })
            
            fig2 = px.line(df_curve, title="Strategy Edge vs Benchmark (Base 100)",
                           color_discrete_map={"Quant Strategy": "#00FF00", "Nifty 50": "#FF4444"})
            fig2.update_traces(line=dict(width=3))
            st.plotly_chart(fig2, use_container_width=True)
            
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
        st.plotly_chart(fig3, use_container_width=True)

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
            
            # Select key columns
            display_df = display_df[["Stock", "Sector", "Fundamental_Score", "ROCE", "ProfitGrowth", "SalesGrowth", "DebtEquity"]]
            
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
            
    logger.info("Streamlit Application rendered successfully.")

except Exception as e:
    logger.error("Structural Runtime Fault Encountered in Dashboard GUI Pipeline.", exc_info=True)
    st.error("⚠️ System encountered a structural fault. Please review `logs/quant_system.log` for precise technical details.")
    st.stop()
