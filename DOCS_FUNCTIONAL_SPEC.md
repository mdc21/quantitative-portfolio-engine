# Functional Specification: Quantitative Portfolio Engine (v2.0)

## 1. Executive Summary
The Quantitative Portfolio Engine is an institutional-grade investment system designed to manage multi-asset portfolios with a strict focus on **Risk-Adjusted Momentum** and **Hierarchical Mandate Enforcement**. The system is engineered to operate in highly restricted network environments through a multi-layered synthetic failover architecture.

---

## 2. Core Allocation Mandates (The 60/40 Rule)
The engine enforces a top-down hierarchical allocation structure to ensure institutional compliance:

### 2.1 Asset Class Split (Global)
*   **Equities (Target: 60%)**: Directly invested in a curated multi-cap universe. Selection is driven by Adaptive Quality Scoring and Momentum.
*   **Passive / Index (Target: 40%)**: Allocated to ETFs and Mutual Funds. This bucket is optimized for structural stability rather than alpha generation.

### 2.2 Passive Bucket Diversification (Geographic)
Within the 40% Passive Allocation, a rigorous split is maintained:
*   **Domestic Trackers (80% of Passive)**: Targeted at local indices (Nifty 50, Bank Nifty) and domestic commodities (Gold/Silver BEES).
*   **International Trackers (20% of Passive)**: Geographically routed to global trackers (MON100, Nasdaq 100) to ensure global currency hedge and sector diversification.

---

## 3. The Constraint & Optimization Engine

### 3.1 Mandate Targeting Engine (Scaling)
Unlike simple "capping" models, the engine uses an **Upscaling Scalar**. If the optimizer initially under-weights the Passive bucket (e.g., to 10%), the targeting engine mathematically upscales the relative weights to hit the 40% mandate exactly, ensuring no capital remains unintentionally idle in CASH.

### 3.2 Multi-Layered Protection
*   **Sector Weight Constraints**: Hard caps (10-20% default) on broad sectors (Financials, Technology, etc.) to prevent cluster risk.
*   **Cap-Size Guardrails**: Ensures a balanced exposure across Large-cap, Mid-cap, and Small-cap buckets.
*   **Metal Ceiling**: A global hard cap (5%) on all metal-underlying assets (Gold/Silver) to limit commodity-driven volatility.

### 3.3 Dynamic Optimization Modes
*   **HRP (Hierarchical Risk Parity)**: The primary engine for the Passive bucket and "Falling" trend regimes. Minimizes correlation-based risk without requiring expected returns.
*   **Markowitz (Mean-Variance)**: Activated during "Rising" trends for the Equity bucket to maximize alpha through quadratic programming.

---

## 4. Resilience Engineering (The "Fast-Pass" Pipeline)
Designed for extreme reliability, the engine can execute a full rebalance even with ZERO external connectivity:

### 4.1 Global Connectivity Probe
At the start of any run, the system pings core APIs. If it detects a DNS block or network restriction, it immediately pivots to **Offline Mode**, skipping individual asset timeouts.

### 4.2 Synthetic Price Injection
For any asset missing live data, the engine generates **Monte Carlo Simulated Prices** based on the asset's historical volatility profile. This prevents ETFs from being "dropped" from the solver due to data gaps.

### 4.3 Institutional Profile Matrix
A library of **Curated Fundamental Profiles** (141+ assets) ensures that even in offline mode, individual stock quality metrics (PE, ROE, PEG) are high-fidelity anchors for selection.

---

## 5. Risk Management & Auditing

### 5.1 CVaR (Conditional Value at Risk)
The system calculates **Expected Shortfall (CVaR)** at the 95th percentile, providing a "Worst-Case" loss projection that is more accurate than standard standard deviation.

### 5.2 Stress Testing Scenarios
Built-in modules simulate how the current portfolio would have performed during:
*   **2008 GFC** (Systemic Credit Crisis)
*   **2020 COVID Crash** (Black Swan Volatility)
*   **Structural Inflation** (High-Interest Rate Regime)

### 5.3 Dynamic Drawdown Control
The engine monitors the real-world performance of the buy-list. If a trailing drawdown exceeds 15%, the "Circuit Breaker" triggers, automatically liquidating 30% of the position into **CASH SAFETY**.

---

## 6. User Interface & Integration
*   **Streamlit Dashboard**: Real-time visualization of the 60:40 split, geographic heatmaps, and CVaR audit trails.
*   **Headless CLI (`run.py`)**: Designed for backend automation and server-side rebalancing.
*   **Turnover Buffer**: A 30% default rebalance limit to minimize impact costs and tax leakage.
