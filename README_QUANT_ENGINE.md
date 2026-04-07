# Systematic Quantitative Engine: Architecture & Methodology

This document outlines the structural methodology, selection parameters, and risk management protocols for the institutional-grade Quantitative Portfolio Dashboard. 

The system operates as a systematic funnel—distilling a broad universe of assets through consecutive layers of fundamental, mathematical, and macroeconomic constraints to yield an incredibly robust, risk-adjusted portfolio.

---

## 1. The Portfolio Construction Funnel

The methodology is strictly bottom-up, starting with corporate fundamentals and ending with mathematical risk parity. It guarantees that no capital is allocated to mathematically weak or highly volatile assets.

### Layer A: The Fundamental Scoreboard (Continuous Matrix)
The engine does not rely on binary "Pass/Fail" logic, which often fails due to missing API data. Instead, it compiles a **Continuous Scoring Matrix** using Pandas fractional ranking (`.rank(pct=True)`). 

Every stock is evaluated against four pillars:
* **Quality**: Return on Capital Employed (ROCE / ROE) — *Weighted 30%*
* **Profitability**: Earnings Growth — *Weighted 25%*
* **Revenue**: Sales Growth — *Weighted 20%*
* **Leverage**: Debt-to-Equity — *Weighted 25% (Inverse Ranked)*

The matrix ranks the universe linearly, punishing highly leveraged companies and rewarding hyper-growth. Only the absolute **Top 30%** of stocks by Composite Score are permitted to advance to the next phase.

### Layer B: Multi-Timeframe Momentum Selection
Price action is evaluated to ensure capital is only deployed into assets currently favored by the broader market. The engine avoids "Catching Falling Knives".
* **Timeframe Blending**: Instead of looking at a single timeframe which generates extreme noise, the engine blends 30-day, 90-day, and 180-day momentum.
* **Selection**: The user dynamically defines the final top percentile (e.g. `Top 50%`) of the fundamental survivors to extract the ultimate "Winners".

### Layer C: Risk Parity Sizing (Inverse Volatility)
Instead of allocating capital equally (e.g., 25% to 4 stocks), the engine utilizes **Inverse Volatility**.
* It calculates the rolling standard deviation matrix of the surviving assets.
* Safe, structurally low-volatility stocks (e.g., HDFCBANK) receive massive baseline capital allocations.
* Highly explosive, volatile stocks inherently receive tightly restricted capital ceilings to prevent violent portfolio drawdowns.

### Layer D: Valuation & The Momentum Premium (P/E & P/B Metrics)
The quantitative engine frequently scales into assets trading at distinct valuation premiums (higher P/E and P/B ratios) compared to passive, broad market indices (e.g., Nifty 50). This defines a structural **"Quality-Momentum" (Qual-Mom)** matrix:
* **The Premium Justification:** The algorithm strictly buys *winners* (High Momentum) supported by pristine fiscal health (High ROCE, High Growth). The broader index is dragged down by struggling, low-growth companies trading at dirt-cheap multiples. The quant system systematically pays a multiple premium to ride confirmed breakouts and structural quality.
* **Overvaluation Safety Valves:** The engine prevents blind bubble-buying through two strict mechanisms. First, exorbitant valuation ratios are mathematically penalized within the fundamental screener algorithm. Second, if a hyper-inflated stock begins trading erratically, the ensuing volatility spike is instantly caught by the Inverse Volatility optimizer, which forcefully neutralizes its portfolio weighting to mitigate violent drawdowns.

---

## 2. Managing Correlated Risk & Sector Caps

Mathematical volatility alone ignores the danger of industry cluster risk. The optimizer actively defends against correlated Black Swan events through strict constraint overrides.

* **Sector Caps**: Hard-coded boundaries prevent the engine from sinking aggressive capital into a single domain. For example, Technology cannot structurally exceed `20%` of portfolio equity.
* **Factor Clusters**: Sectors are grouped into Macro Clusters (e.g., *Defensives*, *Cyclicals*, *Growth*). Growth clusters cannot exceed `30%`.
* **Regime Adapters**: The engine listens to macro API signals. If the CPI API detects `Inflation = High` or `Rates = Rising`, it physically strangulates the Financials cluster cap from `30%` down to `20%`.
* **The CASH Trapdoor**: If the raw Inverse Volatility weighting tries to breach *any* of those constraints, the engine instantaneously slices off the excess decimal percentage and dumps it into a `CASH` placeholder. It does not force capital into weak assets just to stay fully invested.

---

## 3. Ongoing Review, Rebalancing, and Anti-Churn 

A standard quant algorithm suffers from astronomical commission fees and slippage due to constant buying and selling (Signal Churn). This engine uses a **Stateful Dampening Architecure**.

### Turnover Control (The Execution Parachute)
The system saves exactly what it owned yesterday to a local physical database (`data/portfolio_history.json`).
When today's new Target Allocations are mathematically derived, the engine does not instantly execute the gap. 

Instead, it passes the gap through a **Churn Smoother**:
* `Final Execution Weight = [(1 - Max_Turnover) * Yesterday's Weight] + [Max_Turnover * Today's Target]`
* **Example**: If a sector limit crashes a stock's target from 80% to 20%, and the Turnover constraint is set to `30%`, the system blends the weights to `63%`. 
* **The Result**: The portfolio mathematically glides downward over several trading days to reach 20%, avoiding market impact costs and panic selling algorithms.

### Rebalancing Triggers
Rebalancing is structurally continuous based on the interaction between the time-series datasets:
1. Daily price ticks alter the Momentum blend and Volatility matrices.
2. Quarterly EPS reports alter the Fundamental Scoreboard matrix.
3. Live Macro API events randomly trigger hard-cap compressions.

By continuously running this execution script, the quantitative engine operates completely free of psychological bias—ruthlessly stripping weak companies, aggressively capturing momentum, and securely capping absolute catastrophic risk.
