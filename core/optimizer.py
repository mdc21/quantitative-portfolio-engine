import numpy as np
import pandas as pd
from scipy.optimize import minimize
from scipy.cluster.hierarchy import linkage
from scipy.spatial.distance import squareform
from core.logger import logger

def optimize_weights(prices, selected_stocks, regime, asset_class_map=None, sector_map=None, cap_map=None, limits=None):
    """
    Orchestrates optimization across different asset classes.
    As per user strategy: Equities use regime-based switching, 
    while ETFs/Mutual Funds MUST use HRP for stability.
    """
    equities = [s for s in selected_stocks if asset_class_map.get(s) == "Equity"]
    passives = [s for s in selected_stocks if asset_class_map.get(s) in ["ETF", "MutualFund"]]
    
    # 1. Optimize Equities (Aggressive/Defensive based on Regime)
    mode = regime.get("optimization_mode", "HRP")
    if mode == "Markowitz" and equities:
        eq_weights = _optimize_markowitz(prices, equities, sector_map, cap_map, limits)
    else:
        eq_weights = _optimize_hrp(prices, equities)
        
    # 2. Optimize Passives (FORCE HRP for diversification)
    pass_weights = _optimize_hrp(prices, passives)
    
    # ⚖️ Scaled Merging: Preserve relative HRP/Markowitz importance within each segment
    # Instead of raw merging (which sums to 2.0), we scale by the portfolio's top-level mandate
    equity_target = regime.get("equity_target", 0.60)
    passive_target = 1.0 - equity_target
    
    # Handle edge cases where one bucket might be empty
    if not equities:
        eq_weights = {}
        pass_weights = {k: v for k, v in pass_weights.items()} # Passive gets 100% if no equity
    elif not passives:
        pass_weights = {}
        eq_weights = {k: v for k, v in eq_weights.items()} # Equity gets 100% if no passive
    else:
        eq_weights = {k: v * equity_target for k, v in eq_weights.items()}
        pass_weights = {k: v * passive_target for k, v in pass_weights.items()}

    combined = {**eq_weights, **pass_weights}
    return combined

def calculate_cvar(prices, weights, confidence_level=0.95):
    """
    Computes Conditional Value at Risk (Expected Shortfall)
    """
    returns = prices[list(weights.keys())].pct_change().dropna()
    portfolio_returns = (returns * pd.Series(weights)).sum(axis=1)
    
    var = np.percentile(portfolio_returns, (1 - confidence_level) * 100)
    cvar = portfolio_returns[portfolio_returns <= var].mean()
    return cvar


def _optimize_hrp(prices, selected_stocks):
    """
    Hierarchical Risk Parity (HRP)
    Builds a tree of correlations and allocates inversely proportional to cluster variance.
    Completely ignores expected returns, focusing purely on avoiding correlated crashes.
    """
    if len(selected_stocks) <= 1:
        return {s: 1.0 for s in selected_stocks} if selected_stocks else {}
    returns = prices[selected_stocks].pct_change().dropna(how='all')
    if returns.empty:
        return {s: 1.0/len(selected_stocks) for s in selected_stocks}
        
    # Clean data to avoid LinAlgError (NaN/Inf)
    returns = returns.replace([np.inf, -np.inf], np.nan).fillna(0)
    
    cov, corr = returns.cov(), returns.corr()
    
    # Final guard: if correlation is entirely NaN (all constant prices), return equal weights
    if corr.isna().all().all():
        return {s: 1.0/len(selected_stocks) for s in selected_stocks}
    
    # Distance matrix
    dist = np.sqrt(0.5 * (1 - corr).clip(0, 2))  
    link = linkage(squareform(dist), method='single')
    
    def get_quasi_diag(link):
        link = link.astype(int)
        sort_ix = pd.Series([link[-1, 0], link[-1, 1]])
        num_items = link[-1, 3]
        while sort_ix.max() >= num_items:
            sort_ix.index = range(0, sort_ix.shape[0] * 2, 2)
            df0 = sort_ix[sort_ix >= num_items]
            i = df0.index
            j = df0.values - num_items
            sort_ix[i] = link[j, 0]
            df0 = pd.Series(link[j, 1], index=i + 1)
            sort_ix = pd.concat([sort_ix, df0])
            sort_ix = sort_ix.sort_index()
            sort_ix.index = range(sort_ix.shape[0])
        return sort_ix.tolist()
        
    sort_ix = get_quasi_diag(link)
    sort_ix = returns.columns[sort_ix].tolist()
    
    def get_cluster_var(cov, c_items):
        cov_vals = cov.loc[c_items, c_items].values
        ivp = 1. / np.diag(cov_vals)
        ivp /= ivp.sum()
        w = ivp.reshape(-1, 1)
        return float(np.dot(np.dot(w.T, cov_vals), w).item())
        
    def get_rec_bipart(cov, sort_ix):
        w = pd.Series(1.0, index=sort_ix)
        c_items = [sort_ix]
        while len(c_items) > 0:
            c_items = [i[j:k] for i in c_items for j, k in ((0, len(i) // 2), (len(i) // 2, len(i))) if len(i) > 1]
            for i in range(0, len(c_items), 2):
                c_items0 = c_items[i]
                c_items1 = c_items[i + 1]
                c_var0 = get_cluster_var(cov, c_items0)
                c_var1 = get_cluster_var(cov, c_items1)
                alpha = 1 - c_var0 / (c_var0 + c_var1)
                w[c_items0] *= alpha
                w[c_items1] *= 1 - alpha
        return w
        
    weights = get_rec_bipart(cov, sort_ix)
    return weights.to_dict()

def _optimize_markowitz(prices, selected_stocks, sector_map, cap_map, limits):
    """
    Mean-Variance Optimization (Markowitz)
    Maximizes Sharpe Ratio globally. Integrates sector and cap size inequalities natively.
    """
    returns = prices[selected_stocks].pct_change().dropna()
    mu = returns.mean() * 252
    cov = returns.cov() * 252
    
    num_assets = len(selected_stocks)
    
    def neg_sharpe(w):
        port_ret = np.dot(w, mu)
        port_vol = np.sqrt(np.dot(w.T, np.dot(cov, w)))
        return -port_ret / port_vol if port_vol > 0 else 0
        
    bounds = tuple((0.0, 1.0) for _ in range(num_assets))
    constraints = [{'type': 'eq', 'fun': lambda x: np.sum(x) - 1.0}]
    
    if limits:
        # Sector Inequalities
        for cat, limit in limits.get("category_caps", {}).items():
            idxs = [i for i, s in enumerate(selected_stocks) if sector_map.get(s, "Others") == cat]
            if idxs:
                constraints.append({'type': 'ineq', 'fun': lambda w, i=idxs, l=limit: l - np.sum(w[i])})
                
        # Cap Inequalities
        cap_limits = {"Large": limits.get("cap_large", 0.7), 
                      "Mid": limits.get("cap_mid", 0.2), 
                      "Small": limits.get("cap_small", 0.1)}
        for cap, limit in cap_limits.items():
            idxs = [i for i, s in enumerate(selected_stocks) if cap_map.get(s, "Large") == cap]
            if idxs:
                constraints.append({'type': 'ineq', 'fun': lambda w, i=idxs, l=limit: l - np.sum(w[i])})

    init_guess = num_assets * [1. / num_assets,]
    
    res = minimize(neg_sharpe, init_guess, bounds=bounds, constraints=constraints, method='SLSQP')
    if res.success:
        return dict(zip(selected_stocks, res.x))
    
    print("⚠️ Markowitz solver failed to converge! Defaulting to HRP Engine.")
    return _optimize_hrp(prices, selected_stocks)

def apply_turnover_control(old_weights, new_weights, max_turnover=0.3):
    """
    Blends previous weights with newly calculated target weights.
    
    SPECIAL RULE: If old_weights was effectively CASH (Fresh Capital),
    the damper is bypassed to allow immediate deployment.
    """
    if not old_weights or old_weights.get("CASH", 0.0) > 0.99:
        # Full deployment for fresh capital or first-run
        return new_weights
        
    adjusted_weights = {}

    for stock in set(list(new_weights.keys()) + list(old_weights.keys())):
        new_w = new_weights.get(stock, 0.0)
        old_w = old_weights.get(stock, 0.0)
        
        if stock in old_weights and stock in new_weights:
            adjusted_weights[stock] = (1.0 - max_turnover) * old_w + max_turnover * new_w
        elif stock in new_weights:
            # Dampen entry into BRAND NEW stocks
            adjusted_weights[stock] = max_turnover * new_w
        elif stock in old_weights:
            # Dampen exit from OLD stocks
            adjusted_weights[stock] = (1.0 - max_turnover) * old_w

    # Recalculate total to see how much "Unused Damper Budget" we have
    total = sum(adjusted_weights.values())
    
    # Re-normalize to 1.0, which naturally re-distributes the damped delta
    if total > 0:
        adjusted_weights = {k: v/total for k, v in adjusted_weights.items()}
        
    return adjusted_weights

def apply_macro_overlay(weights, regime):
    adjusted = weights.copy()

    if regime["rate_trend"] == "rising":
        for stock in adjusted:
            if "BAJFINANCE" in stock:
                adjusted[stock] *= 0.7

    if regime["inflation"] == "high":
        for stock in adjusted:
            if "ITC" in stock:
                adjusted[stock] *= 1.2

    total = sum(adjusted.values())
    adjusted = {k: v/total for k, v in adjusted.items()}

    return adjusted

def get_category_caps(regime):
    category_caps = {
        "Financials": 0.30,
        "Technology": 0.20,
        "Industrials_Infra": 0.20,
        "Consumer_FMCG": 0.15,
        "Pharma_Healthcare": 0.15,
        "Chemicals": 0.12, 
        "PSU_Utilities": 0.10,
        "Passive_Index": 0.50,
        "Others": 0.10
    }
    if regime.get("rate_trend") == "rising":
        category_caps["Financials"] = 0.20
    return category_caps

def map_sector(ys):
    ys_lower = str(ys).lower()
    if "financial" in ys_lower: return "Financials", "Financials_Cluster"
    if "technology" in ys_lower: return "Technology", "Growth"
    if "healthcare" in ys_lower: return "Pharma_Healthcare", "Defensives"
    if "consumer" in ys_lower: return "Consumer_FMCG", "Defensives"
    if "utility" in ys_lower or "energy" in ys_lower: return "PSU_Utilities", "Defensives"
    if "basic materials" in ys_lower or "industrial" in ys_lower: return "Industrials_Infra", "Cyclicals"
    if "passive" in ys_lower or "index" in ys_lower: return "Passive_Index", "Defensives"
    return "Others", "Others"

def apply_asset_class_constraints(weights, asset_class_map, underlying_map, region_map, equity_target=0.60, metal_cap=0.05):
    """
    Forces the portfolio into Top-Level buckets: Equities (60%) vs Passive (40%).
    Ensures that shortfalls in any bucket are upscaled to hit the mandate exactly.
    """
    passive_target = max(0.0, 1.0 - equity_target)
    
    # 1. Identify Buckets
    buckets = {"Equity": [], "Passive_Domestic": [], "Passive_International": []}
    for stock in weights:
        if stock == "CASH": continue
        ac = asset_class_map.get(stock, "Equity")
        reg = region_map.get(stock, "Domestic")
        
        if ac == "Equity":
            buckets["Equity"].append(stock)
        else:
            if reg == "International":
                buckets["Passive_International"].append(stock)
            else:
                buckets["Passive_Domestic"].append(stock)
    
    # 2. Target Allocation Scaling
    bucket_targets = {
        "Equity": equity_target,
        "Passive_Domestic": passive_target * 0.80,
        "Passive_International": passive_target * 0.20
    }
    
    new_weights = {}
    total_deployed = 0.0
    
    for b_name, b_stocks in buckets.items():
        target = bucket_targets.get(b_name, 0.0)
        current_sum = sum(weights[s] for s in b_stocks)
        
        if current_sum > 0:
            # Upscale/Downscale this bucket to hit target EXACTLY
            scalar = target / current_sum
            logger.info(f"⚖️ Scaling Bucket '{b_name}': {len(b_stocks)} assets, target {target*100:.1f}%, scalar {scalar:.2f}")
            for s in b_stocks:
                new_weights[s] = weights[s] * scalar
            total_deployed += target
        else:
            # If a bucket is EMPTY, the mandate's target remains as CASH
            logger.warning(f"⚠️ Mandatory bucket {b_name} is empty (0 assets). {target*100:.1f}% remains in Cash Reserve.")
            
    # 3. Metal Ceiling (Hard secondary constraint)
    metal_alloc = 0.0
    for s, w in new_weights.items():
        if underlying_map.get(s) == "Metal":
            metal_alloc += w
            
    if metal_alloc > metal_cap:
        # Scale down metals to hit the ceiling
        scalar = metal_cap / metal_alloc
        for s in new_weights:
            if underlying_map.get(s) == "Metal":
                old_w = new_weights[s]
                new_weights[s] *= scalar
                # The excess from metal ceiling goes to CASH
        
    # Final normalization
    new_total = sum(new_weights.values())
    new_weights["CASH"] = max(0.0, 1.0 - new_total)
    
    return new_weights

def apply_sector_weight_constraints(weights, sector_map, regime):
    """
    Applies aggressive percentage limits on correlated sectors and factor clusters.
    Excess capital is swept directly into a CASH placeholder.
    """
    category_caps = get_category_caps(regime)

    cluster_caps = {
        "Cyclicals": 0.35,
        "Financials_Cluster": category_caps["Financials"], 
        "Defensives": 0.35,
        "Growth": 0.30
    }

    cat_allocs = {k: 0.0 for k in category_caps}
    clust_allocs = {k: 0.0 for k in cluster_caps}
    clust_allocs["Others"] = 0.0
    
    constrained_weights = {}
    cash = 0.0
    
    sorted_stocks = sorted(weights.keys(), key=lambda k: weights[k], reverse=True)
    
    for stock in sorted_stocks:
        w = weights[stock]
        ys = sector_map.get(stock, "Others")
        cat, clust = map_sector(ys)
        
        cat_room = max(0.0, category_caps.get(cat, 0.10) - cat_allocs[cat])
        clust_room = max(0.0, cluster_caps.get(clust, 1.0) - clust_allocs[clust])
        
        allowed = min(w, cat_room, clust_room)
        
        if allowed < w:
            cash += (w - allowed)
            
        if allowed > 0:
            constrained_weights[stock] = allowed
            
        cat_allocs[cat] += allowed
        clust_allocs[clust] += allowed
        
    if cash > 0.001:  # Protect against precision drift
        constrained_weights["CASH"] = cash
        
    return constrained_weights

def apply_cap_size_constraints(weights, cap_map, sector_map, regime, large_limit=0.7, mid_limit=0.2, small_limit=0.1):
    """
    Rationally restricts structural capital exposure into highly volatile Mid and Small Cap segments,
    but applies them proportionally to the Sector limit.
    e.g., if Tech limit is 20%, max Mid-Cap Tech = 20% * mid_limit (0.2) = 4% out of total portfolio.
    """
    try:
        category_caps = get_category_caps(regime)
        # We track how much of each cap size we have allocated *within* each sector
        sector_cap_allocs = {k: {"Large": 0.0, "Mid": 0.0, "Small": 0.0, "Unknown": 0.0} for k in category_caps}
        sector_cap_allocs["Others"] = {"Large": 0.0, "Mid": 0.0, "Small": 0.0, "Unknown": 0.0}
        
        cap_multipliers = {"Large": large_limit, "Mid": mid_limit, "Small": small_limit, "Unknown": 1.0}
        
        constrained_weights = {}
        cash = weights.get("CASH", 0.0) if weights else 0.0
        
        stocks = [s for s in weights.keys() if s != "CASH"] if weights else []
        sorted_stocks = sorted(stocks, key=lambda k: weights[k], reverse=True)
        
        for stock in sorted_stocks:
            w = weights[stock]
            size = cap_map.get(stock, "Large")
            ys = sector_map.get(stock, "Others")
            cat, _ = map_sector(ys)
            
            # Absolute maximum portfolio % this Cap is allowed to take within this Sector
            sector_max = category_caps.get(cat, 0.10)
            sector_cap_limit = sector_max * cap_multipliers.get(size, 1.0)
            
            # How much room is left for this specific Cap size within this specific Sector?
            room = max(0.0, sector_cap_limit - sector_cap_allocs[cat][size])
            allowed = min(w, room)
            
            if allowed < w:
                cash += (w - allowed)
                
            if allowed > 0:
                constrained_weights[stock] = allowed
                
            sector_cap_allocs[cat][size] += allowed
                
        if cash > 0.001:
            constrained_weights["CASH"] = cash
            
        return constrained_weights
    except Exception as e:
        logger.error(f"FATAL: Internal Overflow in apply_cap_size_constraints: {e}")
        return None

def clean_weights(weights, min_weight=0.01):
    """
    Enforces a minimum weight floor to avoid 'dust' positions that can't be executed.
    Weights below the floor are moved to CASH.
    """
    cleaned = {}
    cash = weights.get("CASH", 0.0)
    
    for stock, w in weights.items():
        if stock == "CASH":
            continue
        if w < min_weight:
            cash += w
        else:
            cleaned[stock] = w
            
    if cash > 0.001:
        cleaned["CASH"] = cash
        
    return cleaned
