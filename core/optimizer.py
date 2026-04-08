import numpy as np
import pandas as pd
from scipy.optimize import minimize
from scipy.cluster.hierarchy import linkage
from scipy.spatial.distance import squareform

def optimize_weights(prices, selected_stocks, regime, sector_map=None, cap_map=None, limits=None):
    mode = regime.get("optimization_mode", "HRP")
    
    if mode == "Markowitz":
        return _optimize_markowitz(prices, selected_stocks, sector_map, cap_map, limits)
    else:
        return _optimize_hrp(prices, selected_stocks)

def _optimize_hrp(prices, selected_stocks):
    """
    Hierarchical Risk Parity (HRP)
    Builds a tree of correlations and allocates inversely proportional to cluster variance.
    Completely ignores expected returns, focusing purely on avoiding correlated crashes.
    """
    returns = prices[selected_stocks].pct_change().dropna()
    cov, corr = returns.cov(), returns.corr()
    
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
    Blends previous weights with newly calculated target weights
    to systematically reduce execution churn.
    """
    if not old_weights:
        return new_weights
        
    adjusted_weights = {}

    for stock in set(list(new_weights.keys()) + list(old_weights.keys())):
        new_w = new_weights.get(stock, 0.0)
        old_w = old_weights.get(stock, 0.0)
        
        if stock in old_weights and stock in new_weights:
            adjusted_weights[stock] = (1.0 - max_turnover) * old_w + max_turnover * new_w
        elif stock in new_weights:
            adjusted_weights[stock] = max_turnover * new_w
        elif stock in old_weights:
            adjusted_weights[stock] = (1.0 - max_turnover) * old_w

    total = sum(adjusted_weights.values())
    if total == 0:
        return new_weights
        
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

def apply_sector_weight_constraints(weights, sector_map, regime):
    """
    Applies aggressive percentage limits on correlated sectors and factor clusters.
    Excess capital is swept directly into a CASH placeholder.
    """
    category_caps = {
        "Financials": 0.30,
        "Technology": 0.20,
        "Industrials_Infra": 0.20,
        "Consumer_FMCG": 0.15,
        "Pharma_Healthcare": 0.15,
        "Chemicals": 0.12, 
        "PSU_Utilities": 0.10,
        "Others": 0.10
    }
    
    if regime.get("rate_trend") == "rising":
        category_caps["Financials"] = 0.20

    cluster_caps = {
        "Cyclicals": 0.35,
        "Financials_Cluster": category_caps["Financials"], 
        "Defensives": 0.35,
        "Growth": 0.30
    }

    def map_sector(ys):
        ys_lower = str(ys).lower()
        if "financial" in ys_lower: return "Financials", "Financials_Cluster"
        if "technology" in ys_lower: return "Technology", "Growth"
        if "healthcare" in ys_lower: return "Pharma_Healthcare", "Defensives"
        if "consumer" in ys_lower: return "Consumer_FMCG", "Defensives"
        if "utility" in ys_lower or "energy" in ys_lower: return "PSU_Utilities", "Defensives"
        if "basic materials" in ys_lower or "industrial" in ys_lower: return "Industrials_Infra", "Cyclicals"
        return "Others", "Others"

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

def apply_cap_size_constraints(weights, cap_map, large_limit=0.7, mid_limit=0.2, small_limit=0.1):
    """
    Rationally restricts structural capital exposure into highly volatile Mid and Small Cap segments.
    The excess overflow inherently routes directly back into the CASH preservation mechanic.
    """
    cap_allocs = {"Large": 0.0, "Mid": 0.0, "Small": 0.0, "Unknown": 0.0}
    limits = {"Large": large_limit, "Mid": mid_limit, "Small": small_limit, "Unknown": 1.0}
    
    constrained_weights = {}
    cash = weights.get("CASH", 0.0)
    
    stocks = [s for s in weights.keys() if s != "CASH"]
    sorted_stocks = sorted(stocks, key=lambda k: weights[k], reverse=True)
    
    for stock in sorted_stocks:
        w = weights[stock]
        size = cap_map.get(stock, "Large")
        
        room = max(0.0, limits.get(size, 1.0) - cap_allocs[size])
        allowed = min(w, room)
        
        if allowed < w:
            cash += (w - allowed)
            
        if allowed > 0:
            constrained_weights[stock] = allowed
            
        cap_allocs[size] += allowed
        
    if cash > 0.001:
        constrained_weights["CASH"] = cash
        
    return constrained_weights
