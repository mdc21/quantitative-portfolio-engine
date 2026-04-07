import numpy as np

def optimize_weights(prices, selected_stocks):
    returns = prices[selected_stocks].pct_change().dropna()

    cov = returns.cov()
    vol = np.sqrt(np.diag(cov))

    inv_vol = 1 / vol
    weights = inv_vol / inv_vol.sum()

    return dict(zip(selected_stocks, weights))

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
