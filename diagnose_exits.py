"""Diagnose why TCS & HDFCBANK are recommended for Strategic Exit."""
import core.universe as universe
from core.data_loader import fetch_prices
from core.factors import compute_factor_scores
from core.momentum import select_top_momentum, apply_sector_caps

TARGETS = ["TCS.NS", "HDFCBANK.NS", "RELIANCE.NS"]

# Stage 1: Fundamental Screen
broad = universe.fetch_broad_universe()
tickers, sector_map, cap_map, scoring_df = universe.apply_fundamental_filters(broad, top_percentile=0.3)

print("=" * 70)
print("STAGE 1: FUNDAMENTAL SCREEN (top 30%)")
print("=" * 70)
for t in TARGETS:
    row = scoring_df[scoring_df["Stock"] == t]
    if not row.empty:
        score = row["Fundamental_Score"].values[0]
        rank = scoring_df.index.get_loc(row.index[0]) + 1
        survived = "✅ PASSED" if t in tickers else "❌ FILTERED OUT"
        print(f"  {t}: Score={score:.4f}, Rank={rank}/{len(scoring_df)}, {survived}")
        if t not in tickers:
            cutoff_score = scoring_df.iloc[len(tickers)-1]["Fundamental_Score"]
            print(f"    → Cutoff score was {cutoff_score:.4f} (top {len(tickers)} of {len(scoring_df)})")
    else:
        print(f"  {t}: ⚠️ NOT IN SCORING DF AT ALL")

# Stage 2: Price Fetch & Momentum
prices = fetch_prices(tickers)
buy_list_prices = prices[[t for t in tickers if t in prices.columns]]
scores = compute_factor_scores(buy_list_prices, {"momentum_lookback_days": 90, "volatility_lookback_days": 60})

print("\n" + "=" * 70)
print("STAGE 2: MOMENTUM RANKING")
print("=" * 70)
scores_list = scores.reset_index()
scores_list.columns = ["Stock", "Score"]
for t in TARGETS:
    if t in tickers:
        row = scores_list[scores_list["Stock"] == t]
        if not row.empty:
            rank = scores_list.index.get_loc(row.index[0]) + 1
            print(f"  {t}: Momentum Score={row['Score'].values[0]:.4f}, Rank={rank}/{len(scores_list)}")
        else:
            print(f"  {t}: ⚠️ No momentum score (missing from price data?)")

# Stage 3: Momentum Retention
selected_raw = select_top_momentum(scores, top_percent=0.5)
print(f"\n  Momentum Retention: {len(selected_raw)} of {len(scores)} passed (top 50%)")
for t in TARGETS:
    if t in selected_raw:
        print(f"  {t}: ✅ PASSED momentum retention")
    elif t in tickers:
        print(f"  {t}: ❌ DROPPED by momentum retention")

# Stage 4: Sector Caps (THE LIKELY CULPRIT)
selected = apply_sector_caps(selected_raw, sector_map, max_per_sector=3)
print("\n" + "=" * 70)
print("STAGE 4: SECTOR CAPS (max 3 per sector)")
print("=" * 70)

# Show sector breakdown
from collections import Counter
sector_before = Counter(sector_map.get(s, "Unknown") for s in selected_raw)
sector_after = Counter(sector_map.get(s, "Unknown") for s in selected)
print(f"  Before caps: {len(selected_raw)} stocks")
print(f"  After caps:  {len(selected)} stocks")
print(f"  Sectors before: {dict(sector_before)}")
print(f"  Sectors after:  {dict(sector_after)}")

for t in TARGETS:
    sector = sector_map.get(t, "Unknown")
    if t in selected:
        print(f"\n  {t} ({sector}): ✅ SURVIVED sector caps")
    elif t in selected_raw:
        # Find which 3 stocks from same sector beat it
        same_sector = [s for s in selected_raw if sector_map.get(s, "Unknown") == sector]
        print(f"\n  {t} ({sector}): ❌ DROPPED by sector cap!")
        print(f"    → Sector '{sector}' had {len(same_sector)} stocks competing for 3 slots:")
        for i, s in enumerate(same_sector):
            marker = "✅ kept" if s in selected else "❌ dropped"
            mom = scores.get(s, 0)
            print(f"      {i+1}. {s}: momentum={mom:.4f} [{marker}]")
    elif t in tickers:
        print(f"\n  {t} ({sector}): ❌ Already dropped at momentum stage")
    else:
        print(f"\n  {t}: ❌ Already dropped at fundamental stage")

print("\n" + "=" * 70)
print("CONCLUSION: Why these stocks get 'Strategic Exit'")
print("=" * 70)
print("A stock gets 'Strategic Exit' when it's in your HOLDINGS but NOT")
print("in the final target_weights. This happens when it gets filtered")
print("at ANY of the 4 stages above.")
