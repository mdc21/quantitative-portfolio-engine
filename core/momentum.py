def select_top_momentum(scores, top_percent=0.5):
    cutoff = int(len(scores) * top_percent)
    return scores.head(cutoff).index.tolist()

def apply_sector_caps(ranked_stocks, sector_map, max_per_sector=3):
    final_selection = []
    sector_counts = {}
    
    for stock in ranked_stocks:
        sector = sector_map.get(stock, "Unknown")
        
        if sector == "Unknown" or sector == "Unknown_Sector":
            final_selection.append(stock)
            continue
            
        current_count = sector_counts.get(sector, 0)
        
        if current_count < max_per_sector:
            final_selection.append(stock)
            sector_counts[sector] = current_count + 1
            
    return final_selection

