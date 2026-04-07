import os
import json
from datetime import datetime, timedelta

STATE_FILE = "data/portfolio_history.json"

def load_portfolio_state():
    """Reads the portfolio history and returns the most recent weights."""
    if not os.path.exists(STATE_FILE):
        return {}
    
    with open(STATE_FILE, "r") as f:
        try:
            history = json.load(f)
        except json.JSONDecodeError:
            return {}
            
    if not history:
        return {}
        
    # Sort dates and get the latest
    latest_date = sorted(history.keys())[-1]
    return history[latest_date]

def save_portfolio_state(new_weights, retention_days=60):
    """Saves the new weights and purges any records older than retention_days."""
    # Ensure directory exists
    os.makedirs(os.path.dirname(STATE_FILE), exist_ok=True)
    
    history = {}
    if os.path.exists(STATE_FILE):
        with open(STATE_FILE, "r") as f:
            try:
                history = json.load(f)
            except json.JSONDecodeError:
                pass
                
    today = datetime.now()
    cutoff_date = today - timedelta(days=retention_days)
    
    # Purge old records
    cleaned_history = {}
    for date_str, weights in history.items():
        try:
            record_date = datetime.strptime(date_str, "%Y-%m-%d")
            if record_date >= cutoff_date:
                cleaned_history[date_str] = weights
        except ValueError:
            pass
            
    # Add today's record
    today_str = today.strftime("%Y-%m-%d")
    cleaned_history[today_str] = new_weights
    
    with open(STATE_FILE, "w") as f:
        json.dump(cleaned_history, f, indent=4)
