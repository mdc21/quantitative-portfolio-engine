import time
import random
from curl_cffi import requests
from bs4 import BeautifulSoup
from core.logger import logger

class ScreenerAPI:
    def __init__(self):
        self.session = requests.Session(impersonate="chrome120")
        self.session.headers.update({
            "Accept": "text/html,application/xhtml+xml,application/xml;q=0.9,image/avif,image/webp,*/*;q=0.8",
            "Accept-Language": "en-US,en;q=0.9",
            "Referer": "https://www.google.com/",
            "User-Agent": "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/120.0.0.0 Safari/537.36"
        })

    def _clean_ticker(self, ticker):
        """Cleans Yahoo Finance '.NS' suffix for Screener compatability."""
        return str(ticker).replace(".NS", "").replace(".BO", "").strip().upper()

    def _parse_ratio(self, item_tag):
        """Safely extract float from HTML spans like '₹ 12.5' or '15.2 %'."""
        try:
            val_str = item_tag.find('span', class_='number').text
            # Strip currencies, commas, percentage signs, and 'Cr.' for raw float conversion
            val_str = val_str.replace(",", "").replace("%", "").replace("₹", "").replace("Cr.", "").strip()
            return float(val_str)
        except Exception:
            return None

    def fetch_fundamentals(self, ticker):
        """
        Scrapes Point-in-Time fundamental metrics from Screener.in.
        Handles consolidated vs standalone URLs automatically.
        """
        clean_tick = self._clean_ticker(ticker)
        
        # Jitter to avoid bot detection
        time.sleep(random.uniform(0.5, 1.5))
        
        urls_to_try = [
            f"https://www.screener.in/company/{clean_tick}/consolidated/",
            f"https://www.screener.in/company/{clean_tick}/"
        ]
        
        for url in urls_to_try:
            try:
                response = self.session.get(url, timeout=8)
                if response.status_code == 200:
                    soup = BeautifulSoup(response.text, 'html.parser')
                    
                    if "Cloudflare" in soup.text or "Just a moment" in soup.text:
                        logger.warning(f"[Screener] Cloudflare blocked access for {clean_tick}")
                        return None
                        
                    ratios = soup.find('ul', id='top-ratios')
                    if not ratios:
                        continue # Try next URL format (e.g. standalone)
                        
                    metrics = {}
                    for li in ratios.find_all('li'):
                        name_span = li.find('span', class_='name')
                        if not name_span:
                            continue
                            
                        name = name_span.text.strip().lower()
                        val = self._parse_ratio(li)
                        
                        if val is not None:
                            if "stock p/e" in name: metrics["pe"] = val
                            elif "roce" in name: metrics["roce"] = val / 100.0
                            elif "roe" in name: metrics["roe"] = val / 100.0
                            elif "book value" in name: metrics["pb"] = val # Requires math against price later
                            elif "dividend yield" in name: metrics["div_yield"] = val / 100.0
                            elif "market cap" in name: metrics["market_cap"] = val * 10000000 # Crores to absolute
                            elif "current price" in name: metrics["current_price"] = val
                            elif "debt to equity" in name: metrics["debt_equity"] = val * 100 # Match generic 100 scale

                    # Successful parse
                    if metrics:
                        # Derive PB if possible
                        if "current_price" in metrics and "pb" in metrics and metrics["pb"] > 0:
                            metrics["pb"] = metrics["current_price"] / metrics["pb"]
                            
                        metrics["data_source"] = "Screener.in (Fallback)"
                        return metrics
                        
            except Exception as e:
                logger.warning(f"[Screener] Error fetching {url}: {e}")
                
        return None
