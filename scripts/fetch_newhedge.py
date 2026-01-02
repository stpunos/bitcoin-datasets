import os
import re
import json
import csv
from datetime import datetime
import pandas as pd
from firecrawl import FirecrawlApp
from bs4 import BeautifulSoup
from dotenv import load_dotenv

# Load environment variables
load_dotenv()

# Configuration
FIRECRAWL_API_KEY = os.getenv('FIRECRAWL_API_KEY', 'vrvs9O23HxoZPiEu') # Default from n8n workflow for testing
URL = "https://newhedge.io/bitcoin"
OUTPUT_DIR = os.path.join(os.path.dirname(os.path.dirname(__file__)), 'data', 'newhedge')

# Selector mapping from n8n workflow
SELECTORS = [
    {"key": "Bitcoin Dominance", "cssSelector": "#btc_dominance"},
    {"key": "Market Cap", "cssSelector": "p:contains('Market Cap') + p"},
    {"key": "Sats per Dollar", "cssSelector": "p:contains('Sats per Dollar') + p"},
    {"key": "Block Height", "cssSelector": ".block-height"},
    {"key": "Revenue (BTC) (24hrs)", "cssSelector": "#dailyRevenueBtc"},
    {"key": "Revenue (USD) (24hrs)", "cssSelector": "#dailyRevenueUsd"},
    {"key": "Circulating Supply", "cssSelector": "#supply"},
    {"key": "Percentage Issued", "cssSelector": "#percentIssued"},
    {"key": "Issuance Remaining", "cssSelector": "#IssuanceRemaining"},
    {"key": "Hashrate", "cssSelector": "#hashrate"},
    {"key": "Hashprice", "cssSelector": "#HashPrice"},
    {"key": "Public Company Holdings", "cssSelector": ".dashboard-primary-text"},
    {"key": "Private Company Holdings", "cssSelector": ".dashboard-primary-text"},
    {"key": "BTC Held in Treasuries", "cssSelector": "#btcGovernments"},
    {"key": "Treasury Value (USD)", "cssSelector": "#usdGovernments"},
    {"key": "Number of UTXOs in Profit", "cssSelector": ".dashboard-subcol p:contains('Number of UTXOs in Profit') + p"},
    {"key": "Number of UTXOs in Loss", "cssSelector": ".dashboard-subcol p:contains('Number of UTXOs in Loss') + p"},
    {"key": "Percent UTXOs in Profit", "cssSelector": ".dashboard-subcol p:contains('Percent UTXOs in Profit') + p"},
    {"key": "Transactions Per Second", "cssSelector": "#transactions_per_second"},
    {"key": "Transactions Per Block", "cssSelector": "#transactions_per_block"},
    {"key": "Transactions Per Day", "cssSelector": "#transactions_per_day"},
    {"key": "Transactions Current Month", "cssSelector": ".dashboard-subcol p:contains('Current Month') + p"},
    {"key": "Total Transactions All Time", "cssSelector": "#total_transactions"},
    {"key": "Open Interest", "cssSelector": "#open_interest"},
    {"key": "ATH Price", "cssSelector": "#AthPriceMain"},
    {"key": "Price Drawdown Since ATH", "cssSelector": "p:contains('Price Drawdown Since ATH') + p"},
    {"key": "Days Since ATH", "cssSelector": ".drawdown-days-since-ath"},
    {"key": "ATH Date", "cssSelector": ".ath-date"},
    {"key": "Daily BTC Trading Vol", "cssSelector": "p:contains('Daily BTC Trading Vol') + p"},
    {"key": "Binance Trading Dominance", "cssSelector": "p:contains('Binance Trading Dominance') + p"},
    {"key": "BTC Pairs Trading Dominance ", "cssSelector": "p:contains('BTC Pairs Trading Dominance') + p"},
    {"key": "US Crypto Trading Vol", "cssSelector": "p:contains('US Crypto Trading Vol') + p"},
    {"key": "Offshore Crypto Trading Vol", "cssSelector": "p:contains('Offshore Crypto Trading Vol') + p"},
    {"key": "Daily Price Performance", "cssSelector": "#daily_price_performance"},
    {"key": "Weekly Price Performance", "cssSelector": "#weekly_price_performance"},
    {"key": "Monthly Price Performance", "cssSelector": "#monthly_price_performance"},
    {"key": "Quarterly Price Performance", "cssSelector": "#quarterly_price_performance"},
    {"key": "Gold Price", "cssSelector": "p:contains('Gold Price') + p"},
    {"key": "Gold Marketcap", "cssSelector": "p:contains('Gold Marketcap') + p"},
    {"key": "Bitcoin vs Gold Market Cap", "cssSelector": "p:contains('Bitcoin vs Gold Market Cap') + p"},
    {"key": "Realized Price", "cssSelector": "p:contains('Realized Price') + p"},
    {"key": "Realized Marketcap", "cssSelector": "p:contains('Realized Marketcap') + p"},
    {"key": "STH Realized Price", "cssSelector": "p:contains('STH Realized Price') + p"},
    {"key": "LTH Realized Price", "cssSelector": "p:contains('LTH Realized Price') + p"},
    {"key": "New Addresses", "cssSelector": "p:contains('New Addresses') + p"},
    {"key": "Balance Between 1 sat and .01 BTC", "cssSelector": "p:contains('1 sat to .01 BTC') + p"},
    {"key": "Balance Between .01 BTC and 1 BTC", "cssSelector": "p:contains('.01 BTC to 1 BTC') + p"},
    {"key": "Balance Between 1 BTC and 10 BTC", "cssSelector": "p:contains('1 BTC to 10 BTC') + p"},
    {"key": "Balance Between 10 BTC and 100 BTC", "cssSelector": "p:contains('10 BTC to 100 BTC') + p"},
    {"key": "Balance Between 100 BTC and 1,000 BTC", "cssSelector": "p:contains('100 BTC to 1,000 BTC') + p"},
    {"key": "Long Term Holder Supply", "cssSelector": "p:contains('Long Term Holder Supply') + p"},
    {"key": "Short Term Holder Supply", "cssSelector": "p:contains('Short Term Holder Supply') + p"},
    {"key": "Percent Supply in Profit", "cssSelector": "p:contains('Percent Supply in Profit') + p"},
    {"key": "Total Supply in Profit", "cssSelector": "p:contains('Total Supply in Profit') + p"},
    {"key": "Total Supply in Loss", "cssSelector": "p:contains('Total Supply in Loss') + p"},
    {"key": "Coin Days Destroyed", "cssSelector": "p:contains('Coin Days Destroyed') + p"},
    {"key": "MVRV Z-Score", "cssSelector": "p:contains('MVRV Z-Score') + p"},
    {"key": "NVT Ratio", "cssSelector": "p:contains('NVT Ratio') + p"},
    {"key": "RHODL Ratio", "cssSelector": "p:contains('RHODL Ratio') + p"},
    {"key": "Projected Next Halving Date", "cssSelector": "p:contains('Projected Date') + p"},
    {"key": "Halving at Block", "cssSelector": "p:contains('Halving at Block') + p"},
    {"key": "Blocks Remaining", "cssSelector": "p:contains('Blocks Remaining') + p"},
    {"key": "BTC Until Halving", "cssSelector": "p:contains('BTC Until Halving') + p"},
    {"key": "IBIT - BlackRock", "cssSelector": "p:contains('BlackRock') + p"},
    {"key": "FBTC - Fidelity", "cssSelector": "p:contains('Fidelity') + p"},
    {"key": "GBTC - Grayscale", "cssSelector": "p:contains('Grayscale') + p"},
]

def extract_element(soup, selector):
    """
    Extracts text based on CSS selectors, supporting the special 'contains' pseudo-selector.
    """
    try:
        # Handle custom :contains selector
        if ":contains" in selector:
            parts = selector.split(":contains")
            base_tag = parts[0].strip() or "p"  # Default to p if empty
            
            # Extract content to find
            # Regex to find content inside quotes
            match = re.search(r"['\"](.*?)['\"]", parts[1])
            if not match:
                return None
            search_text = match.group(1)
            
            # Find the element
            # We look for all elements matching the base tag
            elements = soup.select(base_tag) if base_tag else soup.find_all()
            
            for el in elements:
                # Check if text matches
                if search_text in el.get_text():
                    # Check for sibling selector
                    if "+ p" in selector:
                         sibling = el.find_next_sibling('p')
                         if sibling:
                             return sibling.get_text().strip()
                    else:
                        return el.get_text().strip()
            return None
        else:
            # Standard CSS selector
            element = soup.select_one(selector)
            return element.get_text().strip() if element else None
    except Exception as e:
        print(f"Error extracting {selector}: {e}")
        return None

def fetch_data():
    if not FIRECRAWL_API_KEY:
        print("Error: FIRECRAWL_API_KEY not found.")
        return

    print("Initializing Firecrawl...")
    app = FirecrawlApp(api_key=FIRECRAWL_API_KEY)
    
    print(f"Scraping {URL}...")
    try:
        # Get HTML content
        scrape_result = app.scrape_url(URL, params={'formats': ['html']})
        html_content = scrape_result.get('html')
        
        if not html_content:
            print("No HTML content returned.")
            return

        soup = BeautifulSoup(html_content, 'html.parser')
        
        data = {
            'timestamp': datetime.utcnow().isoformat()
        }
        
        print("Extracting data points...")
        for item in SELECTORS:
            key = item['key']
            selector = item['cssSelector']
            value = extract_element(soup, selector)
            data[key] = value
            # print(f"{key}: {value}")

        # Ensure output directory exists
        os.makedirs(OUTPUT_DIR, exist_ok=True)
        
        # Save to CSV
        output_file = os.path.join(OUTPUT_DIR, 'bitcoin_dashboard_data.csv')
        
        # Check if file exists to determine if we need a header
        file_exists = os.path.isfile(output_file)
        
        df = pd.DataFrame([data])
        
        # Append to CSV
        df.to_csv(output_file, mode='a', header=not file_exists, index=False)
        print(f"Data saved to {output_file}")
        
    except Exception as e:
        print(f"An error occurred: {e}")

if __name__ == "__main__":
    fetch_data()
