#!/usr/bin/env python3
"""
Apple Mac Scraper - Simplified Historical Version
Based on working V7 core with streamlined historical tracking
"""

print("🍎 Apple Mac Scraper - Historical Version Starting...")

import requests
import re
import time
import os
from datetime import datetime
from urllib.parse import urljoin, urlparse
from bs4 import BeautifulSoup
from typing import List, Dict, Optional
import pandas as pd

# Google Sheets integration
try:
    import gspread
    from oauth2client.service_account import ServiceAccountCredentials
    GOOGLE_SHEETS_AVAILABLE = True
    print("✅ Google Sheets libraries loaded")
except ImportError:
    GOOGLE_SHEETS_AVAILABLE = False
    print("⚠️ Google Sheets libraries not available")

# Configuration
GOOGLE_SHEET_ID = "1j7npqR9I303etrf67HYkXU6m87DEwg66j0dmqxpMllQ"
CREDENTIALS_FILE = "credentials.json"
CURRENT_SHEET_NAME = "Current Inventory"
PRICE_HISTORY_SHEET_NAME = "Price History"
AVAILABILITY_HISTORY_SHEET_NAME = "Availability History"

class SimpleAppleScraper:
    def __init__(self):
        print("🚀 Initializing scraper...")
        
        self.base_url = "https://www.apple.com"
        self.mac_url = "https://www.apple.com/uk/shop/refurbished/mac"
        
        # Setup session
        self.session = requests.Session()
        self.session.headers.update({
            'User-Agent': 'Mozilla/5.0 (Macintosh; Intel Mac OS X 10_15_7) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/121.0.0.0 Safari/537.36'
        })
        
        # Setup Google Sheets
        self.google_client = None
        self.spreadsheet = None
        if GOOGLE_SHEETS_AVAILABLE:
            self.setup_google_sheets()
        
        print("✅ Scraper initialized")

    def setup_google_sheets(self):
        """Setup Google Sheets connection."""
        try:
            if not os.path.exists(CREDENTIALS_FILE):
                print(f"⚠️ Credentials file not found: {CREDENTIALS_FILE}")
                return
                
            scope = ['https://spreadsheets.google.com/feeds', 'https://www.googleapis.com/auth/drive']
            creds = ServiceAccountCredentials.from_json_keyfile_name(CREDENTIALS_FILE, scope)
            self.google_client = gspread.authorize(creds)
            self.spreadsheet = self.google_client.open_by_key(GOOGLE_SHEET_ID)
            
            print("✅ Google Sheets connected")
            self.ensure_sheets_exist()
            
        except Exception as e:
            print(f"⚠️ Google Sheets setup failed: {e}")

    def ensure_sheets_exist(self):
        """Create necessary sheets if they don't exist."""
        try:
            existing_sheets = [sheet.title for sheet in self.spreadsheet.worksheets()]
            
            # Create sheets if needed
            for sheet_name in [CURRENT_SHEET_NAME, PRICE_HISTORY_SHEET_NAME, AVAILABILITY_HISTORY_SHEET_NAME]:
                if sheet_name not in existing_sheets:
                    print(f"🆕 Creating sheet: {sheet_name}")
                    new_sheet = self.spreadsheet.add_worksheet(title=sheet_name, rows=1000, cols=20)
                    
                    # Add headers based on sheet type
                    if sheet_name == PRICE_HISTORY_SHEET_NAME:
                        headers = ['timestamp', 'model_sku', 'name', 'change_type', 'old_price', 'new_price', 'change_amount', 'url']
                        new_sheet.append_row(headers)
                    elif sheet_name == AVAILABILITY_HISTORY_SHEET_NAME:
                        headers = ['timestamp', 'model_sku', 'name', 'change_type', 'current_price', 'url']
                        new_sheet.append_row(headers)
                        
        except Exception as e:
            print(f"⚠️ Error creating sheets: {e}")

    def get_page(self, url: str) -> Optional[BeautifulSoup]:
        """Fetch and parse a webpage."""
        try:
            print(f"🌐 Fetching: {url}")
            time.sleep(2)  # Be respectful
            response = self.session.get(url, timeout=30)
            response.raise_for_status()
            return BeautifulSoup(response.content, 'lxml')
        except Exception as e:
            print(f"❌ Failed to fetch {url}: {e}")
            return None

    def extract_prices_from_text(self, text: str) -> Dict:
        """Extract pricing information from text."""
        prices = {
            'current_price': None,
            'original_price': None,
            'savings': None,
            'discount_percentage': None
        }
        
        if not text:
            return prices
            
        # Clean text and find prices
        clean_text = re.sub(r'<[^>]+>', '', text)
        clean_text = re.sub(r'\s+', ' ', clean_text)
        price_matches = re.findall(r'£([\d,]+(?:\.\d{2})?)', clean_text)
        
        if not price_matches:
            return prices
            
        # Convert to numbers
        price_values = []
        for match in price_matches:
            try:
                value = float(match.replace(',', ''))
                if 200 <= value <= 20000:
                    price_values.append(value)
            except:
                continue
        
        # Extract pricing based on number of values found
        if len(price_values) == 1:
            prices['current_price'] = price_values[0]
        elif len(price_values) == 2:
            p1, p2 = price_values[0], price_values[1]
            if p2 > p1:
                prices['current_price'] = p1
                prices['original_price'] = p2
                prices['savings'] = p2 - p1
                prices['discount_percentage'] = round((prices['savings'] / p2 * 100), 2)
            else:
                prices['current_price'] = p1
        elif len(price_values) >= 3:
            current, original, savings = price_values[0], price_values[1], price_values[2]
            if abs((original - current) - savings) <= 2:
                prices['current_price'] = current
                prices['original_price'] = original
                prices['savings'] = savings
                prices['discount_percentage'] = round((savings / original * 100), 2)
            else:
                if original > current:
                    prices['current_price'] = current
                    prices['original_price'] = original
                    prices['savings'] = original - current
                    prices['discount_percentage'] = round((prices['savings'] / original * 100), 2)
                else:
                    prices['current_price'] = current
        
        return prices

    def extract_price_near_link(self, link, soup) -> Dict:
        """Extract price from area around a product link."""
        # Try parent container first
        parent = link.find_parent(['div', 'section', 'article', 'li'])
        if parent:
            parent_text = parent.get_text()
            prices = self.extract_prices_from_text(parent_text)
            if prices.get('current_price'):
                return prices
        
        # Try siblings
        next_sibling = link.next_sibling
        attempts = 0
        while next_sibling and attempts < 3:
            if hasattr(next_sibling, 'get_text'):
                sibling_text = next_sibling.get_text()
                prices = self.extract_prices_from_text(sibling_text)
                if prices.get('current_price'):
                    return prices
            elif isinstance(next_sibling, str):
                prices = self.extract_prices_from_text(next_sibling)
                if prices.get('current_price'):
                    return prices
            next_sibling = next_sibling.next_sibling
            attempts += 1
        
        return {'current_price': None, 'original_price': None, 'savings': None, 'discount_percentage': None}

    def scrape_products(self) -> List[Dict]:
        """Main scraping method."""
        print("\n🎯 Starting product scraping...")
        
        # Get main page
        soup = self.get_page(self.mac_url)
        if not soup:
            print("❌ Could not fetch main page")
            return []
        
        # Find product links
        product_links = soup.find_all('a', href=re.compile(r'/uk/shop/product/'))
        print(f"🔍 Found {len(product_links)} product links")
        
        products = []
        seen_urls = set()
        
        for i, link in enumerate(product_links[:50]):  # Limit to 50 for testing
            try:
                product_url = urljoin(self.base_url, link.get('href', ''))
                
                if product_url in seen_urls:
                    continue
                seen_urls.add(product_url)
                
                product_name = link.get_text(strip=True)
                if len(product_name) < 20:
                    continue
                
                # Extract model SKU from URL
                url_parts = urlparse(product_url).path.split('/')
                model_sku = url_parts[4] if len(url_parts) > 4 else f"product_{i}"
                
                # Extract price from category page
                prices = self.extract_price_near_link(link, soup)
                
                product = {
                    'name': product_name,
                    'model_sku': model_sku,
                    'current_price': prices.get('current_price'),
                    'original_price': prices.get('original_price'),
                    'savings': prices.get('savings'),
                    'discount_percentage': prices.get('discount_percentage'),
                    'url': product_url,
                    'scraped_at': datetime.now().strftime("%Y-%m-%d %H:%M:%S")
                }
                
                products.append(product)
                print(f"   📱 [{i+1}] {product_name[:50]}... | £{prices.get('current_price', 'N/A')}")
                
                if len(products) >= 20:  # Limit for testing
                    print("🛑 Stopping at 20 products for testing")
                    break
                    
            except Exception as e:
                print(f"❌ Error processing product {i+1}: {e}")
                continue
        
        print(f"✅ Scraped {len(products)} products")
        return products

    def load_previous_data(self) -> Dict:
        """Load previous data from Current Inventory sheet."""
        previous_data = {}
        
        if not self.google_client:
            print("⚠️ No Google Sheets connection")
            return previous_data
            
        try:
            current_sheet = self.spreadsheet.worksheet(CURRENT_SHEET_NAME)
            records = current_sheet.get_all_records()
            
            for record in records:
                model_sku = record.get('model_sku')
                if model_sku:
                    previous_data[model_sku] = record
                    
            print(f"📊 Loaded {len(previous_data)} previous products")
            
        except Exception as e:
            print(f"⚠️ Could not load previous data: {e}")
            
        return previous_data

    def detect_changes(self, current_products: List[Dict], previous_data: Dict):
        """Detect and log changes."""
        if not self.google_client:
            return
            
        timestamp = datetime.now().strftime("%Y-%m-%d %H:%M:%S")
        price_changes = []
        availability_changes = []
        
        current_lookup = {p.get('model_sku'): p for p in current_products if p.get('model_sku')}
        
        print(f"\n🔍 Detecting changes...")
        
        # Check for new products
        for sku, current_product in current_lookup.items():
            if sku not in previous_data:
                print(f"🆕 NEW: {current_product.get('name', 'Unknown')[:50]}...")
                availability_changes.append([
                    timestamp, sku, current_product.get('name'), 'APPEARED',
                    current_product.get('current_price'), current_product.get('url')
                ])
        
        # Check for removed products
        for sku, previous_product in previous_data.items():
            if sku not in current_lookup:
                print(f"❌ REMOVED: {previous_product.get('name', 'Unknown')[:50]}...")
                availability_changes.append([
                    timestamp, sku, previous_product.get('name'), 'DISAPPEARED',
                    previous_product.get('current_price'), previous_product.get('url')
                ])
        
        # Check for price changes
        for sku, current_product in current_lookup.items():
            if sku in previous_data:
                previous_product = previous_data[sku]
                
                try:
                    current_price = float(current_product.get('current_price', 0) or 0)
                    previous_price = float(previous_product.get('current_price', 0) or 0)
                    
                    if current_price != previous_price and current_price > 0 and previous_price > 0:
                        change_amount = current_price - previous_price
                        change_type = 'INCREASE' if change_amount > 0 else 'DECREASE'
                        
                        print(f"💰 PRICE {change_type}: {current_product.get('name', 'Unknown')[:40]}...")
                        print(f"   £{previous_price} -> £{current_price} ({'+' if change_amount > 0 else ''}£{change_amount:.2f})")
                        
                        price_changes.append([
                            timestamp, sku, current_product.get('name'), change_type,
                            previous_price, current_price, change_amount, current_product.get('url')
                        ])
                        
                except (ValueError, TypeError):
                    pass
        
        # Log changes to sheets
        if price_changes and self.google_client:
            try:
                price_sheet = self.spreadsheet.worksheet(PRICE_HISTORY_SHEET_NAME)
                for change in price_changes:
                    price_sheet.append_row(change)
                print(f"✅ Logged {len(price_changes)} price changes")
            except Exception as e:
                print(f"❌ Error logging price changes: {e}")
        
        if availability_changes and self.google_client:
            try:
                avail_sheet = self.spreadsheet.worksheet(AVAILABILITY_HISTORY_SHEET_NAME)
                for change in availability_changes:
                    avail_sheet.append_row(change)
                print(f"✅ Logged {len(availability_changes)} availability changes")
            except Exception as e:
                print(f"❌ Error logging availability changes: {e}")
        
        if not price_changes and not availability_changes:
            print("✅ No changes detected")

    def update_current_inventory(self, products: List[Dict]):
        """Update Current Inventory sheet."""
        if not self.google_client or not products:
            return
            
        try:
            current_sheet = self.spreadsheet.worksheet(CURRENT_SHEET_NAME)
            current_sheet.clear()
            
            # Prepare headers and data
            headers = ['name', 'model_sku', 'current_price', 'original_price', 'savings', 
                      'discount_percentage', 'url', 'scraped_at']
            
            data = [headers]
            for product in products:
                row = [str(product.get(header, '')) for header in headers]
                data.append(row)
            
            current_sheet.update(data, value_input_option='USER_ENTERED')
            
            # Format header
            current_sheet.format('A1:H1', {
                'backgroundColor': {'red': 0.2, 'green': 0.2, 'blue': 0.2},
                'textFormat': {'foregroundColor': {'red': 1, 'green': 1, 'blue': 1}, 'bold': True}
            })
            
            print(f"✅ Updated Current Inventory with {len(products)} products")
            
        except Exception as e:
            print(f"❌ Failed to update Current Inventory: {e}")

    def save_csv_backup(self, products: List[Dict]):
        """Save CSV backup with timestamp."""
        if not products:
            return None
            
        timestamp = datetime.now().strftime("%Y%m%d_%H%M%S")
        filename = f"apple_mac_products_{timestamp}.csv"
        
        df = pd.DataFrame(products)
        df.to_csv(filename, index=False)
        print(f"💾 Saved CSV backup: {filename}")
        return filename

def main():
    """Main function."""
    print("🍎 Apple Mac Historical Scraper")
    print("=" * 50)
    
    try:
        # Initialize scraper
        scraper = SimpleAppleScraper()
        
        # Load previous data for comparison
        previous_data = scraper.load_previous_data()
        
        # Scrape current products
        current_products = scraper.scrape_products()
        
        if not current_products:
            print("❌ No products found")
            return
        
        # Detect and log changes
        scraper.detect_changes(current_products, previous_data)
        
        # Update current inventory
        scraper.update_current_inventory(current_products)
        
        # Save CSV backup
        scraper.save_csv_backup(current_products)
        
        # Summary
        print(f"\n🎉 COMPLETE!")
        print(f"📊 Found {len(current_products)} products")
        products_with_prices = len([p for p in current_products if p.get('current_price')])
        print(f"💰 {products_with_prices} products have pricing")
        
        if scraper.google_client:
            print(f"🔗 View data: https://docs.google.com/spreadsheets/d/{GOOGLE_SHEET_ID}")
        
    except Exception as e:
        print(f"💥 Script failed: {e}")
        import traceback
        traceback.print_exc()

if __name__ == "__main__":
    main()