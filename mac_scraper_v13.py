#!/usr/bin/env python3
"""
Apple Mac Refurbished Scraper V7 - HISTORICAL DATA TRACKING

This version implements comprehensive historical tracking:
1. Current Inventory Sheet - Latest snapshot
2. Price History Sheet - All price changes over time  
3. Availability History Sheet - Products appearing/disappearing

SETUP INSTRUCTIONS:
1. Install required packages: pip3 install requests beautifulsoup4 lxml pandas gspread oauth2client
2. Put your credentials.json file in the same folder as this script
3. Enable Google Drive API and Google Sheets API in Google Cloud Console

Dependencies:
    pip3 install requests beautifulsoup4 lxml pandas gspread oauth2client
"""

import requests
import re
import json
import time
import os
from datetime import datetime
from urllib.parse import urljoin, urlparse, parse_qs
from bs4 import BeautifulSoup
from typing import List, Dict, Optional, Set, Tuple
import pandas as pd

# Google Sheets integration
try:
    import gspread
    from oauth2client.service_account import ServiceAccountCredentials
    GOOGLE_SHEETS_AVAILABLE = True
except ImportError:
    GOOGLE_SHEETS_AVAILABLE = False
    print("Google Sheets libraries not installed. Install with: pip3 install gspread oauth2client")

# CONFIGURATION - UPDATE THESE VALUES
GOOGLE_SHEET_ID = "1j7npqR9I303etrf67HYkXU6m87DEwg66j0dmqxpMllQ"  # Your existing sheet ID
CREDENTIALS_FILE = "credentials.json"  # Make sure this file is in the same folder

# Sheet names for historical tracking
CURRENT_SHEET_NAME = "Current Inventory"
PRICE_HISTORY_SHEET_NAME = "Price History" 
AVAILABILITY_HISTORY_SHEET_NAME = "Availability History"


class AppleMacScraperV7Historical:
    def __init__(self):
        self.base_url = "https://www.apple.com"
        self.mac_url = "https://www.apple.com/uk/shop/refurbished/mac"
        self.session = requests.Session()
        self.session.headers.update({
            'User-Agent': 'Mozilla/5.0 (Macintosh; Intel Mac OS X 10_15_7) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/121.0.0.0 Safari/537.36',
            'Accept': 'text/html,application/xhtml+xml,application/xml;q=0.9,image/webp,*/*;q=0.8',
            'Accept-Language': 'en-GB,en-US;q=0.9,en;q=0.8',
            'Accept-Encoding': 'gzip, deflate, br',
            'Connection': 'keep-alive',
            'Sec-Fetch-Dest': 'document',
            'Sec-Fetch-Mode': 'navigate',
            'Sec-Fetch-Site': 'same-origin',
            'Cache-Control': 'max-age=0',
        })
        
        # Initialize Google Sheets client
        self.google_client = None
        self.spreadsheet = None
        if GOOGLE_SHEETS_AVAILABLE:
            self.setup_google_sheets()

    def setup_google_sheets(self):
        """Set up Google Sheets connection and create necessary sheets."""
        try:
            if not os.path.exists(CREDENTIALS_FILE):
                print(f"❌ Credentials file '{CREDENTIALS_FILE}' not found!")
                return False
            
            scope = ['https://spreadsheets.google.com/feeds',
                    'https://www.googleapis.com/auth/drive']
            
            creds = ServiceAccountCredentials.from_json_keyfile_name(CREDENTIALS_FILE, scope)
            self.google_client = gspread.authorize(creds)
            
            # Open the spreadsheet
            self.spreadsheet = self.google_client.open_by_key(GOOGLE_SHEET_ID)
            print("✅ Google Sheets connection established")
            
            # Ensure all required sheets exist
            self.setup_historical_sheets()
            return True
            
        except Exception as e:
            print(f"❌ Failed to set up Google Sheets: {e}")
            return False

    def setup_historical_sheets(self):
        """Create the necessary sheets for historical tracking if they don't exist."""
        try:
            existing_sheets = [sheet.title for sheet in self.spreadsheet.worksheets()]
            
            # Create Current Inventory sheet
            if CURRENT_SHEET_NAME not in existing_sheets:
                self.spreadsheet.add_worksheet(title=CURRENT_SHEET_NAME, rows=1000, cols=20)
                print(f"✅ Created '{CURRENT_SHEET_NAME}' sheet")
            
            # Create Price History sheet
            if PRICE_HISTORY_SHEET_NAME not in existing_sheets:
                price_sheet = self.spreadsheet.add_worksheet(title=PRICE_HISTORY_SHEET_NAME, rows=10000, cols=15)
                # Add headers for price history
                headers = ['timestamp', 'model_sku', 'name', 'change_type', 'old_current_price', 
                          'new_current_price', 'old_original_price', 'new_original_price', 
                          'old_savings', 'new_savings', 'old_discount_percentage', 
                          'new_discount_percentage', 'price_change_amount', 'url']
                price_sheet.append_row(headers)
                print(f"✅ Created '{PRICE_HISTORY_SHEET_NAME}' sheet")
            
            # Create Availability History sheet  
            if AVAILABILITY_HISTORY_SHEET_NAME not in existing_sheets:
                avail_sheet = self.spreadsheet.add_worksheet(title=AVAILABILITY_HISTORY_SHEET_NAME, rows=10000, cols=10)
                # Add headers for availability history
                headers = ['timestamp', 'model_sku', 'name', 'change_type', 'current_price', 
                          'original_price', 'savings', 'discount_percentage', 'url']
                avail_sheet.append_row(headers)
                print(f"✅ Created '{AVAILABILITY_HISTORY_SHEET_NAME}' sheet")
                
        except Exception as e:
            print(f"❌ Error setting up historical sheets: {e}")

    def get_page(self, url: str, retries: int = 3) -> Optional[BeautifulSoup]:
        """Fetch and parse a webpage with retries and rate limiting."""
        for attempt in range(retries):
            try:
                # Rate limiting - be respectful
                time.sleep(2)  # 2 second delay between requests
                
                response = self.session.get(url, timeout=30)
                response.raise_for_status()
                return BeautifulSoup(response.content, 'lxml')
            except requests.RequestException as e:
                print(f"❌ Attempt {attempt + 1} failed: {e}")
                if attempt < retries - 1:
                    time.sleep(5)  # Longer delay on retry
                else:
                    print(f"❌ Failed to fetch {url} after {retries} attempts")
                    return None

    def discover_all_pages(self) -> List[str]:
        """Discover all pagination URLs for Mac refurbished products."""
        print("🔍 Discovering all Mac refurbished pages...")
        
        # Start with the main page
        soup = self.get_page(self.mac_url)
        if not soup:
            return [self.mac_url]
        
        page_urls = [self.mac_url]
        
        # Look for pagination links
        pagination_selectors = [
            'a[href*="mac"][href*="page"]',
            '.pagination a',
            'a[href*="?page="]',
            'a[aria-label*="page"]',
            'a[href*="fnode"]'
        ]
        
        found_pages = set([self.mac_url])
        
        for selector in pagination_selectors:
            pagination_links = soup.select(selector)
            for link in pagination_links:
                href = link.get('href')
                if href and '/mac' in href:
                    full_url = urljoin(self.base_url, href)
                    if full_url not in found_pages:
                        found_pages.add(full_url)
                        page_urls.append(full_url)
        
        # If no pagination found, try manual construction
        if len(page_urls) == 1:
            print("🔄 No pagination found, trying manual page construction...")
            for page_num in range(2, 7):
                test_urls = [
                    f"{self.mac_url}?page={page_num}",
                    f"{self.mac_url}/page/{page_num}",
                ]
                
                for test_url in test_urls:
                    test_soup = self.get_page(test_url)
                    if test_soup:
                        product_links = test_soup.find_all('a', href=re.compile(r'/uk/shop/product/'))
                        if len(product_links) > 10:
                            page_urls.append(test_url)
                            break
                    time.sleep(2)
        
        print(f"📄 Found {len(page_urls)} pages to scrape")
        return page_urls

    def extract_products_with_prices_from_category_page(self, soup: BeautifulSoup) -> List[Dict]:
        """Extract products with pricing from category page using the working V7 approach."""
        products = []
        
        # Find all product links (this worked in V7)
        product_links = soup.find_all('a', href=re.compile(r'/uk/shop/product/'))
        print(f"🔍 Found {len(product_links)} product links on category page")
        
        seen_urls = set()
        
        for i, link in enumerate(product_links):
            try:
                product_url = urljoin(self.base_url, link.get('href', ''))
                
                # Skip duplicates
                if product_url in seen_urls:
                    continue
                seen_urls.add(product_url)
                
                product_name = link.get_text(strip=True)
                
                # Skip if product name is too short (likely navigation)
                if len(product_name) < 20:
                    continue
                
                print(f"   📱 Found: {product_name[:60]}...")
                
                # Extract model/SKU from URL
                url_parts = urlparse(product_url).path.split('/')
                model_sku = url_parts[4] if len(url_parts) > 4 else None
                
                # Extract price from the category page around this link
                prices = self.extract_price_near_link(link, soup)
                
                print(f"   💰 Price found: £{prices.get('current_price', 'N/A')}")
                
                product = {
                    'name': product_name,
                    'current_price': prices.get('current_price'),
                    'original_price': prices.get('original_price'),
                    'savings': prices.get('savings'),
                    'discount_percentage': prices.get('discount_percentage'),
                    'url': product_url,
                    'model_sku': model_sku,
                    'scraped_at': datetime.now().strftime("%Y-%m-%d %H:%M:%S")
                }
                
                products.append(product)
                
            except Exception as e:
                print(f"❌ Error processing product {i+1}: {e}")
                continue
        
        return products

    def extract_price_near_link(self, link, soup) -> Dict:
        """Extract price information from the area around a product link."""
        prices = {
            'current_price': None,
            'original_price': None,
            'savings': None,
            'discount_percentage': None
        }
        
        try:
            # Method 1: Look in the same parent container as the link
            parent = link.find_parent(['div', 'section', 'article', 'li'])
            if parent:
                parent_text = parent.get_text()
                prices = self.extract_prices_from_text(parent_text)
                if prices.get('current_price'):
                    return prices
            
            # Method 2: Look at siblings of the link
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
            
            # Method 3: Look at the surrounding text in a wider area
            # Find all text nodes within a reasonable distance of this link
            link_position = str(soup).find(str(link))
            if link_position != -1:
                # Get text within 500 characters after the link
                surrounding_html = str(soup)[link_position:link_position + 500]
                # Extract just the text
                temp_soup = BeautifulSoup(surrounding_html, 'lxml')
                surrounding_text = temp_soup.get_text()
                prices = self.extract_prices_from_text(surrounding_text)
                if prices.get('current_price'):
                    return prices
            
        except Exception as e:
            print(f"    ❌ Error extracting price near link: {e}")
        
        return prices

    def extract_prices_from_text(self, price_text: str) -> Dict:
        """Extract pricing information from text - simplified and robust."""
        prices = {
            'current_price': None,
            'original_price': None,
            'savings': None,
            'discount_percentage': None
        }
        
        if not price_text or len(price_text.strip()) == 0:
            return prices
        
        try:
            # Clean up the text - remove HTML tags and normalize whitespace
            clean_text = re.sub(r'<[^>]+>', '', price_text)
            clean_text = re.sub(r'\s+', ' ', clean_text.strip())
            
            # Remove common words that aren't prices
            clean_text = re.sub(r'\b(Now|Was|Save|visuallyhidden|span|class)\b', '', clean_text, flags=re.IGNORECASE)
            
            # Find all price values
            price_matches = re.findall(r'£([\d,]+(?:\.\d{2})?)', clean_text)
            
            if not price_matches:
                return prices
            
            # Convert to float values and filter reasonable prices
            price_values = []
            for match in price_matches:
                try:
                    value = float(match.replace(',', ''))
                    if 200 <= value <= 20000:  # Reasonable range for Apple products
                        price_values.append(value)
                except:
                    continue
            
            if not price_values:
                return prices
            
            # Pattern matching based on number of prices found
            if len(price_values) == 1:
                # Single price - use as current price
                prices['current_price'] = price_values[0]
                
            elif len(price_values) == 2:
                # Two prices - could be current/original
                p1, p2 = price_values[0], price_values[1]
                if p2 > p1:  # Second price is higher, likely original
                    prices['current_price'] = p1
                    prices['original_price'] = p2
                    prices['savings'] = p2 - p1
                    prices['discount_percentage'] = round((prices['savings'] / p2 * 100), 2)
                else:
                    # Just use first price
                    prices['current_price'] = p1
                    
            elif len(price_values) >= 3:
                # Three+ prices - current, original, savings pattern
                current, original, savings = price_values[0], price_values[1], price_values[2]
                
                # Validate the math (allow small rounding differences)
                if abs((original - current) - savings) <= 2:
                    prices['current_price'] = current
                    prices['original_price'] = original
                    prices['savings'] = savings
                    prices['discount_percentage'] = round((savings / original * 100), 2)
                else:
                    # Math doesn't work, treat as two prices
                    if original > current:
                        prices['current_price'] = current
                        prices['original_price'] = original
                        prices['savings'] = original - current
                        prices['discount_percentage'] = round((prices['savings'] / original * 100), 2)
                    else:
                        prices['current_price'] = current
                        
        except Exception as e:
            pass  # Silently fail and return empty prices
            
        return prices

    def extract_detailed_specs(self, product: Dict) -> Dict:
        """Extract detailed specifications from individual product pages."""
        product_url = product.get('url')
        if not product_url:
            return product
        
        print(f"🔍 Getting detailed specs for: {product.get('name', 'Unknown')[:50]}...")
        
        soup = self.get_page(product_url)
        if not soup:
            print(f"❌ Could not fetch product page")
            return product
        
        try:
            # Initialize spec fields
            specs = {
                'memory': None,
                'storage': None,
                'chip': None,
                'display_size': None,
                'color': None,
                'connectivity': None,
                'cpu_cores': None,
                'gpu_cores': None
            }
            
            # Get all page text for analysis
            page_text = soup.get_text()
            product_name = product.get('name', '')
            
            # STORAGE EXTRACTION
            storage_patterns = [
                r'(\d+(?:GB|TB))\s+SSD',
                r'(\d+(?:GB|TB))\s+storage',
                r'Storage[:\s]+(\d+(?:GB|TB))',
                r'(\d+(?:GB|TB))\s+internal storage',
                r'(\d+(?:GB|TB))\s+of storage',
                r'with\s+(\d+(?:GB|TB))\s+SSD',
                r'includes\s+(\d+(?:GB|TB))',
                r'featuring\s+(\d+(?:GB|TB))',
                r'(\d+(?:GB|TB))\s*-\s*',
                r'-\s*(\d+(?:GB|TB))',
                r'Capacity[:\s]*(\d+(?:GB|TB))',
                r'Flash Storage[:\s]*(\d+(?:GB|TB))'
            ]
            
            combined_text = f"{product_name} {page_text}"
            
            for pattern in storage_patterns:
                storage_match = re.search(pattern, combined_text, re.IGNORECASE)
                if storage_match:
                    storage_candidate = storage_match.group(1)
                    storage_num = int(re.search(r'\d+', storage_candidate).group())
                    storage_unit = re.search(r'[GT]B', storage_candidate).group()
                    
                    if (storage_unit == 'GB' and storage_num >= 256) or (storage_unit == 'TB' and storage_num <= 8):
                        specs['storage'] = storage_candidate
                        break
            
            # MEMORY EXTRACTION
            memory_patterns = [
                r'(\d+GB)\s+unified memory',
                r'(\d+GB)\s+memory',
                r'Memory[:\s]+(\d+GB)',
                r'with\s+(\d+GB)\s+of\s+unified\s+memory',
                r'(\d+GB)\s+RAM',
                r'Unified Memory[:\s]*(\d+GB)'
            ]
            
            for pattern in memory_patterns:
                memory_match = re.search(pattern, combined_text, re.IGNORECASE)
                if memory_match:
                    memory_candidate = memory_match.group(1)
                    memory_num = int(re.search(r'\d+', memory_candidate).group())
                    if 8 <= memory_num <= 128:
                        specs['memory'] = memory_candidate
                        break
            
            # CHIP EXTRACTION
            chip_patterns = [
                r'Apple (M\d+(?:\s+Pro|\s+Max|\s+Ultra)?)',
                r'(M\d+(?:\s+Pro|\s+Max|\s+Ultra)?)\s+[Cc]hip',
                r'Apple (M\d+)'
            ]
            
            for pattern in chip_patterns:
                chip_match = re.search(pattern, combined_text, re.IGNORECASE)
                if chip_match:
                    specs['chip'] = chip_match.group(1)
                    break
            
            # CPU/GPU CORES EXTRACTION
            cpu_match = re.search(r'(\d+)[\-‑]Core CPU', combined_text, re.IGNORECASE)
            if cpu_match:
                specs['cpu_cores'] = f"{cpu_match.group(1)}-Core CPU"
            
            gpu_match = re.search(r'(\d+)[\-‑]Core GPU', combined_text, re.IGNORECASE)
            if gpu_match:
                specs['gpu_cores'] = f"{gpu_match.group(1)}-Core GPU"
            
            # DISPLAY SIZE EXTRACTION
            display_patterns = [
                r'(\d+(?:\.\d+)?[\-‑]inch)',
                r'(\d+(?:\.\d+)?")',
                r'(\d+(?:\.\d+)?)\s*inch'
            ]
            
            for pattern in display_patterns:
                display_match = re.search(pattern, product_name, re.IGNORECASE)
                if display_match:
                    size = display_match.group(1)
                    if 'inch' not in size:
                        specs['display_size'] = f"{size}-inch"
                    else:
                        specs['display_size'] = size
                    break
            
            # COLOR EXTRACTION
            color_patterns = [
                r'[\-\s](Space (?:Grey|Gray|Black))',
                r'[\-\s](Silver)', r'[\-\s](Gold)', r'[\-\s](Rose Gold)',
                r'[\-\s](Midnight)', r'[\-\s](Starlight)', r'[\-\s](Blue)',
                r'[\-\s](Green)', r'[\-\s](Pink)', r'[\-\s](Purple)',
                r'[\-\s](Yellow)', r'[\-\s](Orange)', r'[\-\s](Red)',
                r'[\-\s](Sky Blue)'
            ]
            
            for pattern in color_patterns:
                color_match = re.search(pattern, product_name, re.IGNORECASE)
                if color_match:
                    specs['color'] = color_match.group(1).strip()
                    break
            
            # CONNECTIVITY EXTRACTION
            if 'Gigabit Ethernet' in combined_text:
                specs['connectivity'] = 'Gigabit Ethernet'
            elif '10Gb Ethernet' in combined_text:
                specs['connectivity'] = '10Gb Ethernet'
            elif 'Wi-Fi' in combined_text:
                specs['connectivity'] = 'Wi-Fi'
            
            # Add specs to product
            product.update(specs)
            
            print(f"✅ Extracted specs: {specs['chip'] or 'N/A'} | {specs['memory'] or 'N/A'} | {specs['storage'] or 'N/A'}")
            
        except Exception as e:
            print(f"❌ Error extracting specs: {e}")
        
        return product

    def load_previous_data(self) -> Dict[str, Dict]:
        """Load the previous data from Current Inventory sheet for comparison."""
        previous_data = {}
        
        if not self.google_client:
            return previous_data
            
        try:
            current_sheet = self.spreadsheet.worksheet(CURRENT_SHEET_NAME)
            records = current_sheet.get_all_records()
            
            for record in records:
                model_sku = record.get('model_sku')
                if model_sku:
                    previous_data[model_sku] = record
                    
            print(f"📊 Loaded {len(previous_data)} previous products for comparison")
            
        except Exception as e:
            print(f"⚠️ Could not load previous data: {e}")
            
        return previous_data

    def detect_and_log_changes(self, current_products: List[Dict], previous_data: Dict[str, Dict]):
        """Detect changes and log them to history sheets."""
        if not self.google_client:
            return
            
        timestamp = datetime.now().strftime("%Y-%m-%d %H:%M:%S")
        price_changes = []
        availability_changes = []
        
        # Create lookup for current products
        current_lookup = {p.get('model_sku'): p for p in current_products if p.get('model_sku')}
        
        print(f"\n🔍 CHANGE DETECTION")
        print(f"=" * 40)
        
        # Check for new products (appeared)
        for sku, current_product in current_lookup.items():
            if sku not in previous_data:
                print(f"🆕 NEW PRODUCT: {current_product.get('name', 'Unknown')[:50]}...")
                availability_changes.append([
                    timestamp, sku, current_product.get('name'), 'APPEARED', 
                    current_product.get('current_price'), current_product.get('original_price'),
                    current_product.get('savings'), current_product.get('discount_percentage'),
                    current_product.get('url')
                ])
        
        # Check for removed products (disappeared)
        for sku, previous_product in previous_data.items():
            if sku not in current_lookup:
                print(f"❌ REMOVED PRODUCT: {previous_product.get('name', 'Unknown')[:50]}...")
                availability_changes.append([
                    timestamp, sku, previous_product.get('name'), 'DISAPPEARED',
                    previous_product.get('current_price'), previous_product.get('original_price'),
                    previous_product.get('savings'), previous_product.get('discount_percentage'),
                    previous_product.get('url')
                ])
        
        # Check for price changes in existing products
        for sku, current_product in current_lookup.items():
            if sku in previous_data:
                previous_product = previous_data[sku]
                
                # Compare prices (convert to float for comparison)
                try:
                    current_price = float(current_product.get('current_price', 0) or 0)
                    previous_price = float(previous_product.get('current_price', 0) or 0)
                    current_original = float(current_product.get('original_price', 0) or 0)
                    previous_original = float(previous_product.get('original_price', 0) or 0)
                    
                    # Check for any price changes
                    price_changed = (current_price != previous_price) or (current_original != previous_original)
                    
                    if price_changed:
                        change_type = 'PRICE_INCREASE' if current_price > previous_price else 'PRICE_DECREASE'
                        if current_price == previous_price:
                            change_type = 'ORIGINAL_PRICE_CHANGE'
                            
                        price_change_amount = current_price - previous_price
                        
                        print(f"💰 PRICE CHANGE: {current_product.get('name', 'Unknown')[:40]}...")
                        print(f"   £{previous_price} -> £{current_price} ({'+' if price_change_amount > 0 else ''}£{price_change_amount})")
                        
                        price_changes.append([
                            timestamp, sku, current_product.get('name'), change_type,
                            previous_product.get('current_price'), current_product.get('current_price'),
                            previous_product.get('original_price'), current_product.get('original_price'),
                            previous_product.get('savings'), current_product.get('savings'),
                            previous_product.get('discount_percentage'), current_product.get('discount_percentage'),
                            price_change_amount, current_product.get('url')
                        ])
                        
                except (ValueError, TypeError):
                    # Skip if price comparison fails
                    pass
        
        # Log changes to Google Sheets
        if price_changes:
            try:
                price_history_sheet = self.spreadsheet.worksheet(PRICE_HISTORY_SHEET_NAME)
                for change in price_changes:
                    price_history_sheet.append_row(change)
                print(f"✅ Logged {len(price_changes)} price changes")
            except Exception as e:
                print(f"❌ Error logging price changes: {e}")
        
        if availability_changes:
            try:
                availability_sheet = self.spreadsheet.worksheet(AVAILABILITY_HISTORY_SHEET_NAME)
                for change in availability_changes:
                    availability_sheet.append_row(change)
                print(f"✅ Logged {len(availability_changes)} availability changes")
            except Exception as e:
                print(f"❌ Error logging availability changes: {e}")
        
        if not price_changes and not availability_changes:
            print("✅ No changes detected since last run")

    def update_current_inventory(self, products: List[Dict]):
        """Update the Current Inventory sheet with latest data."""
        if not self.google_client:
            return
            
        try:
            current_sheet = self.spreadsheet.worksheet(CURRENT_SHEET_NAME)
            
            # Clear existing data
            current_sheet.clear()
            
            if products:
                # Get all possible headers
                all_headers = set()
                for product in products:
                    all_headers.update(product.keys())
                
                # Order headers logically
                preferred_order = [
                    'name', 'chip', 'cpu_cores', 'gpu_cores', 'memory', 'storage',
                    'display_size', 'color', 'current_price', 'original_price', 
                    'savings', 'discount_percentage', 'connectivity', 'url', 
                    'model_sku', 'scraped_at'
                ]
                
                headers = [h for h in preferred_order if h in all_headers]
                headers.extend([h for h in all_headers if h not in headers])
                
                # Prepare data rows
                data = [headers]  # Start with headers
                
                for product in products:
                    row = []
                    for header in headers:
                        value = product.get(header, '')
                        if value is None:
                            row.append('')
                        else:
                            row.append(str(value))
                    data.append(row)
                
                # Upload all data
                current_sheet.update(data, value_input_option='USER_ENTERED')
                
                # Format the header row
                current_sheet.format('A1:Z1', {
                    'backgroundColor': {'red': 0.2, 'green': 0.2, 'blue': 0.2},
                    'textFormat': {'foregroundColor': {'red': 1, 'green': 1, 'blue': 1}, 'bold': True}
                })
                
                # Format price columns as currency
                try:
                    price_columns = []
                    for i, header in enumerate(headers):
                        if 'price' in header.lower() or 'savings' in header.lower():
                            price_columns.append(chr(65 + i))
                    
                    for col in price_columns:
                        if ord(col) - 65 < 26:
                            current_sheet.format(f'{col}2:{col}1000', {
                                'numberFormat': {'type': 'CURRENCY', 'pattern': '£#,##0.00'}
                            })
                except Exception as e:
                    print(f"⚠️ Could not format currency columns: {e}")
                
                print(f"✅ Updated Current Inventory with {len(products)} products")
                
        except Exception as e:
            print(f"❌ Failed to update Current Inventory: {e}")