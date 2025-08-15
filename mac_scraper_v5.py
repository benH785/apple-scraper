#!/usr/bin/env python3
"""
Apple Mac Refurbished Scraper V5 - Fixed Pricing & Storage Extraction

This version fixes:
1. Incorrect £509.00 prices being assigned to wrong products
2. Missing storage information extraction
3. More accurate price matching to specific products

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
from typing import List, Dict, Optional, Set
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
GOOGLE_SHEET_NAME = "Apple Mac Products V3"  # Using your existing Google Sheet
CREDENTIALS_FILE = "credentials.json"  # Make sure this file is in the same folder


class AppleMacScraperV5:
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
        if GOOGLE_SHEETS_AVAILABLE:
            self.setup_google_sheets()

    def setup_google_sheets(self):
        """Set up Google Sheets connection."""
        try:
            if not os.path.exists(CREDENTIALS_FILE):
                print(f"❌ Credentials file '{CREDENTIALS_FILE}' not found!")
                return False
            
            scope = ['https://spreadsheets.google.com/feeds',
                    'https://www.googleapis.com/auth/drive']
            
            creds = ServiceAccountCredentials.from_json_keyfile_name(CREDENTIALS_FILE, scope)
            self.google_client = gspread.authorize(creds)
            print("✅ Google Sheets connection established")
            return True
            
        except Exception as e:
            print(f"❌ Failed to set up Google Sheets: {e}")
            return False

    def get_page(self, url: str, retries: int = 3) -> Optional[BeautifulSoup]:
        """Fetch and parse a webpage with retries."""
        for attempt in range(retries):
            try:
                response = self.session.get(url, timeout=30)
                response.raise_for_status()
                return BeautifulSoup(response.content, 'lxml')
            except requests.RequestException as e:
                print(f"❌ Attempt {attempt + 1} failed: {e}")
                if attempt < retries - 1:
                    time.sleep(2 ** attempt)  # Exponential backoff
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
                    time.sleep(1)
        
        print(f"📄 Found {len(page_urls)} pages to scrape")
        return page_urls

    def extract_basic_product_info(self, soup: BeautifulSoup) -> List[Dict]:
        """Extract basic product information with improved price matching."""
        products = []
        
        # Find all product links
        product_links = soup.find_all('a', href=re.compile(r'/uk/shop/product/'))
        print(f"🔍 Found {len(product_links)} product links on this page")
        
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
                
                # NEW APPROACH: Get price from individual product page instead of category page
                # This eliminates the £509.00 error from picking up wrong prices
                product_prices = self.get_price_from_product_page(product_url)
                
                current_price = product_prices.get('current_price')
                original_price = product_prices.get('original_price')
                savings = product_prices.get('savings')
                
                # Calculate savings if not found
                if not savings and current_price and original_price:
                    savings = original_price - current_price
                
                # Extract model/SKU from URL
                url_parts = urlparse(product_url).path.split('/')
                model_sku = url_parts[4] if len(url_parts) > 4 else None
                
                product = {
                    'name': product_name,
                    'current_price': current_price,
                    'original_price': original_price,
                    'savings': savings,
                    'discount_percentage': round((savings / original_price * 100), 2) if savings and original_price else None,
                    'url': product_url,
                    'model_sku': model_sku,
                    'scraped_at': datetime.now().strftime("%Y-%m-%d %H:%M:%S")
                }
                
                products.append(product)
                
                # Small delay between product page requests
                time.sleep(0.5)
                
            except Exception as e:
                print(f"❌ Error processing product {i+1}: {e}")
                continue
        
        return products

    def get_price_from_product_page(self, product_url: str) -> Dict:
        """Get accurate pricing directly from the individual product page."""
        prices = {
            'current_price': None,
            'original_price': None,
            'savings': None
        }
        
        try:
            soup = self.get_page(product_url)
            if not soup:
                return prices
            
            page_text = soup.get_text()
            
            # Look for various price patterns on the product page
            # Pattern 1: "Now £X Was £Y Save £Z"
            now_was_pattern = re.search(r'Now\s*£([\d,]+\.?\d*)\s*Was\s*£([\d,]+\.?\d*)\s*Save\s*£([\d,]+\.?\d*)', page_text, re.IGNORECASE)
            if now_was_pattern:
                prices['current_price'] = float(now_was_pattern.group(1).replace(',', ''))
                prices['original_price'] = float(now_was_pattern.group(2).replace(',', ''))
                prices['savings'] = float(now_was_pattern.group(3).replace(',', ''))
                return prices
            
            # Pattern 2: Look for price elements in specific containers
            price_containers = soup.find_all(['div', 'span'], class_=re.compile(r'price', re.IGNORECASE))
            for container in price_containers:
                container_text = container.get_text()
                prices_found = re.findall(r'£([\d,]+\.?\d*)', container_text)
                
                if len(prices_found) >= 2:
                    # Check for "Now" and "Was" keywords
                    if 'Now' in container_text and 'Was' in container_text:
                        now_match = re.search(r'Now[^\d]*£([\d,]+\.?\d*)', container_text)
                        was_match = re.search(r'Was[^\d]*£([\d,]+\.?\d*)', container_text)
                        save_match = re.search(r'Save[^\d]*£([\d,]+\.?\d*)', container_text)
                        
                        if now_match and was_match:
                            prices['current_price'] = float(now_match.group(1).replace(',', ''))
                            prices['original_price'] = float(was_match.group(1).replace(',', ''))
                            if save_match:
                                prices['savings'] = float(save_match.group(1).replace(',', ''))
                            return prices
            
            # Pattern 3: Look for prices in meta tags or structured data
            meta_price = soup.find('meta', {'property': 'product:price:amount'})
            if meta_price:
                prices['current_price'] = float(meta_price.get('content', 0))
            
            # Pattern 4: Look for JSON-LD structured data
            script_tags = soup.find_all('script', type='application/ld+json')
            for script in script_tags:
                try:
                    data = json.loads(script.string)
                    if isinstance(data, dict) and 'offers' in data:
                        offer = data['offers']
                        if isinstance(offer, dict) and 'price' in offer:
                            prices['current_price'] = float(offer['price'])
                except:
                    continue
            
            # Pattern 5: Fallback - look for any prices on the page
            all_prices = re.findall(r'£([\d,]+\.?\d*)', page_text)
            if len(all_prices) >= 2:
                # Filter out obviously wrong prices (too low or too high)
                valid_prices = []
                for price_str in all_prices:
                    price_val = float(price_str.replace(',', ''))
                    if 300 <= price_val <= 10000:  # Reasonable price range for Macs
                        valid_prices.append(price_val)
                
                if len(valid_prices) >= 2:
                    # Assume first valid price is current, second is original
                    valid_prices.sort()
                    prices['current_price'] = valid_prices[0]
                    prices['original_price'] = valid_prices[-1] if valid_prices[-1] > valid_prices[0] else None
        
        except Exception as e:
            print(f"❌ Error getting price from {product_url}: {e}")
        
        return prices

    def extract_detailed_specs(self, product: Dict) -> Dict:
        """Extract detailed specifications with improved storage detection."""
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
            
            # IMPROVED STORAGE EXTRACTION
            # Look for storage patterns in multiple ways
            storage_patterns = [
                # Standard patterns
                r'(\d+(?:GB|TB))\s+SSD',
                r'(\d+(?:GB|TB))\s+storage',
                r'Storage[:\s]+(\d+(?:GB|TB))',
                r'(\d+(?:GB|TB))\s+internal storage',
                r'(\d+(?:GB|TB))\s+of storage',
                # More specific patterns
                r'with\s+(\d+(?:GB|TB))\s+SSD',
                r'includes\s+(\d+(?:GB|TB))',
                r'featuring\s+(\d+(?:GB|TB))',
                # From product name
                r'(\d+(?:GB|TB))\s*-\s*',
                r'-\s*(\d+(?:GB|TB))',
                # Technical specs patterns
                r'Capacity[:\s]*(\d+(?:GB|TB))',
                r'Flash Storage[:\s]*(\d+(?:GB|TB))'
            ]
            
            combined_text = f"{product_name} {page_text}"
            
            for pattern in storage_patterns:
                storage_match = re.search(pattern, combined_text, re.IGNORECASE)
                if storage_match:
                    storage_candidate = storage_match.group(1)
                    # Validate it's a reasonable storage size
                    storage_num = int(re.search(r'\d+', storage_candidate).group())
                    storage_unit = re.search(r'[GT]B', storage_candidate).group()
                    
                    # Only accept reasonable storage sizes
                    if (storage_unit == 'GB' and storage_num >= 256) or (storage_unit == 'TB' and storage_num <= 8):
                        specs['storage'] = storage_candidate
                        break
            
            # IMPROVED MEMORY EXTRACTION
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
                    # Only accept reasonable memory sizes (8GB-128GB)
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
                r'[\-\s](Silver)',
                r'[\-\s](Gold)',
                r'[\-\s](Rose Gold)',
                r'[\-\s](Midnight)',
                r'[\-\s](Starlight)',
                r'[\-\s](Blue)',
                r'[\-\s](Green)',
                r'[\-\s](Pink)',
                r'[\-\s](Purple)',
                r'[\-\s](Yellow)',
                r'[\-\s](Orange)',
                r'[\-\s](Red)',
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

    def scrape_all_mac_products(self) -> List[Dict]:
        """Main method to scrape all Mac products with accurate pricing and specs."""
        print("🍎 Apple Mac Scraper V5 - Fixed Pricing & Storage")
        print("=" * 60)
        
        # Discover all pages
        page_urls = self.discover_all_pages()
        
        all_products = []
        seen_urls = set()
        
        # Scrape each page
        for page_num, page_url in enumerate(page_urls, 1):
            print(f"\n📄 SCRAPING PAGE {page_num}/{len(page_urls)}")
            print(f"🔗 URL: {page_url}")
            
            soup = self.get_page(page_url)
            if not soup:
                continue
            
            # Extract basic product info with accurate pricing
            page_products = self.extract_basic_product_info(soup)
            
            # Filter out duplicates
            new_products = []
            for product in page_products:
                if product['url'] not in seen_urls:
                    seen_urls.add(product['url'])
                    new_products.append(product)
            
            print(f"📦 Found {len(new_products)} new products on this page")
            all_products.extend(new_products)
            
            # Be respectful between pages
            time.sleep(2)
        
        print(f"\n🎯 TOTAL PRODUCTS FOUND: {len(all_products)}")
        
        # Now get detailed specs for each product (they already have accurate prices)
        print(f"\n🔍 EXTRACTING DETAILED SPECS...")
        print("=" * 40)
        
        detailed_products = []
        for i, product in enumerate(all_products, 1):
            print(f"\n📱 [{i}/{len(all_products)}] Processing: {product.get('name', 'Unknown')[:50]}...")
            
            # Extract detailed specs (pricing already done)
            detailed_product = self.extract_detailed_specs(product)
            detailed_products.append(detailed_product)
            
            # Be respectful between product pages
            time.sleep(0.5)
        
        return detailed_products

    def save_to_csv(self, products: List[Dict], filename: str = "mac_products_v5_fixed.csv"):
        """Save products to CSV file."""
        if not products:
            print("❌ No products to save")
            return None
        
        df = pd.DataFrame(products)
        
        # Reorder columns for better readability
        preferred_order = [
            'name', 'chip', 'cpu_cores', 'gpu_cores', 'memory', 'storage', 
            'display_size', 'color', 'current_price', 'original_price', 
            'savings', 'discount_percentage', 'connectivity', 'url', 
            'model_sku', 'scraped_at'
        ]
        
        # Only include columns that exist
        existing_columns = [col for col in preferred_order if col in df.columns]
        remaining_columns = [col for col in df.columns if col not in existing_columns]
        final_columns = existing_columns + remaining_columns
        
        df = df[final_columns]
        df.to_csv(filename, index=False)
        print(f"💾 Saved {len(products)} products to {filename}")
        return filename

    def upload_to_google_sheets(self, products: List[Dict]) -> bool:
        """Upload products to Google Sheets."""
        if not self.google_client:
            print("❌ Google Sheets not available")
            return False
        
        if not products:
            print("❌ No products to upload")
            return False
        
        try:
            print(f"📊 Uploading {len(products)} products to Google Sheets...")
            
            sheet = self.google_client.open(GOOGLE_SHEET_NAME).sheet1
            print(f"✅ Opened existing sheet: {GOOGLE_SHEET_NAME}")
            
            # Clear existing data
            sheet.clear()
            
            # Prepare data for upload
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
                sheet.update(data, value_input_option='USER_ENTERED')
                
                # Format the header row
                sheet.format('A1:Z1', {
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
                            sheet.format(f'{col}2:{col}1000', {
                                'numberFormat': {'type': 'CURRENCY', 'pattern': '£#,##0.00'}
                            })
                except Exception as e:
                    print(f"⚠️ Could not format currency columns: {e}")
                
                print(f"✅ Successfully uploaded to Google Sheets!")
                print(f"🔗 View your sheet: https://docs.google.com/spreadsheets/d/{sheet.spreadsheet.id}")
                return True
                
        except Exception as e:
            print(f"❌ Failed to upload to Google Sheets: {e}")
            return False

    def print_summary(self, products: List[Dict]):
        """Print a comprehensive summary of scraped products."""
        if not products:
            print("\n❌ No products found")
            return
        
        print(f"\n🎉 SCRAPING COMPLETE!")
        print(f"=" * 60)
        print(f"📊 Total products found: {len(products)}")
        
        # Calculate totals
        products_with_prices = [p for p in products if p.get('current_price')]
        total_value = sum(p.get('current_price', 0) for p in products_with_prices)
        total_savings = sum(p.get('savings', 0) for p in products if p.get('savings'))
        
        products_with_savings = [p for p in products if p.get('savings')]
        avg_discount = sum(p.get('discount_percentage', 0) for p in products_with_savings) / len(products_with_savings) if products_with_savings else 0
        
        print(f"💰 Products with pricing: {len(products_with_prices)}/{len(products)}")
        print(f"💰 Total catalog value: £{total_value:,.2f}")
        print(f"💸 Total potential savings: £{total_savings:,.2f}")
        print(f"📈 Average discount: {avg_discount:.1f}%")
        
        # Specs coverage
        print(f"\n🔧 Specs coverage:")
        specs_coverage = {
            'memory': len([p for p in products if p.get('memory')]),
            'storage': len([p for p in products if p.get('storage')]),
            'chip': len([p for p in products if p.get('chip')]),
            'color': len([p for p in products if p.get('color')])
        }
        
        for spec, count in specs_coverage.items():
            percentage = (count / len(products)) * 100
            print(f"   {spec.title()}: {count}/{len(products)} ({percentage:.1f}%)")
        
        # Best deals
        if products_with_savings:
            best_saving = max(products_with_savings, key=lambda x: x.get('savings', 0))
            print(f"\n🏆 BEST DEAL:")
            print(f"💰 {best_saving.get('name', 'Unknown')[:60]}...")
            print(f"    £{best_saving.get('current_price', 0)} (save £{best_saving.get('savings', 0)})")


def main():
    """Main function to run the scraper."""
    print("🍎 Apple Mac Refurbished Scraper V5")
    print("🔧 Fixed Pricing & Storage Extraction")
    print("=" * 50)
    
    scraper = AppleMacScraperV5()
    
    # Check Google Sheets setup
    if scraper.google_client:
        print(f"📊 Google Sheets: ✅ Ready")
    else:
        print(f"📊 Google Sheets: ❌ Not available")
    
    print(f"\n⏰ Started at: {datetime.now().strftime('%Y-%m-%d %H:%M:%S')}")
    
    # Scrape all Mac products with fixed pricing and specs
    products = scraper.scrape_all_mac_products()
    
    # Print comprehensive summary
    scraper.print_summary(products)
    
    # Save results
    if products:
        print(f"\n💾 Saving data...")
        csv_file = scraper.save_to_csv(products)
        
        # Upload to Google Sheets
        if scraper.google_client:
            scraper.upload_to_google_sheets(products)
        else:
            print("💡 Enable Google Sheets to automatically sync data")
        
        print(f"\n✅ All done! Found {len(products)} products with accurate pricing and specs")
        if csv_file:
            print(f"📄 Local file: {csv_file}")
        if scraper.google_client:
            print(f"📊 Google Sheet: {GOOGLE_SHEET_NAME}")
    
    print(f"\n⏰ Finished at: {datetime.now().strftime('%Y-%m-%d %H:%M:%S')}")
    return products


if __name__ == "__main__":
    products = main()