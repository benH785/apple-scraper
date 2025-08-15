#!/usr/bin/env python3
"""
Apple Mac Refurbished Scraper V4 - Full Pagination & Detailed Specs

This version scrapes ALL pages of Mac products and extracts detailed specifications
from individual product pages including memory, storage, and other key specs.

SETUP INSTRUCTIONS:
1. Install required packages: pip3 install requests beautifulsoup4 lxml pandas gspread oauth2client
2. Put your credentials.json file in the same folder as this script
3. Enable Google Drive API and Google Sheets API in Google Cloud Console
4. Update the GOOGLE_SHEET_NAME variable below

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


class AppleMacScraperV4:
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
                print(f"📡 Fetching: {url}")
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
        # Apple might use different pagination patterns
        pagination_selectors = [
            'a[href*="mac"][href*="page"]',  # Links containing "mac" and "page"
            '.pagination a',                 # Standard pagination
            'a[href*="?page="]',            # Query parameter pagination
            'a[aria-label*="page"]',        # Accessible pagination
            'a[href*="fnode"]'              # Apple's specific pagination
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
        
        # If no pagination found, try to construct page URLs manually
        if len(page_urls) == 1:
            print("🔄 No pagination found, trying manual page construction...")
            # Try common pagination patterns
            for page_num in range(2, 7):  # Try pages 2-6
                test_urls = [
                    f"{self.mac_url}?page={page_num}",
                    f"{self.mac_url}/page/{page_num}",
                ]
                
                for test_url in test_urls:
                    test_soup = self.get_page(test_url)
                    if test_soup:
                        # Check if page has products
                        product_links = test_soup.find_all('a', href=re.compile(r'/uk/shop/product/'))
                        if len(product_links) > 10:  # If it has a reasonable number of products
                            page_urls.append(test_url)
                            break
                    time.sleep(1)  # Be respectful
        
        print(f"📄 Found {len(page_urls)} pages to scrape")
        for i, url in enumerate(page_urls, 1):
            print(f"   Page {i}: {url}")
        
        return page_urls

    def extract_basic_product_info(self, soup: BeautifulSoup) -> List[Dict]:
        """Extract basic product information from a Mac category page."""
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
                
                # Try to find pricing in nearby elements
                current_price = None
                original_price = None
                savings = None
                
                # Walk up the DOM tree to find pricing
                current_element = link.parent
                attempts = 0
                max_attempts = 5
                
                while current_element and attempts < max_attempts:
                    attempts += 1
                    element_text = current_element.get_text() if current_element else ""
                    
                    # Look for price patterns
                    prices = re.findall(r'£([\d,]+\.?\d*)', element_text)
                    
                    if len(prices) >= 2:
                        # Try to find specific patterns
                        now_match = re.search(r'Now\s*£([\d,]+\.?\d*)', element_text)
                        was_match = re.search(r'Was\s*£([\d,]+\.?\d*)', element_text)
                        save_match = re.search(r'Save\s*£([\d,]+\.?\d*)', element_text)
                        
                        if now_match and was_match:
                            current_price = float(now_match.group(1).replace(',', ''))
                            original_price = float(was_match.group(1).replace(',', ''))
                            if save_match:
                                savings = float(save_match.group(1).replace(',', ''))
                            break
                        elif len(prices) >= 2:
                            # Infer from first few prices
                            current_price = float(prices[0].replace(',', ''))
                            original_price = float(prices[1].replace(',', ''))
                            if len(prices) >= 3:
                                savings = float(prices[2].replace(',', ''))
                            break
                    
                    current_element = current_element.parent
                
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
                
            except Exception as e:
                print(f"❌ Error processing product {i+1}: {e}")
                continue
        
        return products

    def extract_detailed_specs(self, product: Dict) -> Dict:
        """Extract detailed specifications from an individual product page."""
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
                'product_info': None
            }
            
            # Look for Product Information section
            product_info_section = None
            
            # Try different selectors for product information
            info_selectors = [
                'section:has(h2:contains("Product Information"))',
                'div:has(h3:contains("Product Information"))',
                '[data-module="product-information"]',
                '.pd-product-information',
                'section[data-analytics-region*="product"]'
            ]
            
            for selector in info_selectors:
                try:
                    sections = soup.select(selector)
                    if sections:
                        product_info_section = sections[0]
                        break
                except:
                    continue
            
            # If we found a product info section, extract specs from it
            if product_info_section:
                info_text = product_info_section.get_text()
                specs['product_info'] = info_text[:500]  # Store first 500 chars for reference
                
                # Extract memory information
                memory_patterns = [
                    r'(\d+GB)\s+unified memory',
                    r'(\d+GB)\s+memory',
                    r'Memory[:\s]+(\d+GB)',
                    r'(\d+GB)\s+RAM'
                ]
                
                for pattern in memory_patterns:
                    memory_match = re.search(pattern, info_text, re.IGNORECASE)
                    if memory_match:
                        specs['memory'] = memory_match.group(1)
                        break
                
                # Extract storage information
                storage_patterns = [
                    r'(\d+(?:GB|TB))\s+SSD',
                    r'(\d+(?:GB|TB))\s+storage',
                    r'Storage[:\s]+(\d+(?:GB|TB))',
                    r'(\d+(?:GB|TB))\s+internal storage'
                ]
                
                for pattern in storage_patterns:
                    storage_match = re.search(pattern, info_text, re.IGNORECASE)
                    if storage_match:
                        specs['storage'] = storage_match.group(1)
                        break
            
            # Also try to extract specs from the product name and page content
            page_text = soup.get_text()
            product_name = product.get('name', '')
            combined_text = f"{product_name} {page_text}"
            
            # Extract chip information from name
            chip_patterns = [
                r'Apple (M\d+(?:\s+Pro|\s+Max|\s+Ultra)?)',
                r'(M\d+(?:\s+Pro|\s+Max|\s+Ultra)?)\s+Chip',
                r'Apple (M\d+)'
            ]
            
            for pattern in chip_patterns:
                chip_match = re.search(pattern, combined_text, re.IGNORECASE)
                if chip_match:
                    specs['chip'] = chip_match.group(1)
                    break
            
            # Extract display size from name (for MacBooks and iMacs)
            display_patterns = [
                r'(\d+(?:\.\d+)?-inch)',
                r'(\d+)"'
            ]
            
            for pattern in display_patterns:
                display_match = re.search(pattern, product_name, re.IGNORECASE)
                if display_match:
                    specs['display_size'] = display_match.group(1)
                    break
            
            # Extract color from name
            color_patterns = [
                r'- (Space (?:Grey|Gray|Black))',
                r'- (Silver)',
                r'- (Gold)',
                r'- (Rose Gold)',
                r'- (Midnight)',
                r'- (Starlight)',
                r'- (Blue)',
                r'- (Green)',
                r'- (Pink)',
                r'- (Purple)',
                r'- (Yellow)',
                r'- (Orange)',
                r'- (Red)',
                r'- (Sky Blue)'
            ]
            
            for pattern in color_patterns:
                color_match = re.search(pattern, product_name, re.IGNORECASE)
                if color_match:
                    specs['color'] = color_match.group(1)
                    break
            
            # Extract connectivity info from name
            if 'Gigabit Ethernet' in combined_text:
                specs['connectivity'] = 'Gigabit Ethernet'
            elif '10Gb Ethernet' in combined_text:
                specs['connectivity'] = '10Gb Ethernet'
            elif 'Wi-Fi' in combined_text:
                specs['connectivity'] = 'Wi-Fi'
            
            # If we still don't have memory/storage, try more aggressive patterns
            if not specs['memory']:
                # Look for memory in various formats
                memory_fallback = re.search(r'(\d+)\s*GB(?:\s+unified)?(?:\s+memory)?', combined_text, re.IGNORECASE)
                if memory_fallback:
                    memory_gb = memory_fallback.group(1)
                    if int(memory_gb) >= 8 and int(memory_gb) <= 128:  # Reasonable memory range
                        specs['memory'] = f"{memory_gb}GB"
            
            if not specs['storage']:
                # Look for storage in various formats
                storage_fallback = re.search(r'(\d+(?:GB|TB))(?:\s+SSD)?', combined_text, re.IGNORECASE)
                if storage_fallback:
                    storage_size = storage_fallback.group(1)
                    # Check if it's a reasonable storage size
                    if ('TB' in storage_size) or (int(re.search(r'\d+', storage_size).group()) >= 256):
                        specs['storage'] = storage_size
            
            # Add specs to product
            product.update(specs)
            
            print(f"✅ Extracted specs: {specs['chip'] or 'N/A'} | {specs['memory'] or 'N/A'} | {specs['storage'] or 'N/A'}")
            
        except Exception as e:
            print(f"❌ Error extracting specs: {e}")
        
        return product

    def scrape_all_mac_products(self) -> List[Dict]:
        """Main method to scrape all Mac products from all pages with detailed specs."""
        print("🍎 Apple Mac Scraper V4 - Full Pagination & Detailed Specs")
        print("=" * 70)
        
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
            
            # Extract basic product info from this page
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
        
        print(f"\n🎯 TOTAL BASIC PRODUCTS FOUND: {len(all_products)}")
        
        # Now get detailed specs for each product
        print(f"\n🔍 EXTRACTING DETAILED SPECS FOR ALL PRODUCTS...")
        print("=" * 50)
        
        detailed_products = []
        for i, product in enumerate(all_products, 1):
            print(f"\n📱 [{i}/{len(all_products)}] Processing: {product.get('name', 'Unknown')[:50]}...")
            
            # Extract detailed specs
            detailed_product = self.extract_detailed_specs(product)
            detailed_products.append(detailed_product)
            
            # Be respectful between product pages
            time.sleep(1)
        
        return detailed_products

    def save_to_csv(self, products: List[Dict], filename: str = "mac_products_v4_full.csv"):
        """Save products to CSV file."""
        if not products:
            print("❌ No products to save")
            return None
        
        df = pd.DataFrame(products)
        
        # Reorder columns for better readability
        preferred_order = [
            'name', 'chip', 'memory', 'storage', 'display_size', 'color', 
            'current_price', 'original_price', 'savings', 'discount_percentage',
            'connectivity', 'url', 'model_sku', 'scraped_at'
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
            
            # Try to open existing sheet, create if it doesn't exist
            try:
                sheet = self.google_client.open(GOOGLE_SHEET_NAME).sheet1
                print(f"✅ Opened existing sheet: {GOOGLE_SHEET_NAME}")
            except gspread.SpreadsheetNotFound:
                print(f"📝 Creating new sheet: {GOOGLE_SHEET_NAME}")
                sheet = self.google_client.create(GOOGLE_SHEET_NAME).sheet1
            
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
                    'name', 'chip', 'memory', 'storage', 'display_size', 'color',
                    'current_price', 'original_price', 'savings', 'discount_percentage',
                    'connectivity', 'url', 'model_sku', 'scraped_at'
                ]
                
                headers = [h for h in preferred_order if h in all_headers]
                headers.extend([h for h in all_headers if h not in headers])
                
                # Prepare data rows
                data = [headers]  # Start with headers
                
                for product in products:
                    row = []
                    for header in headers:
                        value = product.get(header, '')
                        # Convert to string and handle None values
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
                            price_columns.append(chr(65 + i))  # Convert to column letter
                    
                    for col in price_columns:
                        if ord(col) - 65 < 26:  # Only format first 26 columns
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
        total_value = sum(p.get('current_price', 0) for p in products if p.get('current_price'))
        total_savings = sum(p.get('savings', 0) for p in products if p.get('savings'))
        
        products_with_savings = [p for p in products if p.get('savings')]
        avg_discount = sum(p.get('discount_percentage', 0) for p in products_with_savings) / len(products_with_savings) if products_with_savings else 0
        
        print(f"💰 Total catalog value: £{total_value:,.2f}")
        print(f"💸 Total potential savings: £{total_savings:,.2f}")
        print(f"📈 Average discount: {avg_discount:.1f}%")
        
        # Product type breakdown
        print(f"\n📱 Product breakdown:")
        product_types = {}
        for product in products:
            name = product.get('name', 'Unknown')
            if 'MacBook Pro' in name:
                category = 'MacBook Pro'
            elif 'MacBook Air' in name:
                category = 'MacBook Air'
            elif 'iMac' in name:
                category = 'iMac'
            elif 'Mac mini' in name:
                category = 'Mac mini'
            elif 'Mac Studio' in name:
                category = 'Mac Studio'
            else:
                category = 'Other'
            
            if category not in product_types:
                product_types[category] = []
            product_types[category].append(product)
        
        for category, prods in product_types.items():
            avg_price = sum(p.get('current_price', 0) for p in prods) / len(prods) if prods else 0
            print(f"   {category}: {len(prods)} products (avg: £{avg_price:,.0f})")
        
        # Specs summary
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
            best_discount = max(products_with_savings, key=lambda x: x.get('discount_percentage', 0))
            
            print(f"\n🏆 BEST DEALS:")
            print(f"💰 Highest saving: {best_saving.get('name', 'Unknown')[:50]}...")
            print(f"    £{best_saving.get('current_price', 0)} (save £{best_saving.get('savings', 0)})")
            print(f"📈 Best percentage: {best_discount.get('name', 'Unknown')[:50]}...")
            print(f"    {best_discount.get('discount_percentage', 0):.1f}% off")


def main():
    """Main function to run the scraper."""
    print("🍎 Apple Mac Refurbished Scraper V4")
    print("🚀 Full Pagination & Detailed Specs")
    print("=" * 50)
    
    scraper = AppleMacScraperV4()
    
    # Check Google Sheets setup
    if scraper.google_client:
        print(f"📊 Google Sheets: ✅ Ready")
    else:
        print(f"📊 Google Sheets: ❌ Not available")
    
    print(f"\n⏰ Started at: {datetime.now().strftime('%Y-%m-%d %H:%M:%S')}")
    
    # Scrape all Mac products with detailed specs
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
        
        print(f"\n✅ All done! Found {len(products)} products with detailed specs")
        if csv_file:
            print(f"📄 Local file: {csv_file}")
        if scraper.google_client:
            print(f"📊 Google Sheet: {GOOGLE_SHEET_NAME}")
    
    print(f"\n⏰ Finished at: {datetime.now().strftime('%Y-%m-%d %H:%M:%S')}")
    return products


if __name__ == "__main__":
    products = main()