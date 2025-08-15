#!/usr/bin/env python3
"""
Apple Mac Refurbished Scraper V2 - Fixed Price Extraction

This version looks for prices in parent elements and surrounding content,
as Apple stores product info and prices separately.

SETUP INSTRUCTIONS:
1. Install required packages: pip3 install requests beautifulsoup4 lxml pandas gspread oauth2client
2. Put your credentials.json file in the same folder as this script
3. Update the GOOGLE_SHEET_NAME variable below

Dependencies:
    pip3 install requests beautifulsoup4 lxml pandas gspread oauth2client
"""

import requests
import re
import json
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
except ImportError:
    GOOGLE_SHEETS_AVAILABLE = False
    print("Google Sheets libraries not installed. Install with: pip3 install gspread oauth2client")

# CONFIGURATION - UPDATE THESE VALUES
GOOGLE_SHEET_NAME = "Apple Mac Products V2"  # Change this to your Google Sheet name
CREDENTIALS_FILE = "credentials.json"  # Make sure this file is in the same folder


class AppleMacScraperV2:
    def __init__(self):
        self.base_url = "https://www.apple.com"
        self.mac_url = "https://www.apple.com/uk/shop/refurbished/mac"
        self.session = requests.Session()
        self.session.headers.update({
            'User-Agent': 'Mozilla/5.0 (Macintosh; Intel Mac OS X 10_15_7) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/120.0.0.0 Safari/537.36',
            'Accept': 'text/html,application/xhtml+xml,application/xml;q=0.9,image/webp,*/*;q=0.8',
            'Accept-Language': 'en-GB,en-US;q=0.9,en;q=0.8',
            'Accept-Encoding': 'gzip, deflate, br',
            'Connection': 'keep-alive',
            'Upgrade-Insecure-Requests': '1',
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

    def get_page(self, url: str) -> Optional[BeautifulSoup]:
        """Fetch and parse a webpage."""
        try:
            print(f"📡 Fetching: {url}")
            response = self.session.get(url, timeout=30)
            response.raise_for_status()
            return BeautifulSoup(response.content, 'lxml')
        except requests.RequestException as e:
            print(f"❌ Failed to fetch {url}: {e}")
            return None

    def find_price_elements(self, soup: BeautifulSoup) -> List:
        """Find all elements that contain prices."""
        price_patterns = [
            r'£[\d,]+\.?\d*',  # £1,234.56 or £1234
            r'\$[\d,]+\.?\d*'  # Just in case there are dollar prices
        ]
        
        price_elements = []
        
        # Search for price patterns in all text
        for element in soup.find_all(text=True):
            text = element.strip()
            if text:
                for pattern in price_patterns:
                    if re.search(pattern, text):
                        price_elements.append({
                            'element': element.parent if hasattr(element, 'parent') else element,
                            'text': text,
                            'prices': re.findall(pattern, text)
                        })
        
        return price_elements

    def extract_clean_price(self, price_text: str) -> Optional[float]:
        """Extract and clean price from text."""
        if not price_text:
            return None
        
        # Remove currency symbol and convert to float
        price_match = re.search(r'£([\d,]+\.?\d*)', price_text)
        if price_match:
            return float(price_match.group(1).replace(',', ''))
        return None

    def find_product_containers(self, soup: BeautifulSoup) -> List:
        """Find the main containers that hold product information."""
        print("🔍 Looking for product containers...")
        
        # Try various container selectors
        container_selectors = [
            'div:has(a[href*="/uk/shop/product/"])',  # Divs containing product links
            'li:has(a[href*="/uk/shop/product/"])',   # List items containing product links
            'section:has(a[href*="/uk/shop/product/"])', # Sections containing product links
            '.rf-psp-column',
            '.tiles-item',
            '.product-tile'
        ]
        
        containers = []
        
        for selector in container_selectors:
            try:
                elements = soup.select(selector)
                if elements:
                    print(f"Found {len(elements)} containers with selector: {selector}")
                    containers.extend(elements)
                    break  # Use the first selector that finds containers
            except Exception as e:
                print(f"Error with selector {selector}: {e}")
                continue
        
        # If no containers found, find parent elements of product links
        if not containers:
            print("🔄 Falling back to finding parent elements of product links...")
            product_links = soup.find_all('a', href=re.compile(r'/uk/shop/product/'))
            for link in product_links:
                parent = link.parent
                if parent and parent not in containers:
                    containers.append(parent)
        
        print(f"📦 Found {len(containers)} potential product containers")
        return containers

    def extract_product_from_container(self, container, index: int) -> Optional[Dict]:
        """Extract product information from a container element."""
        try:
            print(f"\n--- Processing Container {index + 1} ---")
            
            # Find product link
            product_link = container.find('a', href=re.compile(r'/uk/shop/product/'))
            if not product_link:
                print("❌ No product link found in container")
                return None
            
            product_name = product_link.get_text(strip=True)
            product_url = urljoin(self.base_url, product_link.get('href', ''))
            
            print(f"📱 Product: {product_name[:60]}...")
            print(f"🔗 URL: {product_url}")
            
            # Look for prices in the entire container
            container_text = container.get_text()
            print(f"📄 Container text sample: {container_text[:200]}...")
            
            # Extract prices from container text
            prices = re.findall(r'£([\d,]+\.?\d*)', container_text)
            print(f"💰 Found prices in container: {prices}")
            
            # Look for specific price indicators
            current_price = None
            original_price = None
            savings = None
            
            # Try to find "Now" price (current price)
            now_match = re.search(r'Now\s*£([\d,]+\.?\d*)', container_text)
            if now_match:
                current_price = float(now_match.group(1).replace(',', ''))
                print(f"✅ Found current price: £{current_price}")
            
            # Try to find "Was" price (original price)
            was_match = re.search(r'Was\s*£([\d,]+\.?\d*)', container_text)
            if was_match:
                original_price = float(was_match.group(1).replace(',', ''))
                print(f"✅ Found original price: £{original_price}")
            
            # Try to find "Save" amount
            save_match = re.search(r'Save\s*£([\d,]+\.?\d*)', container_text)
            if save_match:
                savings = float(save_match.group(1).replace(',', ''))
                print(f"✅ Found savings: £{savings}")
            
            # If we didn't find Now/Was format, try to parse prices in order
            if not current_price and prices:
                if len(prices) >= 2:
                    # Assume first price is current, second is original
                    current_price = float(prices[0].replace(',', ''))
                    original_price = float(prices[1].replace(',', ''))
                    print(f"📊 Inferred current: £{current_price}, original: £{original_price}")
                elif len(prices) == 1:
                    current_price = float(prices[0].replace(',', ''))
                    print(f"📊 Found single price: £{current_price}")
            
            # Calculate savings if not found directly
            if not savings and current_price and original_price:
                savings = original_price - current_price
            
            # Only return product if we found at least a current price
            if current_price:
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
                
                print(f"✅ Successfully extracted product!")
                return product
            else:
                print(f"❌ No price found for this product")
                return None
                
        except Exception as e:
            print(f"❌ Error processing container {index + 1}: {e}")
            return None

    def scrape_mac_products(self) -> List[Dict]:
        """Main scraping method for Mac products."""
        print("🍎 Apple Mac Scraper V2 - Enhanced Price Detection")
        print("=" * 60)
        
        # Get the Mac refurbished page
        soup = self.get_page(self.mac_url)
        if not soup:
            return []
        
        # Find product containers
        containers = self.find_product_containers(soup)
        
        if not containers:
            print("❌ No product containers found")
            return []
        
        print(f"🔄 Processing {len(containers)} containers...")
        
        products = []
        
        # Process each container (limit to first 10 for testing)
        for i, container in enumerate(containers[:10]):
            product = self.extract_product_from_container(container, i)
            if product:
                products.append(product)
                print(f"📦 Added product: {product['name'][:50]}... - £{product['current_price']}")
            
            # Small delay to be respectful
            time.sleep(0.1)
        
        return products

    def save_to_csv(self, products: List[Dict], filename: str = "mac_products_v2.csv"):
        """Save products to CSV file."""
        if not products:
            print("❌ No products to save")
            return None
        
        df = pd.DataFrame(products)
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
            headers = list(products[0].keys())
            data = [headers]  # Start with headers
            
            for product in products:
                row = [str(product.get(header, '')) for header in headers]
                data.append(row)
            
            # Upload all data
            sheet.update(data, value_input_option='USER_ENTERED')
            
            # Format the header row
            sheet.format('A1:Z1', {
                'backgroundColor': {'red': 0.2, 'green': 0.2, 'blue': 0.2},
                'textFormat': {'foregroundColor': {'red': 1, 'green': 1, 'blue': 1}, 'bold': True}
            })
            
            print(f"✅ Successfully uploaded to Google Sheets!")
            print(f"🔗 View your sheet: https://docs.google.com/spreadsheets/d/{sheet.spreadsheet.id}")
            return True
            
        except Exception as e:
            print(f"❌ Failed to upload to Google Sheets: {e}")
            return False

    def print_summary(self, products: List[Dict]):
        """Print a summary of scraped products."""
        if not products:
            print("\n❌ No products found with the enhanced method")
            print("\n🔧 Next steps:")
            print("1. Check if Apple's website has changed significantly")
            print("2. Try visiting the URL manually to see if products are visible")
            print("3. Apple might be using JavaScript to load prices dynamically")
            return
        
        print(f"\n🎉 SUCCESS! Found Mac products")
        print(f"=" * 40)
        print(f"📊 Total products: {len(products)}")
        
        if products:
            total_value = sum(p.get('current_price', 0) for p in products)
            total_savings = sum(p.get('savings', 0) for p in products if p.get('savings'))
            avg_discount = sum(p.get('discount_percentage', 0) for p in products if p.get('discount_percentage')) / len([p for p in products if p.get('discount_percentage')])
            
            print(f"💰 Total value: £{total_value:,.2f}")
            print(f"💸 Total savings: £{total_savings:,.2f}")
            print(f"📈 Average discount: {avg_discount:.1f}%")
            
            # Show all products found
            print(f"\n📱 Products found:")
            for i, product in enumerate(products, 1):
                name = product.get('name', 'Unknown')[:60]
                current = product.get('current_price', 0)
                original = product.get('original_price', 0)
                savings = product.get('savings', 0)
                
                if original and savings:
                    print(f"{i:2d}. {name}...")
                    print(f"    £{current} (was £{original}, save £{savings})")
                else:
                    print(f"{i:2d}. {name}...")
                    print(f"    £{current}")
                print()


def main():
    """Main function to run the scraper."""
    print("🍎 Apple Mac Refurbished Scraper V2")
    print("=" * 50)
    
    scraper = AppleMacScraperV2()
    
    # Check Google Sheets setup
    if scraper.google_client:
        print(f"📊 Google Sheets: ✅ Ready")
    else:
        print(f"📊 Google Sheets: ❌ Not available")
    
    print(f"\n⏰ Started at: {datetime.now().strftime('%Y-%m-%d %H:%M:%S')}")
    
    # Scrape Mac products
    products = scraper.scrape_mac_products()
    
    # Print summary
    scraper.print_summary(products)
    
    # Save results
    if products:
        print(f"\n💾 Saving data...")
        csv_file = scraper.save_to_csv(products)
        
        # Upload to Google Sheets
        if scraper.google_client:
            scraper.upload_to_google_sheets(products)
        
        print(f"\n✅ All done!")
        if csv_file:
            print(f"📄 Local file: {csv_file}")
        if scraper.google_client:
            print(f"📊 Google Sheet: {GOOGLE_SHEET_NAME}")
    
    print(f"\n⏰ Finished at: {datetime.now().strftime('%Y-%m-%d %H:%M:%S')}")
    return products


if __name__ == "__main__":
    products = main()