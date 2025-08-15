#!/usr/bin/env python3
"""
Apple UK Refurbished Store Scraper with Google Sheets Integration

This scraper extracts product information from Apple's UK refurbished store
and automatically uploads the data to Google Sheets.

SETUP INSTRUCTIONS:
1. Install Python from python.org
2. Install required packages:
   pip install requests beautifulsoup4 lxml pandas gspread oauth2client
3. Set up Google Cloud credentials (see README)
4. Put your credentials.json file in the same folder as this script
5. Create a Google Sheet and share it with your service account email
6. Update the GOOGLE_SHEET_NAME variable below

Dependencies:
    pip install requests beautifulsoup4 lxml pandas gspread oauth2client
"""

import requests
import re
import json
import time
import csv
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
    print("Google Sheets libraries not installed. Install with: pip install gspread oauth2client")

# CONFIGURATION - UPDATE THESE VALUES
GOOGLE_SHEET_NAME = "Apple Refurb Products"  # Change this to your Google Sheet name
CREDENTIALS_FILE = "credentials.json"  # Make sure this file is in the same folder


class AppleRefurbScraper:
    def __init__(self):
        self.base_url = "https://www.apple.com"
        self.uk_refurb_base = "https://www.apple.com/uk/shop/refurbished"
        self.session = requests.Session()
        self.session.headers.update({
            'User-Agent': 'Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/120.0.0.0 Safari/537.36',
            'Accept': 'text/html,application/xhtml+xml,application/xml;q=0.9,image/webp,*/*;q=0.8',
            'Accept-Language': 'en-GB,en;q=0.5',
            'Accept-Encoding': 'gzip, deflate, br',
            'Connection': 'keep-alive',
            'Upgrade-Insecure-Requests': '1',
        })
        
        # Category mappings
        self.categories = {
            'mac': 'https://www.apple.com/uk/shop/refurbished/mac',
            'ipad': 'https://www.apple.com/uk/shop/refurbished/ipad',
            'iphone': 'https://www.apple.com/uk/shop/refurbished/iphone',
            'watch': 'https://www.apple.com/uk/shop/refurbished/watch',
            'airpods': 'https://www.apple.com/uk/shop/refurbished/airpods',
            'appletv': 'https://www.apple.com/uk/shop/refurbished/appletv',
            'homepod': 'https://www.apple.com/uk/shop/refurbished/homepod',
            'accessories': 'https://www.apple.com/uk/shop/refurbished/accessories'
        }
        
        # Initialize Google Sheets client
        self.google_client = None
        if GOOGLE_SHEETS_AVAILABLE:
            self.setup_google_sheets()

    def setup_google_sheets(self):
        """Set up Google Sheets connection."""
        try:
            if not os.path.exists(CREDENTIALS_FILE):
                print(f"❌ Credentials file '{CREDENTIALS_FILE}' not found!")
                print("Please follow the setup instructions to get your credentials.json file")
                return False
            
            # Define the scope
            scope = ['https://spreadsheets.google.com/feeds',
                    'https://www.googleapis.com/auth/drive']
            
            # Add credentials to the account
            creds = ServiceAccountCredentials.from_json_keyfile_name(CREDENTIALS_FILE, scope)
            
            # Authorize the clientsheet
            self.google_client = gspread.authorize(creds)
            print("✅ Google Sheets connection established")
            return True
            
        except Exception as e:
            print(f"❌ Failed to set up Google Sheets: {e}")
            print("Don't worry - the scraper will still work and save local files")
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
                print(f"⚠️  Attempt {attempt + 1} failed for {url}: {e}")
                if attempt < retries - 1:
                    time.sleep(2 ** attempt)  # Exponential backoff
                else:
                    print(f"❌ Failed to fetch {url} after {retries} attempts")
                    return None

    def extract_price(self, price_text: str) -> Optional[float]:
        """Extract price from text like '£1,609.00' or 'Now£1,609.00Was£1,899.00'."""
        if not price_text:
            return None
        
        # Find price patterns
        price_matches = re.findall(r'£([\d,]+\.?\d*)', price_text)
        if price_matches:
            # Take the first price (current price)
            return float(price_matches[0].replace(',', ''))
        return None

    def extract_savings(self, text: str) -> Optional[float]:
        """Extract savings amount from text like 'Save £290.00'."""
        if not text:
            return None
            
        savings_match = re.search(r'Save £([\d,]+\.?\d*)', text)
        if savings_match:
            return float(savings_match.group(1).replace(',', ''))
        return None

    def extract_original_price(self, text: str) -> Optional[float]:
        """Extract original price from text like 'Was £1,899.00'."""
        if not text:
            return None
            
        was_match = re.search(r'Was £([\d,]+\.?\d*)', text)
        if was_match:
            return float(was_match.group(1).replace(',', ''))
        return None

    def extract_product_details(self, soup: BeautifulSoup, category: str) -> List[Dict]:
        """Extract product details from a category page."""
        products = []
        
        # Find all product links - they typically contain '/uk/shop/product/'
        product_links = soup.find_all('a', href=re.compile(r'/uk/shop/product/'))
        
        print(f"🔍 Found {len(product_links)} product links to process...")
        
        for i, link in enumerate(product_links):
            try:
                # Extract product information
                product_name = link.get_text(strip=True)
                product_url = urljoin(self.base_url, link.get('href', ''))
                
                # Skip empty product names or navigation links
                if not product_name or len(product_name) < 10:
                    continue
                
                # Extract prices and savings from the text
                link_text = link.get_text()
                current_price = self.extract_price(link_text)
                original_price = self.extract_original_price(link_text)
                savings = self.extract_savings(link_text)
                
                # Calculate savings if not explicitly stated
                if not savings and current_price and original_price:
                    savings = original_price - current_price
                
                # Extract model/SKU from URL
                url_parts = urlparse(product_url).path.split('/')
                model_sku = None
                if len(url_parts) > 4:
                    model_sku = url_parts[4]  # Usually the product code
                
                # Clean product name - remove price text
                clean_name = re.sub(r'(Now|Was)?£[\d,]+\.?\d*(Save £[\d,]+\.?\d*)?', '', product_name).strip()
                clean_name = re.sub(r'\s+', ' ', clean_name)  # Remove extra whitespace
                
                product = {
                    'name': clean_name,
                    'category': category,
                    'current_price': current_price,
                    'original_price': original_price,
                    'savings': savings,
                    'discount_percentage': round((savings / original_price * 100), 2) if savings and original_price else None,
                    'url': product_url,
                    'model_sku': model_sku,
                    'scraped_at': datetime.now().strftime("%Y-%m-%d %H:%M:%S")
                }
                
                # Only add products with valid pricing data
                if current_price:
                    products.append(product)
                    print(f"✅ Found: {clean_name[:50]}... - £{current_price}")
                    
            except Exception as e:
                print(f"⚠️  Error processing product link {i+1}: {e}")
                continue
        
        return products

    def scrape_category(self, category: str, url: str) -> List[Dict]:
        """Scrape all products from a specific category."""
        print(f"\n🛍️  Scraping {category.upper()} category...")
        print(f"📍 URL: {url}")
        
        soup = self.get_page(url)
        if not soup:
            return []
        
        products = self.extract_product_details(soup, category)
        print(f"✅ Found {len(products)} products in {category}")
        
        return products

    def scrape_all_categories(self, categories: Optional[List[str]] = None) -> List[Dict]:
        """Scrape all categories or specified categories."""
        all_products = []
        
        if categories is None:
            categories = list(self.categories.keys())
        
        print(f"🚀 Starting to scrape {len(categories)} categories...")
        
        for i, category in enumerate(categories, 1):
            if category not in self.categories:
                print(f"❌ Unknown category: {category}")
                continue
                
            print(f"\n📦 [{i}/{len(categories)}] Processing {category}...")
            url = self.categories[category]
            products = self.scrape_category(category, url)
            all_products.extend(products)
            
            # Be respectful to the server
            print(f"⏳ Waiting 2 seconds before next category...")
            time.sleep(2)
        
        return all_products

    def scrape_main_page(self) -> List[Dict]:
        """Scrape featured products from the main refurbished page."""
        print("🏠 Scraping main refurbished page...")
        
        soup = self.get_page(self.uk_refurb_base)
        if not soup:
            return []
        
        products = self.extract_product_details(soup, 'featured')
        print(f"✅ Found {len(products)} featured products")
        
        return products

    def save_to_csv(self, products: List[Dict], filename: str = None):
        """Save products to CSV file."""
        if not filename:
            timestamp = datetime.now().strftime("%Y%m%d_%H%M%S")
            filename = f"apple_refurb_products_{timestamp}.csv"
        
        if not products:
            print("❌ No products to save")
            return
        
        df = pd.DataFrame(products)
        df.to_csv(filename, index=False)
        print(f"💾 Saved {len(products)} products to {filename}")
        return filename

    def save_to_json(self, products: List[Dict], filename: str = None):
        """Save products to JSON file."""
        if not filename:
            timestamp = datetime.now().strftime("%Y%m%d_%H%M%S")
            filename = f"apple_refurb_products_{timestamp}.json"
        
        with open(filename, 'w', encoding='utf-8') as f:
            json.dump(products, f, indent=2, ensure_ascii=False)
        
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
                
                # Get the service account email to share instructions
                service_account_email = self.google_client.auth.service_account_email
                print(f"🔗 Please share your Google Sheet with: {service_account_email}")
                print("   Give it 'Editor' permissions")
            
            # Clear existing data
            sheet.clear()
            
            # Prepare data for upload
            if products:
                # Get headers from first product
                headers = list(products[0].keys())
                
                # Prepare data rows
                data = [headers]  # Start with headers
                for product in products:
                    row = [str(product.get(header, '')) for header in headers]
                    data.append(row)
                
                # Upload all data at once (more efficient)
                sheet.update(data, value_input_option='USER_ENTERED')
                
                # Format the header row
                sheet.format('A1:Z1', {
                    'backgroundColor': {'red': 0.2, 'green': 0.2, 'blue': 0.2},
                    'textFormat': {'foregroundColor': {'red': 1, 'green': 1, 'blue': 1}, 'bold': True}
                })
                
                print(f"✅ Successfully uploaded {len(products)} products to Google Sheets!")
                print(f"🔗 View your sheet: https://docs.google.com/spreadsheets/d/{sheet.spreadsheet.id}")
                return True
                
        except Exception as e:
            print(f"❌ Failed to upload to Google Sheets: {e}")
            return False

    def print_summary(self, products: List[Dict]):
        """Print a summary of scraped products."""
        if not products:
            print("❌ No products found")
            return
        
        print(f"\n🎉 SCRAPING COMPLETE!")
        print(f"=" * 50)
        print(f"📊 Total products: {len(products)}")
        
        # Group by category
        categories = {}
        total_savings = 0
        total_value = 0
        
        for product in products:
            category = product.get('category', 'Unknown')
            if category not in categories:
                categories[category] = []
            categories[category].append(product)
            
            if product.get('savings'):
                total_savings += product['savings']
            if product.get('current_price'):
                total_value += product['current_price']
        
        print(f"\n📦 Products by category:")
        for category, prods in categories.items():
            avg_price = sum(p.get('current_price', 0) for p in prods) / len(prods)
            print(f"   {category.title()}: {len(prods)} products (avg price: £{avg_price:.2f})")
        
        print(f"\n💰 Total catalog value: £{total_value:,.2f}")
        print(f"💸 Total potential savings: £{total_savings:,.2f}")
        
        # Find best deals
        if products:
            best_saving = max(products, key=lambda x: x.get('savings', 0))
            best_discount = max(products, key=lambda x: x.get('discount_percentage', 0))
            
            print(f"\n🏆 BEST DEALS:")
            print(f"💵 Biggest saving: {best_saving.get('name', 'Unknown')[:50]}...")
            print(f"    Save £{best_saving.get('savings', 0):.2f}")
            print(f"📈 Best percentage: {best_discount.get('name', 'Unknown')[:50]}...")
            print(f"    {best_discount.get('discount_percentage', 0):.1f}% off")


def main():
    """Main function to run the scraper."""
    print("🍎 Apple UK Refurbished Store Scraper")
    print("=" * 50)
    print("🚀 Starting scraper...")
    
    scraper = AppleRefurbScraper()
    
    # Check Google Sheets setup
    if scraper.google_client:
        print(f"📊 Google Sheets integration: ✅ Ready")
        print(f"📋 Target sheet: {GOOGLE_SHEET_NAME}")
    else:
        print(f"📊 Google Sheets integration: ❌ Not available")
        print(f"   Data will be saved locally instead")
    
    print(f"\n⏰ Started at: {datetime.now().strftime('%Y-%m-%d %H:%M:%S')}")
    
    # Scrape all categories
    print("\n🔄 Scraping all categories...")
    all_products = scraper.scrape_all_categories()
    
    # Alternative options (uncomment to use):
    # Scrape specific categories only:
    # all_products = scraper.scrape_all_categories(['mac', 'ipad'])
    
    # Scrape main page only:
    # all_products = scraper.scrape_main_page()
    
    # Print summary
    scraper.print_summary(all_products)
    
    # Save results
    if all_products:
        print(f"\n💾 Saving data...")
        
        # Save locally
        csv_file = scraper.save_to_csv(all_products)
        json_file = scraper.save_to_json(all_products)
        
        # Upload to Google Sheets
        if scraper.google_client:
            scraper.upload_to_google_sheets(all_products)
        
        print(f"\n✅ All done! Check your files:")
        print(f"   📄 CSV: {csv_file}")
        print(f"   📄 JSON: {json_file}")
        if scraper.google_client:
            print(f"   📊 Google Sheet: {GOOGLE_SHEET_NAME}")
    
    print(f"\n⏰ Finished at: {datetime.now().strftime('%Y-%m-%d %H:%M:%S')}")
    return all_products


if __name__ == "__main__":
    products = main()