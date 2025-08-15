#!/usr/bin/env python3
"""
Apple Mac Refurbished Scraper V3 - Individual Product Extraction

This version properly extracts individual products by finding the specific
elements that contain each product's data, avoiding duplicates.

SETUP INSTRUCTIONS:
1. Install required packages: pip3 install requests beautifulsoup4 lxml pandas gspread oauth2client
2. Put your credentials.json file in the same folder as this script
3. Enable Google Drive API in Google Cloud Console
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
from urllib.parse import urljoin, urlparse
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
GOOGLE_SHEET_NAME = "Apple Mac Products V3"  # Change this to your Google Sheet name
CREDENTIALS_FILE = "credentials.json"  # Make sure this file is in the same folder


class AppleMacScraperV3:
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
            print("💡 Make sure you've enabled both Google Sheets API AND Google Drive API")
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

    def extract_individual_products(self, soup: BeautifulSoup) -> List[Dict]:
        """Extract individual products by finding specific product elements."""
        products = []
        seen_urls: Set[str] = set()  # Track URLs to avoid duplicates
        
        print("🔍 Looking for individual product elements...")
        
        # Find all product links first
        product_links = soup.find_all('a', href=re.compile(r'/uk/shop/product/'))
        print(f"📦 Found {len(product_links)} product links")
        
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
                
                print(f"\n--- Product {len(products) + 1} ---")
                print(f"📱 Name: {product_name[:60]}...")
                print(f"🔗 URL: {product_url}")
                
                # Find the closest parent element that might contain pricing
                current_element = link.parent
                prices_found = False
                attempts = 0
                max_attempts = 5
                
                current_price = None
                original_price = None
                savings = None
                
                # Walk up the DOM tree to find pricing information
                while current_element and attempts < max_attempts and not prices_found:
                    attempts += 1
                    element_text = current_element.get_text() if current_element else ""
                    
                    # Look for pricing patterns in this element
                    prices = re.findall(r'£([\d,]+\.?\d*)', element_text)
                    
                    if len(prices) >= 2:  # Need at least current and original price
                        print(f"🔍 Found prices in parent level {attempts}: {prices[:5]}...")  # Show first 5 prices
                        
                        # Look for "Now" and "Was" patterns
                        now_match = re.search(r'Now\s*£([\d,]+\.?\d*)', element_text)
                        was_match = re.search(r'Was\s*£([\d,]+\.?\d*)', element_text)
                        save_match = re.search(r'Save\s*£([\d,]+\.?\d*)', element_text)
                        
                        if now_match and was_match:
                            current_price = float(now_match.group(1).replace(',', ''))
                            original_price = float(was_match.group(1).replace(',', ''))
                            if save_match:
                                savings = float(save_match.group(1).replace(',', ''))
                            prices_found = True
                            print(f"✅ Found Now/Was format: £{current_price} was £{original_price}")
                        
                        # If no Now/Was, look for a pattern around this specific product
                        elif not prices_found:
                            # Find the position of the product name in the text
                            product_name_clean = re.sub(r'[^a-zA-Z0-9\s]', '', product_name)[:30]
                            name_position = element_text.find(product_name_clean)
                            
                            if name_position != -1:
                                # Look for prices near the product name
                                text_around_product = element_text[max(0, name_position-100):name_position+200]
                                prices_near_product = re.findall(r'£([\d,]+\.?\d*)', text_around_product)
                                
                                if len(prices_near_product) >= 2:
                                    current_price = float(prices_near_product[0].replace(',', ''))
                                    original_price = float(prices_near_product[1].replace(',', ''))
                                    if len(prices_near_product) >= 3:
                                        savings = float(prices_near_product[2].replace(',', ''))
                                    prices_found = True
                                    print(f"✅ Found prices near product: £{current_price} was £{original_price}")
                    
                    # Move to parent element
                    current_element = current_element.parent if current_element else None
                
                # If we still haven't found prices, try a different approach
                if not prices_found:
                    # Look for sibling elements or following elements that might contain prices
                    siblings = link.parent.find_next_siblings() if link.parent else []
                    for sibling in siblings[:3]:  # Check first 3 siblings
                        sibling_text = sibling.get_text()
                        prices = re.findall(r'£([\d,]+\.?\d*)', sibling_text)
                        if len(prices) >= 2:
                            current_price = float(prices[0].replace(',', ''))
                            original_price = float(prices[1].replace(',', ''))
                            if len(prices) >= 3:
                                savings = float(prices[2].replace(',', ''))
                            prices_found = True
                            print(f"✅ Found prices in sibling: £{current_price} was £{original_price}")
                            break
                
                # Calculate savings if not found
                if not savings and current_price and original_price:
                    savings = original_price - current_price
                
                # Create product record if we found pricing
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
                    
                    products.append(product)
                    print(f"✅ Successfully extracted: £{current_price} (was £{original_price}, save £{savings})")
                else:
                    print(f"❌ No pricing found for this product")
                    
            except Exception as e:
                print(f"❌ Error processing product {i+1}: {e}")
                continue
        
        # Remove duplicates based on URL
        unique_products = []
        seen_urls_final = set()
        
        for product in products:
            if product['url'] not in seen_urls_final:
                unique_products.append(product)
                seen_urls_final.add(product['url'])
        
        print(f"\n📊 Found {len(products)} total products, {len(unique_products)} unique products")
        return unique_products

    def scrape_mac_products(self) -> List[Dict]:
        """Main scraping method for Mac products."""
        print("🍎 Apple Mac Scraper V3 - Individual Product Extraction")
        print("=" * 65)
        
        # Get the Mac refurbished page
        soup = self.get_page(self.mac_url)
        if not soup:
            return []
        
        # Extract individual products
        products = self.extract_individual_products(soup)
        
        return products

    def save_to_csv(self, products: List[Dict], filename: str = "mac_products_v3.csv"):
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
                
                # Share with your email (optional - you can manually share it)
                print(f"💡 Don't forget to share the sheet with your personal Google account!")
            
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
            
            # Format price columns as currency
            try:
                # Find price columns
                price_columns = []
                for i, header in enumerate(headers):
                    if 'price' in header.lower() or 'savings' in header.lower():
                        price_columns.append(chr(65 + i))  # Convert to column letter
                
                for col in price_columns:
                    sheet.format(f'{col}2:{col}1000', {'numberFormat': {'type': 'CURRENCY', 'pattern': '£#,##0.00'}})
            except Exception as e:
                print(f"⚠️ Could not format currency columns: {e}")
            
            print(f"✅ Successfully uploaded to Google Sheets!")
            print(f"🔗 View your sheet: https://docs.google.com/spreadsheets/d/{sheet.spreadsheet.id}")
            return True
            
        except Exception as e:
            print(f"❌ Failed to upload to Google Sheets: {e}")
            if "API has not been used" in str(e) or "disabled" in str(e):
                print("💡 You need to enable the Google Drive API:")
                print("   1. Go to Google Cloud Console")
                print("   2. Enable Google Drive API")
                print("   3. Wait a few minutes and try again")
            return False

    def print_summary(self, products: List[Dict]):
        """Print a summary of scraped products."""
        if not products:
            print("\n❌ No products found")
            return
        
        print(f"\n🎉 SUCCESS! Found {len(products)} unique Mac products")
        print(f"=" * 50)
        
        if products:
            total_value = sum(p.get('current_price', 0) for p in products)
            total_savings = sum(p.get('savings', 0) for p in products if p.get('savings'))
            avg_discount = sum(p.get('discount_percentage', 0) for p in products if p.get('discount_percentage')) / len([p for p in products if p.get('discount_percentage')])
            
            print(f"💰 Total value: £{total_value:,.2f}")
            print(f"💸 Total savings available: £{total_savings:,.2f}")
            print(f"📈 Average discount: {avg_discount:.1f}%")
            
            # Show products with best deals
            products_with_savings = [p for p in products if p.get('savings')]
            if products_with_savings:
                best_deal = max(products_with_savings, key=lambda x: x.get('savings', 0))
                print(f"\n🏆 Best deal: {best_deal['name'][:50]}...")
                print(f"   Save £{best_deal['savings']:.2f} ({best_deal.get('discount_percentage', 0):.1f}% off)")
            
            # Show all products
            print(f"\n📱 All products found:")
            for i, product in enumerate(products, 1):
                name = product.get('name', 'Unknown')[:50]
                current = product.get('current_price', 0)
                original = product.get('original_price', 0)
                savings = product.get('savings', 0)
                
                print(f"{i:2d}. {name}...")
                if original and savings:
                    print(f"    £{current} (was £{original}, save £{savings})")
                else:
                    print(f"    £{current}")


def main():
    """Main function to run the scraper."""
    print("🍎 Apple Mac Refurbished Scraper V3")
    print("=" * 50)
    
    scraper = AppleMacScraperV3()
    
    # Check Google Sheets setup
    if scraper.google_client:
        print(f"📊 Google Sheets: ✅ Ready")
    else:
        print(f"📊 Google Sheets: ❌ Not available")
        print(f"💡 Enable Google Drive API to use Google Sheets integration")
    
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
        else:
            print("💡 Fix Google Sheets setup to automatically upload data")
        
        print(f"\n✅ All done!")
        if csv_file:
            print(f"📄 Local file: {csv_file}")
        if scraper.google_client:
            print(f"📊 Google Sheet: {GOOGLE_SHEET_NAME}")
    
    print(f"\n⏰ Finished at: {datetime.now().strftime('%Y-%m-%d %H:%M:%S')}")
    return products


if __name__ == "__main__":
    products = main()