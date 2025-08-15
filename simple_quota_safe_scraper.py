#!/usr/bin/env python3
"""
Simple Quota-Safe Apple Scraper
Creates a fresh Google Sheet to avoid legacy quota issues.
"""

import requests
import time
import random
from datetime import datetime
from bs4 import BeautifulSoup

try:
    import gspread
    from oauth2client.service_account import ServiceAccountCredentials
    GOOGLE_SHEETS_AVAILABLE = True
except ImportError:
    GOOGLE_SHEETS_AVAILABLE = False
    print("❌ Google Sheets libraries not installed!")
    exit(1)

# CONFIGURATION
CREDENTIALS_FILE = "credentials.json"

def sheets_api_call_with_backoff(func, *args, max_retries=5, **kwargs):
    """Execute Google Sheets API call with exponential backoff for quota errors."""
    for attempt in range(max_retries):
        try:
            return func(*args, **kwargs)
        except Exception as e:
            error_str = str(e).lower()
            if '429' in error_str or 'quota exceeded' in error_str or 'too many requests' in error_str:
                if attempt < max_retries - 1:
                    wait_time = (2 ** attempt) + random.uniform(0, 1)
                    print(f"⚠️  Quota exceeded, waiting {wait_time:.1f}s before retry {attempt + 1}/{max_retries}")
                    time.sleep(wait_time)
                    continue
                else:
                    print(f"❌ Max retries exceeded for quota error: {e}")
                    raise
            else:
                print(f"❌ API error (non-quota): {e}")
                raise
    return None

def create_fresh_sheet():
    """Create a brand new Google Sheet for testing."""
    try:
        scope = ['https://spreadsheets.google.com/feeds',
                'https://www.googleapis.com/auth/drive']
        
        creds = ServiceAccountCredentials.from_json_keyfile_name(CREDENTIALS_FILE, scope)
        client = gspread.authorize(creds)
        
        # Create new spreadsheet
        sheet_name = f"Apple Scraper Test {datetime.now().strftime('%Y-%m-%d %H-%M-%S')}"
        spreadsheet = sheets_api_call_with_backoff(client.create, sheet_name)
        
        print(f"✅ Created fresh sheet: {sheet_name}")
        print(f"📋 Sheet ID: {spreadsheet.id}")
        print(f"🔗 URL: https://docs.google.com/spreadsheets/d/{spreadsheet.id}")
        
        return spreadsheet
        
    except Exception as e:
        print(f"❌ Failed to create fresh sheet: {e}")
        return None

def simple_scrape_test():
    """Scrape just a few products for testing."""
    print("🍎 Simple Apple Scraper Test...")
    
    session = requests.Session()
    session.headers.update({
        'User-Agent': 'Mozilla/5.0 (Macintosh; Intel Mac OS X 10_15_7) AppleWebKit/537.36'
    })
    
    try:
        # Get just the first page
        url = "https://www.apple.com/uk/shop/refurbished/mac"
        response = session.get(url, timeout=30)
        response.raise_for_status()
        
        soup = BeautifulSoup(response.content, 'html.parser')
        
        # Find product tiles (simplified extraction)
        products = []
        tiles = soup.find_all('div', class_='rf-ccard-content')[:5]  # Just first 5 products
        
        for tile in tiles:
            try:
                name_elem = tile.find('h3')
                price_elem = tile.find('span', string=lambda x: x and '£' in x)
                
                if name_elem and price_elem:
                    name = name_elem.get_text(strip=True)
                    price_text = price_elem.get_text(strip=True)
                    
                    products.append({
                        'name': name,
                        'price': price_text,
                        'scraped_at': datetime.now().strftime('%Y-%m-%d %H:%M:%S')
                    })
                    
            except Exception as e:
                print(f"⚠️  Error parsing product: {e}")
                continue
        
        print(f"✅ Scraped {len(products)} products")
        return products
        
    except Exception as e:
        print(f"❌ Scraping failed: {e}")
        return []

def upload_to_fresh_sheet(products):
    """Upload data to a fresh Google Sheet with minimal API calls."""
    if not products:
        print("❌ No products to upload")
        return False
    
    print("📤 Creating fresh sheet and uploading data...")
    
    # Create fresh sheet
    spreadsheet = create_fresh_sheet()
    if not spreadsheet:
        return False
    
    try:
        # Get the default worksheet
        worksheet = spreadsheet.sheet1
        
        # Prepare data
        headers = ['Name', 'Price', 'Scraped At']
        data = [headers]
        
        for product in products:
            row = [
                product.get('name', ''),
                product.get('price', ''),
                product.get('scraped_at', '')
            ]
            data.append(row)
        
        # Single batch upload (1 API call)
        sheets_api_call_with_backoff(
            worksheet.update,
            data,
            value_input_option='USER_ENTERED'
        )
        
        # Simple header formatting (1 API call)
        sheets_api_call_with_backoff(
            worksheet.format,
            'A1:C1',
            {
                'backgroundColor': {'red': 0.2, 'green': 0.2, 'blue': 0.2},
                'textFormat': {'foregroundColor': {'red': 1, 'green': 1, 'blue': 1}, 'bold': True}
            }
        )
        
        print(f"✅ Successfully uploaded {len(products)} products")
        print(f"📊 Total API calls: ~3 (create + update + format)")
        print(f"🔗 View sheet: https://docs.google.com/spreadsheets/d/{spreadsheet.id}")
        
        return True
        
    except Exception as e:
        print(f"❌ Upload failed: {e}")
        return False

if __name__ == "__main__":
    print("🧪 Quota-Safe Apple Scraper Test")
    print("=" * 50)
    
    # Test 1: Simple scraping
    products = simple_scrape_test()
    
    if products:
        print(f"\n📋 Found products:")
        for i, product in enumerate(products, 1):
            print(f"   {i}. {product['name'][:50]}... - {product['price']}")
        
        # Test 2: Upload to fresh sheet
        success = upload_to_fresh_sheet(products)
        
        if success:
            print(f"\n🎉 SUCCESS! Fresh sheet approach works.")
            print(f"💡 This suggests the issue might be:")
            print(f"   1. Quota accumulation on your existing sheet")
            print(f"   2. Too many operations in the main scraper")
            print(f"   3. Multiple concurrent sheet access")
        else:
            print(f"\n❌ FAILED! Issue is with quota management or permissions.")
    else:
        print(f"\n❌ No products scraped - check internet connection or Apple site changes")
