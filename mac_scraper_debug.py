#!/usr/bin/env python3
"""
DEBUG VERSION - Minimal Apple Scraper to isolate issues
"""

print("🔥 STARTING DEBUG SCRIPT")

try:
    print("📦 Importing basic modules...")
    import requests
    import re
    import time
    import os
    from datetime import datetime
    from urllib.parse import urljoin, urlparse
    from bs4 import BeautifulSoup
    from typing import List, Dict, Optional
    print("✅ Basic imports successful")
except Exception as e:
    print(f"❌ Basic imports failed: {e}")
    exit(1)

try:
    print("📦 Importing pandas...")
    import pandas as pd
    print("✅ Pandas import successful")
except Exception as e:
    print(f"❌ Pandas import failed: {e}")

try:
    print("📦 Importing Google Sheets modules...")
    import gspread
    from oauth2client.service_account import ServiceAccountCredentials
    print("✅ Google Sheets imports successful")
    GOOGLE_SHEETS_AVAILABLE = True
except ImportError as e:
    print(f"⚠️ Google Sheets import failed: {e}")
    GOOGLE_SHEETS_AVAILABLE = False

# Configuration
GOOGLE_SHEET_ID = "1j7npqR9I303etrf67HYkXU6m87DEwg66j0dmqxpMllQ"
CREDENTIALS_FILE = "credentials.json"

print("🌐 Testing basic web request...")
try:
    response = requests.get("https://httpbin.org/get", timeout=10)
    print(f"✅ Web request successful: {response.status_code}")
except Exception as e:
    print(f"❌ Web request failed: {e}")

print("🍎 Testing Apple website access...")
try:
    response = requests.get("https://www.apple.com/uk/shop/refurbished/mac", timeout=30)
    print(f"✅ Apple website accessible: {response.status_code}")
    
    # Test BeautifulSoup parsing
    soup = BeautifulSoup(response.content, 'lxml')
    print(f"✅ HTML parsing successful, title: {soup.title.string if soup.title else 'No title'}")
    
    # Test product link detection
    product_links = soup.find_all('a', href=re.compile(r'/uk/shop/product/'))
    print(f"✅ Found {len(product_links)} potential product links")
    
except Exception as e:
    print(f"❌ Apple website test failed: {e}")

print("📊 Testing Google Sheets connection...")
if GOOGLE_SHEETS_AVAILABLE and os.path.exists(CREDENTIALS_FILE):
    try:
        scope = ['https://spreadsheets.google.com/feeds',
                'https://www.googleapis.com/auth/drive']
        creds = ServiceAccountCredentials.from_json_keyfile_name(CREDENTIALS_FILE, scope)
        client = gspread.authorize(creds)
        spreadsheet = client.open_by_key(GOOGLE_SHEET_ID)
        print("✅ Google Sheets connection successful")
    except Exception as e:
        print(f"❌ Google Sheets connection failed: {e}")
else:
    print("⚠️ Skipping Google Sheets test (missing dependencies or credentials)")

print("🎯 Testing minimal scraping...")
try:
    # Minimal scraping test
    url = "https://www.apple.com/uk/shop/refurbished/mac"
    session = requests.Session()
    session.headers.update({
        'User-Agent': 'Mozilla/5.0 (Macintosh; Intel Mac OS X 10_15_7) AppleWebKit/537.36'
    })
    
    response = session.get(url, timeout=30)
    soup = BeautifulSoup(response.content, 'lxml')
    
    # Find product links
    product_links = soup.find_all('a', href=re.compile(r'/uk/shop/product/'))
    
    print(f"✅ Found {len(product_links)} product links")
    
    # Test first few products
    for i, link in enumerate(product_links[:3]):
        product_name = link.get_text(strip=True)
        if len(product_name) > 20:
            product_url = urljoin("https://www.apple.com", link.get('href', ''))
            print(f"   📱 Product {i+1}: {product_name[:50]}...")
            print(f"      URL: {product_url[:80]}...")
            
            # Test price extraction in surrounding text
            parent = link.find_parent(['div', 'section', 'article', 'li'])
            if parent:
                parent_text = parent.get_text()
                prices = re.findall(r'£([\d,]+(?:\.\d{2})?)', parent_text)
                if prices:
                    print(f"      Prices found: {prices}")
                else:
                    print("      No prices found in parent")
            break
    
except Exception as e:
    print(f"❌ Minimal scraping test failed: {e}")
    import traceback
    traceback.print_exc()

print("🏁 DEBUG SCRIPT COMPLETE")
print("=" * 50)
print("If you see this message, the core functionality works!")
print("The issue might be in the full scraper's class initialization or method calls.")