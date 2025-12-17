#!/usr/bin/env python3
"""
Apple Refurbished Scraper V7 - Mac, iPad & iPhone Support
Now with dual-write to PostgreSQL database

Features:
1. Scrapes Mac, iPad, and iPhone refurbished products from Apple UK
2. Historical tracking with price and availability change detection
3. Standardized output format for dashboard integration
4. Dual-write to PostgreSQL database (optional)
5. Google Sheets integration for data storage

USAGE:
    python histv7.py                    # Scrape all (mac, ipad, iphone)
    python histv7.py mac                # Scrape Mac only
    python histv7.py ipad               # Scrape iPad only  
    python histv7.py iphone             # Scrape iPhone only
    python histv7.py mac ipad           # Scrape Mac and iPad

SETUP INSTRUCTIONS:
1. Install required packages: pip3 install requests beautifulsoup4 lxml pandas gspread oauth2client psycopg2-binary python-dotenv
2. Put your credentials.json file in the same folder as this script
3. Enable Google Drive API and Google Sheets API in Google Cloud Console
4. (Optional) Set DATABASE_URL environment variable for database writes

Dependencies:
    pip3 install requests beautifulsoup4 lxml pandas gspread oauth2client psycopg2-binary python-dotenv
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

# Database writer for dual-write capability
try:
    from database_writer import DatabaseWriter
    DATABASE_AVAILABLE = True
except ImportError:
    DATABASE_AVAILABLE = False
    print("Database writer not available - continuing with Sheets only")

# PostgreSQL for variant lookup
try:
    import psycopg2
    from psycopg2.extras import RealDictCursor
    PSYCOPG2_AVAILABLE = True
except ImportError:
    PSYCOPG2_AVAILABLE = False
    print("psycopg2 not available - variant lookup from database disabled")

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
GOOGLE_SHEET_ID = "1j7npqR9I303etrf67HYkXU6m87DEwg66j0dmqxpMllQ"  # Your specific sheet ID
CREDENTIALS_FILE = "credentials.json"  # Make sure this file is in the same folder

# NEW: Fresh spreadsheet for historical tracking (avoids rate limits on existing sheet)
NEW_SPREADSHEET_NAME = f"Apple Mac Historical Tracker {datetime.now().strftime('%Y-%m-%d')}"
CURRENT_SHEET_NAME = "Current Inventory"
PRICE_HISTORY_SHEET_NAME = "Price History"
AVAILABILITY_HISTORY_SHEET_NAME = "Availability History"


class AppleDataStandardizer:
    """Embedded standardizer for converting Apple scraper data to dashboard format."""
    
    def __init__(self, google_client=None):
        # Field mapping from Apple scraper to user's format
        self.field_mapping = {
            'name': 'Title',
            'current_price': 'Price',
            'url': 'URL',
            'chip': 'CPU',
            'cpu_cores': 'CPU Cores',
            'memory': 'RAM (GB)',
            'storage': 'HD (GB)',
            'gpu_cores': 'GPU',
            'color': 'Colour'
        }
        
        # Fixed values for all Apple products
        self.fixed_values = {
            'Condition': 'Refurbished',
            'Grade': 'Excellent',
            'Seller': 'Apple',
            'Warning': ''
        }
        
        # Machine type mappings
        self.machine_mappings = {
            'mac mini': 'Mac mini',
            'macbook air': 'MacBook Air',
            'macbook pro': 'MacBook Pro', 
            'imac': 'iMac',
            'mac studio': 'Mac Studio',
            'mac pro': 'Mac Pro',
            # iPad mappings
            'ipad pro': 'iPad Pro',
            'ipad air': 'iPad Air',
            'ipad mini': 'iPad mini',
            'ipad': 'iPad',
            # iPhone mappings
            'iphone': 'iPhone'
        }
        
        # Model mappings for display sizes
        self.model_mappings = {
            'macbook air': {
                '13': 'MacBook Air 13-inch',
                '15': 'MacBook Air 15-inch'
            },
            'macbook pro': {
                '13': 'MacBook Pro 13-inch',
                '14': 'MacBook Pro 14-inch', 
                '16': 'MacBook Pro 16-inch'
            },
            'imac': {
                '21': 'iMac 21.5-inch',
                '24': 'iMac 24-inch',
                '27': 'iMac 27-inch'
            },
            'ipad pro': {
                '11': 'iPad Pro 11-inch',
                '12': 'iPad Pro 12.9-inch',
                '12.9': 'iPad Pro 12.9-inch',
                '13': 'iPad Pro 13-inch'
            },
            'ipad air': {
                '10': 'iPad Air 10.9-inch',
                '10.9': 'iPad Air 10.9-inch',
                '11': 'iPad Air 11-inch',
                '13': 'iPad Air 13-inch'
            },
            'ipad mini': {
                '8': 'iPad mini 8.3-inch',
                '8.3': 'iPad mini 8.3-inch'
            },
            'ipad': {
                '10': 'iPad 10.9-inch',
                '10.2': 'iPad 10.2-inch',
                '10.9': 'iPad 10.9-inch'
            }
        }
        
        # Variant ID lookup - now uses database instead of Google Sheets
        self.google_client = google_client
        self.variant_lookup = None
        self.variant_lookup_simple = None  # For iPhone/iPad (machine|storage|colour|grade)
        self.database_url = os.environ.get('DATABASE_URL')
        self.load_variant_lookup()
    
    def load_variant_lookup(self):
        """Load Variant ID lookup table from PostgreSQL database."""
        if not PSYCOPG2_AVAILABLE:
            print("❌ psycopg2 not available - variant lookup disabled")
            return
        
        if not self.database_url:
            # Try to load from .env file in backmarket-scraper
            env_path = os.path.join(os.path.dirname(__file__), '..', 'backmarket-scraper', '.env')
            if os.path.exists(env_path):
                with open(env_path) as f:
                    for line in f:
                        if line.startswith('DATABASE_URL='):
                            self.database_url = line.strip().split('=', 1)[1]
                            break
        
        if not self.database_url:
            print("❌ DATABASE_URL not set - variant lookup disabled")
            return
            
        try:
            # Connect to database
            conn = psycopg2.connect(self.database_url, sslmode='require')
            cursor = conn.cursor(cursor_factory=RealDictCursor)
            
            # Fetch all variants
            cursor.execute("""
                SELECT variant_id, machine, model, year, cpu, cpu_cores, 
                       storage, ram, gpu, colour, grade
                FROM variants
            """)
            rows = cursor.fetchall()
            
            print(f"✅ Loaded {len(rows)} variants from database")
            
            # Build lookup tables
            self.variant_lookup = {}  # Full lookup for Macs
            self.variant_lookup_simple = {}  # Simple lookup for iPhone/iPad
            
            for row in rows:
                variant_id = str(row['variant_id'])
                machine = row['machine'] or ''
                model = row['model'] or ''
                year = str(row['year']) if row['year'] else ''
                cpu = row['cpu'] or ''
                cpu_cores = row['cpu_cores'] or ''
                storage = row['storage'] or ''
                ram = row['ram'] or ''
                gpu = row['gpu'] or ''
                colour = row['colour'] or ''
                grade = row['grade'] or ''
                
                # Full lookup key for Macs (machine|year|cpu|cpu_cores|storage|ram|gpu|colour|grade)
                if cpu and ram:  # Mac products have CPU and RAM
                    lookup_key = f"{machine}|{year}|{cpu}|{cpu_cores}|{storage}|{ram}|{gpu}|{colour}|{grade}".lower()
                    self.variant_lookup[lookup_key] = variant_id
                
                # Simple lookup key for iPhone/iPad (machine|storage|colour|grade)
                # These products don't have CPU/RAM in the traditional sense
                if 'iphone' in machine.lower() or 'ipad' in machine.lower():
                    simple_key = f"{machine}|{storage}|{colour}|{grade}".lower()
                    self.variant_lookup_simple[simple_key] = variant_id
            
            cursor.close()
            conn.close()
            
            print(f"✅ Created lookup tables: {len(self.variant_lookup)} Mac entries, {len(self.variant_lookup_simple)} iPhone/iPad entries")
            
        except Exception as e:
            print(f"❌ Error loading Variant ID lookup from database: {e}")
            self.variant_lookup = None
            self.variant_lookup_simple = None
    
    def find_variant_id_and_model(self, machine: str, year: str, cpu: str, cpu_cores: str, hd: str, ram: str, gpu: str, colour: str, grade: str) -> tuple:
        """Find Variant ID for given specifications using database lookup."""
        
        # For iPhone/iPad, use simple lookup (machine|storage|colour|grade)
        if self.variant_lookup_simple and ('iphone' in machine.lower() or 'ipad' in machine.lower()):
            # Convert storage format: "256" or "1TB" -> just the number
            storage = hd.replace('TB', '000').replace('GB', '') if hd else ''
            # Handle TB conversion properly
            if 'TB' in str(hd).upper():
                try:
                    tb_val = int(str(hd).upper().replace('TB', ''))
                    storage = str(tb_val * 1000)
                except:
                    storage = hd
            
            # Try exact match first
            simple_key = f"{machine}|{storage}|{colour}|{grade}".lower()
            if simple_key in self.variant_lookup_simple:
                return self.variant_lookup_simple[simple_key], ''
            
            # Try with different grade variations
            for try_grade in ['Excellent', 'Good', 'Fair']:
                simple_key = f"{machine}|{storage}|{colour}|{try_grade}".lower()
                if simple_key in self.variant_lookup_simple:
                    return self.variant_lookup_simple[simple_key], ''
            
            # Try without colour (some variants might not have colour specified)
            simple_key_no_colour = f"{machine}|{storage}||{grade}".lower()
            if simple_key_no_colour in self.variant_lookup_simple:
                return self.variant_lookup_simple[simple_key_no_colour], ''
            
            return '', ''
        
        # For Macs, use full lookup
        if not self.variant_lookup:
            return '', ''
            
        # Create lookup key matching the format from load_variant_lookup
        # Format: machine|year|cpu|cpu_cores|hd|ram|gpu|colour|grade
        lookup_key = f"{machine}|{year}|{cpu}|{cpu_cores}|{hd}|{ram}|{gpu}|{colour}|{grade}".lower()
        
        if lookup_key in self.variant_lookup:
            variant_id = self.variant_lookup[lookup_key]
            return variant_id, ''  # Return Variant ID, empty model (will be populated from Variant Map if needed)
        
        # Try without GPU (common case where GPU field might be empty)
        lookup_key_no_gpu = f"{machine}|{year}|{cpu}|{cpu_cores}|{hd}|{ram}||{colour}|{grade}".lower()
        if lookup_key_no_gpu in self.variant_lookup:
            variant_id = self.variant_lookup[lookup_key_no_gpu]
            return variant_id, ''
        
        # Try without color (common case where color field might be empty)
        lookup_key_no_color = f"{machine}|{year}|{cpu}|{cpu_cores}|{hd}|{ram}|{gpu}||{grade}".lower()
        if lookup_key_no_color in self.variant_lookup:
            variant_id = self.variant_lookup[lookup_key_no_color]
            return variant_id, ''
        
        # Try without both GPU and color
        lookup_key_minimal = f"{machine}|{year}|{cpu}|{cpu_cores}|{hd}|{ram}|||{grade}".lower()
        if lookup_key_minimal in self.variant_lookup:
            variant_id = self.variant_lookup[lookup_key_minimal]
            return variant_id, ''
        
        # Try without CPU cores (might not always be available)
        lookup_key_no_cores = f"{machine}|{year}|{cpu}||{hd}|{ram}|{gpu}|{colour}|{grade}".lower()
        if lookup_key_no_cores in self.variant_lookup:
            variant_id = self.variant_lookup[lookup_key_no_cores]
            return variant_id, ''
                
        return '', ''
    
    def _standardize_machine_type(self, name: str) -> str:
        """Extract and standardize machine type with screen size from product name."""
        if not name:
            return 'Unknown'
            
        # Normalize Unicode characters
        name_normalized = str(name).replace('‑', '-').replace('\xa0', ' ').lower()
        
        # Extract screen size first
        screen_size = ''
        size_patterns = [
            r'(\d+(?:\.\d+)?)-inch',
            r'(\d+(?:\.\d+)?)\s*inch'
        ]
        
        for pattern in size_patterns:
            size_match = re.search(pattern, name_normalized)
            if size_match:
                screen_size = f"{size_match.group(1)}-inch"
                break
        
        # Determine machine type and combine with screen size
        # Mac products
        if 'macbook air' in name_normalized:
            base_type = 'MacBook Air'
            if screen_size:
                return f"{base_type} {screen_size}"
            return f"{base_type} 13-inch"
            
        elif 'macbook pro' in name_normalized:
            base_type = 'MacBook Pro'
            if screen_size:
                return f"{base_type} {screen_size}"
            return base_type
            
        elif 'imac' in name_normalized:
            base_type = 'iMac'
            if screen_size:
                return f"{base_type} {screen_size}"
            return base_type
            
        elif 'mac mini' in name_normalized:
            return 'Mac mini'
            
        elif 'mac studio' in name_normalized:
            return 'Mac Studio'
            
        elif 'mac pro' in name_normalized:
            return 'Mac Pro'
        
        # iPad products - check specific types first (Pro, Air, mini) before generic iPad
        elif 'ipad pro' in name_normalized:
            base_type = 'iPad Pro'
            if screen_size:
                return f"{base_type} {screen_size}"
            return base_type
            
        elif 'ipad air' in name_normalized:
            base_type = 'iPad Air'
            if screen_size:
                return f"{base_type} {screen_size}"
            return base_type
            
        elif 'ipad mini' in name_normalized:
            # iPad mini doesn't typically include screen size in name
            return 'iPad mini'
            
        elif 'ipad' in name_normalized:
            base_type = 'iPad'
            if screen_size:
                return f"{base_type} {screen_size}"
            return base_type
        
        # iPhone products - extract model number and variant
        elif 'iphone' in name_normalized:
            # Try to extract iPhone model (e.g., iPhone 14, iPhone 15 Pro Max)
            iphone_match = re.search(r'iphone\s*(\d+)\s*(pro\s*max|pro|plus|mini)?', name_normalized)
            if iphone_match:
                model_num = iphone_match.group(1)
                variant = iphone_match.group(2) or ''
                if variant:
                    variant = variant.replace('pro max', 'Pro Max').replace('pro', 'Pro').replace('plus', 'Plus').replace('mini', 'mini')
                    return f"iPhone {model_num} {variant}"
                return f"iPhone {model_num}"
            return 'iPhone'
            
        return 'Unknown'
    
    # iPad model number lookup table
    # Format: (device_type, screen_size, chip/generation, connectivity) -> model_number
    # Note: iPad Pro 12.9-inch (M1/M2) was replaced by iPad Pro 13-inch (M4/M5) - different product lines
    IPAD_MODEL_NUMBERS = {
        # iPad Pro 13-inch (M5) - 2025 (newest)
        ('ipad pro', '13', 'm5', 'wifi'): 'A3325',  # TODO: Verify model numbers when available
        ('ipad pro', '13', 'm5', 'cellular'): 'A3326',
        # iPad Pro 11-inch (M5) - 2025 (newest)
        ('ipad pro', '11', 'm5', 'wifi'): 'A3323',
        ('ipad pro', '11', 'm5', 'cellular'): 'A3324',
        # iPad Pro 13-inch (M4) - 2024
        ('ipad pro', '13', 'm4', 'wifi'): 'A2925',
        ('ipad pro', '13', 'm4', 'cellular'): 'A2926',
        # iPad Pro 11-inch (M4) - 2024
        ('ipad pro', '11', 'm4', 'wifi'): 'A2836',
        ('ipad pro', '11', 'm4', 'cellular'): 'A2837',
        # iPad Pro 12.9-inch (6th generation / M2) - 2022 (last 12.9-inch model)
        ('ipad pro', '12.9', '6th', 'wifi'): 'A2436',
        ('ipad pro', '12.9', '6th', 'cellular'): 'A2437',
        ('ipad pro', '12.9', 'm2', 'wifi'): 'A2436',
        ('ipad pro', '12.9', 'm2', 'cellular'): 'A2437',
        # iPad Pro 11-inch (4th generation / M2) - 2022
        ('ipad pro', '11', '4th', 'wifi'): 'A2759',
        ('ipad pro', '11', '4th', 'cellular'): 'A2761',
        ('ipad pro', '11', 'm2', 'wifi'): 'A2759',
        ('ipad pro', '11', 'm2', 'cellular'): 'A2761',
        # iPad Pro 12.9-inch (5th generation / M1) - 2021
        ('ipad pro', '12.9', '5th', 'wifi'): 'A2378',
        ('ipad pro', '12.9', '5th', 'cellular'): 'A2461',
        ('ipad pro', '12.9', 'm1', 'wifi'): 'A2378',
        ('ipad pro', '12.9', 'm1', 'cellular'): 'A2461',
        # iPad Pro 11-inch (3rd generation / M1) - 2021
        ('ipad pro', '11', '3rd', 'wifi'): 'A2377',
        ('ipad pro', '11', '3rd', 'cellular'): 'A2459',
        ('ipad pro', '11', 'm1', 'wifi'): 'A2377',
        ('ipad pro', '11', 'm1', 'cellular'): 'A2459',
        
        # iPad Air 13-inch (M3) - 2025
        ('ipad air', '13', 'm3', 'wifi'): 'A3268',
        ('ipad air', '13', 'm3', 'cellular'): 'A3269',
        # iPad Air 11-inch (M3) - 2025
        ('ipad air', '11', 'm3', 'wifi'): 'A3266',
        ('ipad air', '11', 'm3', 'cellular'): 'A3267',
        # iPad Air 13-inch (M2) - 2024
        ('ipad air', '13', 'm2', 'wifi'): 'A2898',
        ('ipad air', '13', 'm2', 'cellular'): 'A2899',
        # iPad Air 11-inch (M2) - 2024
        ('ipad air', '11', 'm2', 'wifi'): 'A2902',
        ('ipad air', '11', 'm2', 'cellular'): 'A2903',
        # iPad Air (5th generation / M1) - 2022
        ('ipad air', '10.9', '5th', 'wifi'): 'A2588',
        ('ipad air', '10.9', '5th', 'cellular'): 'A2589',
        ('ipad air', '10.9', 'm1', 'wifi'): 'A2588',
        ('ipad air', '10.9', 'm1', 'cellular'): 'A2589',
        ('ipad air', '', '5th', 'wifi'): 'A2588',
        ('ipad air', '', '5th', 'cellular'): 'A2589',
        ('ipad air', '', 'm1', 'wifi'): 'A2588',
        ('ipad air', '', 'm1', 'cellular'): 'A2589',
        
        # iPad mini (6th generation) - 2021
        ('ipad mini', '', '6th', 'wifi'): 'A2567',
        ('ipad mini', '', '6th', 'cellular'): 'A2568',
        ('ipad mini', '', '6', 'wifi'): 'A2567',
        ('ipad mini', '', '6', 'cellular'): 'A2568',
        # iPad mini (A17 Pro) - 2024
        ('ipad mini', '', 'a17', 'wifi'): 'A2993',
        ('ipad mini', '', 'a17', 'cellular'): 'A2995',
    }
    
    # iPhone model number lookup table (using "other countries and regions" model numbers)
    # Format: (model_number, variant) -> model_number
    # Variants: '', 'pro', 'pro max', 'plus', 'mini', 'se', 'air', 'e'
    IPHONE_MODEL_NUMBERS = {
        # iPhone 17 series - 2025
        ('17', 'pro max'): 'A3526',
        ('17', 'pro'): 'A3523',
        ('17', ''): 'A3520',
        # iPhone Air - 2025
        ('air', ''): 'A3517',
        # iPhone 16e - 2025
        ('16e', ''): 'A3409',
        # iPhone 16 series - 2024
        ('16', 'pro max'): 'A3296',
        ('16', 'pro'): 'A3293',
        ('16', 'plus'): 'A3290',
        ('16', ''): 'A3287',
        # iPhone 15 series - 2023
        ('15', 'pro max'): 'A3106',
        ('15', 'pro'): 'A3102',
        ('15', 'plus'): 'A3094',
        ('15', ''): 'A3090',
        # iPhone 14 series - 2022
        ('14', 'pro max'): 'A2894',
        ('14', 'pro'): 'A2890',
        ('14', 'plus'): 'A2886',
        ('14', ''): 'A2882',
        # iPhone SE (3rd generation) - 2022
        ('se', '3rd'): 'A2783',
        ('se', ''): 'A2783',
        # iPhone 13 series - 2021
        ('13', 'pro max'): 'A2643',
        ('13', 'pro'): 'A2638',
        ('13', 'mini'): 'A2628',
        ('13', ''): 'A2633',
        # iPhone 12 series - 2020
        ('12', 'pro max'): 'A2411',
        ('12', 'pro'): 'A2407',
        ('12', 'mini'): 'A2399',
        ('12', ''): 'A2403',
        # iPhone SE (2nd generation) - 2020
        ('se', '2nd'): 'A2296',
        # iPhone 11 series - 2019
        ('11', 'pro max'): 'A2218',
        ('11', 'pro'): 'A2215',
        ('11', ''): 'A2221',
    }
    
    def _extract_model_info(self, name: str, machine_type: str) -> str:
        """Extract Apple model number from product name.
        
        For iPads: Returns Apple model number (e.g., A2567, A2898)
        For iPhones: Returns Apple model number if available
        For Macs: Returns empty string (will be populated from Variant lookup)
        """
        if not name:
            return ''
        
        name_normalized = str(name).replace('‑', '-').replace('\xa0', ' ').lower()
        
        # iPad models - look up Apple model number
        if 'ipad' in machine_type.lower():
            # Determine device type
            if 'ipad pro' in name_normalized:
                device_type = 'ipad pro'
            elif 'ipad air' in name_normalized:
                device_type = 'ipad air'
            elif 'ipad mini' in name_normalized:
                device_type = 'ipad mini'
            else:
                device_type = 'ipad'
            
            # Extract screen size
            screen_size = ''
            size_match = re.search(r'(\d+(?:\.\d+)?)-?inch', name_normalized)
            if size_match:
                screen_size = size_match.group(1)
            
            # Extract chip/generation
            chip_gen = ''
            # Check for M-series chip (M1, M2, M3, M4)
            m_chip_match = re.search(r'\(m(\d+)\)', name_normalized)
            if m_chip_match:
                chip_gen = f'm{m_chip_match.group(1)}'
            else:
                # Check for generation (e.g., "6th Generation", "5th Gen")
                gen_match = re.search(r'(\d+)(?:st|nd|rd|th)\s*gen', name_normalized)
                if gen_match:
                    chip_gen = f'{gen_match.group(1)}th'
                else:
                    # Check for iPad mini with number (e.g., "iPad mini 6")
                    mini_match = re.search(r'ipad\s+mini\s+(\d+)', name_normalized)
                    if mini_match:
                        chip_gen = mini_match.group(1)
                    # Check for A-series chip (A17 Pro)
                    a_chip_match = re.search(r'\(a(\d+)', name_normalized)
                    if a_chip_match:
                        chip_gen = f'a{a_chip_match.group(1)}'
            
            # Determine connectivity
            connectivity = 'wifi'
            if 'cellular' in name_normalized or 'wi-fi + cellular' in name_normalized or 'wi-fi+cellular' in name_normalized:
                connectivity = 'cellular'
            
            # Look up model number
            lookup_key = (device_type, screen_size, chip_gen, connectivity)
            if lookup_key in self.IPAD_MODEL_NUMBERS:
                return self.IPAD_MODEL_NUMBERS[lookup_key]
            
            # Try without screen size for iPad mini and older iPad Air
            lookup_key_no_size = (device_type, '', chip_gen, connectivity)
            if lookup_key_no_size in self.IPAD_MODEL_NUMBERS:
                return self.IPAD_MODEL_NUMBERS[lookup_key_no_size]
            
            # Return empty if not found - will need to add more mappings
            return ''
        
        # iPhone models - look up Apple model number
        if 'iphone' in machine_type.lower():
            # Extract iPhone model number and variant from name
            # Examples: "iPhone 14 Pro 256GB", "iPhone 15 Pro Max 512GB", "iPhone SE"
            
            # Check for iPhone SE
            if 'iphone se' in name_normalized:
                gen_match = re.search(r'(\d+)(?:st|nd|rd|th)\s*gen', name_normalized)
                if gen_match:
                    gen = f'{gen_match.group(1)}rd' if gen_match.group(1) == '3' else f'{gen_match.group(1)}nd'
                    lookup_key = ('se', gen)
                else:
                    lookup_key = ('se', '')
                if lookup_key in self.IPHONE_MODEL_NUMBERS:
                    return self.IPHONE_MODEL_NUMBERS[lookup_key]
                return ''
            
            # Check for iPhone Air
            if 'iphone air' in name_normalized:
                lookup_key = ('air', '')
                if lookup_key in self.IPHONE_MODEL_NUMBERS:
                    return self.IPHONE_MODEL_NUMBERS[lookup_key]
                return ''
            
            # Check for iPhone 16e
            if 'iphone 16e' in name_normalized:
                lookup_key = ('16e', '')
                if lookup_key in self.IPHONE_MODEL_NUMBERS:
                    return self.IPHONE_MODEL_NUMBERS[lookup_key]
                return ''
            
            # Extract model number (14, 15, 16, 17, etc.) and variant (Pro, Pro Max, Plus, mini)
            iphone_match = re.search(r'iphone\s*(\d+)\s*(pro\s*max|pro|plus|mini)?', name_normalized)
            if iphone_match:
                model_num = iphone_match.group(1)
                variant = (iphone_match.group(2) or '').strip().replace('  ', ' ')
                
                lookup_key = (model_num, variant)
                if lookup_key in self.IPHONE_MODEL_NUMBERS:
                    return self.IPHONE_MODEL_NUMBERS[lookup_key]
            
            return ''
        
        # For Macs, return empty - will be populated from Variant lookup
        return ''
    
    def _standardize_model(self, chip_or_model: str) -> str:
        """Extract model/generation from chip or model field."""
        if not chip_or_model:
            return ''
        
        # Extract M-series chip info
        chip_match = re.search(r'M(\d+)(?:\s+(Pro|Max|Ultra))?', chip_or_model, re.IGNORECASE)
        if chip_match:
            base = f"M{chip_match.group(1)}"
            variant = chip_match.group(2)
            if variant:
                return f"{base} {variant}"
            return base
        
        return chip_or_model
    
    def _standardize_storage_format(self, storage_gb: str) -> str:
        """Convert storage to proper format (256, 512, 1TB, 2TB, etc.)."""
        if not storage_gb:
            return ''
        
        try:
            gb_value = float(storage_gb)
            # Convert values >= 1000 GB to TB format
            if gb_value >= 1000 and gb_value % 1000 == 0:
                tb_value = int(gb_value / 1000)
                return f'{tb_value}TB'
            else:
                # Keep as number for values < 1000
                return str(int(gb_value)) if gb_value == int(gb_value) else str(gb_value)
        except (ValueError, TypeError):
            return str(storage_gb)
    
    def _extract_year_from_chip(self, chip: str) -> str:
        """Estimate year based on chip generation."""
        if not chip:
            return ''
        
        # Year mapping for M-series chips
        year_mapping = {
            'M1': '2020',
            'M1 Pro': '2021', 
            'M1 Max': '2021',
            'M1 Ultra': '2022',
            'M2': '2022',
            'M2 Pro': '2023',
            'M2 Max': '2023',
            'M2 Ultra': '2023',
            'M3': '2023',
            'M3 Pro': '2023',
            'M3 Max': '2023',
            'M3 Ultra': '2025',  # Special case
            'M4': '2024',
            'M4 Pro': '2024',
            'M4 Max': '2024'
        }
        
        return year_mapping.get(chip, '')
    
    def _standardize_colour(self, color: str) -> str:
        """Standardize color names."""
        if not color or pd.isna(color):
            return ''
        
        color = str(color).strip()
        
        # Standardize common color variations
        color_mapping = {
            'space grey': 'Space Grey',
            'space gray': 'Space Grey', 
            'silver': 'Silver',
            'gold': 'Gold',
            'rose gold': 'Rose Gold',
            'midnight': 'Midnight',
            'starlight': 'Starlight',
            'blue': 'Blue',
            'sky blue': 'Sky Blue',
            'green': 'Green',
            'pink': 'Pink',
            'purple': 'Purple',
            'yellow': 'Yellow',
            'orange': 'Orange',
            'red': 'Red'
        }
        
        return color_mapping.get(color.lower(), color)
    
    def _extract_colour_from_name(self, name: str) -> str:
        """Extract colour from product name for iPhones/iPads."""
        if not name:
            return ''
        
        name_lower = name.lower()
        
        # iPhone/iPad colour patterns (order matters - check longer patterns first)
        colour_patterns = [
            # iPhone Titanium colours
            ('natural titanium', 'Natural Titanium'),
            ('blue titanium', 'Blue Titanium'),
            ('white titanium', 'White Titanium'),
            ('black titanium', 'Black Titanium'),
            ('desert titanium', 'Desert Titanium'),
            # iPhone Pro colours
            ('space black', 'Space Black'),
            ('deep purple', 'Deep Purple'),
            ('sierra blue', 'Sierra Blue'),
            ('alpine green', 'Alpine Green'),
            # Standard colours
            ('space grey', 'Space Grey'),
            ('space gray', 'Space Grey'),
            ('midnight', 'Midnight'),
            ('starlight', 'Starlight'),
            ('product red', 'Red'),
            ('(product)red', 'Red'),
            ('rose gold', 'Rose Gold'),
            ('sky blue', 'Sky Blue'),
            ('ultramarine', 'Ultramarine'),
            ('silver', 'Silver'),
            ('gold', 'Gold'),
            ('blue', 'Blue'),
            ('green', 'Green'),
            ('pink', 'Pink'),
            ('purple', 'Purple'),
            ('yellow', 'Yellow'),
            ('orange', 'Orange'),
            ('red', 'Red'),
            ('teal', 'Teal'),
            ('white', 'White'),
            ('black', 'Black'),
        ]
        
        for pattern, colour in colour_patterns:
            if pattern in name_lower:
                return colour
        
        return ''
    
    def _extract_year_from_model(self, machine_type: str, model_number: str) -> str:
        """Extract year based on iPhone/iPad model."""
        if not machine_type:
            return ''
        
        machine_lower = machine_type.lower()
        
        # iPhone year mapping based on model number
        if 'iphone' in machine_lower:
            iphone_years = {
                # iPhone 17 series - 2025
                '17': '2025',
                # iPhone 16 series - 2024
                '16': '2024',
                # iPhone 15 series - 2023
                '15': '2023',
                # iPhone 14 series - 2022
                '14': '2022',
                # iPhone 13 series - 2021
                '13': '2021',
                # iPhone 12 series - 2020
                '12': '2020',
                # iPhone 11 series - 2019
                '11': '2019',
            }
            # Extract model number from machine type (e.g., "iPhone 14 Pro" -> "14")
            import re
            model_match = re.search(r'iphone\s*(\d+)', machine_lower)
            if model_match:
                return iphone_years.get(model_match.group(1), '')
            # Special cases
            if 'se' in machine_lower:
                return '2022'  # iPhone SE 3rd gen
            if 'air' in machine_lower:
                return '2025'  # iPhone Air
        
        # iPad year mapping based on chip/generation
        if 'ipad' in machine_lower:
            # For iPads, we can use the model number to determine year
            ipad_model_years = {
                # iPad Pro 13-inch/11-inch M5 - 2025 (newest)
                'A3325': '2025', 'A3326': '2025', 'A3323': '2025', 'A3324': '2025',
                # iPad Pro 13-inch/11-inch M4 - 2024
                'A2925': '2024', 'A2926': '2024', 'A2836': '2024', 'A2837': '2024',
                # iPad Pro 12.9-inch M2 (6th gen) / 11-inch M2 (4th gen) - 2022 (last 12.9-inch models)
                'A2436': '2022', 'A2437': '2022', 'A2759': '2022', 'A2761': '2022',
                # iPad Pro 12.9-inch M1 (5th gen) / 11-inch M1 (3rd gen) - 2021
                'A2378': '2021', 'A2461': '2021', 'A2377': '2021', 'A2459': '2021',
                # iPad Air M3 - 2025
                'A3268': '2025', 'A3269': '2025', 'A3266': '2025', 'A3267': '2025',
                # iPad Air M2 - 2024
                'A2898': '2024', 'A2899': '2024', 'A2902': '2024', 'A2903': '2024',
                # iPad Air M1 (5th gen) - 2022
                'A2588': '2022', 'A2589': '2022',
                # iPad mini 6th gen - 2021
                'A2567': '2021', 'A2568': '2021',
                # iPad mini A17 Pro - 2024
                'A2993': '2024', 'A2995': '2024',
            }
            if model_number in ipad_model_years:
                return ipad_model_years[model_number]
        
        return ''
    
    def standardize_apple_product(self, apple_product: Dict) -> Dict:
        """Convert single Apple product to user's dashboard format."""
        standardized = {}
        
        # Map direct fields
        for apple_field, user_field in self.field_mapping.items():
            value = apple_product.get(apple_field, '')
            
            if user_field == 'RAM (GB)':
                # Extract number from memory field
                ram_match = re.search(r'(\d+)', str(value))
                value = ram_match.group(1) if ram_match else str(value)
            elif user_field == 'HD (GB)':
                # Convert storage to GB first, then format
                storage_str = str(value).upper()
                storage_match = re.search(r'(\d+)(GB|TB)', storage_str)
                if storage_match:
                    number = int(storage_match.group(1))
                    unit = storage_match.group(2)
                    gb_value = number * 1000 if unit == 'TB' else number
                    value = self._standardize_storage_format(str(gb_value))
                else:
                    value = self._standardize_storage_format(str(value))
            elif user_field == 'CPU Cores':
                # Extract number from CPU cores
                core_match = re.search(r'(\d+)', str(value))
                value = core_match.group(1) if core_match else str(value)
            elif user_field == 'GPU':
                # Extract number from GPU cores and add "-Core" suffix
                gpu_match = re.search(r'(\d+)', str(value))
                if gpu_match:
                    value = f"{gpu_match.group(1)}-Core"
                else:
                    value = str(value)
            elif user_field == 'Colour':
                value = self._standardize_colour(value)
            
            standardized[user_field] = value
        
        # Add fixed values
        standardized.update(self.fixed_values)
        
        # Compute derived fields
        machine_type = self._standardize_machine_type(apple_product.get('name', ''))
        standardized['Machine'] = machine_type
        
        # Extract Model - for iPads/iPhones, extract Apple model number; for Macs, will be populated from Variant lookup
        product_name = apple_product.get('name', '')
        model_number = self._extract_model_info(product_name, machine_type)
        standardized['Model'] = model_number
        
        # Extract Year - from chip for Macs, from model for iPhones/iPads
        year = self._extract_year_from_chip(apple_product.get('chip', ''))
        if not year:
            # Try to get year from iPhone/iPad model
            year = self._extract_year_from_model(machine_type, model_number)
        standardized['Year'] = year
        
        # Extract Colour - if not already populated, extract from product name
        if not standardized.get('Colour'):
            standardized['Colour'] = self._extract_colour_from_name(product_name)
        
        # Extract Storage from product name for iPhones/iPads if not already populated
        if not standardized.get('HD (GB)') and ('iphone' in machine_type.lower() or 'ipad' in machine_type.lower()):
            # Match storage with unit (e.g., "512GB" or "1TB")
            storage_match = re.search(r'(\d+)\s*(GB|TB)', product_name, re.IGNORECASE)
            if storage_match:
                storage_val = int(storage_match.group(1))
                storage_unit = storage_match.group(2).upper()
                # Convert TB to GB
                if storage_unit == 'TB':
                    storage_val = storage_val * 1000
                standardized['HD (GB)'] = str(storage_val)
        
        # Calculate price change (will be 0 for initial load)
        standardized['Change'] = 0
        
        # Add timestamp fields
        timestamp = apple_product.get('scraped_at', datetime.now().strftime("%Y-%m-%d %H:%M:%S"))
        standardized['Timestamp'] = timestamp
        
        # Extract day from timestamp
        try:
            dt = datetime.strptime(timestamp, "%Y-%m-%d %H:%M:%S")
            standardized['Timestamp Day'] = dt.strftime("%Y-%m-%d")
        except:
            standardized['Timestamp Day'] = datetime.now().strftime("%Y-%m-%d")
        
        # NEW: Find Variant ID using ALL specification fields
        machine = standardized.get('Machine', '')
        model = standardized.get('Model', '')
        year = standardized.get('Year', '')
        cpu = standardized.get('CPU', '')
        cpu_cores = standardized.get('CPU Cores', '')
        hd = standardized.get('HD (GB)', '')
        ram = standardized.get('RAM (GB)', '')
        gpu = standardized.get('GPU', '')
        colour = standardized.get('Colour', '')
        grade = standardized.get('Grade', '')
        
        variant_id, model_number = self.find_variant_id_and_model(machine, year, cpu, cpu_cores, hd, ram, gpu, colour, grade)
        standardized['Variant ID'] = variant_id
        # Only overwrite Model if we found one from Variant lookup (for Macs)
        # Keep the extracted model info for iPads/iPhones
        if model_number:
            standardized['Model'] = model_number
        
        return standardized
    
    def standardize_apple_data(self, apple_products: List[Dict]) -> List[Dict]:
        """Convert list of Apple products to standardized format."""
        standardized_products = []
        
        for product in apple_products:
            try:
                standardized = self.standardize_apple_product(product)
                standardized_products.append(standardized)
            except Exception as e:
                print(f"⚠️ Error standardizing product {product.get('name', 'Unknown')}: {e}")
                continue
        
        return standardized_products


class AppleMacScraperV7Historical:
    def __init__(self):
        self.base_url = "https://www.apple.com"
        self.mac_url = "https://www.apple.com/uk/shop/refurbished/mac"
        self.ipad_url = "https://www.apple.com/uk/shop/refurbished/ipad"
        self.iphone_url = "https://www.apple.com/uk/shop/refurbished/iphone"
        
        # Product categories to scrape (can be configured)
        self.product_categories = {
            'mac': self.mac_url,
            'ipad': self.ipad_url,
            'iphone': self.iphone_url
        }
        
        self.session = requests.Session()
        
        # Initialize standardizer for dual output (pass Google client for Variant ID lookup)
        self.standardizer = None  # Will be initialized after Google Sheets setup
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
            
            # NEW: Get the spreadsheet and ensure historical sheets exist
            self.spreadsheet = self.google_client.open_by_key(GOOGLE_SHEET_ID)
            self.ensure_historical_sheets_exist()
            
            # Initialize standardizer with Google client for Variant ID lookup
            self.standardizer = AppleDataStandardizer(self.google_client)
            
            return True
            
        except Exception as e:
            print(f"❌ Failed to set up Google Sheets: {e}")
            return False

    def ensure_historical_sheets_exist(self):
        """NEW: Create historical tracking sheets if they don't exist."""
        try:
            existing_sheets = [sheet.title for sheet in self.spreadsheet.worksheets()]
            
            # Create Current Inventory sheet if needed
            if CURRENT_SHEET_NAME not in existing_sheets:
                print(f"🆕 Creating '{CURRENT_SHEET_NAME}' sheet")
                self.spreadsheet.add_worksheet(title=CURRENT_SHEET_NAME, rows=1000, cols=20)
            
            # Create Price History sheet if needed
            if PRICE_HISTORY_SHEET_NAME not in existing_sheets:
                print(f"🆕 Creating '{PRICE_HISTORY_SHEET_NAME}' sheet")
                price_sheet = self.spreadsheet.add_worksheet(title=PRICE_HISTORY_SHEET_NAME, rows=5000, cols=15)
                # Add headers
                headers = ['timestamp', 'model_sku', 'name', 'change_type', 'old_price', 'new_price', 'change_amount', 'url']
                price_sheet.append_row(headers)
            
            # Create Availability History sheet if needed
            if AVAILABILITY_HISTORY_SHEET_NAME not in existing_sheets:
                print(f"🆕 Creating '{AVAILABILITY_HISTORY_SHEET_NAME}' sheet")
                avail_sheet = self.spreadsheet.add_worksheet(title=AVAILABILITY_HISTORY_SHEET_NAME, rows=5000, cols=10)
                # Add headers
                headers = ['timestamp', 'model_sku', 'name', 'change_type', 'current_price', 'url']
                avail_sheet.append_row(headers)
                
            print("✅ Historical tracking sheets ready")
            
        except Exception as e:
            print(f"⚠️ Error setting up historical sheets: {e}")

    # ALL YOUR EXISTING WORKING METHODS - UNCHANGED
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

    def discover_all_pages(self, category: str = 'mac') -> List[str]:
        """Discover all pagination URLs for a given product category."""
        category_url = self.product_categories.get(category, self.mac_url)
        category_path = f'/{category}'
        
        print(f"🔍 Discovering all {category.upper()} refurbished pages...")
        
        # Start with the main page
        soup = self.get_page(category_url)
        if not soup:
            return [category_url]
        
        page_urls = [category_url]
        
        # Look for pagination links
        pagination_selectors = [
            f'a[href*="{category}"][href*="page"]',
            '.pagination a',
            'a[href*="?page="]',
            'a[aria-label*="page"]',
            'a[href*="fnode"]'
        ]
        
        found_pages = set([category_url])
        
        for selector in pagination_selectors:
            pagination_links = soup.select(selector)
            print(f"🔍 Selector '{selector}' found {len(pagination_links)} links")
            for link in pagination_links:
                href = link.get('href')
                if href and category_path in href:
                    full_url = urljoin(self.base_url, href)
                    if full_url not in found_pages:
                        found_pages.add(full_url)
                        page_urls.append(full_url)
                        print(f"   ✅ Added page: {full_url}")
        
        # If no pagination found, try manual construction
        if len(page_urls) == 1:
            print("🔄 No pagination found, trying manual page construction...")
            for page_num in range(2, 7):
                test_urls = [
                    f"{category_url}?page={page_num}",
                    f"{category_url}/page/{page_num}",
                ]
                
                for test_url in test_urls:
                    print(f"   🧪 Testing: {test_url}")
                    test_soup = self.get_page(test_url)
                    if test_soup:
                        product_links = test_soup.find_all('a', href=re.compile(r'/uk/shop/product/'))
                        print(f"   📱 Found {len(product_links)} product links")
                        if len(product_links) > 10:
                            page_urls.append(test_url)
                            print(f"   ✅ Added manual page: {test_url}")
                            break
                else:
                    # No valid URL found for this page number, stop trying
                    print(f"   ❌ No valid page found for page {page_num}")
                    break
        
        print(f"🎯 TOTAL {category.upper()} PAGES DISCOVERED: {len(page_urls)}")
        for i, url in enumerate(page_urls, 1):
            print(f"   {i}. {url}")
        
        return page_urls

    def extract_products_with_prices_from_category_page(self, soup: BeautifulSoup) -> List[Dict]:
        """UNCHANGED: Your working price extraction method."""
        products = []
        
        # Find all product links (this worked in V6)
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
        """UNCHANGED: Your working price extraction method."""
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
        """UNCHANGED: Your working price text extraction."""
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
        """UNCHANGED: Your working specs extraction method."""
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
            
            # STORAGE EXTRACTION (unchanged from your working version)
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
            
            # MEMORY EXTRACTION (unchanged)
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
            
            # CHIP EXTRACTION (unchanged)
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
            
            # CPU/GPU CORES EXTRACTION (unchanged)
            cpu_match = re.search(r'(\d+)[\-‑]Core CPU', combined_text, re.IGNORECASE)
            if cpu_match:
                specs['cpu_cores'] = f"{cpu_match.group(1)}-Core CPU"
            
            gpu_match = re.search(r'(\d+)[\-‑]Core GPU', combined_text, re.IGNORECASE)
            if gpu_match:
                specs['gpu_cores'] = f"{gpu_match.group(1)}-Core GPU"
            
            # DISPLAY SIZE EXTRACTION (unchanged)
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
            
            # COLOR EXTRACTION - Sky Blue must come before Blue to match correctly
            color_patterns = [
                r'[\-\s](Space (?:Grey|Gray|Black))',
                r'[\-\s](Silver)', r'[\-\s](Gold)', r'[\-\s](Rose Gold)',
                r'[\-\s](Midnight)', r'[\-\s](Starlight)', r'[\-\s](Sky Blue)',
                r'[\-\s](Blue)', r'[\-\s](Green)', r'[\-\s](Pink)', r'[\-\s](Purple)',
                r'[\-\s](Yellow)', r'[\-\s](Orange)', r'[\-\s](Red)'
            ]
            
            for pattern in color_patterns:
                color_match = re.search(pattern, product_name, re.IGNORECASE)
                if color_match:
                    specs['color'] = color_match.group(1).strip()
                    break
            
            # CONNECTIVITY EXTRACTION - enhanced for iPad/iPhone
            if 'Gigabit Ethernet' in combined_text:
                specs['connectivity'] = 'Gigabit Ethernet'
            elif '10Gb Ethernet' in combined_text:
                specs['connectivity'] = '10Gb Ethernet'
            elif 'Wi-Fi + Cellular' in combined_text or 'WiFi + Cellular' in combined_text:
                specs['connectivity'] = 'Wi-Fi + Cellular'
            elif 'Cellular' in combined_text and ('iPad' in product_name or 'iPhone' in product_name):
                specs['connectivity'] = 'Wi-Fi + Cellular'
            elif 'Wi-Fi' in combined_text:
                specs['connectivity'] = 'Wi-Fi'
            
            # IPHONE/IPAD SPECIFIC: Extract model variant (e.g., iPhone 15 Pro Max, iPad Pro)
            iphone_match = re.search(r'iPhone\s+(\d+)(?:\s+(Pro|Pro Max|Plus|mini))?', product_name, re.IGNORECASE)
            if iphone_match:
                iphone_num = iphone_match.group(1)
                iphone_variant = iphone_match.group(2) or ''
                specs['model_variant'] = f"iPhone {iphone_num} {iphone_variant}".strip()
            
            ipad_match = re.search(r'iPad\s+(Pro|Air|mini)?(?:\s+(\d+(?:\.\d+)?)-?inch)?', product_name, re.IGNORECASE)
            if ipad_match:
                ipad_type = ipad_match.group(1) or ''
                specs['model_variant'] = f"iPad {ipad_type}".strip()
            
            # Add specs to product
            product.update(specs)
            
            print(f"✅ Extracted specs: {specs['chip'] or 'N/A'} | {specs['memory'] or 'N/A'} | {specs['storage'] or 'N/A'}")
            
        except Exception as e:
            print(f"❌ Error extracting specs: {e}")
        
        return product

    # NEW: Historical tracking methods
    def load_previous_data(self) -> Dict[str, Dict]:
        """NEW: Load previous data from Current Inventory sheet for comparison."""
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
            print(f"⚠️ Could not load previous data (first run?): {e}")
            
        return previous_data

    def detect_and_log_changes(self, current_products: List[Dict], previous_data: Dict[str, Dict]):
        """NEW: Detect changes and log them to history sheets."""
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
                    current_product.get('current_price'), current_product.get('url')
                ])
        
        # Check for removed products (disappeared)
        for sku, previous_product in previous_data.items():
            if sku not in current_lookup:
                print(f"❌ REMOVED PRODUCT: {previous_product.get('name', 'Unknown')[:50]}...")
                availability_changes.append([
                    timestamp, sku, previous_product.get('name'), 'DISAPPEARED',
                    previous_product.get('current_price'), previous_product.get('url')
                ])
        
        # Check for price changes in existing products
        for sku, current_product in current_lookup.items():
            if sku in previous_data:
                previous_product = previous_data[sku]
                
                # Compare prices (convert to float for comparison)
                try:
                    current_price = float(current_product.get('current_price', 0) or 0)
                    previous_price = float(previous_product.get('current_price', 0) or 0)
                    
                    if current_price != previous_price and current_price > 0 and previous_price > 0:
                        change_amount = current_price - previous_price
                        change_type = 'PRICE_INCREASE' if change_amount > 0 else 'PRICE_DECREASE'
                        
                        print(f"💰 PRICE CHANGE: {current_product.get('name', 'Unknown')[:40]}...")
                        print(f"   £{previous_price} -> £{current_price} ({'+' if change_amount > 0 else ''}£{change_amount:.2f})")
                        
                        price_changes.append([
                            timestamp, sku, current_product.get('name'), change_type,
                            previous_price, current_price, change_amount, current_product.get('url')
                        ])
                        
                except (ValueError, TypeError):
                    # Skip if price comparison fails
                    pass
        
        # Log changes to Google Sheets using BATCH operations
        if price_changes:
            try:
                price_history_sheet = self.spreadsheet.worksheet(PRICE_HISTORY_SHEET_NAME)
                # Batch append all price changes in one API call
                if len(price_changes) > 0:
                    self.batch_append_to_sheet(price_history_sheet, price_changes)
                print(f"✅ Logged {len(price_changes)} price changes (batched)")
            except Exception as e:
                print(f"❌ Error logging price changes: {e}")
        
        if availability_changes:
            try:
                availability_sheet = self.spreadsheet.worksheet(AVAILABILITY_HISTORY_SHEET_NAME)
                # Batch append all availability changes in one API call
                if len(availability_changes) > 0:
                    self.batch_append_to_sheet(availability_sheet, availability_changes)
                print(f"✅ Logged {len(availability_changes)} availability changes (batched)")
            except Exception as e:
                print(f"❌ Error logging availability changes: {e}")
        
        if not price_changes and not availability_changes:
            print("✅ No changes detected since last run")

    def batch_append_to_sheet(self, sheet, data):
        """Batch append data to a Google Sheet."""
        try:
            sheet.append_rows(data, value_input_option='USER_ENTERED')
        except Exception as e:
            print(f"❌ Error batch appending to sheet: {e}")

    def update_current_inventory(self, products: List[Dict]):
        """NEW: Update the Current Inventory sheet with latest data."""
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

    def upload_standardized_to_sheets(self, standardized_products: List[Dict]):
        """Upload standardized products to Apple Products Standardized sheet."""
        if not self.google_client or not standardized_products:
            return
            
        try:
            # Try to get existing sheet or create new one
            try:
                standardized_sheet = self.spreadsheet.worksheet("Apple Products Standardized")
                print(f"📊 Using existing sheet: Apple Products Standardized")
            except:
                standardized_sheet = self.spreadsheet.add_worksheet(
                    title="Apple Products Standardized", 
                    rows=len(standardized_products) + 100, 
                    cols=25
                )
                print(f"📊 Created new sheet: Apple Products Standardized")
            
            # Clear existing data
            standardized_sheet.clear()
            print(f"🧹 Cleared existing data")
            
            # Define standardized headers in correct order
            standardized_headers = [
                'Title', 'Condition', 'Price', 'Change', 'URL', 'Machine', 'Model', 'Year',
                'CPU', 'CPU Cores', 'HD (GB)', 'RAM (GB)', 'GPU', 'Colour', 'Grade', 
                'Seller', 'Warning', 'Timestamp', 'Timestamp Day', 'Timestamp Day', 'Variant ID'
            ]
            
            # Prepare data with headers
            data = [standardized_headers]
            
            for product in standardized_products:
                row = []
                for header in standardized_headers:
                    value = product.get(header, '')
                    if value is None:
                        row.append('')
                    else:
                        row.append(str(value))
                data.append(row)
            
            # Upload all data
            print(f"📤 Uploading standardized data to Google Sheets...")
            standardized_sheet.update(data, value_input_option='USER_ENTERED')
            
            # Format the header row
            standardized_sheet.format('A1:U1', {
                'backgroundColor': {'red': 0.2, 'green': 0.2, 'blue': 0.2},
                'textFormat': {'foregroundColor': {'red': 1, 'green': 1, 'blue': 1}, 'bold': True}
            })
            
            # Format price column as currency (Column C)
            standardized_sheet.format('C2:C1000', {
                'numberFormat': {'type': 'CURRENCY', 'pattern': '£#,##0.00'}
            })
            
            print(f"✅ Successfully uploaded {len(standardized_products)} standardized products to Google Sheets!")
            print(f"📊 Sheet: Apple Products Standardized")
            
        except Exception as e:
            print(f"❌ Failed to upload standardized data: {e}")
    
    def update_history_tab(self, products: List[Dict]):
        """Update the History tab with latest data (append, don't clear)."""
        if not self.google_client:
            return
            
        try:
            history_sheet = self.spreadsheet.worksheet("History")
            
            if products:
                # Get all possible headers
                all_headers = set()
                for product in products:
                    all_headers.update(product.keys())
                
                # Order headers logically (same as Current Inventory)
                preferred_order = [
                    'name', 'chip', 'cpu_cores', 'gpu_cores', 'memory', 'storage',
                    'display_size', 'color', 'current_price', 'original_price', 
                    'savings', 'discount_percentage', 'connectivity', 'url', 
                    'model_sku', 'scraped_at'
                ]
                
                headers = [h for h in preferred_order if h in all_headers]
                headers.extend([h for h in all_headers if h not in headers])
                
                # Prepare data rows
                data = []
                
                for product in products:
                    row = []
                    for header in headers:
                        value = product.get(header, '')
                        if value is None:
                            row.append('')
                        else:
                            row.append(str(value))
                    data.append(row)
                
                # Append all data to History tab (don't clear)
                self.batch_append_to_sheet(history_sheet, data)
                
                print(f"✅ Updated History tab with {len(products)} products")
                
        except Exception as e:
            print(f"❌ Failed to update History tab: {e}")
    
    def update_standardized_history_tab(self, standardized_products: List[Dict]):
        """Update the Standardized History tab with latest standardized data (append, don't clear)."""
        if not self.google_client:
            return
            
        try:
            # Try to get existing sheet or create new one
            try:
                standardized_history_sheet = self.spreadsheet.worksheet("Standardized History")
                print(f"📊 Using existing sheet: Standardized History")
            except:
                standardized_history_sheet = self.spreadsheet.add_worksheet(
                    title="Standardized History", 
                    rows=5000, 
                    cols=21
                )
                print(f"📊 Created new sheet: Standardized History")
                
                # Add headers for new sheet
                standardized_headers = [
                    'Title', 'Condition', 'Price', 'Change', 'URL', 'Machine', 'Model', 'Year',
                    'CPU', 'CPU Cores', 'HD (GB)', 'RAM (GB)', 'GPU', 'Colour', 'Grade', 
                    'Seller', 'Warning', 'Timestamp', 'Timestamp Day', 'Timestamp Day', 'Variant ID'
                ]
                standardized_history_sheet.append_row(standardized_headers)
                
                # Format the header row
                standardized_history_sheet.format('A1:U1', {
                    'backgroundColor': {'red': 0.2, 'green': 0.2, 'blue': 0.2},
                    'textFormat': {'foregroundColor': {'red': 1, 'green': 1, 'blue': 1}, 'bold': True}
                })
            
            if standardized_products:
                # Define standardized headers in correct order
                standardized_headers = [
                    'Title', 'Condition', 'Price', 'Change', 'URL', 'Machine', 'Model', 'Year',
                    'CPU', 'CPU Cores', 'HD (GB)', 'RAM (GB)', 'GPU', 'Colour', 'Grade', 
                    'Seller', 'Warning', 'Timestamp', 'Timestamp Day', 'Timestamp Day', 'Variant ID'
                ]
                
                # Prepare data rows
                data = []
                
                for product in standardized_products:
                    row = []
                    for header in standardized_headers:
                        value = product.get(header, '')
                        if value is None:
                            row.append('')
                        else:
                            row.append(str(value))
                    data.append(row)
                
                # Append all data to Standardized History tab (don't clear)
                self.batch_append_to_sheet(standardized_history_sheet, data)
                
                print(f"✅ Updated Standardized History tab with {len(standardized_products)} products")
                
        except Exception as e:
            print(f"❌ Failed to update Standardized History tab: {e}")

    def scrape_category(self, category: str) -> List[Dict]:
        """Scrape a single product category (mac, ipad, or iphone)."""
        print(f"\n{'='*60}")
        print(f"📱 SCRAPING {category.upper()} PRODUCTS")
        print(f"{'='*60}")
        
        # Discover all pages for this category
        page_urls = self.discover_all_pages(category)
        
        all_products = []
        seen_urls = set()
        
        for page_num, page_url in enumerate(page_urls, 1):
            print(f"\n📄 SCRAPING {category.upper()} PAGE {page_num}/{len(page_urls)}")
            print(f"🔗 URL: {page_url}")
            
            soup = self.get_page(page_url)
            if not soup:
                continue
            
            # Extract products with pricing from category page
            page_products = self.extract_products_with_prices_from_category_page(soup)
            
            # Add category tag to each product
            for product in page_products:
                product['category'] = category
            
            # Filter out duplicates
            new_products = []
            for product in page_products:
                if product['url'] not in seen_urls:
                    seen_urls.add(product['url'])
                    new_products.append(product)
            
            print(f"📦 Found {len(new_products)} new {category} products with pricing")
            all_products.extend(new_products)
            
            # Be respectful between pages
            time.sleep(2)
        
        print(f"\n🎯 TOTAL {category.upper()} PRODUCTS: {len(all_products)}")
        return all_products

    def scrape_all_mac_products(self, categories: List[str] = None) -> List[Dict]:
        """MAIN SCRAPING METHOD - Now supports Mac, iPad, and iPhone.
        
        Args:
            categories: List of categories to scrape. Defaults to ['mac'] for backward compatibility.
                       Options: 'mac', 'ipad', 'iphone'
        """
        if categories is None:
            categories = ['mac']  # Default to Mac only for backward compatibility
        
        print("🍎 Apple Refurbished Scraper V7 - Mac, iPad & iPhone Support")
        print("=" * 60)
        print(f"📋 Categories to scrape: {', '.join(categories)}")
        
        # NEW: Load previous data for comparison
        previous_data = self.load_previous_data()
        
        all_products = []
        seen_urls = set()
        
        # STEP 1: Extract products WITH PRICES from all category pages
        print(f"\n🎯 STEP 1: EXTRACTING PRODUCTS WITH PRICES FROM CATEGORY PAGES")
        print("=" * 60)
        
        for category in categories:
            if category not in self.product_categories:
                print(f"⚠️ Unknown category: {category}, skipping...")
                continue
            
            category_products = self.scrape_category(category)
            
            # Filter out duplicates across categories
            for product in category_products:
                if product['url'] not in seen_urls:
                    seen_urls.add(product['url'])
                    all_products.append(product)
        
        print(f"\n🎯 TOTAL PRODUCTS WITH PRICING: {len(all_products)}")
        products_with_prices = len([p for p in all_products if p.get('current_price')])
        print(f"💰 Products with valid prices: {products_with_prices}/{len(all_products)}")
        
        # STEP 2: Extract detailed specs from individual product pages (unchanged)
        print(f"\n🔍 STEP 2: EXTRACTING DETAILED SPECS FROM PRODUCT PAGES")
        print("=" * 60)
        
        detailed_products = []
        for i, product in enumerate(all_products, 1):
            print(f"\n📱 [{i}/{len(all_products)}] Processing: {product.get('name', 'Unknown')[:50]}...")
            
            # Extract detailed specs (pricing already done)
            detailed_product = self.extract_detailed_specs(product)
            detailed_products.append(detailed_product)
            
            # Be respectful between product pages
            time.sleep(0.5)
        
        # NEW: STEP 3: Generate standardized format
        print(f"\n🔄 STEP 3: GENERATING STANDARDIZED FORMAT")
        print("=" * 60)
        standardized_products = self.standardizer.standardize_apple_data(detailed_products)
        print(f"✅ Standardized {len(standardized_products)} products for dashboard")
        
        # NEW: STEP 4: Detect and log changes
        print(f"\n📊 STEP 4: HISTORICAL CHANGE DETECTION")
        print("=" * 60)
        self.detect_and_log_changes(detailed_products, previous_data)
        
        # NEW: STEP 5: Update current inventory
        print(f"\n💾 STEP 5: UPDATING CURRENT INVENTORY")
        print("=" * 60)
        self.update_current_inventory(detailed_products)
        
        # NEW: STEP 6: Upload standardized data
        print(f"\n🎯 STEP 6: UPLOADING STANDARDIZED DATA")
        print("=" * 60)
        if self.google_client:
            self.upload_standardized_to_sheets(standardized_products)
        
        # NEW: STEP 7: Update History tab
        print(f"\n📊 STEP 7: UPDATING HISTORY TAB")
        print("=" * 60)
        self.update_history_tab(detailed_products)
        
        # NEW: STEP 8: Update Standardized History tab
        print(f"\n📊 STEP 8: UPDATING STANDARDIZED HISTORY TAB")
        print("=" * 60)
        self.update_standardized_history_tab(standardized_products)
        
        # Store standardized products for database write
        self.standardized_products = standardized_products
        
        return detailed_products

    # UNCHANGED: All your existing working methods
    def save_to_csv(self, products: List[Dict], filename: str = "mac_products_v7_historical.csv"):
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
        """MODIFIED: Upload to the original sheet (for compatibility)."""
        if not self.google_client:
            print("❌ Google Sheets not available")
            return False
        
        if not products:
            print("❌ No products to upload")
            return False
        
        try:
            print(f"📊 Also updating original sheet: {GOOGLE_SHEET_NAME}")
            
            # Get the first sheet (original sheet)
            sheet = self.spreadsheet.sheet1
            
            # Clear existing data
            sheet.clear()
            
            # Prepare data for upload (same as your original code)
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
                
                print(f"✅ Successfully updated original sheet!")
                return True
                
        except Exception as e:
            print(f"❌ Failed to upload to original sheet: {e}")
            return False

    def print_summary(self, products: List[Dict]):
        """ENHANCED: Your original summary + historical info."""
        if not products:
            print("\n❌ No products found")
            return
        
        print(f"\n🎉 SCRAPING COMPLETE!")
        print(f"=" * 60)
        print(f"📊 Total products found: {len(products)}")
        
        # Calculate totals (unchanged)
        products_with_prices = [p for p in products if p.get('current_price')]
        total_value = sum(p.get('current_price', 0) for p in products_with_prices)
        total_savings = sum(p.get('savings', 0) for p in products if p.get('savings'))
        
        products_with_savings = [p for p in products if p.get('savings')]
        avg_discount = sum(p.get('discount_percentage', 0) for p in products_with_savings) / len(products_with_savings) if products_with_savings else 0
        
        print(f"💰 Products with pricing: {len(products_with_prices)}/{len(products)} ({len(products_with_prices)/len(products)*100:.1f}%)")
        print(f"💰 Total catalog value: £{total_value:,.2f}")
        print(f"💸 Total potential savings: £{total_savings:,.2f}")
        print(f"📈 Average discount: {avg_discount:.1f}%")
        
        # Specs coverage (unchanged)
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
        
        # Best deals (unchanged)
        if products_with_savings:
            best_saving = max(products_with_savings, key=lambda x: x.get('savings', 0))
            print(f"\n🏆 BEST DEAL:")
            print(f"💰 {best_saving.get('name', 'Unknown')[:60]}...")
            print(f"    £{best_saving.get('current_price', 0)} (save £{best_saving.get('savings', 0)})")
        
        # NEW: Historical tracking summary  
        print(f"\n📊 HISTORICAL TRACKING:")
        print(f"   📋 Current Inventory: Updated with latest data")
        print(f"   📈 Price History: Tracking all price changes over time")
        print(f"   📦 Availability History: Tracking products appearing/disappearing")
        print(f"   🎯 Standardized History: Tracking standardized format over time")
        print(f"   🔗 View data: https://docs.google.com/spreadsheets/d/{GOOGLE_SHEET_ID}")


def main(categories: List[str] = None):
    """Main function to run the historical scraper.
    
    Args:
        categories: List of categories to scrape. Options: 'mac', 'ipad', 'iphone'
                   Defaults to ['mac', 'ipad', 'iphone'] to scrape all.
    """
    if categories is None:
        # Default: scrape all categories
        categories = ['mac', 'ipad', 'iphone']
    
    print("🍎 Apple Refurbished Scraper V7")
    print("🔧 Now with Mac, iPad & iPhone Support!")
    print("=" * 50)
    print(f"📋 Categories: {', '.join(categories)}")
    
    scraper = AppleMacScraperV7Historical()
    
    # Initialize database writer for dual-write
    db_writer = None
    if DATABASE_AVAILABLE:
        db_writer = DatabaseWriter()
    
    # Check Google Sheets setup
    if scraper.google_client:
        print(f"📊 Google Sheets: ✅ Ready")
        print(f"📋 Historical Tracking: ✅ Ready")
    else:
        print(f"📊 Google Sheets: ❌ Not available")
    
    # Check database setup
    if db_writer and db_writer.enabled:
        print(f"💾 Database: ✅ Ready (dual-write enabled)")
    else:
        print(f"💾 Database: ❌ Not available (Sheets only)")
    
    print(f"\n⏰ Started at: {datetime.now().strftime('%Y-%m-%d %H:%M:%S')}")
    
    # Scrape products from specified categories
    products = scraper.scrape_all_mac_products(categories=categories)
    
    # Print comprehensive summary
    scraper.print_summary(products)
    
    # Save results
    if products:
        print(f"\n💾 Saving data...")
        csv_file = scraper.save_to_csv(products)
        
        # Upload to original Google Sheet (for compatibility)
        if scraper.google_client:
            scraper.upload_to_google_sheets(products)
            
            # Standardized format already uploaded during scraping process
            print(f"\n✅ Standardized format already uploaded during scraping")
        else:
            print("💡 Enable Google Sheets to automatically sync data")
        
        # Write standardized products to database (dual-write)
        if db_writer and db_writer.enabled and hasattr(scraper, 'standardized_products'):
            print(f"\n💾 Writing to database...")
            count = db_writer.write_to_apple_history(scraper.standardized_products)
            if count > 0:
                print(f"✅ Written {count} products to database")
            db_writer.close()
        
        print(f"\n✅ All done! Found {len(products)} products with historical tracking")
        if csv_file:
            print(f"📄 Local file: {csv_file}")
        if scraper.google_client:
            print(f"📊 Original Sheet: {GOOGLE_SHEET_NAME}")
            print(f"📋 Current Inventory: {CURRENT_SHEET_NAME}")
            print(f"📈 Price History: {PRICE_HISTORY_SHEET_NAME}")
            print(f"📦 Availability History: {AVAILABILITY_HISTORY_SHEET_NAME}")
            print(f"🎯 Standardized Sheet: Apple Products Standardized")
            print(f"📊 Standardized History: Standardized History")
    
    print(f"\n⏰ Finished at: {datetime.now().strftime('%Y-%m-%d %H:%M:%S')}")
    return products


if __name__ == "__main__":
    import sys
    
    # Parse command line arguments for categories
    # Usage: python histv7.py [categories]
    # Examples:
    #   python histv7.py                    # Scrape all (mac, ipad, iphone)
    #   python histv7.py mac                # Scrape Mac only
    #   python histv7.py ipad               # Scrape iPad only  
    #   python histv7.py iphone             # Scrape iPhone only
    #   python histv7.py mac ipad           # Scrape Mac and iPad
    #   python histv7.py mac ipad iphone    # Scrape all
    
    if len(sys.argv) > 1:
        categories = [arg.lower() for arg in sys.argv[1:] if arg.lower() in ['mac', 'ipad', 'iphone']]
        if not categories:
            print("⚠️ Invalid categories. Valid options: mac, ipad, iphone")
            print("   Using default: all categories")
            categories = None
    else:
        categories = None  # Default to all
    
    products = main(categories=categories)