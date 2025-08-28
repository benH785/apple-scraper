#!/usr/bin/env python3
"""
Apple Data Standardizer
Converts Apple scraper data to user's React dashboard format and handles Variant ID matching.
"""

import pandas as pd
import re
from datetime import datetime
from typing import Dict, List, Optional, Any
import json

class AppleDataStandardizer:
    def __init__(self):
        """Initialize the standardizer with mapping configurations."""
        self.variant_lookup = {}  # Will be populated from lookup table
        
        # Mapping from Apple data to user format
        self.field_mapping = {
            # Direct mappings
            'name': 'Title',
            'current_price': 'Price', 
            'url': 'URL',
            'color': 'Colour',
            'scraped_at': 'Timestamp',
            
            # Computed/transformed fields
            'chip': 'CPU',
            'cpu_cores': 'CPU Cores',
            'memory': 'RAM (GB)',
            'storage': 'HD (GB)',
            'gpu_cores': 'GPU',
        }
        
        # Fixed values for Apple products
        self.fixed_values = {
            'Condition': 'Refurbished',
            'Grade': 'Excellent',
            'Seller': 'Apple',
            'Warning': '',
        }
    
    def load_variant_lookup_table(self, lookup_file_path: str) -> bool:
        """Load the Variant ID lookup table from CSV/JSON file."""
        try:
            if lookup_file_path.endswith('.csv'):
                df = pd.read_csv(lookup_file_path)
                # Convert to dictionary for fast lookups
                # Assuming columns: Machine, Model, Year, CPU, CPU Cores, HD (GB), RAM (GB), GPU, Colour, Variant ID
                for _, row in df.iterrows():
                    if row.get('Grade') == 'Excellent':  # Only Excellent grade
                        spec_key = self._create_spec_key(row)
                        self.variant_lookup[spec_key] = row.get('Variant ID')
            elif lookup_file_path.endswith('.json'):
                with open(lookup_file_path, 'r') as f:
                    self.variant_lookup = json.load(f)
            
            print(f"✅ Loaded {len(self.variant_lookup)} Variant ID mappings")
            return True
            
        except Exception as e:
            print(f"❌ Error loading Variant lookup table: {e}")
            return False
    
    def _create_spec_key(self, product_data: Dict) -> str:
        """Create a standardized specification key for lookup matching."""
        # Extract key specifications for matching
        machine = self._standardize_machine_type(product_data.get('Machine', ''))
        model = self._standardize_model(product_data.get('Model', ''))
        cpu = self._standardize_cpu(product_data.get('CPU', ''))
        cpu_cores = self._standardize_cpu_cores(product_data.get('CPU Cores', ''))
        ram = self._standardize_ram(product_data.get('RAM (GB)', ''))
        storage = self._standardize_storage(product_data.get('HD (GB)', ''))
        gpu = self._standardize_gpu(product_data.get('GPU', ''))
        colour = self._standardize_colour(product_data.get('Colour', ''))
        
        # Create composite key
        spec_key = f"{machine}|{model}|{cpu}|{cpu_cores}|{ram}|{storage}|{gpu}|{colour}"
        return spec_key.lower().strip()
    
    def _standardize_machine_type(self, name: str) -> str:
        """Extract and standardize machine type with screen size from product name."""
        if not name:
            return 'Unknown'
            
        name_lower = name.lower()
        
        # Extract screen size first
        screen_size = ''
        size_patterns = [
            r'(\d+(?:\.\d+)?)\-inch',
            r'(\d+(?:\.\d+)?)\s*inch'
        ]
        
        for pattern in size_patterns:
            size_match = re.search(pattern, name_lower)
            if size_match:
                screen_size = f"{size_match.group(1)}-inch"
                break
        
        # Determine machine type and combine with screen size
        if 'macbook air' in name_lower:
            base_type = 'MacBook Air'
            if screen_size:
                return f"{base_type} {screen_size}"
            # Default to 13-inch if no size found (most common)
            return f"{base_type} 13-inch"
            
        elif 'macbook pro' in name_lower:
            base_type = 'MacBook Pro'
            if screen_size:
                return f"{base_type} {screen_size}"
            return base_type
            
        elif 'imac' in name_lower:
            base_type = 'iMac'
            if screen_size:
                return f"{base_type} {screen_size}"
            return base_type
            
        elif 'mac mini' in name_lower:
            return 'Mac mini'
            
        elif 'mac studio' in name_lower:
            return 'Mac Studio'
            
        elif 'mac pro' in name_lower:
            return 'Mac Pro'
            
        return 'Unknown'
    
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
    
    def _standardize_cpu(self, chip: str) -> str:
        """Standardize CPU field."""
        if not chip:
            return ''
        
        # Extract M-series chip
        chip_match = re.search(r'(M\d+(?:\s+(?:Pro|Max|Ultra))?)', chip, re.IGNORECASE)
        if chip_match:
            return chip_match.group(1)
        
        return chip
    
    def _standardize_cpu_cores(self, cpu_cores: str) -> str:
        """Extract CPU core count."""
        if not cpu_cores:
            return ''
        
        # Extract number from "10-Core CPU" format
        core_match = re.search(r'(\d+)', str(cpu_cores))
        if core_match:
            return core_match.group(1)
        
        return str(cpu_cores)
    
    def _standardize_ram(self, memory: str) -> str:
        """Convert memory to GB number."""
        if not memory:
            return ''
        
        # Extract number from "16GB" format
        ram_match = re.search(r'(\d+)', str(memory))
        if ram_match:
            return ram_match.group(1)
        
        return str(memory)
    
    def _standardize_storage(self, storage: str) -> str:
        """Convert storage to GB."""
        if not storage:
            return ''
        
        storage_str = str(storage).upper()
        
        # Extract number and unit
        storage_match = re.search(r'(\d+)(GB|TB)', storage_str)
        if storage_match:
            number = int(storage_match.group(1))
            unit = storage_match.group(2)
            
            if unit == 'TB':
                return str(number * 1000)  # Convert TB to GB
            else:
                return str(number)
        
        return str(storage)
    
    def _standardize_gpu(self, gpu_cores: str) -> str:
        """Extract GPU core count."""
        if not gpu_cores:
            return ''
        
        # Extract number from "10-Core GPU" format
        gpu_match = re.search(r'(\d+)', str(gpu_cores))
        if gpu_match:
            return gpu_match.group(1)
        
        return str(gpu_cores)
    
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
    
    def _extract_year_from_chip(self, chip: str) -> str:
        """Estimate year based on chip generation."""
        if not chip:
            return ''
        
        # Rough mapping of M-series chips to years
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
            'M4': '2024',
            'M4 Pro': '2024',
            'M4 Max': '2024'
        }
        
        return year_mapping.get(chip, '')
    
    def standardize_apple_product(self, apple_product: Dict) -> Dict:
        """Convert single Apple product to user's format."""
        standardized = {}
        
        # Map direct fields
        for apple_field, user_field in self.field_mapping.items():
            value = apple_product.get(apple_field, '')
            
            if user_field == 'RAM (GB)':
                value = self._standardize_ram(value)
            elif user_field == 'HD (GB)':
                value = self._standardize_storage(value)
            elif user_field == 'CPU Cores':
                value = self._standardize_cpu_cores(value)
            elif user_field == 'GPU':
                value = self._standardize_gpu(value)
            elif user_field == 'Colour':
                value = self._standardize_colour(value)
            
            standardized[user_field] = value
        
        # Add fixed values
        standardized.update(self.fixed_values)
        
        # Compute derived fields
        machine_type = self._standardize_machine_type(apple_product.get('name', ''))
        standardized['Machine'] = machine_type
        standardized['Model'] = self._standardize_model(apple_product.get('chip', ''))
        standardized['Year'] = self._extract_year_from_chip(apple_product.get('chip', ''))
        
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
        
        # Lookup Variant ID
        standardized['Variant ID'] = self._lookup_variant_id(standardized)
        
        return standardized
    
    def _lookup_variant_id(self, standardized_product: Dict) -> str:
        """Look up Variant ID based on specifications."""
        if not self.variant_lookup:
            return ''  # No lookup table loaded
        
        spec_key = self._create_spec_key(standardized_product)
        variant_id = self.variant_lookup.get(spec_key, '')
        
        if variant_id:
            print(f"✅ Found Variant ID {variant_id} for {standardized_product.get('Title', 'Unknown')[:50]}...")
        else:
            print(f"❓ No Variant ID found for {standardized_product.get('Title', 'Unknown')[:50]}...")
            print(f"   Spec key: {spec_key}")
        
        return str(variant_id) if variant_id else ''
    
    def standardize_apple_data(self, apple_products: List[Dict]) -> List[Dict]:
        """Convert list of Apple products to user's format."""
        print(f"🔄 Standardizing {len(apple_products)} Apple products...")
        
        standardized_products = []
        for i, product in enumerate(apple_products, 1):
            print(f"   [{i}/{len(apple_products)}] Processing: {product.get('name', 'Unknown')[:50]}...")
            standardized = self.standardize_apple_product(product)
            standardized_products.append(standardized)
        
        print(f"✅ Standardized {len(standardized_products)} products")
        return standardized_products
    
    def save_standardized_data_to_sheets(self, standardized_products: List[Dict], google_client=None, spreadsheet_id: str = None, sheet_name: str = "Apple Products Standardized") -> bool:
        """Save standardized data directly to Google Sheets in user's format."""
        if not standardized_products:
            print("❌ No standardized products to save")
            return False
        
        if not google_client:
            print("❌ Google Sheets client not available")
            return False
        
        try:
            # Get or create the spreadsheet
            if spreadsheet_id:
                spreadsheet = google_client.open_by_key(spreadsheet_id)
            else:
                print("❌ No spreadsheet ID provided")
                return False
            
            # Define column order to match user's format (A to U)
            column_order = [
                'Title', 'Condition', 'Price', 'Change', 'URL', 'Machine', 'Model', 'Year',
                'CPU', 'CPU Cores', 'HD (GB)', 'RAM (GB)', 'GPU', 'Colour', 'Grade', 
                'Seller', 'Warning', 'Timestamp', 'Timestamp Day', 'Timestamp Day', 'Variant ID'
            ]
            
            # Try to get existing sheet or create new one
            try:
                sheet = spreadsheet.worksheet(sheet_name)
                print(f"📊 Using existing sheet: {sheet_name}")
            except:
                sheet = spreadsheet.add_worksheet(title=sheet_name, rows=1000, cols=21)
                print(f"🆕 Created new sheet: {sheet_name}")
            
            # Clear existing data
            sheet.clear()
            
            # Prepare data for upload
            df = pd.DataFrame(standardized_products)
            
            # Ensure all required columns exist
            for col in column_order:
                if col not in df.columns:
                    df[col] = ''
            
            # Reorder columns
            df = df[column_order]
            
            # Convert to list format for Google Sheets
            data = [column_order]  # Headers first
            
            for _, row in df.iterrows():
                row_data = []
                for col in column_order:
                    value = row[col]
                    if pd.isna(value) or value is None:
                        row_data.append('')
                    else:
                        row_data.append(str(value))
                data.append(row_data)
            
            # Upload all data in one batch operation
            sheet.update(data, value_input_option='USER_ENTERED')
            
            # Format the header row
            sheet.format('A1:U1', {
                'backgroundColor': {'red': 0.2, 'green': 0.2, 'blue': 0.2},
                'textFormat': {'foregroundColor': {'red': 1, 'green': 1, 'blue': 1}, 'bold': True}
            })
            
            # Format price column (C) as currency
            sheet.format('C2:C1000', {
                'numberFormat': {'type': 'CURRENCY', 'pattern': '£#,##0.00'}
            })
            
            # Format storage and RAM columns as numbers
            sheet.format('K2:L1000', {  # HD (GB) and RAM (GB)
                'numberFormat': {'type': 'NUMBER', 'pattern': '#,##0'}
            })
            
            print(f"✅ Successfully uploaded {len(standardized_products)} standardized products to Google Sheets")
            print(f"📊 Sheet: {sheet_name}")
            print(f"🔗 URL: https://docs.google.com/spreadsheets/d/{spreadsheet_id}")
            
            return True
            
        except Exception as e:
            print(f"❌ Error uploading to Google Sheets: {e}")
            return False
    
    def save_standardized_data(self, standardized_products: List[Dict], filename: str = "apple_products_standardized.csv") -> str:
        """Save standardized data to CSV in user's format (backup method)."""
        if not standardized_products:
            print("❌ No standardized products to save")
            return ""
        
        # Define column order to match user's format (A to U)
        column_order = [
            'Title', 'Condition', 'Price', 'Change', 'URL', 'Machine', 'Model', 'Year',
            'CPU', 'CPU Cores', 'HD (GB)', 'RAM (GB)', 'GPU', 'Colour', 'Grade', 
            'Seller', 'Warning', 'Timestamp', 'Timestamp Day', 'Timestamp Day', 'Variant ID'
        ]
        
        df = pd.DataFrame(standardized_products)
        
        # Ensure all required columns exist
        for col in column_order:
            if col not in df.columns:
                df[col] = ''
        
        # Reorder columns
        df = df[column_order]
        
        # Save to CSV
        df.to_csv(filename, index=False)
        print(f"💾 Saved {len(standardized_products)} standardized products to {filename}")
        
        return filename
    
    def create_comparison_report(self, standardized_products: List[Dict], existing_data_file: str = None) -> Dict:
        """Create a comparison report between Apple data and existing data."""
        comparison_report = {
            'total_apple_products': len(standardized_products),
            'products_with_variant_ids': 0,
            'potential_matches': [],
            'price_differentials': [],
            'new_products': []
        }
        
        # Count products with Variant IDs
        products_with_variants = [p for p in standardized_products if p.get('Variant ID')]
        comparison_report['products_with_variant_ids'] = len(products_with_variants)
        
        if existing_data_file and products_with_variants:
            try:
                # Load existing data for comparison
                existing_df = pd.read_csv(existing_data_file)
                
                # Compare prices for matching Variant IDs
                for apple_product in products_with_variants:
                    variant_id = apple_product.get('Variant ID')
                    apple_price = float(apple_product.get('Price', 0) or 0)
                    
                    # Find matching product in existing data
                    existing_match = existing_df[existing_df['Variant ID'] == int(variant_id)]
                    
                    if not existing_match.empty and apple_price > 0:
                        existing_price = float(existing_match.iloc[0].get('Price', 0) or 0)
                        
                        if existing_price > 0:
                            price_diff = apple_price - existing_price
                            price_diff_percent = (price_diff / existing_price) * 100
                            
                            comparison_report['price_differentials'].append({
                                'variant_id': variant_id,
                                'product_name': apple_product.get('Title'),
                                'apple_price': apple_price,
                                'existing_price': existing_price,
                                'difference': price_diff,
                                'difference_percent': price_diff_percent
                            })
                
            except Exception as e:
                print(f"⚠️ Could not load existing data for comparison: {e}")
        
        return comparison_report
    
    def print_comparison_summary(self, comparison_report: Dict):
        """Print a summary of the comparison results."""
        print(f"\n📊 COMPARISON SUMMARY")
        print(f"=" * 50)
        print(f"📦 Total Apple products: {comparison_report['total_apple_products']}")
        print(f"🔗 Products with Variant IDs: {comparison_report['products_with_variant_ids']}")
        
        price_diffs = comparison_report['price_differentials']
        if price_diffs:
            print(f"💰 Price comparisons found: {len(price_diffs)}")
            
            # Show significant price differences
            significant_diffs = [p for p in price_diffs if abs(p['difference_percent']) > 5]
            if significant_diffs:
                print(f"⚠️  Significant price differences (>5%):")
                for diff in significant_diffs[:5]:  # Show top 5
                    print(f"   • {diff['product_name'][:50]}...")
                    print(f"     Apple: £{diff['apple_price']:.2f} vs Existing: £{diff['existing_price']:.2f}")
                    print(f"     Difference: £{diff['difference']:.2f} ({diff['difference_percent']:+.1f}%)")
        else:
            print(f"💰 No price comparisons available (no matching Variant IDs)")


def main():
    """Main function to demonstrate the standardization process with Google Sheets output."""
    print("🍎 Apple Data Standardizer - Google Sheets Integration")
    print("=" * 60)
    
    # Initialize standardizer
    standardizer = AppleDataStandardizer()
    
    # Load Apple data from latest CSV
    try:
        apple_df = pd.read_csv("mac_products_v7_historical.csv")
        apple_products = apple_df.to_dict('records')
        print(f"📊 Loaded {len(apple_products)} Apple products")
    except Exception as e:
        print(f"❌ Error loading Apple data: {e}")
        return
    
    # Initialize Google Sheets (using same config as histv7.py)
    google_client = None
    spreadsheet_id = "1j7npqR9I303etrf67HYkXU6m87DEwg66j0dmqxpMllQ"  # From histv7.py
    
    try:
        import gspread
        from oauth2client.service_account import ServiceAccountCredentials
        import os
        
        if os.path.exists("credentials.json"):
            print("📁 Found credentials.json file")
            scope = ['https://spreadsheets.google.com/feeds',
                    'https://www.googleapis.com/auth/drive']
            
            print("🔐 Authenticating with Google Sheets...")
            creds = ServiceAccountCredentials.from_json_keyfile_name("credentials.json", scope)
            google_client = gspread.authorize(creds)
            print("✅ Google Sheets connection established")
            
            # Test connection by trying to open the spreadsheet
            try:
                test_sheet = google_client.open_by_key(spreadsheet_id)
                print(f"✅ Successfully connected to spreadsheet: {test_sheet.title}")
            except Exception as e:
                print(f"❌ Cannot access spreadsheet: {e}")
                google_client = None
                
        else:
            print("⚠️ credentials.json not found - will save to CSV instead")
    except ImportError as e:
        print(f"⚠️ Google Sheets libraries not available: {e} - will save to CSV instead")
    except Exception as e:
        print(f"⚠️ Google Sheets setup failed: {e} - will save to CSV instead")
    
    # TODO: Load Variant ID lookup table
    # standardizer.load_variant_lookup_table("variant_lookup.csv")
    
    # Standardize the data
    standardized_products = standardizer.standardize_apple_data(apple_products)
    
    # Save to Google Sheets if available, otherwise CSV
    if google_client:
        success = standardizer.save_standardized_data_to_sheets(
            standardized_products, 
            google_client, 
            spreadsheet_id, 
            "Apple Products Standardized"
        )
        if not success:
            print("📄 Falling back to CSV...")
            output_file = standardizer.save_standardized_data(standardized_products)
    else:
        output_file = standardizer.save_standardized_data(standardized_products)
        print(f"📄 CSV Output file: {output_file}")
    
    # Create comparison report
    comparison_report = standardizer.create_comparison_report(standardized_products)
    standardizer.print_comparison_summary(comparison_report)
    
    print(f"\n✅ Standardization complete!")


if __name__ == "__main__":
    main()
