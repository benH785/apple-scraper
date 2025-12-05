#!/usr/bin/env python3
"""
Migrate existing History tab data to Standardized History format.
This script reads data from the History tab, converts it to standardized format,
and appends it to the Standardized History tab.
"""

import gspread
from oauth2client.service_account import ServiceAccountCredentials
import os
from datetime import datetime

# Import the standardizer from histv7
import sys
sys.path.append(os.path.dirname(os.path.abspath(__file__)))
from histv7 import AppleDataStandardizer

# Configuration
GOOGLE_SHEET_ID = "1j7npqR9I303etrf67HYkXU6m87DEwg66j0dmqxpMllQ"
CREDENTIALS_FILE = "credentials.json"
HISTORY_SHEET_NAME = "History"
STANDARDIZED_HISTORY_SHEET_NAME = "Standardized History"


def setup_google_sheets():
    """Set up Google Sheets connection."""
    try:
        if not os.path.exists(CREDENTIALS_FILE):
            print(f"❌ Credentials file '{CREDENTIALS_FILE}' not found!")
            return None
        
        scope = ['https://spreadsheets.google.com/feeds',
                'https://www.googleapis.com/auth/drive']
        
        creds = ServiceAccountCredentials.from_json_keyfile_name(CREDENTIALS_FILE, scope)
        google_client = gspread.authorize(creds)
        print("✅ Google Sheets connection established")
        
        return google_client
        
    except Exception as e:
        print(f"❌ Failed to set up Google Sheets: {e}")
        return None


def get_history_data(google_client):
    """Load all data from the History tab."""
    try:
        spreadsheet = google_client.open_by_key(GOOGLE_SHEET_ID)
        history_sheet = spreadsheet.worksheet(HISTORY_SHEET_NAME)
        
        # Get all records as dictionaries
        records = history_sheet.get_all_records()
        print(f"✅ Loaded {len(records)} records from History tab")
        
        return records, spreadsheet
        
    except Exception as e:
        print(f"❌ Error loading History data: {e}")
        return [], None


def ensure_standardized_history_sheet(spreadsheet):
    """Ensure Standardized History sheet exists with proper headers."""
    try:
        # Try to get existing sheet
        try:
            standardized_history_sheet = spreadsheet.worksheet(STANDARDIZED_HISTORY_SHEET_NAME)
            print(f"📊 Using existing sheet: {STANDARDIZED_HISTORY_SHEET_NAME}")
        except:
            # Create new sheet
            standardized_history_sheet = spreadsheet.add_worksheet(
                title=STANDARDIZED_HISTORY_SHEET_NAME, 
                rows=5000, 
                cols=21
            )
            print(f"📊 Created new sheet: {STANDARDIZED_HISTORY_SHEET_NAME}")
            
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
        
        return standardized_history_sheet
        
    except Exception as e:
        print(f"❌ Error setting up Standardized History sheet: {e}")
        return None


def convert_history_to_standardized(history_records, google_client):
    """Convert History records to standardized format."""
    print(f"\n🔄 Converting {len(history_records)} history records to standardized format...")
    
    # Initialize standardizer with Google client for Variant ID lookup
    standardizer = AppleDataStandardizer(google_client)
    
    standardized_records = []
    
    for i, record in enumerate(history_records, 1):
        if i % 100 == 0:
            print(f"   Processing record {i}/{len(history_records)}...")
        
        try:
            # Convert the history record to standardized format
            standardized = standardizer.standardize_apple_product(record)
            standardized_records.append(standardized)
        except Exception as e:
            print(f"⚠️ Error converting record {i}: {e}")
            continue
    
    print(f"✅ Converted {len(standardized_records)} records to standardized format")
    return standardized_records


def append_to_standardized_history(standardized_history_sheet, standardized_records):
    """Append standardized records to Standardized History sheet."""
    if not standardized_records:
        print("❌ No records to append")
        return False
    
    try:
        print(f"\n📤 Appending {len(standardized_records)} records to Standardized History...")
        
        # Define standardized headers in correct order
        standardized_headers = [
            'Title', 'Condition', 'Price', 'Change', 'URL', 'Machine', 'Model', 'Year',
            'CPU', 'CPU Cores', 'HD (GB)', 'RAM (GB)', 'GPU', 'Colour', 'Grade', 
            'Seller', 'Warning', 'Timestamp', 'Timestamp Day', 'Timestamp Day', 'Variant ID'
        ]
        
        # Prepare data rows
        data = []
        
        for product in standardized_records:
            row = []
            for header in standardized_headers:
                value = product.get(header, '')
                if value is None:
                    row.append('')
                else:
                    row.append(str(value))
            data.append(row)
        
        # Batch append all data (more efficient than row-by-row)
        # Split into chunks of 1000 rows to avoid API limits
        chunk_size = 1000
        for i in range(0, len(data), chunk_size):
            chunk = data[i:i + chunk_size]
            standardized_history_sheet.append_rows(chunk, value_input_option='USER_ENTERED')
            print(f"   ✅ Appended rows {i+1} to {min(i+chunk_size, len(data))}")
        
        print(f"✅ Successfully appended {len(standardized_records)} records to Standardized History!")
        return True
        
    except Exception as e:
        print(f"❌ Error appending to Standardized History: {e}")
        return False


def main():
    """Main migration function."""
    print("🔄 History to Standardized History Migration Tool")
    print("=" * 60)
    print(f"⏰ Started at: {datetime.now().strftime('%Y-%m-%d %H:%M:%S')}\n")
    
    # Step 1: Set up Google Sheets
    print("STEP 1: Setting up Google Sheets connection")
    print("-" * 60)
    google_client = setup_google_sheets()
    if not google_client:
        return
    
    # Step 2: Load History data
    print("\nSTEP 2: Loading data from History tab")
    print("-" * 60)
    history_records, spreadsheet = get_history_data(google_client)
    if not history_records:
        print("❌ No data to migrate")
        return
    
    # Step 3: Ensure Standardized History sheet exists
    print("\nSTEP 3: Setting up Standardized History sheet")
    print("-" * 60)
    standardized_history_sheet = ensure_standardized_history_sheet(spreadsheet)
    if not standardized_history_sheet:
        return
    
    # Step 4: Convert to standardized format
    print("\nSTEP 4: Converting to standardized format")
    print("-" * 60)
    standardized_records = convert_history_to_standardized(history_records, google_client)
    if not standardized_records:
        print("❌ No records converted")
        return
    
    # Step 5: Append to Standardized History
    print("\nSTEP 5: Appending to Standardized History sheet")
    print("-" * 60)
    success = append_to_standardized_history(standardized_history_sheet, standardized_records)
    
    # Summary
    print("\n" + "=" * 60)
    if success:
        print(f"✅ MIGRATION COMPLETE!")
        print(f"📊 Migrated {len(standardized_records)} records from History to Standardized History")
        print(f"🔗 View: https://docs.google.com/spreadsheets/d/{GOOGLE_SHEET_ID}")
    else:
        print(f"❌ Migration failed")
    
    print(f"⏰ Finished at: {datetime.now().strftime('%Y-%m-%d %H:%M:%S')}")


if __name__ == "__main__":
    # Confirm before running
    print("\n⚠️  WARNING: This will append all History tab data to Standardized History.")
    print("⚠️  Make sure you haven't already migrated this data to avoid duplicates.\n")
    
    response = input("Do you want to proceed? (yes/no): ").strip().lower()
    
    if response in ['yes', 'y']:
        main()
    else:
        print("❌ Migration cancelled")
