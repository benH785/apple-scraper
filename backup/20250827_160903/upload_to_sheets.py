#!/usr/bin/env python3
"""
Direct upload of standardized Apple data to Google Sheets
"""

import pandas as pd
import gspread
from oauth2client.service_account import ServiceAccountCredentials
import os

def upload_standardized_data_to_sheets():
    """Upload the standardized Apple data directly to Google Sheets."""
    
    # Load the standardized CSV data
    try:
        df = pd.read_csv("apple_products_standardized.csv")
        print(f"📊 Loaded {len(df)} standardized Apple products")
    except Exception as e:
        print(f"❌ Error loading standardized data: {e}")
        return False
    
    # Connect to Google Sheets
    try:
        scope = ['https://spreadsheets.google.com/feeds', 'https://www.googleapis.com/auth/drive']
        creds = ServiceAccountCredentials.from_json_keyfile_name('credentials.json', scope)
        client = gspread.authorize(creds)
        
        # Open the spreadsheet
        spreadsheet_id = "1j7npqR9I303etrf67HYkXU6m87DEwg66j0dmqxpMllQ"
        spreadsheet = client.open_by_key(spreadsheet_id)
        print(f"✅ Connected to: {spreadsheet.title}")
        
        # Create or get the "Apple Products Standardized" sheet
        sheet_name = "Apple Products Standardized"
        try:
            sheet = spreadsheet.worksheet(sheet_name)
            print(f"📊 Using existing sheet: {sheet_name}")
        except:
            sheet = spreadsheet.add_worksheet(title=sheet_name, rows=1000, cols=21)
            print(f"🆕 Created new sheet: {sheet_name}")
        
        # Clear existing data
        sheet.clear()
        print("🧹 Cleared existing data")
        
        # Convert DataFrame to list format for Google Sheets
        # Get headers
        headers = df.columns.tolist()
        
        # Convert all data to strings and handle NaN values
        data = [headers]  # Start with headers
        
        for _, row in df.iterrows():
            row_data = []
            for col in headers:
                value = row[col]
                if pd.isna(value) or value is None:
                    row_data.append('')
                else:
                    row_data.append(str(value))
            data.append(row_data)
        
        # Upload all data in one batch operation
        print("📤 Uploading data to Google Sheets...")
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
        
        print(f"✅ Successfully uploaded {len(df)} products to Google Sheets!")
        print(f"📊 Sheet: {sheet_name}")
        print(f"🔗 URL: https://docs.google.com/spreadsheets/d/{spreadsheet_id}")
        
        return True
        
    except Exception as e:
        print(f"❌ Error uploading to Google Sheets: {e}")
        return False

if __name__ == "__main__":
    print("🍎 Uploading Apple Products to Google Sheets")
    print("=" * 50)
    upload_standardized_data_to_sheets()
