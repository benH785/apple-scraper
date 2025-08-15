#!/usr/bin/env python3
"""
Simple Google Sheets Quota Test
This script makes minimal API calls to test quota limits and permissions.
"""

import time
import random
from datetime import datetime

try:
    import gspread
    from oauth2client.service_account import ServiceAccountCredentials
    GOOGLE_SHEETS_AVAILABLE = True
except ImportError:
    GOOGLE_SHEETS_AVAILABLE = False
    print("❌ Google Sheets libraries not installed!")
    exit(1)

# CONFIGURATION
GOOGLE_SHEET_ID = "1j7npqR9I303etrf67HYkXU6m87DEwg66j0dmqxpMllQ"
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

def test_sheets_access():
    """Test basic Google Sheets access with minimal API calls."""
    print("🧪 Testing Google Sheets Access...")
    print("=" * 50)
    
    try:
        # Setup credentials
        scope = ['https://spreadsheets.google.com/feeds',
                'https://www.googleapis.com/auth/drive']
        
        creds = ServiceAccountCredentials.from_json_keyfile_name(CREDENTIALS_FILE, scope)
        client = gspread.authorize(creds)
        print("✅ Credentials loaded successfully")
        
        # Open spreadsheet
        spreadsheet = client.open_by_key(GOOGLE_SHEET_ID)
        print(f"✅ Opened spreadsheet: {spreadsheet.title}")
        
        # List worksheets (1 API call)
        worksheets = sheets_api_call_with_backoff(spreadsheet.worksheets)
        print(f"✅ Found {len(worksheets)} worksheets:")
        for ws in worksheets:
            print(f"   - {ws.title}")
        
        # Test read access (1 API call)
        first_sheet = worksheets[0]
        print(f"\n🔍 Testing read access on '{first_sheet.title}'...")
        
        try:
            data = sheets_api_call_with_backoff(first_sheet.get, 'A1:C3')
            print(f"✅ Read access successful - got {len(data)} rows")
        except Exception as e:
            print(f"❌ Read access failed: {e}")
            return False
        
        # Test write access with minimal data (1 API call)
        print(f"\n✏️  Testing write access...")
        test_data = [["Test", "Timestamp", datetime.now().strftime("%Y-%m-%d %H:%M:%S")]]
        
        try:
            # Find an empty area to test write (avoid overwriting data)
            sheets_api_call_with_backoff(
                first_sheet.update,
                'Z1:AB1',  # Use far right columns to avoid data
                test_data,
                value_input_option='USER_ENTERED'
            )
            print("✅ Write access successful")
            
            # Clean up the test data
            sheets_api_call_with_backoff(first_sheet.update, 'Z1:AB1', [[""]])
            print("✅ Cleanup successful")
            
        except Exception as e:
            print(f"❌ Write access failed: {e}")
            return False
        
        print(f"\n🎉 All tests passed! Sheet is accessible and writable.")
        print(f"📊 Total API calls made: ~5 (well under quota)")
        return True
        
    except Exception as e:
        print(f"❌ Setup failed: {e}")
        return False

if __name__ == "__main__":
    success = test_sheets_access()
    if success:
        print(f"\n✅ Sheet access confirmed. The quota issue is likely from:")
        print(f"   1. Too many API calls in the main scraper")
        print(f"   2. Previous quota accumulation")
        print(f"   3. Multiple concurrent operations")
        print(f"\n💡 Recommendation: Wait 1 minute, then try the main scraper")
    else:
        print(f"\n❌ Basic sheet access failed. Check:")
        print(f"   1. credentials.json file exists and is valid")
        print(f"   2. Sheet ID is correct: {GOOGLE_SHEET_ID}")
        print(f"   3. Service account has edit permissions on the sheet")
