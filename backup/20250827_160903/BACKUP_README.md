# Apple Data Standardization System Backup
**Created:** 2025-08-27 16:09:03  
**Status:** Production Ready ✅

## 📋 System Overview

This backup contains a complete Apple data standardization system that converts Apple refurbished Mac product data into a standardized format compatible with your React dashboard and Google Sheets.

## 🗂️ Files Included

### Core Scripts
- **`apple_data_standardizer.py`** - Main standardization script that converts Apple scraper data to your dashboard format
- **`upload_to_sheets.py`** - Standalone script to upload standardized data to Google Sheets
- **`histv7.py`** - Main Apple scraper that collects product data from Apple UK website

### Data Files
- **`apple_products_standardized.csv`** - Latest standardized data (208 products) ready for dashboard
- **`mac_products_v7_historical.csv`** - Raw Apple scraper data source
- **`requirements.txt`** - Python dependencies

### Configuration
- **`credentials.json`** - Google Sheets API credentials (if available)

## 🎯 Current System State

### Data Quality ✅
- **208 Apple products** fully standardized
- **Machine types** correctly detected with screen sizes (MacBook Air 13-inch, etc.)
- **Storage format** properly formatted (256, 512, 1TB, 2TB, etc.)
- **Year column** complete including Mac Studio M3 Ultra (2025)
- **Unicode characters** properly handled in product names

### Google Sheets Integration ✅
- Connected to spreadsheet: `1j7npqR9I303etrf67HYkXU6m87DEwg66j0dmqxpMllQ`
- Sheet: "Apple Products Standardized"
- All 21 columns (A-U) matching React dashboard format
- Proper formatting applied (currency, headers, numbers)

### Field Mapping
| Apple Data | Dashboard Column | Notes |
|------------|------------------|-------|
| name | Title | Full product name |
| - | Condition | Fixed: "Refurbished" |
| current_price | Price | Numeric price |
| - | Change | Fixed: 0 |
| url | URL | Product URL |
| Derived | Machine | e.g., "MacBook Air 13-inch" |
| chip | Model | e.g., "M4", "M3 Pro" |
| Derived | Year | Based on chip (M4=2024, etc.) |
| chip | CPU | Same as Model |
| cpu_cores | CPU Cores | Numeric |
| storage | HD (GB) | 256, 512, 1TB, 2TB format |
| memory | RAM (GB) | Numeric |
| gpu_cores | GPU | Numeric |
| color | Colour | Standardized colors |
| - | Grade | Fixed: "Excellent" |
| - | Seller | Fixed: "Apple" |
| scraped_at | Timestamp | ISO format |

## 🚀 Usage Instructions

### Run Standardization
```bash
python3 apple_data_standardizer.py
```

### Upload to Google Sheets
```bash
python3 upload_to_sheets.py
```

### Dependencies
```bash
pip install -r requirements.txt
```

## 🔄 Next Steps

1. **Variant ID Integration** - Add your SKU lookup table for price comparison
2. **Price Alerts** - Implement differential alerts for overlapping products
3. **Automation** - Integrate into daily scraper workflow
4. **Dashboard Integration** - Connect to React dashboard data source

## 📊 Data Statistics

- **Total Products:** 208
- **Machine Types:** MacBook Air (46), MacBook Pro (42), iMac (90), Mac Studio (25), Mac mini (5)
- **Chip Types:** M4 (71), M3 (55), M3 Max (21), M1 (12), M3 Pro (8), etc.
- **Storage Range:** 256GB to 8TB
- **Price Range:** £509 - £4,999

## 🔧 System Requirements

- Python 3.7+
- Google Sheets API access
- Internet connection for scraping and uploads
- Valid `credentials.json` for Google Sheets

---
**System Status:** Fully functional and production ready ✅
