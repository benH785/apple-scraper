# Apple Mac Refurbished Scraper - Cloud Deployment

A comprehensive scraper for Apple's UK refurbished Mac products with historical tracking and Google Sheets integration.

## Features

- 🍎 **Complete Mac Product Scraping**: Extracts all refurbished Mac products from Apple UK
- 💰 **Price Tracking**: Monitors current prices, original prices, and savings
- 📊 **Historical Analysis**: Tracks price changes and product availability over time
- 🔧 **Detailed Specs**: Extracts chip, memory, storage, display size, and color information
- 🎯 **Dashboard Integration**: Automatically generates standardized format for React dashboards
- 📈 **Dual Google Sheets Output**: 
  - Raw Apple data for historical tracking
  - Standardized format for dashboard consumption ("Apple Products Standardized" sheet)
- ⏰ **Daily Automation**: Runs automatically every day via GitHub Actions

## Cloud Deployment Setup

### Prerequisites

1. **GitHub Repository**: Push this code to a GitHub repository
2. **Google Sheets API**: Ensure your Google service account has access to your spreadsheet
3. **GitHub Secrets**: Configure the required secrets in your repository

### Step 1: Create GitHub Repository

```bash
# Initialize git repository (if not already done)
git init
git add .
git commit -m "Initial commit: Apple scraper with cloud deployment"

# Add your GitHub repository as origin
git remote add origin https://github.com/yourusername/apple-scraper.git
git push -u origin main
```

### Step 2: Configure GitHub Secrets

In your GitHub repository, go to **Settings > Secrets and variables > Actions** and add:

- **`GOOGLE_CREDENTIALS`**: Copy the entire contents of your `credentials.json` file

### Step 3: Enable GitHub Actions

The workflow is configured to run daily at 9:00 AM UTC. You can also trigger it manually:

1. Go to **Actions** tab in your GitHub repository
2. Select "Daily Apple Scraper" workflow
3. Click "Run workflow" to test immediately

## Local Development

### Using Docker

```bash
# Build the Docker image
docker build -t apple-scraper .

# Run the container
docker run --rm -v $(pwd)/credentials.json:/app/credentials.json apple-scraper
```

### Using Python directly

```bash
# Install dependencies
pip install -r requirements.txt

# Run the scraper
python histv7.py
```

## Configuration

The scraper is configured via constants in `histv7.py`:

- **`GOOGLE_SHEET_ID`**: Your Google Sheets document ID
- **`GOOGLE_SHEET_NAME`**: Name of your main sheet
- **`CURRENT_SHEET_NAME`**: Sheet for current inventory
- **`PRICE_HISTORY_SHEET_NAME`**: Sheet for price change tracking
- **`AVAILABILITY_HISTORY_SHEET_NAME`**: Sheet for availability tracking

## Data Structure

### Current Inventory Sheet
- Live snapshot of all available refurbished Mac products
- Updated completely on each run

### Price History Sheet
- Logs all price changes with timestamps
- Tracks increases, decreases, and amounts

### Availability History Sheet
- Logs when products appear or disappear
- Tracks product lifecycle

## Monitoring

- **GitHub Actions**: Check the Actions tab for run history and logs
- **Google Sheets**: Monitor data updates in your spreadsheet
- **CSV Exports**: Historical CSV files are generated for backup

## Troubleshooting

### Common Issues

1. **Google Sheets API Quota**: The scraper uses batch operations to minimize API calls
2. **Rate Limiting**: Built-in delays between requests to respect Apple's servers
3. **Credential Issues**: Ensure your service account has edit access to the spreadsheet

### Logs

Check GitHub Actions logs for detailed execution information:
- Navigation to repository > Actions > Latest workflow run
- Expand each step to see detailed logs

## Scheduling Options

Current schedule: **Daily at 9:00 AM UTC**

To modify the schedule, edit `.github/workflows/daily-scrape.yml`:

```yaml
schedule:
  # Run twice daily (9 AM and 9 PM UTC)
  - cron: '0 9,21 * * *'
  
  # Run every 6 hours
  - cron: '0 */6 * * *'
  
  # Run only on weekdays at 9 AM UTC
  - cron: '0 9 * * 1-5'
```

## Cost Considerations

- **GitHub Actions**: 2,000 minutes/month free for public repos, 500 for private
- **Google Sheets API**: 300 requests per minute per project (free tier)
- **Estimated Usage**: ~50-100 API calls per run (well within limits)

## Security

- Credentials are stored as GitHub Secrets (encrypted)
- Credentials file is automatically cleaned up after each run
- No sensitive data is logged or exposed

---

**Last Updated**: $(date)
**Version**: 7.0 (Historical Tracking)
