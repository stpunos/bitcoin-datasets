# Bitcoin Open Data Platform

This repository serves as a public database for Bitcoin data, designed for ML projects and research. It includes automated pipelines to fetch, clean, and publish data from various sources.

## 🚀 Production Status

✅ **Currently Active**: CoinDesk data fetching (runs every 4 hours via GitHub Actions)
⏸️ **Paused**: NewHedge scraping (pending testing)

## Data Sources

### 1. **CoinDesk** (via CryptoCompare API) - ✅ Active
- **Price Data**: Real-time BTC/USD pricing
- **Historical OHLCV**: Daily and hourly candlestick data (2000 bars)
- **Blockchain Metrics**: Balance distribution across addresses
- **Trading Signals**: IntoTheBlock sentiment indicators
- **Social Data**: Twitter and Reddit engagement metrics
- **News**: Latest Bitcoin news articles

### 2. **NewHedge** - ⏸️ Coming Soon
- Comprehensive Bitcoin dashboard metrics (Dominance, Hashrate, ETF Holdings, On-chain data)
- Data scraped using [Firecrawl](https://firecrawl.dev)

## 🏗️ Architecture

- **Automation**: GitHub Actions runs every 4 hours (configurable via cron)
- **Data Fetching**: Always fetches 2000 rows and merges with existing data
- **Storage Strategy**:
  - **CSV Files**: Stored in `data/` directory (publicly accessible via GitHub)
  - **Snowflake Database**: Automatic upload with MERGE operations on unique keys
- **Logging**: Comprehensive console logging for monitoring and debugging

## 📁 Directory Structure

```
bitcoin-datasets/
├── data/
│   └── coindesk/              # CoinDesk data CSVs
│       ├── pricemultifull.csv           # Current BTC/USD price
│       ├── histoday.csv                 # Daily OHLCV (2000 bars)
│       ├── histohour.csv                # Hourly OHLCV (2000 bars)
│       ├── blockchain_balancedistribution.csv  # Address balance distribution
│       ├── tradingsignals.csv           # Trading sentiment signals
│       ├── hourly_social_data.csv       # Social media metrics
│       └── news.csv                     # Latest Bitcoin news
├── scripts/
│   ├── fetch_coindesk.py      # Main data fetcher (uses CryptoCompare API)
│   ├── fetch_newhedge.py      # NewHedge scraper (paused)
│   ├── config.yml             # API endpoint configurations
│   └── update_snowflake.py    # Snowflake uploader (deprecated - now integrated)
├── migrations/                 # Snowflake schema migrations
├── .github/
│   └── workflows/
│       └── update_data.yml    # GitHub Actions workflow
├── requirements.txt           # Python dependencies
├── .env.example              # Environment variables template
└── README.md                 # This file
```

## 🛠️ Setup & Usage

### Local Development

#### 1. Clone the Repository

```bash
git clone https://github.com/yourusername/bitcoin-datasets.git
cd bitcoin-datasets
```

#### 2. Install Dependencies

```bash
python -m venv venv
source venv/bin/activate  # On Windows: venv\Scripts\activate
pip install -r requirements.txt
```

#### 3. Configure Environment Variables

Copy the example environment file and fill in your credentials:

```bash
cp .env.example .env
```

Edit `.env` with your credentials:

```ini
# CryptoCompare API Key (required)
# Get free key at: https://min-api.cryptocompare.com/
CRYPTOCOMPARE_API_KEY=your_api_key_here
API_KEY=your_api_key_here

# Snowflake Credentials (optional for local testing)
SNOWFLAKE_USER=your_username
SNOWFLAKE_PASSWORD=your_password
SNOWFLAKE_ACCOUNT=your_account
SNOWFLAKE_WAREHOUSE=COMPUTE_WH
SNOWFLAKE_DATABASE=BITCOIN_DATA
SNOWFLAKE_SCHEMA=PUBLIC
```

#### 4. Run Data Fetcher

```bash
python scripts/fetch_coindesk.py
```

The script will:
- ✅ Fetch data from CryptoCompare API (always 2000 rows)
- ✅ Upload to Snowflake (if credentials configured)
- ✅ Merge with existing data using unique keys
- ✅ Export CSV files to `data/coindesk/`
- ✅ Log all operations to console

### 🚀 Production Deployment (GitHub Actions)

#### 1. Fork/Clone this Repository

#### 2. Configure GitHub Secrets

Go to your repository Settings → Secrets and variables → Actions, and add:

**Required Secrets:**
- `CRYPTOCOMPARE_API_KEY` - Your CryptoCompare API key

**Optional (for Snowflake integration):**
- `SNOWFLAKE_USER`
- `SNOWFLAKE_PASSWORD`
- `SNOWFLAKE_ACCOUNT`
- `SNOWFLAKE_WAREHOUSE`
- `SNOWFLAKE_DATABASE`
- `SNOWFLAKE_SCHEMA`

#### 3. Enable GitHub Actions

The workflow will automatically run:
- **Schedule**: Every 4 hours
- **Manual**: Via "Actions" tab → "Run workflow"

#### 4. Monitor Workflow

- Check the "Actions" tab to view workflow runs
- Data files will be automatically committed to the `data/` directory
- Each commit includes "[skip ci]" to prevent recursive builds

## 📊 Dataset Details

### CoinDesk Data (via CryptoCompare)

All datasets are updated every 4 hours and stored in `data/coindesk/`:

| Dataset | Description | Update Frequency |
|---------|------------ |------------------|
| `pricemultifull.csv` | Current BTC/USD price with volume and market cap | Every 4 hours |
| `histoday.csv` | Daily OHLCV candlestick data | Every 4 hours |
| `histohour.csv` | Hourly OHLCV candlestick data | Every 4 hours |
| `blockchain_balancedistribution.csv` | Bitcoin address balance distribution | Every 4 hours |
| `tradingsignals.csv` | Trading sentiment signals (IntoTheBlock) | Every 4 hours |
| `hourly_social_data.csv` | Twitter/Reddit social metrics | Every 4 hours |
| `news.csv` | Latest Bitcoin news articles | Every 4 hours |

### Data Fields

#### Trading Signals
- `INOUTVAR_SENTIMENT/VALUE` - In/Out the Money analysis
- `LTHANDSTH_SENTIMENT/VALUE` - Long-term vs Short-term holder metrics
- `CONCENTRATION_SENTIMENT/VALUE` - Whale concentration indicators
- `LARGESURPLUS_SENTIMENT/VALUE` - Large transaction surplus

#### Blockchain Balance Distribution
- Distribution of BTC across address ranges (0.001 to 100,000+ BTC)
- Total volume and address count per range
- Historical daily snapshots

## 🤝 Contributing

Contributions are welcome! To add new data sources:

1. Fork the repository
2. Create a new fetcher in `scripts/`
3. Add configuration to GitHub Actions workflow
4. Submit a pull request

## 📝 License

This project is open source and available for public use. Data is sourced from public APIs.

## 🔗 Resources

- **CryptoCompare API**: https://min-api.cryptocompare.com/
- **Snowflake**: https://www.snowflake.com/
- **GitHub Actions**: https://docs.github.com/en/actions

---

**Built for open-source contribution** | Data updated every 4 hours via GitHub Actions 🤖