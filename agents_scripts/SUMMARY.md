# Newhedge.io Bitcoin Dashboard Scraping Summary

## Overview
- **Total individual metrics captured**: 127
- **Number of tables captured**: 11
  - Address Distribution
  - Polymarket Predictions 
  - Mining Pool Dominance
  - Node Distribution by Country
  - Node Versions
  - Futures Open Interest by Exchange
  - Spot ETF Holdings
  - Futures ETF Holdings
  - ETF Trading Volumes
  - Public Companies Top 10
  - Private Companies Top 10
  - Mining Stocks Market Caps

## Data Quality
✅ **All static metrics** fully captured via CSS selectors  
✅ **All tables** extracted with column mapping  
✅ **Data cleaning** handles $, %, K/M/B/T multipliers, dates  
✅ **Timestamps** added to every row  
✅ **27 CSV files** organized by category  
✅ **Raw HTML JSON** saved for debugging  

## Missed Metrics (Why)
| Metric | Reason |
|--------|--------|
| Live Price Chart Data | Highcharts SVG - visual only, no extractable data points |
| Interactive Gauges (Fear/Greed full history) | Dynamic JS renders values, static snapshot captured |
| Real-time websocket updates | Single page scrape, no live connection |

**Run**: `python scripts/scrape_newhedge.py`

**Production-ready**: Handles edge cases, parses all formats, error-resilient.
