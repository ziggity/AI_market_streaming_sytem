#!/usr/bin/env python3
"""
Historical + Live Market Data Stream (Version 2)

Downloads 5 years of historical data, then continuously updates with live prices.
This creates a comprehensive dataset combining historical context with real-time data.

Usage:
    uv run historical_live_stream.py
    
Features:
- Downloads 5 years of historical data on startup
- Appends live updates every 10 seconds
- Single CSV file with complete market history
"""

import yfinance as yf
import pandas as pd
import time
import csv
import os
from datetime import datetime, timedelta

print("="*80)
print("HISTORICAL + LIVE MARKET DATA STREAM (VERSION 2)")
print("="*80)

# Configuration
TICKERS = ["AAPL", "MSFT", "GOOGL"]
LIVE_INTERVAL_SECONDS = 10
OUTPUT_DIR = 'output'
HISTORICAL_YEARS = 5

# Create output directory
os.makedirs(OUTPUT_DIR, exist_ok=True)

# Setup CSV file
timestamp = time.strftime('%Y%m%d_%H%M%S')
csv_filename = f'{OUTPUT_DIR}/market_data_{timestamp}.csv'

print(f"\n📊 Configuration:")
print(f"   Tickers: {', '.join(TICKERS)}")
print(f"   Historical: {HISTORICAL_YEARS} years")
print(f"   Live interval: {LIVE_INTERVAL_SECONDS}s")
print(f"   Output: {csv_filename}")
print(f"\n⏳ Downloading historical data... (this may take a minute)")
print("="*80)

# Step 1: Download historical data
def download_historical_data(tickers, years=5):
    """Download historical data for the past N years"""
    end_date = datetime.now()
    start_date = end_date - timedelta(days=years*365)
    
    all_data = []
    
    for ticker in tickers:
        print(f"   Downloading {ticker}...")
        try:
            stock = yf.Ticker(ticker)
            hist = stock.history(start=start_date, end=end_date, interval='1d')
            
            if not hist.empty:
                # Use Close price for historical data
                hist = hist[['Close']].copy()
                hist.columns = [f'{ticker}_price']
                hist['timestamp'] = hist.index
                hist = hist.reset_index(drop=True)
                all_data.append(hist)
                print(f"   ✅ {ticker}: {len(hist)} days of data")
            else:
                print(f"   ⚠️  {ticker}: No data available")
        except Exception as e:
            print(f"   ❌ {ticker}: Error - {e}")
    
    if not all_data:
        print("\n❌ ERROR: No historical data downloaded")
        exit(1)
    
    # Merge all tickers by timestamp
    merged_data = all_data[0]
    for data in all_data[1:]:
        merged_data = pd.merge(merged_data, data, on='timestamp', how='outer')
    
    merged_data = merged_data.sort_values('timestamp').reset_index(drop=True)
    
    # Remove timezone info to match live data format
    merged_data['timestamp'] = merged_data['timestamp'].dt.tz_localize(None)
    
    merged_data['iteration'] = range(1, len(merged_data) + 1)
    
    # Reorder columns
    cols = ['timestamp', 'iteration'] + [col for col in merged_data.columns if col.endswith('_price')]
    merged_data = merged_data[cols]
    
    return merged_data

# Download historical data
historical_df = download_historical_data(TICKERS, HISTORICAL_YEARS)

# Format timestamp as string with consistent datetime format
historical_df['timestamp'] = historical_df['timestamp'].dt.strftime('%Y-%m-%d %H:%M:%S')

# Save to CSV
historical_df.to_csv(csv_filename, index=False)

print(f"\n✅ Historical data saved: {len(historical_df)} rows")
print(f"   Date range: {historical_df['timestamp'].iloc[0]} to {historical_df['timestamp'].iloc[-1]}")
print(f"\n🔴 LIVE STREAMING STARTED")
print(f"   Appending live updates every {LIVE_INTERVAL_SECONDS}s")
print(f"   Press Ctrl+C to stop")
print("="*80)

# Step 2: Start live streaming
iteration = len(historical_df) + 1

try:
    while True:
        current_time = datetime.now().strftime('%Y-%m-%d %H:%M:%S')
        
        print(f"\n⏰ Live Update #{iteration - len(historical_df)} - {time.strftime('%H:%M:%S')}")
        print("-" * 60)
        
        # Fetch current prices
        prices = {}
        for ticker in TICKERS:
            try:
                stock = yf.Ticker(ticker)
                price = stock.info.get('currentPrice') or stock.info.get('regularMarketPrice')
                
                if price:
                    prices[ticker] = price
                    print(f"  {ticker:6s}     $  {price:>8.2f}")
                else:
                    prices[ticker] = None
                    print(f"  {ticker:6s}     N/A")
            except Exception as e:
                prices[ticker] = None
                print(f"  {ticker:6s}     Error: {e}")
        
        # Write to CSV
        with open(csv_filename, 'a', newline='') as f:
            writer = csv.writer(f)
            row = [current_time, iteration] + [prices.get(t) for t in TICKERS]
            writer.writerow(row)
        
        print(f"✅ Written to CSV (Total rows: {iteration})")
        print(f"⏳ Next update in {LIVE_INTERVAL_SECONDS}s...")
        
        iteration += 1
        time.sleep(LIVE_INTERVAL_SECONDS)
        
except KeyboardInterrupt:
    print(f"\n\n⚠️  Streaming stopped by user")
    print(f"✅ Total rows: {iteration - 1}")
    print(f"   Historical: {len(historical_df)}")
    print(f"   Live updates: {iteration - len(historical_df) - 1}")
    print(f"📁 Data saved to: {csv_filename}")
    print("="*80)
