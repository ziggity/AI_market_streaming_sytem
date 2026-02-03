#!/usr/bin/env python3
"""
Market Data Analyzer and Report Generator (Version 2)

Analyzes complete market dataset (historical + live) and generates comprehensive
markdown report with statistics, trends, and insights.

Usage:
    uv run analyze_and_report.py <csv_filename>
    uv run analyze_and_report.py <csv_filename> --watch
    
Example:
    uv run analyze_and_report.py output/market_data_20260130_164520.csv --watch
    
Features:
- Analyzes entire dataset (years of data)
- Statistical analysis: mean, median, volatility, correlations
- Trend analysis: price movements, momentum
- Generates structured markdown report
- Watch mode: regenerates report every 60 seconds as new live data arrives
"""

import sys
import os
import time
import pandas as pd
import numpy as np
from datetime import datetime

print("="*80)
print("MARKET DATA ANALYZER & REPORT GENERATOR (VERSION 2)")
print("="*80)

# Check arguments
if len(sys.argv) < 2:
    print("\n❌ ERROR: Please provide CSV filename")
    print("\nUsage:")
    print("  uv run analyze_and_report.py <csv_filename>")
    print("  uv run analyze_and_report.py <csv_filename> --watch")
    print("\nExample:")
    print("  uv run analyze_and_report.py output/market_data_20260130_164520.csv --watch")
    print("")
    sys.exit(1)

csv_filename = sys.argv[1]
watch_mode = '--watch' in sys.argv

if not os.path.exists(csv_filename):
    print(f"\n❌ ERROR: File not found: {csv_filename}")
    sys.exit(1)

# Create reports directory
REPORTS_DIR = 'reports'
os.makedirs(REPORTS_DIR, exist_ok=True)

print(f"\n📁 CSV File: {csv_filename}")
print(f"📊 Mode: {'Watch (regenerates every 60s)' if watch_mode else 'One-time analysis'}")
print("="*80 + "\n")

def analyze_data(csv_filename):
    """Perform comprehensive analysis of market data"""
    
    print("📊 Reading CSV data...")
    df = pd.read_csv(csv_filename)
    
    # Convert timestamp to datetime (simple conversion, no timezone handling needed)
    df['timestamp'] = pd.to_datetime(df['timestamp'])
    
    print(f"   Total rows: {len(df)}")
    print(f"   Date range: {df['timestamp'].min()} to {df['timestamp'].max()}")
    print(f"   Duration: {(df['timestamp'].max() - df['timestamp'].min()).days} days\n")
    
    # Get price columns
    price_cols = [col for col in df.columns if col.endswith('_price')]
    tickers = [col.replace('_price', '') for col in price_cols]
    
    print("📈 Calculating statistics...")
    
    stats = {}
    for ticker, col in zip(tickers, price_cols):
        prices = df[col].dropna()
        
        if len(prices) > 0:
            stats[ticker] = {
                'current': prices.iloc[-1],
                'mean': prices.mean(),
                'median': prices.median(),
                'min': prices.min(),
                'max': prices.max(),
                'std': prices.std(),
                'total_change': prices.iloc[-1] - prices.iloc[0],
                'pct_change': ((prices.iloc[-1] - prices.iloc[0]) / prices.iloc[0]) * 100,
                'volatility': (prices.std() / prices.mean()) * 100,
                'data_points': len(prices)
            }
    
    # Calculate correlations
    print("🔗 Calculating correlations...")
    price_df = df[price_cols].dropna()
    correlations = price_df.corr() if len(price_df) > 1 else None
    
    return df, stats, correlations, tickers

def generate_markdown_report(df, stats, correlations, tickers):
    """Generate comprehensive markdown report"""
    
    timestamp = datetime.now().strftime('%Y-%m-%d %H:%M:%S')
    report_lines = []
    
    # Header
    report_lines.append("# Market Data Analysis Report")
    report_lines.append(f"**Generated:** {timestamp}")
    report_lines.append(f"**Data Points:** {len(df):,} rows")
    report_lines.append(f"**Date Range:** {df['timestamp'].min()} to {df['timestamp'].max()}")
    report_lines.append(f"**Duration:** {(df['timestamp'].max() - df['timestamp'].min()).days} days")
    report_lines.append("")
    report_lines.append("---")
    report_lines.append("")
    
    # Executive Summary
    report_lines.append("## Executive Summary")
    report_lines.append("")
    
    best_performer = max(stats.items(), key=lambda x: x[1]['pct_change'])
    worst_performer = min(stats.items(), key=lambda x: x[1]['pct_change'])
    most_volatile = max(stats.items(), key=lambda x: x[1]['volatility'])
    
    report_lines.append(f"- **Best Performer:** {best_performer[0]} ({best_performer[1]['pct_change']:+.2f}%)")
    report_lines.append(f"- **Worst Performer:** {worst_performer[0]} ({worst_performer[1]['pct_change']:+.2f}%)")
    report_lines.append(f"- **Most Volatile:** {most_volatile[0]} ({most_volatile[1]['volatility']:.2f}% volatility)")
    report_lines.append("")
    report_lines.append("---")
    report_lines.append("")
    
    # Individual Stock Analysis
    report_lines.append("## Individual Stock Analysis")
    report_lines.append("")
    
    for ticker in tickers:
        if ticker in stats:
            s = stats[ticker]
            report_lines.append(f"### {ticker}")
            report_lines.append("")
            report_lines.append("| Metric | Value |")
            report_lines.append("|--------|-------|")
            report_lines.append(f"| Current Price | ${s['current']:.2f} |")
            report_lines.append(f"| Average Price | ${s['mean']:.2f} |")
            report_lines.append(f"| Median Price | ${s['median']:.2f} |")
            report_lines.append(f"| Min Price | ${s['min']:.2f} |")
            report_lines.append(f"| Max Price | ${s['max']:.2f} |")
            report_lines.append(f"| Price Range | ${s['max'] - s['min']:.2f} |")
            report_lines.append(f"| Std Deviation | ${s['std']:.2f} |")
            report_lines.append(f"| Total Change | ${s['total_change']:+.2f} ({s['pct_change']:+.2f}%) |")
            report_lines.append(f"| Volatility | {s['volatility']:.2f}% |")
            report_lines.append(f"| Data Points | {s['data_points']:,} |")
            report_lines.append("")
    
    report_lines.append("---")
    report_lines.append("")
    
    # Correlation Analysis
    if correlations is not None:
        report_lines.append("## Correlation Analysis")
        report_lines.append("")
        report_lines.append("Price correlations between stocks:")
        report_lines.append("")
        
        for i, ticker1 in enumerate(tickers):
            for ticker2 in tickers[i+1:]:
                col1 = f"{ticker1}_price"
                col2 = f"{ticker2}_price"
                if col1 in correlations.columns and col2 in correlations.columns:
                    corr = correlations.loc[col1, col2]
                    report_lines.append(f"- **{ticker1} ↔ {ticker2}:** {corr:.3f}")
        
        report_lines.append("")
        report_lines.append("*Correlation ranges from -1 (inverse) to +1 (perfect). Values near 0 indicate no correlation.*")
        report_lines.append("")
        report_lines.append("---")
        report_lines.append("")
    
    # Market Insights
    report_lines.append("## Market Insights")
    report_lines.append("")
    
    # Overall market trend
    avg_change = np.mean([s['pct_change'] for s in stats.values()])
    if avg_change > 1:
        trend = "📈 **Bullish** - Market showing strong upward momentum"
    elif avg_change > 0:
        trend = "↗️ **Slightly Bullish** - Market trending upward"
    elif avg_change > -1:
        trend = "↘️ **Slightly Bearish** - Market trending downward"
    else:
        trend = "📉 **Bearish** - Market showing downward pressure"
    
    report_lines.append(f"**Overall Market Trend:** {trend}")
    report_lines.append(f"**Average Performance:** {avg_change:+.2f}%")
    report_lines.append("")
    
    # Volatility assessment
    avg_volatility = np.mean([s['volatility'] for s in stats.values()])
    if avg_volatility > 3:
        vol_assessment = "⚠️ **High Volatility** - Significant price swings"
    elif avg_volatility > 1.5:
        vol_assessment = "📊 **Moderate Volatility** - Normal market fluctuations"
    else:
        vol_assessment = "✅ **Low Volatility** - Stable price action"
    
    report_lines.append(f"**Market Volatility:** {vol_assessment}")
    report_lines.append(f"**Average Volatility:** {avg_volatility:.2f}%")
    report_lines.append("")
    
    report_lines.append("---")
    report_lines.append("")
    
    # Footer
    report_lines.append("## Data Summary")
    report_lines.append("")
    report_lines.append(f"- **Total Data Points:** {len(df):,}")
    report_lines.append(f"- **Stocks Analyzed:** {len(tickers)}")
    report_lines.append(f"- **Analysis Timestamp:** {timestamp}")
    report_lines.append("")
    report_lines.append("*This report is generated automatically from market data. Ready for AI analysis.*")
    
    return "\n".join(report_lines)

def generate_report_once(csv_filename):
    """Generate report one time"""
    df, stats, correlations, tickers = analyze_data(csv_filename)
    
    print("📝 Generating markdown report...")
    report_content = generate_markdown_report(df, stats, correlations, tickers)
    
    # Save report
    timestamp = datetime.now().strftime('%Y%m%d_%H%M%S')
    report_filename = f"{REPORTS_DIR}/market_analysis_{timestamp}.md"
    
    with open(report_filename, 'w') as f:
        f.write(report_content)
    
    print(f"✅ Report saved: {report_filename}")
    print(f"📊 Analyzed {len(df):,} data points")
    print(f"📈 Stocks: {', '.join(tickers)}")
    print("")
    
    # Show preview
    print("📄 REPORT PREVIEW:")
    print("-" * 80)
    lines = report_content.split('\n')
    for line in lines[:20]:
        print(line)
    if len(lines) > 20:
        print(f"\n... ({len(lines) - 20} more lines)")
    print("-" * 80)
    print(f"\n💡 Full report: {report_filename}\n")
    
    return report_filename

# Main execution
if watch_mode:
    print("👀 WATCH MODE: Monitoring CSV file and regenerating report...")
    print("   Report regenerates every 60 seconds")
    print("   Press Ctrl+C to stop\n")
    
    last_size = 0
    report_interval = 60  # Generate report every 60 seconds
    last_report_time = 0
    
    try:
        while True:
            current_time = time.time()
            current_size = os.path.getsize(csv_filename)
            
            # Check if file has grown and enough time has passed
            if current_size > last_size and (current_time - last_report_time) >= report_interval:
                print(f"\n{'='*80}")
                print(f"🔔 New data detected! Regenerating report...")
                print(f"{'='*80}\n")
                
                generate_report_once(csv_filename)
                
                last_size = current_size
                last_report_time = current_time
            
            time.sleep(10)  # Check every 10 seconds
            
    except KeyboardInterrupt:
        print(f"\n\n⚠️  Monitoring stopped by user")
        print("="*80)
else:
    # One-time report
    generate_report_once(csv_filename)
