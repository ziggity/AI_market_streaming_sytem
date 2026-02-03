#!/usr/bin/env python3
"""
AI Report Watcher & Insights Generator (Version 2)

Monitors markdown analysis reports and uses Groq AI to generate
comprehensive insights, predictions, and trading recommendations.

Usage:
    uv run ai_watch_report.py <report_folder> --watch
    
Example:
    uv run ai_watch_report.py reports/ --watch
    
Features:
- Watches for new/updated analysis reports
- Reads markdown report content
- Sends to Groq AI for deep analysis
- Generates actionable insights
- Saves AI insights to separate folder
"""

import sys
import os
import time
from datetime import datetime
from dotenv import load_dotenv

# Load environment variables
load_dotenv()
load_dotenv('.env.model')

try:
    from groq import Groq
    client = Groq(api_key=os.getenv('GROQ_API_KEY'))
    GROQ_MODEL = os.getenv('GROQ_MODEL', 'llama-3.3-70b-versatile')
except ImportError:
    print("\n❌ ERROR: Groq library not installed")
    print("Install with: uv pip install groq")
    sys.exit(1)

print("="*80)
print("AI REPORT WATCHER & INSIGHTS GENERATOR (VERSION 2)")
print("="*80)

# Check arguments
if len(sys.argv) < 2:
    print("\n❌ ERROR: Please provide reports folder path")
    print("\nUsage:")
    print("  uv run ai_watch_report.py <report_folder> --watch")
    print("\nExample:")
    print("  uv run ai_watch_report.py reports/ --watch")
    print("")
    sys.exit(1)

reports_folder = sys.argv[1].rstrip('/')
watch_mode = '--watch' in sys.argv

if not os.path.exists(reports_folder):
    print(f"\n❌ ERROR: Folder not found: {reports_folder}")
    sys.exit(1)

# Create AI analysis directory
AI_DIR = 'ai_analysis'
os.makedirs(AI_DIR, exist_ok=True)

print(f"\n📁 Reports Folder: {reports_folder}")
print(f"🤖 AI Model: {GROQ_MODEL}")
print(f"💰 Cost: FREE (Groq)")
print(f"👀 Watch Mode: {'ENABLED' if watch_mode else 'DISABLED'}")
print("="*80 + "\n")

def get_latest_report(folder):
    """Get the most recent markdown report from folder"""
    md_files = [f for f in os.listdir(folder) if f.endswith('.md')]
    
    if not md_files:
        return None
    
    # Sort by modification time, newest first
    md_files.sort(key=lambda f: os.path.getmtime(os.path.join(folder, f)), reverse=True)
    return os.path.join(folder, md_files[0])

def read_report(report_path):
    """Read markdown report content"""
    with open(report_path, 'r') as f:
        return f.read()

def generate_ai_insights(report_content):
    """Use Groq AI to generate deep insights from report"""
    
    prompt = f"""You are a professional financial analyst with expertise in market analysis, technical indicators, and trading strategies.

You have been provided with a comprehensive market analysis report that includes:
- Historical data (up to 5 years)
- Statistical analysis (mean, median, volatility, correlations)
- Individual stock performance metrics
- Market trends and insights

Your task is to provide ACTIONABLE insights including:

1. **Deep Market Analysis**: Interpret the patterns, correlations, and trends
2. **Trading Opportunities**: Specific buy/sell recommendations with reasoning
3. **Risk Assessment**: Identify potential risks and market vulnerabilities
4. **Price Predictions**: Based on historical trends and current momentum
5. **Portfolio Strategy**: How to position for the next 30-90 days
6. **Technical Insights**: What the volatility and correlations tell us

Be specific, quantitative when possible, and always explain your reasoning.

Here is the market analysis report:

---
{report_content}
---

Generate your comprehensive AI insights now:"""

    try:
        response = client.chat.completions.create(
            messages=[
                {
                    "role": "system",
                    "content": "You are a senior financial analyst providing actionable market insights."
                },
                {
                    "role": "user",
                    "content": prompt
                }
            ],
            model=GROQ_MODEL,
            temperature=0.7,
            max_tokens=8000
        )
        
        return response.choices[0].message.content
    
    except Exception as e:
        return f"ERROR generating AI insights: {e}"

def save_ai_insights(insights, original_report_path):
    """Save AI insights to file"""
    timestamp = datetime.now().strftime('%Y%m%d_%H%M%S')
    insights_filename = f"{AI_DIR}/ai_insights_{timestamp}.md"
    
    # Create formatted output
    output = []
    output.append("# AI-Generated Market Insights")
    output.append(f"**Generated:** {datetime.now().strftime('%Y-%m-%d %H:%M:%S')}")
    output.append(f"**Source Report:** {os.path.basename(original_report_path)}")
    output.append(f"**AI Model:** {GROQ_MODEL}")
    output.append(f"**Cost:** $0.00 (FREE)")
    output.append("")
    output.append("---")
    output.append("")
    output.append(insights)
    
    with open(insights_filename, 'w') as f:
        f.write("\n".join(output))
    
    return insights_filename

def process_report(report_path):
    """Process a single report and generate AI insights"""
    
    print(f"📊 Reading report: {os.path.basename(report_path)}")
    report_content = read_report(report_path)
    
    # Extract key info from report
    lines = report_content.split('\n')
    data_points = "Unknown"
    for line in lines:
        if 'Data Points:' in line:
            data_points = line.split(':')[1].strip()
            break
    
    print(f"📈 Report contains: {data_points}")
    print(f"🤖 Generating AI insights with {GROQ_MODEL}...")
    
    insights = generate_ai_insights(report_content)
    
    print(f"💾 Saving AI insights...")
    insights_file = save_ai_insights(insights, report_path)
    
    print(f"\n{'='*80}")
    print(f"✅ AI INSIGHTS GENERATED!")
    print(f"{'='*80}")
    print(f"📁 File: {insights_file}")
    print(f"📊 Source: {os.path.basename(report_path)}")
    print(f"💰 Cost: $0.00 (FREE)")
    print(f"{'='*80}\n")
    
    # Show preview
    print("📄 INSIGHTS PREVIEW:")
    print("-" * 80)
    insight_lines = insights.split('\n')
    for line in insight_lines[:20]:
        print(line)
    if len(insight_lines) > 20:
        print(f"\n... ({len(insight_lines) - 20} more lines)")
    print("-" * 80)
    print(f"\n💡 Full insights: {insights_file}\n")

# Main execution
if watch_mode:
    print("👀 WATCH MODE: Monitoring for new reports...")
    print("   Checks for new reports every 30 seconds")
    print("   Press Ctrl+C to stop\n")
    
    processed_reports = set()
    
    try:
        while True:
            latest_report = get_latest_report(reports_folder)
            
            if latest_report and latest_report not in processed_reports:
                print(f"\n{'='*80}")
                print(f"🔔 New report detected!")
                print(f"{'='*80}\n")
                
                process_report(latest_report)
                processed_reports.add(latest_report)
                
                print("⏳ Waiting for next report...\n")
            
            time.sleep(30)  # Check every 30 seconds
            
    except KeyboardInterrupt:
        print(f"\n\n⚠️  Monitoring stopped by user")
        print(f"✅ Total reports processed: {len(processed_reports)}")
        print("="*80)
else:
    # Process latest report once
    latest_report = get_latest_report(reports_folder)
    
    if latest_report:
        process_report(latest_report)
    else:
        print("❌ No reports found in folder")
        print(f"   Generate a report first with: analyze_and_report.py")
