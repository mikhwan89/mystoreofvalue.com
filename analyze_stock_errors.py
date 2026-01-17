#!/usr/bin/env python3

import os
import psycopg2
from dotenv import load_dotenv
from datetime import datetime, timedelta

# Load environment variables
load_dotenv()

DB_CONFIG = {
    'dbname': os.getenv('DB_NAME'),
    'user': os.getenv('DB_USER'),
    'password': os.getenv('DB_PASSWORD'),
    'host': os.getenv('DB_HOST', 'localhost'),
    'port': os.getenv('DB_PORT', '5432')
}

def get_db_connection():
    """Create a new database connection"""
    return psycopg2.connect(**DB_CONFIG)

def analyze_missing_stocks():
    """
    Find stocks that should have been loaded but have no price data
    """
    conn = get_db_connection()
    cursor = conn.cursor()
    
    print("=" * 70)
    print("MISSING STOCK DATA ANALYSIS")
    print("=" * 70)
    
    # Find actively trading stocks with no price data
    cursor.execute("""
        SELECT 
            am.symbol,
            am.name,
            am.exchange,
            am.currency
        FROM asset_metadata am
        LEFT JOIN stock_prices sp ON am.symbol = sp.symbol
        WHERE am.asset_type = 'stock'
        AND am.is_actively_trading = true
        AND sp.symbol IS NULL
        ORDER BY am.exchange, am.symbol
    """)
    
    missing_stocks = cursor.fetchall()
    
    print(f"\nStocks with NO price data: {len(missing_stocks)}")
    
    if missing_stocks:
        print("\n--- Missing Stock Details ---")
        
        # Group by exchange
        by_exchange = {}
        for symbol, name, exchange, currency in missing_stocks:
            if exchange not in by_exchange:
                by_exchange[exchange] = []
            by_exchange[exchange].append((symbol, name, currency))
        
        for exchange, stocks in sorted(by_exchange.items(), key=lambda x: len(x[1]), reverse=True):
            print(f"\n{exchange}: {len(stocks)} missing stocks")
            for symbol, name, currency in stocks[:5]:  # Show first 5
                print(f"  - {symbol} ({name}) [{currency}]")
            if len(stocks) > 5:
                print(f"  ... and {len(stocks) - 5} more")
    
    # Find stocks with incomplete data (less than expected records)
    print("\n\n--- Stocks with Incomplete Data ---")
    
    cursor.execute("""
        SELECT 
            sp.symbol,
            am.name,
            am.exchange,
            COUNT(sp.id) as record_count,
            MIN(sp.date) as earliest_date,
            MAX(sp.date) as latest_date
        FROM stock_prices sp
        JOIN asset_metadata am ON sp.symbol = am.symbol
        WHERE am.asset_type = 'stock'
        GROUP BY sp.symbol, am.name, am.exchange
        HAVING COUNT(sp.id) < 100  -- Less than 100 records is suspicious
        ORDER BY record_count ASC
        LIMIT 50
    """)
    
    incomplete = cursor.fetchall()
    
    print(f"\nStocks with less than 100 records: {len(incomplete)}")
    
    if incomplete:
        print("\nTop 20 stocks with fewest records:")
        for symbol, name, exchange, count, earliest, latest in incomplete[:20]:
            print(f"  {symbol:<10} {count:>4} records | {earliest} to {latest} | {exchange}")
    
    # Find stocks with recent gaps (missing last 5 days)
    print("\n\n--- Stocks with Recent Data Gaps ---")
    
    cursor.execute("""
        SELECT 
            sp.symbol,
            am.name,
            am.exchange,
            MAX(sp.date) as last_date,
            CURRENT_DATE - MAX(sp.date)::date as days_behind
        FROM stock_prices sp
        JOIN asset_metadata am ON sp.symbol = am.symbol
        WHERE am.asset_type = 'stock'
        GROUP BY sp.symbol, am.name, am.exchange
        HAVING MAX(sp.date) < CURRENT_DATE - INTERVAL '5 days'
        ORDER BY days_behind DESC
        LIMIT 50
    """)
    
    gaps = cursor.fetchall()
    
    print(f"\nStocks not updated in last 5 days: {len(gaps)}")
    
    if gaps:
        print("\nTop 20 most outdated stocks:")
        for symbol, name, exchange, last_date, days_behind in gaps[:20]:
            print(f"  {symbol:<10} Last: {last_date} ({int(days_behind)} days ago) | {exchange}")
    
    cursor.close()
    conn.close()

def analyze_data_quality():
    """
    Analyze overall data quality and coverage
    """
    conn = get_db_connection()
    cursor = conn.cursor()
    
    print("\n\n" + "=" * 70)
    print("DATA QUALITY ANALYSIS")
    print("=" * 70)
    
    # Overall coverage
    cursor.execute("""
        SELECT 
            COUNT(DISTINCT am.symbol) as total_stocks,
            COUNT(DISTINCT sp.symbol) as stocks_with_data,
            COUNT(DISTINCT am.symbol) - COUNT(DISTINCT sp.symbol) as missing_data
        FROM asset_metadata am
        LEFT JOIN stock_prices sp ON am.symbol = sp.symbol
        WHERE am.asset_type = 'stock'
        AND am.is_actively_trading = true
    """)
    
    total, with_data, missing = cursor.fetchone()
    coverage = (with_data / total * 100) if total > 0 else 0
    
    print(f"\nOverall Coverage:")
    print(f"  Total actively trading stocks: {total:,}")
    print(f"  Stocks with price data: {with_data:,}")
    print(f"  Stocks missing data: {missing:,}")
    print(f"  Coverage: {coverage:.2f}%")
    
    # Records by exchange
    print("\n\nData Coverage by Exchange:")
    cursor.execute("""
        SELECT 
            am.exchange,
            COUNT(DISTINCT am.symbol) as total_stocks,
            COUNT(DISTINCT sp.symbol) as with_data,
            SUM(CASE WHEN sp.symbol IS NULL THEN 1 ELSE 0 END) as missing
        FROM asset_metadata am
        LEFT JOIN stock_prices sp ON am.symbol = sp.symbol
        WHERE am.asset_type = 'stock'
        AND am.is_actively_trading = true
        GROUP BY am.exchange
        HAVING COUNT(DISTINCT am.symbol) > 10
        ORDER BY total_stocks DESC
        LIMIT 20
    """)
    
    print(f"\n{'Exchange':<15} {'Total':>10} {'With Data':>10} {'Missing':>10} {'Coverage':>10}")
    print("-" * 60)
    for exchange, total, with_data, missing in cursor.fetchall():
        cov = (with_data / total * 100) if total > 0 else 0
        print(f"{exchange:<15} {total:>10,} {with_data:>10,} {missing:>10,} {cov:>9.1f}%")
    
    # Average records per stock
    cursor.execute("""
        SELECT 
            AVG(record_count) as avg_records,
            MIN(record_count) as min_records,
            MAX(record_count) as max_records,
            PERCENTILE_CONT(0.5) WITHIN GROUP (ORDER BY record_count) as median_records
        FROM (
            SELECT symbol, COUNT(*) as record_count
            FROM stock_prices
            GROUP BY symbol
        ) subq
    """)
    
    avg, min_rec, max_rec, median = cursor.fetchone()
    
    print(f"\n\nRecords per Stock:")
    print(f"  Average: {avg:,.0f} records")
    print(f"  Median: {median:,.0f} records")
    print(f"  Range: {min_rec:,} to {max_rec:,} records")
    
    cursor.close()
    conn.close()

def export_missing_stocks_csv():
    """
    Export list of missing stocks to CSV for further investigation
    """
    conn = get_db_connection()
    cursor = conn.cursor()
    
    cursor.execute("""
        SELECT 
            am.symbol,
            am.name,
            am.exchange,
            am.currency,
            am.sector,
            am.industry,
            am.is_etf,
            am.is_fund
        FROM asset_metadata am
        LEFT JOIN stock_prices sp ON am.symbol = sp.symbol
        WHERE am.asset_type = 'stock'
        AND am.is_actively_trading = true
        AND sp.symbol IS NULL
        ORDER BY am.exchange, am.symbol
    """)
    
    missing = cursor.fetchall()
    
    if missing:
        filename = f"/home/{os.getenv('USER', 'claude')}/projects/mystoreofvalue.com/missing_stocks_{datetime.now().strftime('%Y%m%d_%H%M%S')}.csv"
        
        with open(filename, 'w') as f:
            f.write("symbol,name,exchange,currency,sector,industry,is_etf,is_fund\n")
            for row in missing:
                # Escape commas in names
                name = str(row[1]).replace('"', '""') if row[1] else ''
                f.write(f'"{row[0]}","{name}","{row[2]}","{row[3]}","{row[4] or ""}","{row[5] or ""}",{row[6]},{row[7]}\n')
        
        print(f"\n✓ Exported {len(missing)} missing stocks to: {filename}")
    
    cursor.close()
    conn.close()

def main():
    """Main analysis function"""
    
    print("\n" + "=" * 70)
    print("STOCK DATA ERROR ANALYSIS")
    print("=" * 70)
    print(f"Analysis Time: {datetime.now().strftime('%Y-%m-%d %H:%M:%S')}")
    print("=" * 70)
    
    # Run all analyses
    analyze_missing_stocks()
    analyze_data_quality()
    export_missing_stocks_csv()
    
    print("\n" + "=" * 70)
    print("✓ Analysis completed!")
    print("=" * 70)
    print("\nTo retry failed stocks, you can:")
    print("1. Check the exported CSV file")
    print("2. Manually test specific symbols with the API")
    print("3. Re-run fetch_asset_light.py (it will skip existing data)")
    print("=" * 70)

if __name__ == "__main__":
    main()
