#!/usr/bin/env python3

import os
import sys
import requests
import psycopg2
from psycopg2.extras import execute_batch
from datetime import datetime, timedelta
from concurrent.futures import ThreadPoolExecutor, as_completed
from dotenv import load_dotenv
import time
import threading

# Load environment variables
load_dotenv()

# Configuration from environment variables
API_KEY = os.getenv('FMP_API_KEY')
DB_CONFIG = {
    'dbname': os.getenv('DB_NAME'),
    'user': os.getenv('DB_USER'),
    'password': os.getenv('DB_PASSWORD'),
    'host': os.getenv('DB_HOST', 'localhost'),
    'port': os.getenv('DB_PORT', '5432')
}

# Performance configuration for N2-standard-8
MAX_WORKERS = 6  # Use 6 threads for API calls (75% of 8 cores)
BATCH_SIZE = 1000  # Insert records in batches for efficiency
API_RETRY_DELAY = 2  # Seconds to wait on rate limit
MAX_RETRIES = 3

# Thread-safe counters
stats_lock = threading.Lock()
stats = {
    'fetched': 0,
    'inserted': 0,
    'errors': 0
}

def update_stats(key, value=1):
    """Thread-safe statistics update"""
    with stats_lock:
        stats[key] += value

def get_db_connection():
    """Create a new database connection"""
    return psycopg2.connect(**DB_CONFIG)

def fill_missing_dates(symbol, conn):
    """
    Fill missing dates in the database with previous available data (forward-fill)
    This handles gaps in API data by carrying forward the last known price
    """
    cursor = conn.cursor()
    
    try:
        # Get the date range and find gaps
        cursor.execute("""
            WITH date_series AS (
                SELECT generate_series(
                    (SELECT MIN(date)::date FROM crypto_prices WHERE symbol = %s),
                    (SELECT MAX(date)::date FROM crypto_prices WHERE symbol = %s),
                    '1 day'::interval
                )::date AS expected_date
            ),
            existing_dates AS (
                SELECT date::date AS existing_date 
                FROM crypto_prices 
                WHERE symbol = %s
            ),
            missing_dates AS (
                SELECT ds.expected_date
                FROM date_series ds
                LEFT JOIN existing_dates ed ON ds.expected_date = ed.existing_date
                WHERE ed.existing_date IS NULL
            )
            SELECT expected_date FROM missing_dates ORDER BY expected_date;
        """, (symbol, symbol, symbol))
        
        missing_dates = cursor.fetchall()
        
        if not missing_dates:
            print(f"  ✓ No missing dates for {symbol}")
            return 0
        
        print(f"  → Found {len(missing_dates)} missing dates for {symbol}, filling with forward-fill...")
        
        filled_count = 0
        
        # For each missing date, fill with previous available data
        for (missing_date,) in missing_dates:
            cursor.execute("""
                INSERT INTO crypto_prices (symbol, date, price, volume)
                SELECT 
                    symbol,
                    %s::date,
                    price,
                    0  -- Set volume to 0 for filled dates to distinguish from real data
                FROM crypto_prices
                WHERE symbol = %s 
                AND date < %s
                ORDER BY date DESC
                LIMIT 1
                ON CONFLICT (symbol, date) DO NOTHING
            """, (missing_date, symbol, missing_date))
            
            if cursor.rowcount > 0:
                filled_count += 1
        
        conn.commit()
        print(f"  ✓ Filled {filled_count} missing dates for {symbol}")
        return filled_count
        
    except Exception as e:
        print(f"  ✗ Error filling missing dates for {symbol}: {e}")
        conn.rollback()
        return 0
    finally:
        cursor.close()

def get_db_connection():
    """Create a new database connection"""
    return psycopg2.connect(**DB_CONFIG)

def fetch_historical_price_data(symbol, daily_update=False, retries=0):
    """
    Fetch historical EOD price data from Financial Modeling Prep API (light endpoint)
    Returns daily data: symbol, date, price, volume
    
    Args:
        symbol: Asset symbol to fetch
        daily_update: If True, only fetch last 10 days. If False, fetch from 2009
        retries: Current retry count
    """
    url = "https://financialmodelingprep.com/stable/historical-price-eod/light"
    
    # Determine the 'from' date based on mode
    if daily_update:
        from_date = (datetime.now() - timedelta(days=10)).strftime('%Y-%m-%d')
    else:
        from_date = '2009-01-01'  # Full historical data
    
    params = {
        'symbol': symbol,
        'from': from_date,
        'apikey': API_KEY
    }
    
    try:
        response = requests.get(url, params=params, timeout=30)
        
        # Handle rate limiting
        if response.status_code == 429:
            if retries < MAX_RETRIES:
                print(f"⚠ Rate limit hit for {symbol}. Waiting {API_RETRY_DELAY}s... (Retry {retries+1}/{MAX_RETRIES})")
                time.sleep(API_RETRY_DELAY)
                return fetch_historical_price_data(symbol, daily_update, retries + 1)
            else:
                print(f"✗ Max retries reached for {symbol}")
                return []
        
        response.raise_for_status()
        data = response.json()
        
        # Handle API response format for light endpoint
        # Light endpoint returns array directly: [{"symbol": "BTCUSD", "date": "2024-01-01", "price": 50000, "volume": 1000}, ...]
        if isinstance(data, list):
            print(f"✓ Fetched {len(data)} records for {symbol}")
            update_stats('fetched', len(data))
            return data
        elif isinstance(data, dict) and 'Error Message' in data:
            print(f"✗ API Error for {symbol}: {data['Error Message']}")
            return []
        else:
            print(f"✗ Unexpected response format for {symbol}")
            return []
        
    except requests.exceptions.RequestException as e:
        print(f"✗ Error fetching {symbol} data: {e}")
        update_stats('errors')
        return []

def insert_batch_to_db(batch_data, conn):
    """
    Insert a batch of data efficiently using execute_batch
    Uses ON CONFLICT to update existing records (which triggers updated_at)
    """
    if not batch_data:
        return 0, 0, 0
    
    cursor = conn.cursor()
    
    try:
        # Prepare data for batch insert/update
        values = [
            (record['symbol'], record['date'], float(record['price']), float(record['volume']))
            for record in batch_data
        ]
        
        # Use execute_batch with UPSERT logic
        # ON CONFLICT UPDATE will trigger the updated_at trigger
        execute_batch(cursor, """
            INSERT INTO crypto_prices (symbol, date, price, volume)
            VALUES (%s, %s, %s, %s)
            ON CONFLICT (symbol, date) 
            DO UPDATE SET 
                price = EXCLUDED.price,
                volume = EXCLUDED.volume
            WHERE crypto_prices.price != EXCLUDED.price 
               OR crypto_prices.volume != EXCLUDED.volume
        """, values, page_size=BATCH_SIZE)
        
        # Get row count (total affected rows)
        affected = cursor.rowcount
        
        conn.commit()
        
        # Return simple counts (we can't easily distinguish insert vs update with execute_batch)
        return affected, 0, 0
        
    except Exception as e:
        print(f"✗ Error inserting batch: {e}")
        conn.rollback()
        update_stats('errors')
        return 0, 0, 0
    finally:
        cursor.close()

def process_and_insert_data(data, symbol):
    """Process fetched data and insert into database in batches, then fill missing dates"""
    if not data:
        return
    
    conn = get_db_connection()
    
    try:
        # Process in batches for memory efficiency
        for i in range(0, len(data), BATCH_SIZE):
            batch = data[i:i + BATCH_SIZE]
            affected, _, _ = insert_batch_to_db(batch, conn)
            update_stats('inserted', affected)
            
            if (i // BATCH_SIZE + 1) % 5 == 0:
                print(f"  → Processed {i + len(batch)}/{len(data)} records...")
        
        # After inserting all data, fill missing dates with forward-fill
        filled = fill_missing_dates(symbol, conn)
        update_stats('inserted', filled)
    
    finally:
        conn.close()

def fetch_and_store_symbol(symbol, daily_update=False):
    """Fetch data for a specific symbol and store it"""
    print(f"\n--- Processing {symbol} ---")
    data = fetch_historical_price_data(symbol, daily_update)
    
    if data:
        process_and_insert_data(data, symbol)
        return True
    return False

def main():
    """Main function with parallel processing"""
    
    # Check for daily update mode
    daily_update = '--daily' in sys.argv or '--update' in sys.argv
    
    mode_text = "Daily Update Mode (Last 10 Days)" if daily_update else "Full Historical Mode (From 2009)"
    
    print("=" * 70)
    print("Asset Historical Price Data Fetcher - High Performance Mode")
    print(f"Mode: {mode_text}")
    print("Supports: Crypto, Stocks, Indices, Commodities")
    print(f"VM Resources: 8 cores, 32GB RAM | Workers: {MAX_WORKERS}")
    print("Data: Daily EOD (End of Day) prices - Light endpoint (4 columns)")
    print("=" * 70)
    
    # Validate environment variables
    if not API_KEY:
        print("✗ ERROR: FMP_API_KEY not found in .env file")
        return
    
    if not DB_CONFIG['password']:
        print("✗ ERROR: DB_PASSWORD not found in .env file")
        return
    
    print("✓ Environment variables loaded")
    
    # Define symbols to fetch (supports multiple asset types)
    symbols = [
        'BTCUSD',   # Bitcoin (Crypto)
        'ETHUSD',   # Ethereum (Crypto)
        # Add more symbols here:
        # Crypto:
        # 'SOLUSD',   # Solana
        # Stocks:
        # 'AAPL',     # Apple
        # 'GOOGL',    # Google
        # Indices:
        # '^GSPC',    # S&P 500
        # '^DJI',     # Dow Jones
        # Commodities:
        # 'GCUSD',    # Gold
        # 'CLUSD',    # Crude Oil
    ]
    
    start_time = time.time()
    
    # Use ThreadPoolExecutor for parallel API calls
    workers_text = f"{len(symbols)} symbol(s) with {MAX_WORKERS} workers"
    print(f"\n🚀 Starting parallel data fetch for {workers_text}...\n")
    
    with ThreadPoolExecutor(max_workers=MAX_WORKERS) as executor:
        # Submit all tasks
        future_to_symbol = {
            executor.submit(fetch_and_store_symbol, symbol, daily_update): symbol 
            for symbol in symbols
        }
        
        # Process completed tasks
        for future in as_completed(future_to_symbol):
            symbol = future_to_symbol[future]
            try:
                success = future.result()
                if success:
                    print(f"✓ Completed {symbol}")
                else:
                    print(f"⚠ No data for {symbol}")
            except Exception as e:
                print(f"✗ Exception for {symbol}: {e}")
                update_stats('errors')
    
    elapsed_time = time.time() - start_time
    
    # Show final statistics
    print("\n" + "=" * 70)
    print("EXECUTION SUMMARY")
    print("=" * 70)
    print(f"Time elapsed: {elapsed_time:.2f} seconds")
    print(f"Records fetched: {stats['fetched']:,}")
    print(f"Records processed: {stats['inserted']:,}")
    print(f"Errors: {stats['errors']}")
    
    print("\n" + "=" * 70)
    print("✓ Data fetch completed!")
    print("=" * 70)

if __name__ == "__main__":
    main()
