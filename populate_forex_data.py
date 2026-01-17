#!/usr/bin/env python3

import os
import requests
import psycopg2
from psycopg2.extras import execute_batch
from datetime import datetime, timedelta
from concurrent.futures import ThreadPoolExecutor, as_completed
from dotenv import load_dotenv
import time

# Load environment variables
load_dotenv()

API_KEY = os.getenv('FMP_API_KEY')
DB_CONFIG = {
    'dbname': os.getenv('DB_NAME'),
    'user': os.getenv('DB_USER'),
    'password': os.getenv('DB_PASSWORD'),
    'host': os.getenv('DB_HOST', 'localhost'),
    'port': os.getenv('DB_PORT', '5432')
}

# Performance settings
MAX_WORKERS = 7
BATCH_SIZE = 5000
RETRY_DELAY = 2
MAX_RETRIES = 3
START_DATE = '2009-01-01'

# Thread-safe counters
import threading
stats_lock = threading.Lock()
stats = {'fetched': 0, 'inserted': 0, 'errors': 0}

def update_stats(key, value=1):
    with stats_lock:
        stats[key] += value

def get_db_connection():
    return psycopg2.connect(**DB_CONFIG)

def fetch_forex_list():
    """
    Fetch list of available forex pairs, filtered to only USD pairs
    (e.g., EURUSD, GBPUSD, JPYUSD - anything ending in USD)
    """
    url = "https://financialmodelingprep.com/stable/forex-list"
    
    params = {'apikey': API_KEY}
    
    try:
        response = requests.get(url, params=params, timeout=30)
        response.raise_for_status()
        data = response.json()
        
        if isinstance(data, list):
            # Filter to only USD pairs (quote currency = USD)
            usd_pairs = [
                pair for pair in data 
                if pair.get('symbol', '').endswith('USD') and len(pair.get('symbol', '')) == 6
            ]
            print(f"✓ Fetched {len(data)} total forex pairs")
            print(f"✓ Filtered to {len(usd_pairs)} USD pairs")
            return usd_pairs
        else:
            print("✗ Unexpected response format from forex-list")
            return []
            
    except requests.exceptions.RequestException as e:
        print(f"✗ Error fetching forex list: {e}")
        return []

def insert_forex_pairs(conn, pairs_data):
    """
    Insert forex pairs metadata
    Note: Only USD pairs are stored (e.g., EURUSD, GBPUSD)
    We don't need cross-pairs like EURGBP since we only convert to USD
    """
    if not pairs_data:
        return 0
    
    cursor = conn.cursor()
    inserted = 0
    
    try:
        for pair in pairs_data:
            symbol = pair.get('symbol')
            name = pair.get('name')
            
            # Extract base and quote currency from symbol (e.g., EURUSD -> EUR, USD)
            if len(symbol) == 6:
                base_currency = symbol[:3]
                quote_currency = symbol[3:]
            else:
                base_currency = None
                quote_currency = None
            
            cursor.execute("""
                INSERT INTO forex_pairs (symbol, name, base_currency, quote_currency)
                VALUES (%s, %s, %s, %s)
                ON CONFLICT (symbol) DO UPDATE SET
                    name = EXCLUDED.name,
                    base_currency = EXCLUDED.base_currency,
                    quote_currency = EXCLUDED.quote_currency,
                    updated_at = CURRENT_TIMESTAMP
                RETURNING (xmax = 0) AS inserted
            """, (symbol, name, base_currency, quote_currency))
            
            result = cursor.fetchone()
            if result and result[0]:
                inserted += 1
        
        conn.commit()
        print(f"✓ Inserted/updated {inserted} forex pairs")
        return inserted
        
    except Exception as e:
        print(f"✗ Error inserting forex pairs: {e}")
        conn.rollback()
        return 0
    finally:
        cursor.close()

def fetch_forex_historical_data(symbol, daily_update=False, retries=0):
    """Fetch historical forex price data"""
    url = "https://financialmodelingprep.com/stable/historical-price-eod/light"
    
    if daily_update:
        from_date = (datetime.now() - timedelta(days=10)).strftime('%Y-%m-%d')
    else:
        from_date = START_DATE
    
    params = {
        'symbol': symbol,
        'from': from_date,
        'apikey': API_KEY
    }
    
    try:
        response = requests.get(url, params=params, timeout=30)
        
        if response.status_code == 429:
            if retries < MAX_RETRIES:
                time.sleep(RETRY_DELAY)
                return fetch_forex_historical_data(symbol, daily_update, retries + 1)
            else:
                return []
        
        response.raise_for_status()
        data = response.json()
        
        if isinstance(data, list):
            print(f"✓ Fetched {len(data)} records for {symbol}")
            update_stats('fetched', len(data))
            return data
        else:
            return []
            
    except requests.exceptions.RequestException as e:
        print(f"✗ Error fetching {symbol}: {e}")
        update_stats('errors')
        return []

def insert_forex_prices_batch(conn, price_data):
    """Insert forex prices in batch"""
    if not price_data:
        return 0
    
    cursor = conn.cursor()
    
    try:
        values = [
            (record['symbol'], record['date'], float(record['price']), float(record.get('volume', 0)))
            for record in price_data
        ]
        
        execute_batch(cursor, """
            INSERT INTO forex_prices (symbol, date, price, volume)
            VALUES (%s, %s, %s, %s)
            ON CONFLICT (symbol, date) 
            DO UPDATE SET 
                price = EXCLUDED.price,
                volume = EXCLUDED.volume
            WHERE forex_prices.price != EXCLUDED.price 
               OR forex_prices.volume != EXCLUDED.volume
        """, values, page_size=BATCH_SIZE)
        
        affected = cursor.rowcount
        conn.commit()
        return affected
        
    except Exception as e:
        print(f"✗ Error inserting batch: {e}")
        conn.rollback()
        update_stats('errors')
        return 0
    finally:
        cursor.close()

def fill_missing_dates_forex(symbol, conn, extend_to_today=False):
    """
    Fill missing dates for forex with forward-fill
    
    This ensures forex rates are available for ALL dates including:
    - Weekends (Saturday, Sunday)
    - Public holidays (any exchange holidays)
    - Any other missing dates
    
    When extend_to_today=True (daily mode), fills up to today
    When extend_to_today=False (initial load), fills only between existing data
    """
    cursor = conn.cursor()
    
    try:
        if extend_to_today:
            cursor.execute("""
                WITH date_series AS (
                    SELECT generate_series(
                        (SELECT MIN(date)::date FROM forex_prices WHERE symbol = %s),
                        CURRENT_DATE,
                        '1 day'::interval
                    )::date AS expected_date
                ),
                existing_dates AS (
                    SELECT date::date AS existing_date 
                    FROM forex_prices
                    WHERE symbol = %s
                ),
                missing_dates AS (
                    SELECT ds.expected_date
                    FROM date_series ds
                    LEFT JOIN existing_dates ed ON ds.expected_date = ed.existing_date
                    WHERE ed.existing_date IS NULL
                )
                SELECT expected_date FROM missing_dates ORDER BY expected_date;
            """, (symbol, symbol))
        else:
            cursor.execute("""
                WITH date_series AS (
                    SELECT generate_series(
                        (SELECT MIN(date)::date FROM forex_prices WHERE symbol = %s),
                        (SELECT MAX(date)::date FROM forex_prices WHERE symbol = %s),
                        '1 day'::interval
                    )::date AS expected_date
                ),
                existing_dates AS (
                    SELECT date::date AS existing_date 
                    FROM forex_prices
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
            return 0
        
        filled = 0
        for (missing_date,) in missing_dates:
            cursor.execute("""
                INSERT INTO forex_prices (symbol, date, price, volume)
                SELECT 
                    symbol,
                    %s::date,
                    price,
                    0
                FROM forex_prices
                WHERE symbol = %s 
                AND date < %s
                ORDER BY date DESC
                LIMIT 1
                ON CONFLICT (symbol, date) DO NOTHING
            """, (missing_date, symbol, missing_date))
            
            if cursor.rowcount > 0:
                filled += 1
        
        conn.commit()
        return filled
        
    except Exception as e:
        print(f"  ✗ Error filling dates for {symbol}: {e}")
        conn.rollback()
        return 0
    finally:
        cursor.close()

def process_forex_pair(symbol, daily_update=False):
    """Fetch and store data for a single forex pair"""
    data = fetch_forex_historical_data(symbol, daily_update)
    
    if data:
        conn = get_db_connection()
        
        # Insert prices in batches
        for i in range(0, len(data), BATCH_SIZE):
            batch = data[i:i + BATCH_SIZE]
            inserted = insert_forex_prices_batch(conn, batch)
            update_stats('inserted', inserted)
        
        # Fill missing dates
        filled = fill_missing_dates_forex(symbol, conn, extend_to_today=daily_update)
        update_stats('inserted', filled)
        
        conn.close()
        return True
    
    return False

def main():
    import sys
    
    daily_update = '--daily' in sys.argv
    mode_text = "Daily Update (Last 10 Days)" if daily_update else "Full Historical (From 2009)"
    
    print("=" * 70)
    print("Forex Data Populator")
    print(f"Mode: {mode_text}")
    print("=" * 70)
    
    if not API_KEY or not DB_CONFIG['password']:
        print("✗ ERROR: Missing environment variables")
        return
    
    print("✓ Environment variables loaded\n")
    
    # Step 1: Fetch and store forex pairs metadata
    print("--- Fetching Forex Pairs List ---")
    pairs = fetch_forex_list()
    
    if pairs:
        conn = get_db_connection()
        insert_forex_pairs(conn, pairs)
        conn.close()
    
    # Get forex symbols from database
    conn = get_db_connection()
    cursor = conn.cursor()
    cursor.execute("SELECT symbol FROM forex_pairs ORDER BY symbol")
    forex_symbols = [row[0] for row in cursor.fetchall()]
    cursor.close()
    conn.close()
    
    print(f"\n--- Processing {len(forex_symbols)} Forex Pairs ---\n")
    
    start_time = time.time()
    
    with ThreadPoolExecutor(max_workers=MAX_WORKERS) as executor:
        future_to_symbol = {
            executor.submit(process_forex_pair, symbol, daily_update): symbol
            for symbol in forex_symbols
        }
        
        completed = 0
        for future in as_completed(future_to_symbol):
            symbol = future_to_symbol[future]
            try:
                future.result()
                completed += 1
                if completed % 10 == 0:
                    print(f"  Progress: {completed}/{len(forex_symbols)}")
            except Exception as e:
                print(f"✗ Error processing {symbol}: {e}")
                update_stats('errors')
    
    elapsed = time.time() - start_time
    
    print("\n" + "=" * 70)
    print("EXECUTION SUMMARY")
    print("=" * 70)
    print(f"Time elapsed: {elapsed:.2f} seconds")
    print(f"Forex pairs processed: {len(forex_symbols)}")
    print(f"Records fetched: {stats['fetched']:,}")
    print(f"Records processed: {stats['inserted']:,}")
    print(f"Errors: {stats['errors']}")
    print("=" * 70)
    print("✓ Forex data population completed!")
    print("=" * 70)

if __name__ == "__main__":
    main()
