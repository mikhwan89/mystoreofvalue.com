#!/usr/bin/env python3

import os
import requests
import psycopg2
from psycopg2.extras import execute_batch
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

MAX_WORKERS = 6
RETRY_DELAY = 2
MAX_RETRIES = 3

def get_db_connection():
    """Create a new database connection"""
    return psycopg2.connect(**DB_CONFIG)

def get_exchanges_from_db():
    """Fetch all exchanges from the database"""
    conn = get_db_connection()
    cursor = conn.cursor()
    
    try:
        cursor.execute("""
            SELECT DISTINCT exchange
            FROM exchanges
            WHERE exchange IS NOT NULL
            ORDER BY exchange
        """)
        
        exchanges = [row[0] for row in cursor.fetchall()]
        return exchanges
        
    finally:
        cursor.close()
        conn.close()

def fetch_holidays_for_exchange(exchange, retries=0):
    """Fetch holidays for a specific exchange from FMP API"""
    url = "https://financialmodelingprep.com/stable/holidays-by-exchange"
    
    params = {
        'exchange': exchange,
        'apikey': API_KEY
    }
    
    try:
        response = requests.get(url, params=params, timeout=30)
        
        # Handle rate limiting
        if response.status_code == 429:
            if retries < MAX_RETRIES:
                print(f"⚠ Rate limit hit for {exchange}. Waiting {RETRY_DELAY}s...")
                time.sleep(RETRY_DELAY)
                return fetch_holidays_for_exchange(exchange, retries + 1)
            else:
                print(f"✗ Max retries reached for {exchange}")
                return []
        
        response.raise_for_status()
        data = response.json()
        
        if isinstance(data, list):
            print(f"✓ Fetched {len(data)} holidays for {exchange}")
            return data
        else:
            # Some exchanges might not have holiday data
            return []
            
    except requests.exceptions.RequestException as e:
        print(f"✗ Error fetching holidays for {exchange}: {e}")
        return []

def insert_holidays_batch(conn, holidays_data):
    """Insert holidays in batch"""
    if not holidays_data:
        return 0, 0
    
    cursor = conn.cursor()
    inserted = 0
    updated = 0
    
    try:
        values = [
            (h['exchange'], h['date'], h.get('name', 'Holiday'))
            for h in holidays_data
        ]
        
        execute_batch(cursor, """
            INSERT INTO exchange_holidays (exchange, holiday_date, holiday_name)
            VALUES (%s, %s, %s)
            ON CONFLICT (exchange, holiday_date) 
            DO UPDATE SET 
                holiday_name = EXCLUDED.holiday_name,
                updated_at = CURRENT_TIMESTAMP
            RETURNING (xmax = 0) AS inserted
        """, values, page_size=1000)
        
        results = cursor.fetchall()
        inserted = sum(1 for r in results if r[0])
        updated = len(results) - inserted
        
        conn.commit()
        
    except Exception as e:
        print(f"✗ Error inserting holidays: {e}")
        conn.rollback()
    finally:
        cursor.close()
    
    return inserted, updated

def process_exchange(exchange):
    """Fetch and store holidays for a single exchange"""
    holidays = fetch_holidays_for_exchange(exchange)
    
    if holidays:
        # Add exchange to each holiday record
        for h in holidays:
            h['exchange'] = exchange
        
        conn = get_db_connection()
        inserted, updated = insert_holidays_batch(conn, holidays)
        conn.close()
        
        return (exchange, len(holidays), inserted, updated)
    
    return (exchange, 0, 0, 0)

def main():
    """Main function to populate exchange holidays"""
    
    print("=" * 70)
    print("Exchange Holidays Populator")
    print("=" * 70)
    
    # Validate environment variables
    if not API_KEY:
        print("✗ ERROR: FMP_API_KEY not found in .env file")
        return
    
    if not DB_CONFIG['password']:
        print("✗ ERROR: DB_PASSWORD not found in .env file")
        return
    
    print("✓ Environment variables loaded\n")
    
    # Get exchanges
    print("--- Fetching Exchanges ---")
    exchanges = get_exchanges_from_db()
    print(f"✓ Found {len(exchanges)} exchanges\n")
    
    # Process exchanges in parallel
    print(f"--- Fetching Holidays for {len(exchanges)} Exchanges ---\n")
    start_time = time.time()
    
    total_holidays = 0
    total_inserted = 0
    total_updated = 0
    
    with ThreadPoolExecutor(max_workers=MAX_WORKERS) as executor:
        future_to_exchange = {
            executor.submit(process_exchange, exchange): exchange
            for exchange in exchanges
        }
        
        for future in as_completed(future_to_exchange):
            exchange = future_to_exchange[future]
            try:
                ex, holidays, inserted, updated = future.result()
                total_holidays += holidays
                total_inserted += inserted
                total_updated += updated
                
                if holidays > 0:
                    print(f"✓ {ex}: {holidays} holidays ({inserted} new, {updated} updated)")
                    
            except Exception as e:
                print(f"✗ Exception for {exchange}: {e}")
    
    elapsed_time = time.time() - start_time
    
    # Summary
    print("\n" + "=" * 70)
    print("EXECUTION SUMMARY")
    print("=" * 70)
    print(f"Time elapsed: {elapsed_time:.2f} seconds")
    print(f"Exchanges processed: {len(exchanges)}")
    print(f"Total holidays fetched: {total_holidays:,}")
    print(f"New holidays inserted: {total_inserted:,}")
    print(f"Existing holidays updated: {total_updated:,}")
    
    # Database statistics
    conn = get_db_connection()
    cursor = conn.cursor()
    
    cursor.execute("""
        SELECT 
            exchange,
            COUNT(*) as holiday_count
        FROM exchange_holidays
        GROUP BY exchange
        ORDER BY holiday_count DESC
        LIMIT 10
    """)
    
    print("\n" + "=" * 70)
    print("TOP 10 EXCHANGES BY HOLIDAY COUNT")
    print("=" * 70)
    for exchange, count in cursor.fetchall():
        print(f"  {exchange}: {count} holidays")
    
    cursor.close()
    conn.close()
    
    print("\n" + "=" * 70)
    print("✓ Exchange holidays population completed!")
    print("=" * 70)

if __name__ == "__main__":
    main()
