#!/usr/bin/env python3

import os
import psycopg2
from concurrent.futures import ThreadPoolExecutor, as_completed
from dotenv import load_dotenv
import time
import threading

load_dotenv()

DB_CONFIG = {
    'dbname': os.getenv('DB_NAME'),
    'user': os.getenv('DB_USER'),
    'password': os.getenv('DB_PASSWORD'),
    'host': os.getenv('DB_HOST', 'localhost'),
    'port': os.getenv('DB_PORT', '5432')
}

MAX_WORKERS = 7
BATCH_SIZE = 5000

# Thread-safe counters
stats_lock = threading.Lock()
stats = {'usd_updated': 0, 'converted': 0}

def update_stats(key, value=1):
    with stats_lock:
        stats[key] += value

def get_db_connection():
    return psycopg2.connect(**DB_CONFIG)

def get_symbols_by_currency(table_name, asset_type, daily_mode=False):
    """
    Group symbols by their currency for efficient batch processing
    Returns dict: {currency: [symbols]}
    """
    conn = get_db_connection()
    cursor = conn.cursor()
    
    date_filter = "AND EXISTS (SELECT 1 FROM {} WHERE symbol = am.symbol AND date >= CURRENT_DATE - INTERVAL '10 days')".format(table_name) if daily_mode else ""
    
    cursor.execute(f"""
        SELECT am.currency, array_agg(DISTINCT am.symbol)
        FROM asset_metadata am
        WHERE am.asset_type = %s
        AND am.currency IS NOT NULL
        {date_filter}
        GROUP BY am.currency
    """, (asset_type,))
    
    result = {row[0]: row[1] for row in cursor.fetchall()}
    
    cursor.close()
    conn.close()
    
    return result

def normalize_usd_symbols(table_name, symbols, daily_mode=False):
    """Normalize symbols that are already in USD (no conversion needed)"""
    if not symbols:
        return 0
    
    conn = get_db_connection()
    cursor = conn.cursor()
    
    date_filter = "AND date >= CURRENT_DATE - INTERVAL '10 days'" if daily_mode else ""
    
    try:
        # Process in batches
        total_updated = 0
        for i in range(0, len(symbols), BATCH_SIZE):
            batch_symbols = symbols[i:i + BATCH_SIZE]
            
            cursor.execute(f"""
                UPDATE {table_name}
                SET price_usd = price
                WHERE symbol = ANY(%s)
                AND (price_usd IS NULL OR price_usd != price)
                {date_filter}
            """, (batch_symbols,))
            
            total_updated += cursor.rowcount
            conn.commit()
        
        return total_updated
        
    finally:
        cursor.close()
        conn.close()

def normalize_currency_batch(table_name, currency, symbols, daily_mode=False):
    """Normalize a batch of symbols for a specific currency"""
    if not symbols:
        return 0
    
    forex_symbol = f"{currency}USD"
    conn = get_db_connection()
    cursor = conn.cursor()
    
    # Check if forex pair exists
    cursor.execute("SELECT COUNT(*) FROM forex_pairs WHERE symbol = %s", (forex_symbol,))
    if cursor.fetchone()[0] == 0:
        cursor.close()
        conn.close()
        return 0
    
    date_filter = "AND p.date >= CURRENT_DATE - INTERVAL '10 days'" if daily_mode else ""
    
    try:
        # Process in batches
        total_updated = 0
        for i in range(0, len(symbols), BATCH_SIZE):
            batch_symbols = symbols[i:i + BATCH_SIZE]
            
            cursor.execute(f"""
                UPDATE {table_name} p
                SET price_usd = p.price * f.price
                FROM forex_prices f
                WHERE p.symbol = ANY(%s)
                AND f.symbol = %s
                AND f.date::date = p.date::date
                AND (p.price_usd IS NULL OR p.price_usd != p.price * f.price)
                {date_filter}
            """, (batch_symbols, forex_symbol))
            
            total_updated += cursor.rowcount
            conn.commit()
        
        return total_updated
        
    finally:
        cursor.close()
        conn.close()

def process_currency_group(args):
    """Process a single currency group (for parallel execution)"""
    table_name, currency, symbols, daily_mode = args
    
    if currency == 'USD':
        updated = normalize_usd_symbols(table_name, symbols, daily_mode)
        update_stats('usd_updated', updated)
        return (currency, len(symbols), updated, 'USD (no conversion)')
    else:
        updated = normalize_currency_batch(table_name, currency, symbols, daily_mode)
        update_stats('converted', updated)
        return (currency, len(symbols), updated, f'{currency}USD')

def normalize_prices_for_table(table_name, asset_type, daily_mode=False):
    """
    Update price_usd for all records in a price table using parallel processing
    Groups symbols by currency and processes each currency group in parallel
    """
    mode_text = "last 10 days" if daily_mode else "all records"
    print(f"\n--- Normalizing {table_name} ({mode_text}) ---")
    
    # Get symbols grouped by currency
    currency_groups = get_symbols_by_currency(table_name, asset_type, daily_mode)
    
    if not currency_groups:
        print(f"  ⚠ No symbols found for {asset_type}")
        return
    
    total_symbols = sum(len(symbols) for symbols in currency_groups.values())
    print(f"  Found {total_symbols:,} symbols across {len(currency_groups)} currencies")
    
    # Prepare tasks for parallel processing
    tasks = [
        (table_name, currency, symbols, daily_mode)
        for currency, symbols in currency_groups.items()
    ]
    
    # Process in parallel
    start_time = time.time()
    
    with ThreadPoolExecutor(max_workers=MAX_WORKERS) as executor:
        futures = {executor.submit(process_currency_group, task): task for task in tasks}
        
        for future in as_completed(futures):
            task = futures[future]
            try:
                currency, symbol_count, updated, forex_pair = future.result()
                if updated > 0:
                    print(f"  ✓ {currency}: {updated:,} records updated ({symbol_count} symbols, using {forex_pair})")
            except Exception as e:
                print(f"  ✗ Error processing {task[1]}: {e}")
    
    elapsed = time.time() - start_time
    print(f"  Completed in {elapsed:.2f} seconds")

def main():
    import sys
    
    daily_mode = '--daily' in sys.argv
    mode_text = "Daily Update (Last 10 Days)" if daily_mode else "Full Normalization (All Records)"
    
    print("=" * 70)
    print("Asset Price USD Normalization")
    print(f"Mode: {mode_text}")
    print(f"Workers: {MAX_WORKERS}")
    print("=" * 70)
    
    if not DB_CONFIG['password']:
        print("✗ ERROR: DB_PASSWORD not found")
        return
    
    print("✓ Environment variables loaded\n")
    
    # Reset stats
    stats['usd_updated'] = 0
    stats['converted'] = 0
    
    start_time = time.time()
    
    # Process each asset type (stocks removed - focusing on crypto, commodities, indices)
    tables = [
        ('crypto_prices', 'crypto'),
        ('commodity_prices', 'commodity'),
        ('index_prices', 'index')
    ]
    
    for table_name, asset_type in tables:
        normalize_prices_for_table(table_name, asset_type, daily_mode)
    
    elapsed = time.time() - start_time
    
    # Summary statistics
    print("\n" + "=" * 70)
    print("SUMMARY STATISTICS")
    print("=" * 70)
    
    conn = get_db_connection()
    cursor = conn.cursor()
    
    for table_name, asset_type in tables:
        cursor.execute(f"""
            SELECT 
                COUNT(*) as total,
                COUNT(price_usd) as with_usd,
                COUNT(*) - COUNT(price_usd) as missing_usd
            FROM {table_name}
        """)
        
        total, with_usd, missing = cursor.fetchone()
        coverage = (with_usd / total * 100) if total > 0 else 0
        
        print(f"\n{table_name}:")
        print(f"  Total records: {total:,}")
        print(f"  With price_usd: {with_usd:,} ({coverage:.2f}%)")
        print(f"  Missing price_usd: {missing:,}")
    
    cursor.close()
    conn.close()
    
    print("\n" + "=" * 70)
    print(f"USD prices updated: {stats['usd_updated']:,}")
    print(f"Converted prices: {stats['converted']:,}")
    print(f"Time elapsed: {elapsed:.2f} seconds ({elapsed/60:.1f} minutes)")
    print("=" * 70)
    print("✓ USD normalization completed!")
    print("=" * 70)

if __name__ == "__main__":
    main()
