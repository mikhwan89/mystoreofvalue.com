#!/usr/bin/env python3

import os
import requests
import psycopg2
from concurrent.futures import ThreadPoolExecutor, as_completed
from dotenv import load_dotenv
import time

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

# Performance settings
MAX_WORKERS = 6
RETRY_DELAY = 2
MAX_RETRIES = 3

# Exchange to currency mapping (common mappings)
EXCHANGE_CURRENCY_MAP = {
    'US': 'USD',      # United States
    'CA': 'CAD',      # Canada
    'GB': 'GBP',      # United Kingdom
    'UK': 'GBP',      # United Kingdom (alternative)
    'JP': 'JPY',      # Japan
    'CN': 'CNY',      # China
    'HK': 'HKD',      # Hong Kong
    'AU': 'AUD',      # Australia
    'NZ': 'NZD',      # New Zealand
    'IN': 'INR',      # India
    'KR': 'KRW',      # South Korea
    'SG': 'SGD',      # Singapore
    'DE': 'EUR',      # Germany
    'FR': 'EUR',      # France
    'IT': 'EUR',      # Italy
    'ES': 'EUR',      # Spain
    'NL': 'EUR',      # Netherlands
    'BE': 'EUR',      # Belgium
    'AT': 'EUR',      # Austria
    'CH': 'CHF',      # Switzerland
    'SE': 'SEK',      # Sweden
    'NO': 'NOK',      # Norway
    'DK': 'DKK',      # Denmark
    'BR': 'BRL',      # Brazil
    'MX': 'MXN',      # Mexico
    'ZA': 'ZAR',      # South Africa
}

# Special exchange mappings (for exchanges without proper country codes)
SPECIAL_EXCHANGE_CURRENCY = {
    'DXE': 'EUR',     # CBOE Europe (Amsterdam-based, primarily EUR)
    'EURONEXT': 'EUR',
    'XETRA': 'EUR',
    'LSE': 'GBP',     # London Stock Exchange
    'TSX': 'CAD',     # Toronto Stock Exchange
    'ASX': 'AUD',     # Australian Securities Exchange
}

def get_db_connection():
    """Create a new database connection"""
    return psycopg2.connect(**DB_CONFIG)

def get_currency_for_exchange(exchange_code, country_code):
    """
    Determine currency based on exchange code or country code
    Prioritizes exchange-specific mapping over country mapping
    """
    # First check if exchange has a special mapping
    if exchange_code in SPECIAL_EXCHANGE_CURRENCY:
        return SPECIAL_EXCHANGE_CURRENCY[exchange_code]
    
    # Then check country code mapping
    if country_code and country_code in EXCHANGE_CURRENCY_MAP:
        return EXCHANGE_CURRENCY_MAP[country_code]
    
    # Default to USD if no mapping found
    return 'USD'

def fetch_exchanges_from_db():
    """
    Fetch all exchanges from the database
    Returns list of (exchange_code, country_code) tuples
    """
    conn = get_db_connection()
    cursor = conn.cursor()
    
    try:
        cursor.execute("""
            SELECT DISTINCT exchange, country_code
            FROM exchanges
            WHERE exchange IS NOT NULL
            ORDER BY exchange
        """)
        
        exchanges = cursor.fetchall()
        print(f"✓ Found {len(exchanges)} exchanges in database")
        return exchanges
        
    finally:
        cursor.close()
        conn.close()

def fetch_stocks_for_exchange(exchange, country_code, retries=0):
    """
    Fetch actively trading stocks for a specific exchange using company-screener
    """
    url = "https://financialmodelingprep.com/stable/company-screener"
    
    params = {
        'exchange': exchange,
        'isActivelyTrading': 'true',
        'limit': 100000,
        'apikey': API_KEY
    }
    
    try:
        response = requests.get(url, params=params, timeout=60)
        
        # Handle rate limiting
        if response.status_code == 429:
            if retries < MAX_RETRIES:
                print(f"⚠ Rate limit hit for {exchange}. Waiting {RETRY_DELAY}s... (Retry {retries+1}/{MAX_RETRIES})")
                time.sleep(RETRY_DELAY)
                return fetch_stocks_for_exchange(exchange, country_code, retries + 1)
            else:
                print(f"✗ Max retries reached for {exchange}")
                return []
        
        response.raise_for_status()
        data = response.json()
        
        if isinstance(data, list):
            # Add currency information based on exchange country
            currency = get_currency_for_exchange(exchange, country_code)
            for stock in data:
                stock['currency'] = currency
                stock['exchange_code'] = exchange
                stock['country_code'] = country_code
            
            print(f"✓ Fetched {len(data)} stocks from {exchange}")
            return data
        else:
            print(f"✗ Unexpected response format for {exchange}")
            return []
            
    except requests.exceptions.RequestException as e:
        print(f"✗ Error fetching {exchange} stocks: {e}")
        return []

def insert_stocks_metadata_batch(conn, stocks_list):
    """
    Insert or update stocks metadata in batch
    """
    cursor = conn.cursor()
    inserted = 0
    updated = 0
    errors = 0
    
    try:
        for stock in stocks_list:
            try:
                symbol = stock.get('symbol')
                name = stock.get('companyName')
                exchange = stock.get('exchangeShortName') or stock.get('exchange_code')
                currency = stock.get('currency', 'USD')
                is_actively_trading = stock.get('isActivelyTrading', True)
                sector = stock.get('sector')
                industry = stock.get('industry')
                is_etf = stock.get('isEtf', False)
                is_fund = stock.get('isFund', False)
                
                if not symbol or not name:
                    continue
                
                # Insert or update stock metadata
                cursor.execute("""
                    INSERT INTO asset_metadata 
                    (symbol, name, asset_type, exchange, currency, 
                     is_actively_trading, sector, industry, is_etf, is_fund)
                    VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s, %s)
                    ON CONFLICT (symbol) 
                    DO UPDATE SET 
                        name = EXCLUDED.name,
                        exchange = EXCLUDED.exchange,
                        currency = EXCLUDED.currency,
                        is_actively_trading = EXCLUDED.is_actively_trading,
                        sector = EXCLUDED.sector,
                        industry = EXCLUDED.industry,
                        is_etf = EXCLUDED.is_etf,
                        is_fund = EXCLUDED.is_fund,
                        updated_at = CURRENT_TIMESTAMP
                    RETURNING (xmax = 0) AS inserted
                """, (symbol, name, 'stock', exchange, currency, 
                      is_actively_trading, sector, industry, is_etf, is_fund))
                
                result = cursor.fetchone()
                if result and result[0]:
                    inserted += 1
                else:
                    updated += 1
                    
            except Exception as e:
                errors += 1
                continue
        
        conn.commit()
        
    except Exception as e:
        print(f"✗ Error in batch insert: {e}")
        conn.rollback()
    finally:
        cursor.close()
    
    return inserted, updated, errors

def mark_inactive_stocks(conn):
    """
    Mark stocks as inactive if their metadata hasn't been updated in 24 hours
    This indicates they're no longer actively trading
    """
    cursor = conn.cursor()
    
    try:
        cursor.execute("""
            UPDATE asset_metadata
            SET is_actively_trading = false,
                updated_at = CURRENT_TIMESTAMP
            WHERE asset_type = 'stock'
            AND is_actively_trading = true
            AND updated_at < NOW() - INTERVAL '24 hours'
        """)
        
        marked_inactive = cursor.rowcount
        conn.commit()
        
        if marked_inactive > 0:
            print(f"✓ Marked {marked_inactive} stocks as inactive (not updated in 24 hours)")
        
        return marked_inactive
        
    except Exception as e:
        print(f"✗ Error marking inactive stocks: {e}")
        conn.rollback()
        return 0
    finally:
        cursor.close()

def process_exchange(exchange_data):
    """Process a single exchange - fetch and insert stocks"""
    exchange, country_code = exchange_data
    
    stocks_data = fetch_stocks_for_exchange(exchange, country_code)
    
    if stocks_data:
        conn = get_db_connection()
        inserted, updated, errors = insert_stocks_metadata_batch(conn, stocks_data)
        conn.close()
        return (exchange, len(stocks_data), inserted, updated, errors)
    
    return (exchange, 0, 0, 0, 0)

def main():
    """Main function to populate stocks metadata"""
    
    print("=" * 70)
    print("Stocks Metadata Populator (Company Screener)")
    print("=" * 70)
    
    # Validate environment variables
    if not API_KEY:
        print("✗ ERROR: FMP_API_KEY not found in .env file")
        return
    
    if not DB_CONFIG['password']:
        print("✗ ERROR: DB_PASSWORD not found in .env file")
        return
    
    print("✓ Environment variables loaded\n")
    
    # Fetch exchanges from database
    print("--- Fetching Exchanges ---")
    exchanges = fetch_exchanges_from_db()
    
    if not exchanges:
        print("✗ No exchanges found in database")
        return
    
    print()
    
    # Process all exchanges in parallel
    print(f"--- Processing {len(exchanges)} Exchanges ---")
    print(f"Using {MAX_WORKERS} parallel workers\n")
    
    start_time = time.time()
    total_stocks = 0
    total_inserted = 0
    total_updated = 0
    total_errors = 0
    
    with ThreadPoolExecutor(max_workers=MAX_WORKERS) as executor:
        future_to_exchange = {
            executor.submit(process_exchange, exchange_data): exchange_data[0]
            for exchange_data in exchanges
        }
        
        for future in as_completed(future_to_exchange):
            exchange = future_to_exchange[future]
            try:
                exchange_name, stocks_count, inserted, updated, errors = future.result()
                total_stocks += stocks_count
                total_inserted += inserted
                total_updated += updated
                total_errors += errors
                
                if stocks_count > 0:
                    print(f"✓ {exchange_name}: {stocks_count} stocks ({inserted} new, {updated} updated)")
                    
            except Exception as e:
                print(f"✗ Exception for {exchange}: {e}")
    
    elapsed_time = time.time() - start_time
    
    # Mark stocks as inactive if not updated in 24 hours
    print("\n--- Marking Inactive Stocks ---")
    conn = get_db_connection()
    marked_inactive = mark_inactive_stocks(conn)
    conn.close()
    
    # Display summary
    print("\n" + "=" * 70)
    print("EXECUTION SUMMARY")
    print("=" * 70)
    print(f"Time elapsed: {elapsed_time:.2f} seconds")
    print(f"Exchanges processed: {len(exchanges)}")
    print(f"Total stocks fetched: {total_stocks:,}")
    print(f"New stocks inserted: {total_inserted:,}")
    print(f"Existing stocks updated: {total_updated:,}")
    print(f"Stocks marked inactive: {marked_inactive:,}")
    print(f"Errors: {total_errors}")
    
    # Database statistics
    conn = get_db_connection()
    cursor = conn.cursor()
    
    cursor.execute("""
        SELECT 
            asset_type,
            COUNT(*) as total,
            COUNT(*) FILTER (WHERE is_actively_trading = true) as active,
            COUNT(*) FILTER (WHERE is_actively_trading = false) as inactive
        FROM asset_metadata
        GROUP BY asset_type
        ORDER BY asset_type
    """)
    
    print("\n" + "=" * 70)
    print("DATABASE STATISTICS")
    print("=" * 70)
    results = cursor.fetchall()
    for asset_type, total, active, inactive in results:
        print(f"{asset_type}: {total:,} total ({active:,} active, {inactive:,} inactive)")
    
    cursor.close()
    conn.close()
    
    print("\n" + "=" * 70)
    print("✓ Stocks metadata population completed!")
    print("=" * 70)

if __name__ == "__main__":
    main()
