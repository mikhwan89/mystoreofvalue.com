#!/usr/bin/env python3

import os
import requests
import psycopg2
from dotenv import load_dotenv

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

def get_db_connection():
    """Create a new database connection"""
    return psycopg2.connect(**DB_CONFIG)

def fetch_exchanges_data():
    """
    Fetch available exchanges from FMP API
    Returns list of exchange data dictionaries
    """
    url = "https://financialmodelingprep.com/stable/available-exchanges"
    
    params = {
        'apikey': API_KEY
    }
    
    try:
        response = requests.get(url, params=params, timeout=30)
        response.raise_for_status()
        data = response.json()
        
        if isinstance(data, list):
            print(f"✓ Fetched {len(data)} exchanges")
            return data
        else:
            print("✗ Unexpected response format from available-exchanges endpoint")
            return []
            
    except requests.exceptions.RequestException as e:
        print(f"✗ Error fetching exchanges data: {e}")
        return []

def insert_exchanges(conn, exchanges_list):
    """
    Insert or update exchanges in the database
    """
    cursor = conn.cursor()
    inserted = 0
    updated = 0
    
    try:
        for exchange_data in exchanges_list:
            exchange = exchange_data.get('exchange')
            name = exchange_data.get('name')
            country_name = exchange_data.get('countryName')
            country_code = exchange_data.get('countryCode')
            symbol_suffix = exchange_data.get('symbolSuffix')
            delay = exchange_data.get('delay')
            
            if not exchange or not name:
                continue
            
            # Insert or update exchange
            cursor.execute("""
                INSERT INTO exchanges (exchange, name, country_name, country_code, symbol_suffix, delay)
                VALUES (%s, %s, %s, %s, %s, %s)
                ON CONFLICT (exchange) 
                DO UPDATE SET 
                    name = EXCLUDED.name,
                    country_name = EXCLUDED.country_name,
                    country_code = EXCLUDED.country_code,
                    symbol_suffix = EXCLUDED.symbol_suffix,
                    delay = EXCLUDED.delay
                RETURNING (xmax = 0) AS inserted
            """, (exchange, name, country_name, country_code, symbol_suffix, delay))
            
            result = cursor.fetchone()
            if result and result[0]:
                inserted += 1
            else:
                updated += 1
        
        conn.commit()
        print(f"✓ Inserted {inserted} new exchanges")
        print(f"✓ Updated {updated} existing exchanges")
        
    except Exception as e:
        print(f"✗ Error inserting exchanges: {e}")
        conn.rollback()
    finally:
        cursor.close()

def main():
    """Main function to populate exchanges table"""
    
    print("=" * 70)
    print("Exchanges Data Populator")
    print("=" * 70)
    
    # Validate environment variables
    if not API_KEY:
        print("✗ ERROR: FMP_API_KEY not found in .env file")
        return
    
    if not DB_CONFIG['password']:
        print("✗ ERROR: DB_PASSWORD not found in .env file")
        return
    
    print("✓ Environment variables loaded\n")
    
    # Fetch exchanges data from API
    print("--- Fetching Exchanges from API ---")
    exchanges_data = fetch_exchanges_data()
    
    if not exchanges_data:
        print("✗ No exchanges data found")
        return
    
    print()
    
    # Insert into database
    print("--- Inserting into Database ---")
    conn = get_db_connection()
    insert_exchanges(conn, exchanges_data)
    conn.close()
    
    # Display summary
    print("\n--- Summary ---")
    conn = get_db_connection()
    cursor = conn.cursor()
    
    cursor.execute("""
        SELECT 
            country_name,
            COUNT(*) as count
        FROM exchanges
        WHERE country_name IS NOT NULL
        GROUP BY country_name
        ORDER BY count DESC
        LIMIT 10
    """)
    
    results = cursor.fetchall()
    print("\nTop 10 Countries by Exchange Count:")
    for country, count in results:
        print(f"  {country}: {count} exchanges")
    
    cursor.execute("SELECT COUNT(*) FROM exchanges")
    total = cursor.fetchone()[0]
    print(f"\nTotal exchanges in table: {total}")
    
    cursor.close()
    conn.close()
    
    print("\n" + "=" * 70)
    print("✓ Exchanges data population completed!")
    print("=" * 70)

if __name__ == "__main__":
    main()
