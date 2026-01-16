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

def fetch_commodities_metadata():
    """
    Fetch commodities list with metadata from FMP API
    Returns list of commodity metadata dictionaries
    """
    url = "https://financialmodelingprep.com/stable/commodities-list"
    
    params = {
        'apikey': API_KEY
    }
    
    try:
        response = requests.get(url, params=params, timeout=30)
        response.raise_for_status()
        data = response.json()
        
        if isinstance(data, list):
            print(f"✓ Fetched metadata for {len(data)} commodities")
            return data
        else:
            print("✗ Unexpected response format from commodities-list endpoint")
            return []
            
    except requests.exceptions.RequestException as e:
        print(f"✗ Error fetching commodities metadata: {e}")
        return []

def fetch_indices_metadata():
    """
    Fetch indices list with metadata from FMP API
    Returns list of index metadata dictionaries
    """
    url = "https://financialmodelingprep.com/stable/index-list"
    
    params = {
        'apikey': API_KEY
    }
    
    try:
        response = requests.get(url, params=params, timeout=30)
        response.raise_for_status()
        data = response.json()
        
        if isinstance(data, list):
            print(f"✓ Fetched metadata for {len(data)} indices")
            return data
        else:
            print("✗ Unexpected response format from index-list endpoint")
            return []
            
    except requests.exceptions.RequestException as e:
        print(f"✗ Error fetching indices metadata: {e}")
        return []

def normalize_currency(currency):
    """Normalize currency code (USX -> USD)"""
    if currency == 'USX':
        return 'USD'
    return currency

def insert_metadata(conn, metadata_list):
    """
    Insert or update asset metadata in the database
    """
    cursor = conn.cursor()
    inserted = 0
    updated = 0
    
    try:
        for metadata in metadata_list:
            symbol = metadata.get('symbol')
            name = metadata.get('name')
            asset_type = metadata.get('asset_type')
            exchange = metadata.get('exchange')
            currency = normalize_currency(metadata.get('currency', 'USD'))
            
            if not symbol or not name:
                continue
            
            # Insert or update metadata
            cursor.execute("""
                INSERT INTO asset_metadata (symbol, name, asset_type, exchange, currency)
                VALUES (%s, %s, %s, %s, %s)
                ON CONFLICT (symbol) 
                DO UPDATE SET 
                    name = EXCLUDED.name,
                    asset_type = EXCLUDED.asset_type,
                    exchange = EXCLUDED.exchange,
                    currency = EXCLUDED.currency
                RETURNING (xmax = 0) AS inserted
            """, (symbol, name, asset_type, exchange, currency))
            
            result = cursor.fetchone()
            if result and result[0]:
                inserted += 1
            else:
                updated += 1
        
        conn.commit()
        print(f"✓ Inserted {inserted} new records")
        print(f"✓ Updated {updated} existing records")
        
    except Exception as e:
        print(f"✗ Error inserting metadata: {e}")
        conn.rollback()
    finally:
        cursor.close()

def main():
    """Main function to populate asset_metadata table"""
    
    print("=" * 70)
    print("Asset Metadata Populator")
    print("=" * 70)
    
    # Validate environment variables
    if not API_KEY:
        print("✗ ERROR: FMP_API_KEY not found in .env file")
        return
    
    if not DB_CONFIG['password']:
        print("✗ ERROR: DB_PASSWORD not found in .env file")
        return
    
    print("✓ Environment variables loaded\n")
    
    # Prepare crypto metadata (hardcoded)
    crypto_metadata = [
        {
            'symbol': 'BTCUSD',
            'name': 'Bitcoin',
            'asset_type': 'crypto',
            'exchange': None,
            'currency': 'USD'
        },
        {
            'symbol': 'ETHUSD',
            'name': 'Ethereum',
            'asset_type': 'crypto',
            'exchange': None,
            'currency': 'USD'
        }
    ]
    
    print(f"--- Crypto Assets (Hardcoded) ---")
    print(f"✓ Prepared {len(crypto_metadata)} crypto assets\n")
    
    # Fetch commodity metadata from API
    print("--- Commodity Assets (API) ---")
    commodities_data = fetch_commodities_metadata()
    
    # Transform commodity data to match our metadata format
    commodity_metadata = []
    for item in commodities_data:
        commodity_metadata.append({
            'symbol': item.get('symbol'),
            'name': item.get('name'),
            'asset_type': 'commodity',
            'exchange': item.get('stockExchange'),
            'currency': item.get('currency', 'USD')
        })
    
    print()
    
    # Fetch indices metadata from API
    print("--- Index Assets (API) ---")
    indices_data = fetch_indices_metadata()
    
    # Transform indices data to match our metadata format
    index_metadata = []
    for item in indices_data:
        index_metadata.append({
            'symbol': item.get('symbol'),
            'name': item.get('name'),
            'asset_type': 'index',
            'exchange': item.get('exchange'),
            'currency': item.get('currency', 'USD')
        })
    
    print()
    
    # Combine all metadata
    all_metadata = crypto_metadata + commodity_metadata + index_metadata
    
    print(f"--- Inserting into Database ---")
    print(f"Total assets to process: {len(all_metadata)}")
    print(f"  - Crypto: {len(crypto_metadata)}")
    print(f"  - Commodities: {len(commodity_metadata)}")
    print(f"  - Indices: {len(index_metadata)}\n")
    
    # Insert into database
    conn = get_db_connection()
    insert_metadata(conn, all_metadata)
    conn.close()
    
    # Display summary
    print("\n--- Summary ---")
    conn = get_db_connection()
    cursor = conn.cursor()
    
    cursor.execute("""
        SELECT 
            asset_type,
            COUNT(*) as count
        FROM asset_metadata
        GROUP BY asset_type
        ORDER BY asset_type
    """)
    
    results = cursor.fetchall()
    print("\nAsset Metadata Summary:")
    for asset_type, count in results:
        print(f"  {asset_type}: {count} assets")
    
    cursor.execute("SELECT COUNT(*) FROM asset_metadata")
    total = cursor.fetchone()[0]
    print(f"\nTotal assets in metadata table: {total}")
    
    cursor.close()
    conn.close()
    
    print("\n" + "=" * 70)
    print("✓ Asset metadata population completed!")
    print("=" * 70)

if __name__ == "__main__":
    main()
