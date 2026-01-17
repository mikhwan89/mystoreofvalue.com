#!/usr/bin/env python3
# Quick debug script to test why calculate_performance_metrics returns None

import os
import psycopg2
from datetime import datetime
from dotenv import load_dotenv

load_dotenv()

DB_CONFIG = {
    'dbname': os.getenv('DB_NAME'),
    'user': os.getenv('DB_USER'),
    'password': os.getenv('DB_PASSWORD'),
    'host': os.getenv('DB_HOST', 'localhost'),
    'port': os.getenv('DB_PORT', '5432')
}

def get_db_connection():
    return psycopg2.connect(**DB_CONFIG)

# Test with a known symbol
TEST_SYMBOL = 'BTCUSD'  # Bitcoin should definitely have data
TEST_TABLE = 'crypto_prices'
START_DATE = '2010-01-01'
END_DATE = '2013-01-01'
HOLDING_YEARS = 3

print("=" * 70)
print("DEBUG: Testing Performance Calculation")
print("=" * 70)
print(f"Symbol: {TEST_SYMBOL}")
print(f"Table: {TEST_TABLE}")
print(f"Period: {START_DATE} to {END_DATE} ({HOLDING_YEARS} years)")
print()

conn = get_db_connection()
cursor = conn.cursor()

# Check 1: Does the symbol exist?
cursor.execute(f"SELECT COUNT(*) FROM {TEST_TABLE} WHERE symbol = %s", (TEST_SYMBOL,))
total_records = cursor.fetchone()[0]
print(f"✓ Total records for {TEST_SYMBOL}: {total_records:,}")

# Check 2: How many have price_usd?
cursor.execute(f"SELECT COUNT(*) FROM {TEST_TABLE} WHERE symbol = %s AND price_usd IS NOT NULL", (TEST_SYMBOL,))
with_usd = cursor.fetchone()[0]
print(f"✓ Records with price_usd: {with_usd:,}")

# Check 3: Date range
cursor.execute(f"SELECT MIN(date), MAX(date) FROM {TEST_TABLE} WHERE symbol = %s", (TEST_SYMBOL,))
min_date, max_date = cursor.fetchone()
print(f"✓ Date range: {min_date} to {max_date}")

# Check 4: Get data for the test period
cursor.execute(f"""
    SELECT date, price_usd
    FROM {TEST_TABLE}
    WHERE symbol = %s
    AND date >= %s
    AND date <= %s
    AND price_usd IS NOT NULL
    ORDER BY date ASC
""", (TEST_SYMBOL, START_DATE, END_DATE))

price_data = cursor.fetchall()
print(f"✓ Records in test period: {len(price_data)}")

if price_data:
    dates = [d[0] for d in price_data]
    first_date = dates[0]
    last_date = dates[-1]
    
    print(f"  First date: {first_date}")
    print(f"  Last date: {last_date}")
    
    # Check exact match
    start_dt = datetime.strptime(START_DATE, '%Y-%m-%d').date()
    end_dt = datetime.strptime(END_DATE, '%Y-%m-%d').date()
    
    print()
    print("VALIDATION CHECKS:")
    print(f"  Expected start: {start_dt}")
    print(f"  Actual start:   {first_date}")
    print(f"  Match: {first_date == start_dt}")
    
    print(f"  Expected end:   {end_dt}")
    print(f"  Actual end:     {last_date}")
    print(f"  Match: {last_date == end_dt}")
    
    # Check data completeness
    expected_days = HOLDING_YEARS * 365
    min_required_days = int(expected_days * 0.7)
    actual_days = (last_date - first_date).days
    
    print()
    print("DATA COMPLETENESS:")
    print(f"  Expected days: {expected_days}")
    print(f"  Minimum required: {min_required_days}")
    print(f"  Actual records: {len(price_data)}")
    print(f"  Actual day span: {actual_days}")
    print(f"  Passes minimum check: {len(price_data) >= min_required_days}")
    print(f"  Passes span check: {actual_days >= expected_days - 10}")
    
    # Show why it might be failing
    print()
    print("DIAGNOSIS:")
    if first_date != start_dt:
        print(f"  ❌ FAIL: First date ({first_date}) != Expected start ({start_dt})")
        print(f"      Difference: {(first_date - start_dt).days} days")
    else:
        print(f"  ✓ PASS: Start date matches exactly")
    
    if last_date != end_dt:
        print(f"  ❌ FAIL: Last date ({last_date}) != Expected end ({end_dt})")
        print(f"      Difference: {(last_date - end_dt).days} days")
    else:
        print(f"  ✓ PASS: End date matches exactly")
    
    if len(price_data) < min_required_days:
        print(f"  ❌ FAIL: Not enough data ({len(price_data)} < {min_required_days})")
    else:
        print(f"  ✓ PASS: Sufficient data points")
    
    if actual_days < expected_days - 10:
        print(f"  ❌ FAIL: Date span too short ({actual_days} < {expected_days - 10})")
    else:
        print(f"  ✓ PASS: Date span covers full period")

else:
    print("❌ NO DATA found in the test period!")

cursor.close()
conn.close()

print()
print("=" * 70)
