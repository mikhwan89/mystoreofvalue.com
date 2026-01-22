#!/usr/bin/env python3
"""
Monthly DCA Performance Update
Runs on 1st-10th of each month to update DCA analysis for recent periods
"""

import sys
import os

# Add parent directory to path to import from calculate_dca_performance
sys.path.insert(0, os.path.dirname(os.path.abspath(__file__)))

from calculate_dca_performance import *
from datetime import datetime, timedelta

# Override configuration for monthly updates
LOOKBACK_DAYS = 10  # Update last 10 days worth of start dates

def main():
    print("=" * 70)
    print("Monthly DCA Performance Update")
    print("=" * 70)
    print(f"Current date: {datetime.now().strftime('%Y-%m-%d')}")
    print(f"Holding periods: {HOLDING_PERIODS} years")
    print(f"DCA frequencies: {DCA_FREQUENCIES}")
    print(f"Lookback: {LOOKBACK_DAYS} days")
    print(f"Workers: {MAX_WORKERS}")
    print("=" * 70)
    
    print("\n--- Fetching Assets ---")
    assets = get_all_assets_with_data()
    print(f"✓ Found {len(assets)} assets")
    
    # Generate start dates for last LOOKBACK_DAYS days (1st of month only)
    today = datetime.now()
    start_dates = []
    
    for i in range(LOOKBACK_DAYS):
        date = today - timedelta(days=i)
        if date.day == 1:  # Only 1st of month
            start_dates.append(datetime(date.year, date.month, 1))
    
    if not start_dates:
        print("⚠ Not the 1st of month and no recent 1st-of-month dates in lookback window")
        return
    
    print(f"\n--- Generating Tasks ---")
    all_tasks = []
    
    for start_date in start_dates:
        for symbol, asset_type, table_name in assets:
            for holding_years in HOLDING_PERIODS:
                for frequency in DCA_FREQUENCIES:
                    from dateutil.relativedelta import relativedelta
                    end_date = start_date + relativedelta(years=holding_years)
                    
                    # Only calculate if end_date is today or earlier
                    if end_date <= today:
                        all_tasks.append((
                            symbol, asset_type, table_name,
                            start_date.strftime('%Y-%m-%d'),
                            end_date.strftime('%Y-%m-%d'),
                            holding_years, frequency
                        ))
    
    print(f"✓ Generated {len(all_tasks):,} tasks")
    print(f"  Start dates: {[d.strftime('%Y-%m-%d') for d in start_dates]}")
    
    if len(all_tasks) == 0:
        print("  No tasks to process")
        return
    
    print(f"\n--- Processing {len(all_tasks):,} tasks ---\n")
    
    start_time = time.time()
    completed = 0
    results_batch = []
    
    with ThreadPoolExecutor(max_workers=MAX_WORKERS) as executor:
        future_to_task = {
            executor.submit(process_single_period, task): task
            for task in all_tasks
        }
        
        for future in as_completed(future_to_task):
            try:
                result = future.result()
                if result:
                    results_batch.append(result)
                
                completed += 1
                
                # Insert in batches
                if len(results_batch) >= 1000:
                    conn = get_db_connection()
                    inserted = insert_dca_performance_batch(conn, results_batch)
                    conn.close()
                    results_batch = []
                
                # Progress updates
                if completed % 500 == 0 or completed == len(all_tasks):
                    elapsed = time.time() - start_time
                    rate = completed / elapsed * 60 if elapsed > 0 else 0
                    pct = completed / len(all_tasks) * 100
                    print(f"  Progress: {completed:,}/{len(all_tasks):,} ({pct:.1f}%) | {rate:.0f} calcs/min")
                    
            except Exception as e:
                if completed % 500 == 0:
                    print(f"  Error: {e}")
    
    # Insert remaining batch
    if results_batch:
        conn = get_db_connection()
        inserted = insert_dca_performance_batch(conn, results_batch)
        conn.close()
    
    elapsed_time = time.time() - start_time
    
    print("\n" + "=" * 70)
    print("SUMMARY")
    print("=" * 70)
    print(f"Time: {elapsed_time:.2f}s ({elapsed_time/60:.1f} minutes)")
    print(f"Tasks completed: {completed:,}")
    print("=" * 70)
    print("✓ Monthly DCA update completed!")
    print("=" * 70)

if __name__ == "__main__":
    main()
