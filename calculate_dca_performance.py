#!/usr/bin/env python3

import os
import psycopg2
from psycopg2.extras import execute_batch
import numpy as np
from datetime import datetime, timedelta
from concurrent.futures import ThreadPoolExecutor, as_completed
from dotenv import load_dotenv
import time

# Load environment variables
load_dotenv()

DB_CONFIG = {
    'dbname': os.getenv('DB_NAME'),
    'user': os.getenv('DB_USER'),
    'password': os.getenv('DB_PASSWORD'),
    'host': os.getenv('DB_HOST', 'localhost'),
    'port': os.getenv('DB_PORT', '5432')
}

# Configuration
HOLDING_PERIODS = [3, 4, 5, 6, 7, 8, 9, 10]  # Years
DCA_FREQUENCIES = ['daily', 'weekly', 'monthly']
START_DATE = '2010-01-01'
RISK_FREE_RATE = 0.02
MAX_WORKERS = 1
BATCH_SIZE = 1000
INVESTMENT_PER_PERIOD = 100  # $100 per DCA purchase

def get_db_connection():
    return psycopg2.connect(**DB_CONFIG)

def get_all_assets_with_data():
    """Get all assets that have sufficient price data"""
    conn = get_db_connection()
    cursor = conn.cursor()
    
    all_assets = []
    price_tables = [
        ('crypto_prices', 'crypto'),
        ('commodity_prices', 'commodity'),
        ('index_prices', 'index')
    ]
    
    for table_name, asset_type in price_tables:
        cursor.execute(f"""
            SELECT DISTINCT symbol
            FROM {table_name}
            WHERE date >= %s
            GROUP BY symbol
            HAVING COUNT(*) >= 1000
        """, (START_DATE,))
        
        symbols = cursor.fetchall()
        all_assets.extend([(s[0], asset_type, table_name) for s in symbols])
    
    cursor.close()
    conn.close()
    
    return all_assets

def get_price_data(symbol, table_name, start_date, end_date):
    """Fetch USD-normalized price data"""
    conn = get_db_connection()
    cursor = conn.cursor()
    
    cursor.execute(f"""
        SELECT date, price_usd
        FROM {table_name}
        WHERE symbol = %s
        AND date >= %s
        AND date <= %s
        AND price_usd IS NOT NULL
        ORDER BY date ASC
    """, (symbol, start_date, end_date))
    
    data = cursor.fetchall()
    cursor.close()
    conn.close()
    
    return data

def get_dca_purchase_dates(start_date, end_date, frequency):
    """
    Generate DCA purchase dates based on frequency
    
    daily: Every day
    weekly: Every Monday (or first available day of week)
    monthly: First day of each month
    """
    start_dt = datetime.strptime(str(start_date), '%Y-%m-%d')
    end_dt = datetime.strptime(str(end_date), '%Y-%m-%d')
    
    purchase_dates = []
    current = start_dt
    
    if frequency == 'daily':
        while current <= end_dt:
            purchase_dates.append(current.date())
            current += timedelta(days=1)
    
    elif frequency == 'weekly':
        # Start on first Monday (or current day if start is Monday)
        while current.weekday() != 0:  # 0 = Monday
            current += timedelta(days=1)
        
        while current <= end_dt:
            purchase_dates.append(current.date())
            current += timedelta(days=7)
    
    elif frequency == 'monthly':
        # First day of each month
        current = datetime(start_dt.year, start_dt.month, 1)
        while current <= end_dt:
            if current >= start_dt:
                purchase_dates.append(current.date())
            
            # Move to first day of next month
            if current.month == 12:
                current = datetime(current.year + 1, 1, 1)
            else:
                current = datetime(current.year, current.month + 1, 1)
    
    return purchase_dates

def simulate_dca(price_data, purchase_dates, investment_per_period):
    """
    Simulate DCA strategy
    
    Returns:
        dict with DCA simulation results
    """
    # Create price lookup dict
    price_dict = {d[0].date() if hasattr(d[0], 'date') else d[0]: float(d[1]) 
                  for d in price_data}
    
    # Track purchases
    purchases = []
    total_invested = 0
    total_units = 0
    
    for purchase_date in purchase_dates:
        if purchase_date in price_dict:
            price = price_dict[purchase_date]
            units_bought = investment_per_period / price
            
            purchases.append({
                'date': purchase_date,
                'price': price,
                'units': units_bought,
                'invested': investment_per_period
            })
            
            total_invested += investment_per_period
            total_units += units_bought
    
    if not purchases:
        return None
    
    # Calculate metrics
    prices = np.array([float(d[1]) for d in price_data])
    dates = [d[0].date() if hasattr(d[0], 'date') else d[0] for d in price_data]
    
    average_purchase_price = total_invested / total_units if total_units > 0 else 0
    final_price = prices[-1]
    final_value = total_units * final_price
    
    # Calculate portfolio value over time
    portfolio_values = []
    for i, date in enumerate(dates):
        # Sum up units acquired up to this date
        units_so_far = sum(p['units'] for p in purchases if p['date'] <= date)
        portfolio_value = units_so_far * prices[i]
        portfolio_values.append(portfolio_value)
    
    portfolio_values = np.array(portfolio_values)
    
    # Calculate returns
    total_return_pct = ((final_value - total_invested) / total_invested * 100) if total_invested > 0 else 0
    
    # Calculate volatility of portfolio returns (skip zero values)
    # Use only periods after first purchase
    first_purchase_idx = next((i for i, pv in enumerate(portfolio_values) if pv > 0), 0)
    active_portfolio_values = portfolio_values[first_purchase_idx:]
    
    if len(active_portfolio_values) > 1:
        # Safe division - avoid divide by zero
        with np.errstate(divide='ignore', invalid='ignore'):
            portfolio_returns = np.diff(active_portfolio_values) / active_portfolio_values[:-1]
            # Remove any inf or nan values
            portfolio_returns = portfolio_returns[np.isfinite(portfolio_returns)]
        
        if len(portfolio_returns) > 1:
            volatility_pct = np.std(portfolio_returns, ddof=1) * np.sqrt(365) * 100
        else:
            volatility_pct = 0
    else:
        portfolio_returns = np.array([])
        volatility_pct = 0
    
    # Calculate max drawdown (only on active portfolio)
    if len(active_portfolio_values) > 1:
        peak = active_portfolio_values[0]
        max_dd = 0
        max_dd_idx = 0
        for i, val in enumerate(active_portfolio_values):
            if val > peak:
                peak = val
            dd = (val - peak) / peak if peak > 0 else 0
            if dd < max_dd:
                max_dd = dd
                max_dd_idx = i
        
        max_drawdown_pct = abs(max_dd * 100)
        max_drawdown_date = dates[first_purchase_idx + max_dd_idx]
    else:
        max_drawdown_pct = 0
        max_drawdown_date = dates[0]
    
    # Calculate max loss from cost basis (only after first purchase)
    cost_basis = np.array([sum(p['invested'] for p in purchases if p['date'] <= d) for d in dates])
    
    # Safe division for loss from cost
    with np.errstate(divide='ignore', invalid='ignore'):
        loss_from_cost = np.divide(
            portfolio_values - cost_basis, 
            cost_basis,
            out=np.zeros_like(portfolio_values, dtype=float),
            where=cost_basis > 0
        )
    
    # Only consider periods after first purchase
    active_loss_from_cost = loss_from_cost[first_purchase_idx:]
    if len(active_loss_from_cost) > 0:
        max_loss_idx = first_purchase_idx + np.argmin(active_loss_from_cost)
        max_loss_from_cost_pct = loss_from_cost[max_loss_idx] * 100
        max_loss_from_cost_date = dates[max_loss_idx]
    else:
        max_loss_from_cost_pct = 0
        max_loss_from_cost_date = dates[0]
    
    # Calculate downside deviation (only from valid returns)
    if len(portfolio_returns) > 0:
        negative_returns = portfolio_returns[portfolio_returns < 0]
        if len(negative_returns) > 1:
            downside_dev = np.std(negative_returns, ddof=1) * np.sqrt(365)
        else:
            downside_dev = 0
    else:
        downside_dev = 0
    
    # Price statistics
    purchase_prices = [p['price'] for p in purchases]
    best_purchase_price = min(purchase_prices)
    worst_purchase_price = max(purchase_prices)
    price_variance_pct = ((worst_purchase_price - best_purchase_price) / best_purchase_price * 100) if best_purchase_price > 0 else 0
    
    # Calculate lump sum comparison (if invested all at start)
    first_price = prices[0]
    lumpsum_units = total_invested / first_price
    lumpsum_final_value = lumpsum_units * final_price
    lumpsum_return_pct = ((lumpsum_final_value - total_invested) / total_invested * 100) if total_invested > 0 else 0
    dca_vs_lumpsum_diff = total_return_pct - lumpsum_return_pct
    
    # Risk-adjusted metrics
    holding_years = (dates[-1] - dates[0]).days / 365
    annualized_return_pct = (((final_value / total_invested) ** (1 / holding_years)) - 1) * 100 if holding_years > 0 and total_invested > 0 else 0
    
    sharpe_ratio = ((annualized_return_pct / 100 - RISK_FREE_RATE) / (volatility_pct / 100)) if volatility_pct > 0 else 0
    sortino_ratio = ((annualized_return_pct / 100 - RISK_FREE_RATE) / downside_dev) if downside_dev > 0 else 0
    calmar_ratio = ((annualized_return_pct / 100) / (max_drawdown_pct / 100)) if max_drawdown_pct > 0 else 0
    
    return {
        'total_invested': float(total_invested),
        'number_of_purchases': len(purchases),
        'average_purchase_price': float(average_purchase_price),
        'total_units_acquired': float(total_units),
        'final_value': float(final_value),
        'total_return_pct': float(total_return_pct),
        'annualized_return_pct': float(annualized_return_pct),
        'min_price': float(np.min(prices)),
        'max_price': float(np.max(prices)),
        'final_price': float(final_price),
        'volatility_pct': float(volatility_pct),
        'max_drawdown_pct': float(max_drawdown_pct),
        'max_drawdown_date': max_drawdown_date,
        'max_loss_from_cost_pct': float(max_loss_from_cost_pct),
        'max_loss_from_cost_date': max_loss_from_cost_date,
        'sharpe_ratio': float(sharpe_ratio),
        'sortino_ratio': float(sortino_ratio),
        'calmar_ratio': float(calmar_ratio),
        'best_purchase_price': float(best_purchase_price),
        'worst_purchase_price': float(worst_purchase_price),
        'price_variance_pct': float(price_variance_pct),
        'lumpsum_return_pct': float(lumpsum_return_pct),
        'dca_vs_lumpsum_diff': float(dca_vs_lumpsum_diff)
    }

def calculate_dca_performance(symbol, asset_type, table_name, start_date, end_date, holding_years, frequency):
    """
    Calculate DCA performance for a given period and frequency
    
    Ensures:
    1. Asset has data on EXACT start date
    2. Asset has data on EXACT end date  
    3. Asset has sufficient data for full holding period
    """
    
    # Fetch price data
    price_data = get_price_data(symbol, table_name, start_date, end_date)
    
    # Minimum data requirement: at least 70% of expected days
    expected_days = holding_years * 365
    min_required_days = int(expected_days * 0.7)
    
    if len(price_data) < min_required_days:
        return None
    
    # Validate dates
    dates = [d[0].date() if hasattr(d[0], 'date') else d[0] for d in price_data]
    first_date = dates[0]
    last_date = dates[-1]
    
    start_dt = datetime.strptime(str(start_date), '%Y-%m-%d').date()
    end_dt = datetime.strptime(str(end_date), '%Y-%m-%d').date()
    
    # CRITICAL CHECK 1: Exact start date match
    if first_date != start_dt:
        return None
    
    # CRITICAL CHECK 2: Exact end date match
    if last_date != end_dt:
        return None
    
    # CRITICAL CHECK 3: Full period coverage
    actual_days = (last_date - first_date).days
    if actual_days < expected_days - 10:
        return None
    
    # Get purchase dates
    purchase_dates = get_dca_purchase_dates(start_date, end_date, frequency)
    
    if not purchase_dates:
        return None
    
    # Simulate DCA
    metrics = simulate_dca(price_data, purchase_dates, INVESTMENT_PER_PERIOD)
    
    if not metrics:
        return None
    
    # Add metadata
    metrics['symbol'] = symbol
    metrics['asset_type'] = asset_type
    metrics['start_date'] = start_date
    metrics['end_date'] = end_date
    metrics['holding_period_years'] = holding_years
    metrics['dca_frequency'] = frequency
    
    return metrics

def insert_dca_performance_batch(conn, performance_data):
    """Insert DCA performance metrics in batch"""
    if not performance_data:
        return 0
    
    cursor = conn.cursor()
    
    try:
        values = [
            (
                d['symbol'], d['asset_type'], d['start_date'], d['end_date'],
                d['holding_period_years'], d['dca_frequency'],
                d['total_invested'], d['number_of_purchases'], d['average_purchase_price'],
                d['total_units_acquired'], d['final_value'], d['total_return_pct'],
                d['annualized_return_pct'], d['min_price'], d['max_price'],
                d['final_price'], d['volatility_pct'], d['max_drawdown_pct'],
                d['max_drawdown_date'], d['max_loss_from_cost_pct'], d['max_loss_from_cost_date'],
                d['sharpe_ratio'], d['sortino_ratio'], d['calmar_ratio'],
                d['best_purchase_price'], d['worst_purchase_price'], d['price_variance_pct'],
                d['lumpsum_return_pct'], d['dca_vs_lumpsum_diff']
            )
            for d in performance_data
        ]
        
        execute_batch(cursor, """
            INSERT INTO asset_performance_dca (
                symbol, asset_type, start_date, end_date, holding_period_years, dca_frequency,
                total_invested, number_of_purchases, average_purchase_price,
                total_units_acquired, final_value, total_return_pct,
                annualized_return_pct, min_price, max_price, final_price,
                volatility_pct, max_drawdown_pct, max_drawdown_date,
                max_loss_from_cost_pct, max_loss_from_cost_date,
                sharpe_ratio, sortino_ratio, calmar_ratio,
                best_purchase_price, worst_purchase_price, price_variance_pct,
                lumpsum_return_pct, dca_vs_lumpsum_diff
            )
            VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s)
            ON CONFLICT (symbol, start_date, end_date, dca_frequency) DO UPDATE SET
                total_return_pct = EXCLUDED.total_return_pct,
                annualized_return_pct = EXCLUDED.annualized_return_pct,
                volatility_pct = EXCLUDED.volatility_pct,
                sharpe_ratio = EXCLUDED.sharpe_ratio,
                sortino_ratio = EXCLUDED.sortino_ratio,
                calmar_ratio = EXCLUDED.calmar_ratio,
                dca_vs_lumpsum_diff = EXCLUDED.dca_vs_lumpsum_diff,
                updated_at = CURRENT_TIMESTAMP
        """, values, page_size=BATCH_SIZE)
        
        conn.commit()
        return len(performance_data)
        
    except Exception as e:
        print(f"Error inserting batch: {e}")
        conn.rollback()
        return 0
    finally:
        cursor.close()

def process_single_period(args):
    """Process a single period for a single asset (one task per period)"""
    symbol, asset_type, table_name, start_date, end_date, holding_years, frequency = args
    
    metrics = calculate_dca_performance(
        symbol, asset_type, table_name,
        start_date, end_date,
        holding_years, frequency
    )
    
    if metrics:
        return metrics
    return None

def main():
    print("=" * 70)
    print("Asset Performance DCA Analysis Calculator")
    print("=" * 70)
    print(f"Start Date: {START_DATE}")
    print(f"Holding Periods: {HOLDING_PERIODS} years")
    print(f"DCA Frequencies: {DCA_FREQUENCIES}")
    print(f"Investment per period: ${INVESTMENT_PER_PERIOD}")
    print(f"Workers: {MAX_WORKERS}")
    print("=" * 70)
    
    print("\n--- Fetching Assets ---")
    assets = get_all_assets_with_data()
    print(f"✓ Found {len(assets)} assets with sufficient data")
    
    # Generate all tasks (asset × period × frequency)
    print("\n--- Generating Tasks ---")
    all_tasks = []
    start = datetime.strptime(START_DATE, '%Y-%m-%d')
    today = datetime.now()
    
    for symbol, asset_type, table_name in assets:
        for holding_years in HOLDING_PERIODS:
            for frequency in DCA_FREQUENCIES:
                current_start = datetime(start.year, start.month, 1)
                
                while True:
                    end_year = current_start.year + holding_years
                    end_month = current_start.month
                    end_date = datetime(end_year, end_month, 1)
                    
                    if end_date > today:
                        break
                    
                    all_tasks.append((
                        symbol, asset_type, table_name,
                        current_start.strftime('%Y-%m-%d'),
                        end_date.strftime('%Y-%m-%d'),
                        holding_years, frequency
                    ))
                    
                    # Move to first day of next month
                    if current_start.month == 12:
                        current_start = datetime(current_start.year + 1, 1, 1)
                    else:
                        current_start = datetime(current_start.year, current_start.month + 1, 1)
    
    print(f"✓ Generated {len(all_tasks):,} tasks")
    print(f"  ({len(assets)} assets × ~{len(all_tasks)//len(assets)} periods each)")
    
    print(f"\n--- Processing {len(all_tasks):,} tasks with {MAX_WORKERS} workers ---\n")
    
    start_time = time.time()
    completed = 0
    results_batch = []
    
    with ThreadPoolExecutor(max_workers=MAX_WORKERS) as executor:
        future_to_task = {
            executor.submit(process_single_period, task): task
            for task in all_tasks
        }
        
        for future in as_completed(future_to_task):
            task = future_to_task[future]
            try:
                result = future.result()
                if result:
                    results_batch.append(result)
                
                completed += 1
                
                # Insert in batches of 1000
                if len(results_batch) >= 1000:
                    conn = get_db_connection()
                    inserted = insert_dca_performance_batch(conn, results_batch)
                    conn.close()
                    results_batch = []
                
                # Progress every 500 tasks
                if completed % 500 == 0:
                    elapsed = time.time() - start_time
                    rate = completed / elapsed * 60 if elapsed > 0 else 0
                    remaining = (len(all_tasks) - completed) / rate if rate > 0 else 0
                    pct = completed / len(all_tasks) * 100
                    
                    print(f"  Progress: {completed:,}/{len(all_tasks):,} ({pct:.1f}%) | "
                          f"{rate:.0f} calcs/min | ~{remaining:.0f}min left")
                    
            except Exception as e:
                if completed % 500 == 0:
                    print(f"  Error in task: {e}")
    
    # Insert remaining batch
    if results_batch:
        conn = get_db_connection()
        inserted = insert_dca_performance_batch(conn, results_batch)
        conn.close()
    
    elapsed_time = time.time() - start_time
    
    print("\n" + "=" * 70)
    print("EXECUTION SUMMARY")
    print("=" * 70)
    print(f"Time elapsed: {elapsed_time:.2f} seconds ({elapsed_time/60:.1f} minutes)")
    print(f"Total tasks: {len(all_tasks):,}")
    print(f"Tasks completed: {completed:,}")
    print(f"Average rate: {completed/elapsed_time*60:.0f} tasks/min")
    print("=" * 70)
    print("✓ DCA performance analysis completed!")
    print("=" * 70)

if __name__ == "__main__":
    main()
