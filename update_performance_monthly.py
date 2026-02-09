#!/usr/bin/env python3

import os
import psycopg2
from psycopg2.extras import execute_batch
import numpy as np
from datetime import datetime, timedelta
from dateutil.relativedelta import relativedelta
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
RISK_FREE_RATE = 0.02
MAX_WORKERS = 1
BATCH_SIZE = 1000
LOOKBACK_DAYS = 10  # Update last 10 days worth of start dates

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
            WHERE price_usd IS NOT NULL
            GROUP BY symbol
            HAVING COUNT(*) >= 1000
        """)
        
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

# Import all the calculation functions from calculate_performance.py
def calculate_returns(prices):
    """Calculate daily returns"""
    return np.diff(prices) / prices[:-1]

def calculate_max_drawdown(prices):
    """Calculate maximum drawdown and its index"""
    peak = prices[0]
    max_dd = 0.0
    max_dd_idx = 0
    
    for i, price in enumerate(prices):
        if price > peak:
            peak = price
        
        drawdown = (price - peak) / peak
        if drawdown < max_dd:
            max_dd = drawdown
            max_dd_idx = i
    
    return abs(max_dd * 100), max_dd_idx

def calculate_volatility(returns, annualize=True):
    """Calculate volatility (standard deviation of returns)"""
    if len(returns) < 2:
        return 0.0
    
    vol = np.std(returns, ddof=1)
    
    if annualize:
        vol = vol * np.sqrt(365)
    
    return vol * 100

def calculate_downside_deviation(returns, annualize=True):
    """Calculate downside deviation (volatility of negative returns only)"""
    if len(returns) < 2:
        return 0.0
    
    negative_returns = returns[returns < 0]
    
    if len(negative_returns) == 0:
        return 0.0
    
    downside_dev = np.std(negative_returns, ddof=1)
    
    if annualize:
        downside_dev = downside_dev * np.sqrt(365)
    
    return downside_dev

def calculate_sharpe_ratio(annualized_return, volatility, risk_free_rate=RISK_FREE_RATE):
    """Calculate Sharpe ratio"""
    if volatility == 0:
        return 0.0
    return (annualized_return / 100 - risk_free_rate) / (volatility / 100)

def calculate_sortino_ratio(annualized_return, downside_dev, risk_free_rate=RISK_FREE_RATE):
    """Calculate Sortino ratio"""
    if downside_dev == 0:
        return 0.0
    return (annualized_return / 100 - risk_free_rate) / downside_dev

def calculate_calmar_ratio(annualized_return, max_drawdown):
    """Calculate Calmar ratio"""
    if max_drawdown == 0:
        return 0.0
    return (annualized_return / 100) / (max_drawdown / 100)

def calculate_performance_metrics(symbol, asset_type, table_name, start_date, end_date, holding_years):
    """Calculate all performance metrics for a given holding period"""
    
    price_data = get_price_data(symbol, table_name, start_date, end_date)
    
    expected_days = holding_years * 365
    min_required_days = int(expected_days * 0.7)
    
    if len(price_data) < min_required_days:
        return None
    
    dates = [d[0].date() if hasattr(d[0], 'date') else d[0] for d in price_data]
    prices = np.array([float(d[1]) for d in price_data])
    
    first_date = dates[0]
    start_dt = datetime.strptime(str(start_date), '%Y-%m-%d').date()
    
    if first_date != start_dt:
        return None
    
    last_date = dates[-1]
    end_dt = datetime.strptime(str(end_date), '%Y-%m-%d').date()
    
    if last_date != end_dt:
        return None
    
    actual_days = (last_date - first_date).days
    if actual_days < expected_days - 10:
        return None
    
    # Basic metrics
    start_price = prices[0]
    end_price = prices[-1]
    min_price = np.min(prices)
    max_price = np.max(prices)
    
    # Return metrics
    total_return_pct = ((end_price - start_price) / start_price) * 100
    years = holding_years
    annualized_return_pct = (((end_price / start_price) ** (1 / years)) - 1) * 100
    
    # Calculate daily returns
    returns = calculate_returns(prices)
    
    # Risk metrics
    volatility_pct = calculate_volatility(returns)
    max_drawdown_pct, max_dd_idx = calculate_max_drawdown(prices)
    max_drawdown_date = dates[max_dd_idx] if max_dd_idx < len(dates) else dates[-1]
    
    # Maximum loss from entry
    max_loss_from_entry = np.min((prices - start_price) / start_price)
    max_loss_from_entry_pct = max_loss_from_entry * 100
    max_loss_idx = np.argmin((prices - start_price) / start_price)
    max_loss_from_entry_date = dates[max_loss_idx]
    
    # Downside deviation
    downside_dev = calculate_downside_deviation(returns)
    
    # Risk-adjusted metrics
    sharpe_ratio = calculate_sharpe_ratio(annualized_return_pct, volatility_pct)
    sortino_ratio = calculate_sortino_ratio(annualized_return_pct, downside_dev)
    calmar_ratio = calculate_calmar_ratio(annualized_return_pct, max_drawdown_pct)
    
    # Win rate
    positive_days = np.sum(returns > 0)
    negative_days = np.sum(returns < 0)
    win_rate_pct = (positive_days / len(returns) * 100) if len(returns) > 0 else 0
    
    # Data completeness
    total_trading_days = len(prices)
    expected_days_total = (last_date - first_date).days + 1
    data_completeness_pct = (total_trading_days / expected_days_total * 100) if expected_days_total > 0 else 0
    
    return {
        'symbol': symbol,
        'asset_type': asset_type,
        'start_date': start_date,
        'end_date': end_date,
        'holding_period_years': holding_years,
        'start_price': float(start_price),
        'end_price': float(end_price),
        'min_price': float(min_price),
        'max_price': float(max_price),
        'total_return_pct': float(total_return_pct),
        'annualized_return_pct': float(annualized_return_pct),
        'volatility_pct': float(volatility_pct),
        'max_drawdown_pct': float(max_drawdown_pct),
        'max_drawdown_date': max_drawdown_date,
        'max_loss_from_entry_pct': float(max_loss_from_entry_pct),
        'max_loss_from_entry_date': max_loss_from_entry_date,
        'sharpe_ratio': float(sharpe_ratio),
        'sortino_ratio': float(sortino_ratio),
        'calmar_ratio': float(calmar_ratio),
        'positive_days': int(positive_days),
        'negative_days': int(negative_days),
        'win_rate_pct': float(win_rate_pct),
        'total_trading_days': int(total_trading_days),
        'data_completeness_pct': float(data_completeness_pct)
    }

def insert_performance_batch(conn, performance_data):
    """Insert performance metrics in batch"""
    if not performance_data:
        return 0
    
    cursor = conn.cursor()
    
    try:
        values = [
            (
                d['symbol'], d['asset_type'], d['start_date'], d['end_date'],
                d['holding_period_years'], d['start_price'], d['end_price'],
                d['min_price'], d['max_price'], d['total_return_pct'],
                d['annualized_return_pct'], d['volatility_pct'], d['max_drawdown_pct'],
                d['max_drawdown_date'], d['max_loss_from_entry_pct'], d['max_loss_from_entry_date'],
                d['sharpe_ratio'], d['sortino_ratio'], d['calmar_ratio'], 
                d['positive_days'], d['negative_days'], d['win_rate_pct'], 
                d['total_trading_days'], d['data_completeness_pct']
            )
            for d in performance_data
        ]
        
        execute_batch(cursor, """
            INSERT INTO asset_performance_buy_and_hold (
                symbol, asset_type, start_date, end_date, holding_period_years,
                start_price, end_price, min_price, max_price, total_return_pct,
                annualized_return_pct, volatility_pct, max_drawdown_pct, max_drawdown_date,
                max_loss_from_entry_pct, max_loss_from_entry_date,
                sharpe_ratio, sortino_ratio, calmar_ratio, positive_days, negative_days,
                win_rate_pct, total_trading_days, data_completeness_pct
            )
            VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s)
            ON CONFLICT (symbol, start_date, end_date) DO UPDATE SET
                total_return_pct = EXCLUDED.total_return_pct,
                annualized_return_pct = EXCLUDED.annualized_return_pct,
                volatility_pct = EXCLUDED.volatility_pct,
                max_drawdown_pct = EXCLUDED.max_drawdown_pct,
                max_loss_from_entry_pct = EXCLUDED.max_loss_from_entry_pct,
                sharpe_ratio = EXCLUDED.sharpe_ratio,
                sortino_ratio = EXCLUDED.sortino_ratio,
                calmar_ratio = EXCLUDED.calmar_ratio,
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
    """Process a single period calculation"""
    symbol, asset_type, table_name, start_date, end_date, holding_years = args
    
    metrics = calculate_performance_metrics(
        symbol, asset_type, table_name,
        start_date, end_date, holding_years
    )
    
    return metrics

def main():
    print("=" * 70)
    print("Monthly Buy-and-Hold Performance Update")
    print("=" * 70)
    print(f"Current date: {datetime.now().strftime('%Y-%m-%d')}")
    print(f"Holding periods: {HOLDING_PERIODS} years")
    print(f"Lookback: {LOOKBACK_DAYS} days")
    print(f"Workers: {MAX_WORKERS}")
    print("=" * 70)
    
    print("\n--- Fetching Assets ---")
    assets = get_all_assets_with_data()
    print(f"✓ Found {len(assets)} assets")
    
    # Generate start dates for last LOOKBACK_DAYS days (1st of month for each)
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
                end_date = start_date + relativedelta(years=holding_years)
                
                # Only calculate if end_date is today or earlier
                if end_date <= today:
                    all_tasks.append((
                        symbol, asset_type, table_name,
                        start_date.strftime('%Y-%m-%d'),
                        end_date.strftime('%Y-%m-%d'),
                        holding_years
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
                if len(results_batch) >= BATCH_SIZE:
                    conn = get_db_connection()
                    inserted = insert_performance_batch(conn, results_batch)
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
        inserted = insert_performance_batch(conn, results_batch)
        conn.close()
    
    elapsed_time = time.time() - start_time
    
    print("\n" + "=" * 70)
    print("SUMMARY")
    print("=" * 70)
    print(f"Time: {elapsed_time:.2f}s ({elapsed_time/60:.1f} minutes)")
    print(f"Tasks completed: {completed:,}")
    print("=" * 70)
    print("✓ Monthly update completed!")
    print("=" * 70)

if __name__ == "__main__":
    main()
