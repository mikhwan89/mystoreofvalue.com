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
START_DATE = '2010-01-01'
RISK_FREE_RATE = 0.02  # 2% annual risk-free rate (US Treasury)
MAX_WORKERS = 3  # Parallel processing
BATCH_SIZE = 1000

def get_db_connection():
    """Create a new database connection"""
    return psycopg2.connect(**DB_CONFIG)

def get_all_assets_with_data():
    """
    Get all assets that have sufficient price data for analysis
    Returns list of (symbol, asset_type, table_name) tuples
    """
    conn = get_db_connection()
    cursor = conn.cursor()
    
    all_assets = []
    
    # Query each price table (stocks removed)
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
    """
    Fetch USD-normalized price data for a symbol within date range
    Returns list of (date, price_usd) tuples sorted by date
    """
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

def calculate_returns(prices):
    """Calculate daily returns from price series"""
    if len(prices) < 2:
        return []
    
    returns = []
    for i in range(1, len(prices)):
        daily_return = (prices[i] - prices[i-1]) / prices[i-1]
        returns.append(daily_return)
    
    return np.array(returns)

def calculate_max_loss_from_entry(prices, entry_price):
    """
    Calculate maximum loss from entry price (worst floating loss)
    This shows the actual maximum pain an investor would experience
    Returns (max_loss_pct, loss_date_index)
    """
    if len(prices) < 1:
        return 0.0, 0
    
    min_price = prices[0]
    min_price_idx = 0
    
    for i, price in enumerate(prices):
        if price < min_price:
            min_price = price
            min_price_idx = i
    
    max_loss_pct = ((min_price - entry_price) / entry_price) * 100
    
    return max_loss_pct, min_price_idx

def calculate_max_drawdown(prices):
    """
    Calculate maximum drawdown and the date it occurred
    Returns (max_drawdown_pct, drawdown_date_index)
    """
    if len(prices) < 2:
        return 0.0, 0
    
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
    """
    Calculate volatility (standard deviation of returns)
    
    Note: Uses 365 days/year since all assets have forward-filled data
    (weekends and holidays included) for fair comparison
    """
    if len(returns) < 2:
        return 0.0
    
    vol = np.std(returns, ddof=1)
    
    if annualize:
        vol = vol * np.sqrt(365)  # Annualize using 365 days (all dates forward-filled)
    
    return vol * 100  # Convert to percentage

def calculate_downside_deviation(returns, annualize=True):
    """
    Calculate downside deviation (volatility of negative returns only)
    
    Note: Uses 365 days/year since all assets have forward-filled data
    """
    if len(returns) < 2:
        return 0.0
    
    negative_returns = returns[returns < 0]
    
    if len(negative_returns) == 0:
        return 0.0
    
    downside_dev = np.std(negative_returns, ddof=1)
    
    if annualize:
        downside_dev = downside_dev * np.sqrt(365)  # Annualize using 365 days
    
    return downside_dev

def calculate_sharpe_ratio(annualized_return, volatility, risk_free_rate=RISK_FREE_RATE):
    """Calculate Sharpe ratio"""
    if volatility == 0:
        return 0.0
    
    return (annualized_return / 100 - risk_free_rate) / (volatility / 100)

def calculate_sortino_ratio(annualized_return, downside_deviation, risk_free_rate=RISK_FREE_RATE):
    """Calculate Sortino ratio"""
    if downside_deviation == 0:
        return 0.0
    
    return (annualized_return / 100 - risk_free_rate) / downside_deviation

def calculate_calmar_ratio(annualized_return, max_drawdown):
    """Calculate Calmar ratio"""
    if max_drawdown == 0:
        return 0.0
    
    return (annualized_return / 100) / (max_drawdown / 100)

def calculate_performance_metrics(symbol, asset_type, table_name, start_date, end_date, holding_years):
    """
    Calculate all performance metrics for a given holding period
    Returns dictionary with all metrics, or None if insufficient data
    
    This function ensures:
    1. Asset has data on the EXACT start date
    2. Asset has data on the EXACT end date
    3. Asset has been trading for the FULL holding period (e.g., 3 years for 3-year analysis)
    """
    # Fetch price data
    price_data = get_price_data(symbol, table_name, start_date, end_date)
    
    # Minimum data requirement: at least 70% of expected days
    # For 3 years = 1095 days, we need at least 767 days
    expected_days = holding_years * 365
    min_required_days = int(expected_days * 0.7)
    
    if len(price_data) < min_required_days:
        return None  # Insufficient data for this holding period
    
    dates = [d[0] for d in price_data]
    prices = np.array([float(d[1]) for d in price_data])
    
    # CRITICAL CHECK 1: Ensure we have data on EXACT start date
    # Convert datetime to date for comparison (database stores datetime, we compare dates)
    first_date = dates[0].date() if hasattr(dates[0], 'date') else dates[0]
    start_dt = datetime.strptime(str(start_date), '%Y-%m-%d').date()
    
    if first_date != start_dt:
        return None  # No data on start date - asset didn't exist yet or was delisted
    
    # CRITICAL CHECK 2: Ensure we have data on EXACT end date
    last_date = dates[-1].date() if hasattr(dates[-1], 'date') else dates[-1]
    end_dt = datetime.strptime(str(end_date), '%Y-%m-%d').date()
    
    if last_date != end_dt:
        return None  # No data on end date - asset was delisted or stopped trading
    
    # CRITICAL CHECK 3: Ensure the date range matches the expected holding period
    actual_days = (last_date - first_date).days
    if actual_days < expected_days - 10:  # Allow 10 days tolerance for leap years
        return None  # Data doesn't cover the full holding period
    
    # Basic metrics
    start_price = prices[0]
    end_price = prices[-1]
    min_price = np.min(prices)
    max_price = np.max(prices)
    
    # Return metrics
    total_return_pct = ((end_price - start_price) / start_price) * 100
    
    # Calculate CAGR (Compound Annual Growth Rate)
    years = holding_years  # Use exact holding period
    annualized_return_pct = (((end_price / start_price) ** (1 / years)) - 1) * 100
    
    # Calculate daily returns
    returns = calculate_returns(prices)
    
    # Risk metrics
    volatility_pct = calculate_volatility(returns)
    max_drawdown_pct, max_dd_idx = calculate_max_drawdown(prices)
    max_drawdown_date = dates[max_dd_idx] if max_dd_idx < len(dates) else dates[-1]
    
    # Maximum loss from entry (actual investor pain)
    max_loss_from_entry_pct, max_loss_idx = calculate_max_loss_from_entry(prices, start_price)
    max_loss_from_entry_date = dates[max_loss_idx] if max_loss_idx < len(dates) else dates[0]
    
    # Downside deviation for Sortino
    downside_dev = calculate_downside_deviation(returns)
    
    # Risk-adjusted metrics
    sharpe = calculate_sharpe_ratio(annualized_return_pct, volatility_pct)
    sortino = calculate_sortino_ratio(annualized_return_pct, downside_dev)
    calmar = calculate_calmar_ratio(annualized_return_pct, max_drawdown_pct)
    
    # Additional metrics
    positive_days = int(np.sum(returns > 0))
    negative_days = int(np.sum(returns < 0))
    win_rate_pct = (positive_days / len(returns) * 100) if len(returns) > 0 else 0
    
    # Data quality
    total_trading_days = len(price_data)
    expected_days = (datetime.strptime(str(end_date), '%Y-%m-%d') - 
                    datetime.strptime(str(start_date), '%Y-%m-%d')).days
    data_completeness_pct = (total_trading_days / expected_days * 100) if expected_days > 0 else 0
    
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
        'sharpe_ratio': float(sharpe),
        'sortino_ratio': float(sortino),
        'calmar_ratio': float(calmar),
        'positive_days': positive_days,
        'negative_days': negative_days,
        'win_rate_pct': float(win_rate_pct),
        'total_trading_days': total_trading_days,
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

def process_asset(asset_info):
    """Process all holding periods for a single asset"""
    symbol, asset_type, table_name = asset_info
    
    results = []
    
    # Generate all start/end date combinations using monthly intervals
    # Always start on the 1st of the month
    start = datetime.strptime(START_DATE, '%Y-%m-%d')
    today = datetime.now()
    
    for holding_years in HOLDING_PERIODS:
        # Start from the first day of the month
        current_start = datetime(start.year, start.month, 1)
        
        while True:
            # Calculate end date: exactly N years later on the 1st of the month
            end_year = current_start.year + holding_years
            end_month = current_start.month
            end_date = datetime(end_year, end_month, 1)
            
            # Stop if end date is beyond today
            if end_date > today:
                break
            
            # Calculate metrics
            metrics = calculate_performance_metrics(
                symbol, asset_type, table_name,
                current_start.strftime('%Y-%m-%d'),
                end_date.strftime('%Y-%m-%d'),
                holding_years
            )
            
            if metrics:
                results.append(metrics)
            
            # Move to first day of next month
            if current_start.month == 12:
                current_start = datetime(current_start.year + 1, 1, 1)
            else:
                current_start = datetime(current_start.year, current_start.month + 1, 1)
    
    # Insert all results for this asset
    if results:
        conn = get_db_connection()
        inserted = insert_performance_batch(conn, results)
        conn.close()
        
        return (symbol, len(results), inserted)
    
    return (symbol, 0, 0)

def main():
    """Main execution function"""
    
    print("=" * 70)
    print("Asset Performance Buy-and-Hold Analysis Calculator")
    print("=" * 70)
    print(f"Start Date: {START_DATE}")
    print(f"Holding Periods: {HOLDING_PERIODS} years")
    print(f"Workers: {MAX_WORKERS}")
    print("=" * 70)
    
    # Get all assets
    print("\n--- Fetching Assets ---")
    assets = get_all_assets_with_data()
    print(f"✓ Found {len(assets):,} assets with sufficient data")
    
    # Process assets in parallel
    print(f"\n--- Processing {len(assets):,} assets ---\n")
    start_time = time.time()
    
    total_calcs = 0
    total_inserted = 0
    
    with ThreadPoolExecutor(max_workers=MAX_WORKERS) as executor:
        future_to_asset = {
            executor.submit(process_asset, asset): asset
            for asset in assets
        }
        
        completed = 0
        for future in as_completed(future_to_asset):
            asset = future_to_asset[future]
            symbol = asset[0]
            
            try:
                result_symbol, calcs, inserted = future.result()
                total_calcs += calcs
                total_inserted += inserted
                completed += 1
                
                if completed % 100 == 0:
                    elapsed = time.time() - start_time
                    rate = completed / elapsed * 60
                    print(f"  Progress: {completed:,}/{len(assets):,} assets ({rate:.1f} assets/min)")
                
            except Exception as e:
                print(f"✗ Error processing {symbol}: {e}")
    
    elapsed_time = time.time() - start_time
    
    # Summary
    print("\n" + "=" * 70)
    print("EXECUTION SUMMARY")
    print("=" * 70)
    print(f"Time elapsed: {elapsed_time:.2f} seconds ({elapsed_time/60:.1f} minutes)")
    print(f"Assets processed: {len(assets):,}")
    print(f"Total calculations: {total_calcs:,}")
    print(f"Records inserted: {total_inserted:,}")
    print("=" * 70)
    print("✓ Performance analysis completed!")
    print("=" * 70)

if __name__ == "__main__":
    main()
