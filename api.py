#!/usr/bin/env python3
"""
Flask API to serve data from PostgreSQL to mystoreofvalue.com
"""

from flask import Flask, jsonify, request
from flask_cors import CORS
import psycopg2
import os
from dotenv import load_dotenv
from datetime import datetime, timedelta

EXCLUDED_SYMBOLS = ['^FVX', '^TYX', '^TNX', 'ZBUSD', 'ZFUSD', 'ZNUSD', 'ZTUSD', '^VXTLT', '^IRX']

load_dotenv()

app = Flask(__name__)
CORS(app)

DB_CONFIG = {
    'dbname': os.getenv('DB_NAME'),
    'user': os.getenv('DB_USER'),
    'password': os.getenv('DB_PASSWORD'),
    'host': os.getenv('DB_HOST', 'localhost'),
    'port': os.getenv('DB_PORT', '5432')
}

def get_db_connection():
    return psycopg2.connect(**DB_CONFIG)

@app.route('/api/currencies', methods=['GET'])
def get_currencies():
    """Get all currencies with 10-year depreciation vs USD"""
    try:
        conn = get_db_connection()
        cursor = conn.cursor()
        
        today = datetime.now().date()
        ten_years_ago = today - timedelta(days=365*10)
        
        cursor.execute("""
            SELECT 
                fp.symbol,
                COALESCE(ci.currency_name, fp.name) as currency_name,
                COALESCE(ci.country_name, SUBSTRING(fp.symbol FROM 1 FOR 3)) as country_name,
                
                (SELECT price FROM forex_prices WHERE symbol = fp.symbol ORDER BY date DESC LIMIT 1) as latest_price,
                (SELECT price FROM forex_prices WHERE symbol = fp.symbol 
                 AND date >= %s::date - INTERVAL '60 days' AND date <= %s::date + INTERVAL '60 days'
                 ORDER BY date ASC LIMIT 1) as old_price
                
            FROM forex_pairs fp
            LEFT JOIN currency_info ci ON SUBSTRING(fp.symbol FROM 1 FOR 3) = ci.currency_code
            WHERE fp.symbol LIKE '%%USD' AND LENGTH(fp.symbol) = 6
            ORDER BY fp.symbol
        """, (ten_years_ago, ten_years_ago))
        
        results = cursor.fetchall()
        currencies = []
        
        for row in results:
            if not row[3] or not row[4]:
                continue
            depreciation = ((float(row[3]) - float(row[4])) / float(row[4])) * 100
            currencies.append({
                'symbol': row[0],
                'name': row[1],
                'pair': row[0],
                'country': row[2],
                'depreciation': round(depreciation, 1),
                'period': '10 years'
            })
        
        currencies.sort(key=lambda x: x['depreciation'])
        cursor.close()
        conn.close()
        
        return jsonify({'success': True, 'count': len(currencies), 'data': currencies})
    except Exception as e:
        import traceback
        return jsonify({'success': False, 'error': str(e), 'traceback': traceback.format_exc()}), 500

# REPLACE your entire /api/leaderboard endpoint with this corrected version

# REPLACE your /api/leaderboard endpoint - Fix the ambiguous column references

@app.route('/api/leaderboard', methods=['GET'])
def get_leaderboard():
    """Get leaderboard with winners podium and period breakdown"""
    strategy = request.args.get('strategy', 'lumpsum')
    period = int(request.args.get('period', 5))
    asset_type = request.args.get('asset_type', 'all')
    start_date_from = request.args.get('start_date_from', None)
    ranking_metric = request.args.get('ranking_metric', 'cagr')
    exchanges = request.args.get('exchanges', 'NULL,NASDAQ,NYSE,SNP,DJI,MSC,ICEF,NIM')
    
    # Advanced filters
    min_cagr = request.args.get('min_cagr', None)
    max_drawdown = request.args.get('max_drawdown', None)
    max_loss = request.args.get('max_loss', None)
    min_sharpe = request.args.get('min_sharpe', None)
    min_sortino = request.args.get('min_sortino', None)
    min_calmar = request.args.get('min_calmar', None)
    
    try:
        conn = get_db_connection()
        cursor = conn.cursor()
        
        # Determine table and filters - INITIALIZE params here
        if strategy == 'lumpsum':
            table = 'asset_performance_buy_and_hold'
            strategy_filter = ""
            loss_column = 'max_loss_from_entry_pct'
            params = [period]  # START params list
        else:
            table = 'asset_performance_dca'
            frequency_map = {'dca_daily': 'daily', 'dca_weekly': 'weekly', 'dca_monthly': 'monthly'}
            frequency = frequency_map.get(strategy, 'monthly')
            strategy_filter = "AND dca_frequency = %s"
            loss_column = 'max_loss_from_cost_pct'
            params = [period, frequency]  # START params list
        
        # Asset type filter - USE a.asset_type
        asset_type_filter = ""
        if asset_type != 'all':
            asset_type_filter = "AND a.asset_type = %s"
            params.append(asset_type)
        
        # Add treasury exclusion filter - USE a.symbol
        exclusion_placeholders = ','.join(['%s'] * len(EXCLUDED_SYMBOLS))
        treasury_exclusion = f"AND a.symbol NOT IN ({exclusion_placeholders})"
        params.extend(EXCLUDED_SYMBOLS)
        
        # Build exchange filter - USE m.exchange
        exchange_filter = ""
        exchange_list = [e.strip() for e in exchanges.split(',') if e.strip()]
        
        if exchange_list:
            has_null = 'NULL' in exchange_list
            actual_exchanges = [e for e in exchange_list if e != 'NULL']
            
            if has_null and actual_exchanges:
                # Include both NULL and specific exchanges
                exchange_placeholders = ','.join(['%s'] * len(actual_exchanges))
                exchange_filter = f"AND (m.exchange IS NULL OR m.exchange IN ({exchange_placeholders}))"
                params.extend(actual_exchanges)
            elif has_null:
                # Only NULL
                exchange_filter = "AND m.exchange IS NULL"
            elif actual_exchanges:
                # Only specific exchanges
                exchange_placeholders = ','.join(['%s'] * len(actual_exchanges))
                exchange_filter = f"AND m.exchange IN ({exchange_placeholders})"
                params.extend(actual_exchanges)
        
        # Add start date filter - USE a.start_date
        start_date_filter = ""
        if start_date_from:
            start_date_filter = "AND a.start_date >= %s"
            params.append(start_date_from)
        
        # Build advanced filters - all use a. prefix
        advanced_filters = []
        
        if min_cagr:
            advanced_filters.append(f"AND a.annualized_return_pct >= %s")
            params.append(float(min_cagr))
        
        if max_drawdown:
            advanced_filters.append(f"AND a.max_drawdown_pct <= %s")
            params.append(float(max_drawdown))
        
        if max_loss:
            advanced_filters.append(f"AND a.{loss_column} <= %s")
            params.append(float(max_loss))
        
        if min_sharpe:
            advanced_filters.append(f"AND a.sharpe_ratio >= %s")
            params.append(float(min_sharpe))
        
        if min_sortino:
            advanced_filters.append(f"AND a.sortino_ratio >= %s")
            params.append(float(min_sortino))
        
        if min_calmar:
            advanced_filters.append(f"AND a.calmar_ratio >= %s")
            params.append(float(min_calmar))
        
        advanced_filter_str = " ".join(advanced_filters)
        
        # Map ranking metric to database column
        metric_column_map = {
            'cagr': 'annualized_return_pct',
            'sharpe': 'sharpe_ratio',
            'sortino': 'sortino_ratio',
            'calmar': 'calmar_ratio'
        }
        ranking_column = metric_column_map.get(ranking_metric, 'annualized_return_pct')
        
        # Get period-by-period winners
        cursor.execute(f"""
            WITH ranked_periods AS (
                SELECT 
                    a.symbol, a.asset_type, a.start_date, a.end_date, 
                    a.annualized_return_pct,
                    a.{ranking_column} as ranking_value,
                    ROW_NUMBER() OVER (PARTITION BY a.start_date ORDER BY a.{ranking_column} DESC) as rank
                FROM {table} a
                JOIN asset_metadata m ON a.symbol = m.symbol
                WHERE a.holding_period_years = %s {strategy_filter} {asset_type_filter} {treasury_exclusion} {exchange_filter} {start_date_filter} {advanced_filter_str}
            )
            SELECT symbol, asset_type, start_date, end_date, annualized_return_pct, ranking_value, rank
            FROM ranked_periods
            WHERE rank <= 3
            ORDER BY start_date, rank
        """, params)
        
        period_results = cursor.fetchall()
        scores = {}
        period_breakdown = []
        
        for row in period_results:
            symbol, asset_type_val, start_date, end_date, cagr, ranking_value, rank = row[0], row[1], str(row[2]), str(row[3]), float(row[4]), float(row[5]) if row[5] else 0, int(row[6])
            
            if symbol not in scores:
                scores[symbol] = {
                    'symbol': symbol,
                    'asset_type': asset_type_val,
                    'score': 0,
                    'all_cagrs': []
                }
            
            points = {1: 5, 2: 3, 3: 1}
            scores[symbol]['score'] += points[rank]
            scores[symbol]['all_cagrs'].append(cagr)
            
            period_breakdown.append({
                'start_date': start_date,
                'end_date': end_date,
                'rank': rank,
                'symbol': symbol,
                'cagr': round(cagr, 2),
                'ranking_value': round(ranking_value, 2)
            })
        
        # Calculate statistics for each asset
        for symbol in scores:
            cagrs = scores[symbol]['all_cagrs']
            scores[symbol]['avg_cagr'] = round(sum(cagrs) / len(cagrs), 2)
            scores[symbol]['count'] = len(cagrs)
        
        sorted_scores = sorted(scores.values(), key=lambda x: x['score'], reverse=True)
        
        # Get top 3 winners with names
        winners = {'first': None, 'second': None, 'third': None}
        for i, position in enumerate(['first', 'second', 'third']):
            if i < len(sorted_scores):
                winner = sorted_scores[i].copy()
                cursor.execute("SELECT name FROM asset_metadata WHERE symbol = %s LIMIT 1", (winner['symbol'],))
                result = cursor.fetchone()
                winner['name'] = result[0] if result else winner['symbol']
                winners[position] = winner
        
        # Group period breakdown
        periods_grouped = {}
        for item in period_breakdown:
            key = f"{item['start_date']}|{item['end_date']}"
            if key not in periods_grouped:
                periods_grouped[key] = {
                    'start_date': item['start_date'],
                    'end_date': item['end_date'],
                    'first': None,
                    'second': None,
                    'third': None
                }
            
            pos = ['first', 'second', 'third'][item['rank'] - 1]
            periods_grouped[key][pos] = {
                'symbol': item['symbol'], 
                'cagr': item['cagr'],
                'ranking_value': item['ranking_value']
            }
        
        periods_list = list(periods_grouped.values())
        
        cursor.close()
        conn.close()
        
        return jsonify({
            'success': True,
            'strategy': strategy,
            'period': period,
            'asset_type': asset_type,
            'start_date_from': start_date_from,
            'ranking_metric': ranking_metric,
            'filters_applied': {
                'min_cagr': min_cagr,
                'max_drawdown': max_drawdown,
                'max_loss': max_loss,
                'min_sharpe': min_sharpe,
                'min_sortino': min_sortino,
                'min_calmar': min_calmar,
                'exchanges': exchanges
            },
            'winners': winners,
            'full_rankings': sorted_scores[:20],
            'period_breakdown': periods_list
        })
    except Exception as e:
        import traceback
        return jsonify({
            'success': False,
            'error': str(e),
            'traceback': traceback.format_exc()
        }), 500
# UPDATE your /api/leaderboard/stats endpoint in api.py

# REPLACE or UPDATE your /api/leaderboard/stats endpoint to add exchanges parameter

@app.route('/api/leaderboard/stats', methods=['GET'])
def get_leaderboard_stats():
    """Get detailed statistics for top 3 winners"""
    symbols_param = request.args.get('symbols', '')
    strategy = request.args.get('strategy', 'lumpsum')
    period = int(request.args.get('period', 5))
    asset_type = request.args.get('asset_type', 'all')
    start_date_from = request.args.get('start_date_from', None)
    exchanges = request.args.get('exchanges', 'NULL,NASDAQ,NYSE,SNP,DJI,MSC,ICEF,NIM')  # ADD THIS
    
    if not symbols_param:
        return jsonify({'success': False, 'error': 'No symbols provided'}), 400
    
    symbols_list = [s.strip() for s in symbols_param.split(',') if s.strip()]
    
    try:
        conn = get_db_connection()
        cursor = conn.cursor()
        
        # Determine table and loss column - INITIALIZE params
        if strategy == 'lumpsum':
            table = 'asset_performance_buy_and_hold'
            strategy_filter = ""
            loss_column = 'max_loss_from_entry_pct'
            params = [period]
        else:
            table = 'asset_performance_dca'
            frequency_map = {'dca_daily': 'daily', 'dca_weekly': 'weekly', 'dca_monthly': 'monthly'}
            frequency = frequency_map.get(strategy, 'monthly')
            strategy_filter = "AND a.dca_frequency = %s"
            loss_column = 'max_loss_from_cost_pct'
            params = [period, frequency]
        
        # Asset type filter
        asset_type_filter = ""
        if asset_type != 'all':
            asset_type_filter = "AND a.asset_type = %s"
            params.append(asset_type)
        
        # Add treasury exclusion
        exclusion_placeholders = ','.join(['%s'] * len(EXCLUDED_SYMBOLS))
        treasury_exclusion = f"AND a.symbol NOT IN ({exclusion_placeholders})"
        params.extend(EXCLUDED_SYMBOLS)
        
        # Build exchange filter - ADD THIS SECTION
        exchange_filter = ""
        exchange_list = [e.strip() for e in exchanges.split(',') if e.strip()]
        
        if exchange_list:
            has_null = 'NULL' in exchange_list
            actual_exchanges = [e for e in exchange_list if e != 'NULL']
            
            if has_null and actual_exchanges:
                exchange_placeholders = ','.join(['%s'] * len(actual_exchanges))
                exchange_filter = f"AND (m.exchange IS NULL OR m.exchange IN ({exchange_placeholders}))"
                params.extend(actual_exchanges)
            elif has_null:
                exchange_filter = "AND m.exchange IS NULL"
            elif actual_exchanges:
                exchange_placeholders = ','.join(['%s'] * len(actual_exchanges))
                exchange_filter = f"AND m.exchange IN ({exchange_placeholders})"
                params.extend(actual_exchanges)
        
        # Start date filter
        start_date_filter = ""
        if start_date_from:
            start_date_filter = "AND a.start_date >= %s"
            params.append(start_date_from)
        
        # Symbols filter
        symbols_placeholder = ','.join(['%s'] * len(symbols_list))
        params.extend(symbols_list)
        
        # Get all performance data for these symbols - JOIN with asset_metadata for exchange filter
        cursor.execute(f"""
            SELECT 
                a.symbol,
                a.annualized_return_pct,
                a.total_return_pct,
                a.volatility_pct,
                a.max_drawdown_pct,
                a.{loss_column} as max_loss_pct,
                a.sharpe_ratio,
                a.sortino_ratio,
                a.calmar_ratio
            FROM {table} a
            JOIN asset_metadata m ON a.symbol = m.symbol
            WHERE a.holding_period_years = %s 
            {strategy_filter}
            {asset_type_filter}
            {treasury_exclusion}
            {exchange_filter}
            {start_date_filter}
            AND a.symbol IN ({symbols_placeholder})
            ORDER BY a.symbol, a.start_date
        """, params)
        
        results = cursor.fetchall()
        
        # Organize by symbol
        stats_by_symbol = {}
        for row in results:
            symbol = row[0]
            if symbol not in stats_by_symbol:
                stats_by_symbol[symbol] = {
                    'cagr': [],
                    'total_return': [],
                    'volatility': [],
                    'drawdown': [],
                    'max_loss': [],
                    'sharpe': [],
                    'sortino': [],
                    'calmar': []
                }
            
            stats_by_symbol[symbol]['cagr'].append(float(row[1]))
            stats_by_symbol[symbol]['total_return'].append(float(row[2]))
            stats_by_symbol[symbol]['volatility'].append(float(row[3]))
            stats_by_symbol[symbol]['drawdown'].append(-float(row[4]))  # Negative
            stats_by_symbol[symbol]['max_loss'].append(float(row[5]))
            stats_by_symbol[symbol]['sharpe'].append(float(row[6]) if row[6] else 0)
            stats_by_symbol[symbol]['sortino'].append(float(row[7]) if row[7] else 0)
            stats_by_symbol[symbol]['calmar'].append(float(row[8]) if row[8] else 0)
        
        # Calculate statistics
        def calculate_percentiles(data):
            import numpy as np
            sorted_data = sorted(data)
            return {
                'min': round(float(np.min(sorted_data)), 2),
                'p5': round(float(np.percentile(sorted_data, 5)), 2),
                'p10': round(float(np.percentile(sorted_data, 10)), 2),
                'p25': round(float(np.percentile(sorted_data, 25)), 2),
                'median': round(float(np.percentile(sorted_data, 50)), 2),
                'p75': round(float(np.percentile(sorted_data, 75)), 2),
                'p90': round(float(np.percentile(sorted_data, 90)), 2),
                'p95': round(float(np.percentile(sorted_data, 95)), 2),
                'max': round(float(np.max(sorted_data)), 2)
            }
        
        detailed_stats = {}
        # Preserve the order of symbols as passed in
        for symbol in symbols_list:
            if symbol in stats_by_symbol and len(stats_by_symbol[symbol]['cagr']) > 0:
                detailed_stats[symbol] = {
                    'cagr': calculate_percentiles(stats_by_symbol[symbol]['cagr']),
                    'total_return': calculate_percentiles(stats_by_symbol[symbol]['total_return']),
                    'volatility': calculate_percentiles(stats_by_symbol[symbol]['volatility']),
                    'drawdown': calculate_percentiles(stats_by_symbol[symbol]['drawdown']),
                    'max_loss': calculate_percentiles(stats_by_symbol[symbol]['max_loss']),
                    'sharpe': calculate_percentiles(stats_by_symbol[symbol]['sharpe']),
                    'sortino': calculate_percentiles(stats_by_symbol[symbol]['sortino']),
                    'calmar': calculate_percentiles(stats_by_symbol[symbol]['calmar'])
                }
        
        # Get names
        names = {}
        for symbol in symbols_list:
            if symbol in stats_by_symbol:
                cursor.execute("SELECT name FROM asset_metadata WHERE symbol = %s LIMIT 1", (symbol,))
                result = cursor.fetchone()
                names[symbol] = result[0] if result else symbol
        
        cursor.close()
        conn.close()
        
        return jsonify({
            'success': True,
            'stats': detailed_stats,
            'names': names,
            'symbols_order': symbols_list,  # Preserve order
            'loss_metric': 'Max Loss From Entry' if strategy == 'lumpsum' else 'Max Loss From Cost'
        })
        
    except Exception as e:
        import traceback
        return jsonify({
            'success': False,
            'error': str(e),
            'traceback': traceback.format_exc()
        }), 500

# ADD these two endpoints to your api.py

@app.route('/api/assets/list', methods=['GET'])
def get_assets_list():
    """Get list of available assets for selection"""
    asset_type = request.args.get('asset_type', 'all')
    exchanges = request.args.get('exchanges', 'NULL,NASDAQ,NYSE,SNP,DJI,MSC,ICEF,NIM')
    
    try:
        conn = get_db_connection()
        cursor = conn.cursor()
        
        # Initialize params list
        params = []
        
        # Add treasury exclusion
        exclusion_placeholders = ','.join(['%s'] * len(EXCLUDED_SYMBOLS))
        treasury_exclusion = f"AND symbol NOT IN ({exclusion_placeholders})"
        params.extend(EXCLUDED_SYMBOLS)
        
        # Asset type filter
        asset_type_filter = ""
        if asset_type != 'all':
            asset_type_filter = "AND asset_type = %s"
            params.append(asset_type)
        
        # Build exchange filter
        exchange_filter = ""
        exchange_list = [e.strip() for e in exchanges.split(',') if e.strip()]
        
        if exchange_list:
            has_null = 'NULL' in exchange_list
            actual_exchanges = [e for e in exchange_list if e != 'NULL']
            
            if has_null and actual_exchanges:
                exchange_placeholders = ','.join(['%s'] * len(actual_exchanges))
                exchange_filter = f"AND (exchange IS NULL OR exchange IN ({exchange_placeholders}))"
                params.extend(actual_exchanges)
            elif has_null:
                exchange_filter = "AND exchange IS NULL"
            elif actual_exchanges:
                exchange_placeholders = ','.join(['%s'] * len(actual_exchanges))
                exchange_filter = f"AND exchange IN ({exchange_placeholders})"
                params.extend(actual_exchanges)
        
        cursor.execute(f"""
            SELECT DISTINCT symbol, name, asset_type
            FROM asset_metadata
            WHERE 1=1 {treasury_exclusion} {asset_type_filter} {exchange_filter}
            ORDER BY name
        """, params)
        
        results = cursor.fetchall()
        assets = [{'symbol': row[0], 'name': row[1], 'asset_type': row[2]} for row in results]
        
        cursor.close()
        conn.close()
        
        return jsonify({'success': True, 'assets': assets})
    except Exception as e:
        import traceback
        return jsonify({'success': False, 'error': str(e), 'traceback': traceback.format_exc()}), 500



# REPLACE your /api/assets/details endpoint with this version

@app.route('/api/assets/details', methods=['GET'])
def get_asset_details():
    """Get detailed time series performance data for selected assets"""
    strategy = request.args.get('strategy', 'lumpsum')
    period = int(request.args.get('period', 5))
    asset_type = request.args.get('asset_type', 'all')
    start_date_from = request.args.get('start_date_from', None)
    symbols = request.args.get('symbols', '')  # Comma-separated list
    exchanges = request.args.get('exchanges', 'NULL,NASDAQ,NYSE,SNP,DJI,MSC,ICEF,NIM')
    
    # Advanced filters
    min_cagr = request.args.get('min_cagr', None)
    max_drawdown = request.args.get('max_drawdown', None)
    max_loss = request.args.get('max_loss', None)
    min_sharpe = request.args.get('min_sharpe', None)
    min_sortino = request.args.get('min_sortino', None)
    min_calmar = request.args.get('min_calmar', None)
    
    if not symbols:
        return jsonify({'success': False, 'error': 'No symbols provided'}), 400
    
    symbols_list = [s.strip() for s in symbols.split(',') if s.strip()][:3]  # Max 3 assets
    
    try:
        conn = get_db_connection()
        cursor = conn.cursor()
        
        # Determine table and loss column
        if strategy == 'lumpsum':
            table = 'asset_performance_buy_and_hold'
            strategy_filter = ""
            loss_column = 'max_loss_from_entry_pct'
            params = [period]
        else:
            table = 'asset_performance_dca'
            frequency_map = {'dca_daily': 'daily', 'dca_weekly': 'weekly', 'dca_monthly': 'monthly'}
            frequency = frequency_map.get(strategy, 'monthly')
            strategy_filter = "AND a.dca_frequency = %s"
            loss_column = 'max_loss_from_cost_pct'
            params = [period, frequency]
        
        asset_type_filter = ""
        if asset_type != 'all':
            asset_type_filter = "AND a.asset_type = %s"
            params.append(asset_type)
        
        # Add treasury exclusion
        exclusion_placeholders = ','.join(['%s'] * len(EXCLUDED_SYMBOLS))
        treasury_exclusion = f"AND a.symbol NOT IN ({exclusion_placeholders})"
        params.extend(EXCLUDED_SYMBOLS)
        
        # Build exchange filter - ADD THIS SECTION
        exchange_filter = ""
        exchange_list = [e.strip() for e in exchanges.split(',') if e.strip()]
        
        if exchange_list:
            has_null = 'NULL' in exchange_list
            actual_exchanges = [e for e in exchange_list if e != 'NULL']
            
            if has_null and actual_exchanges:
                exchange_placeholders = ','.join(['%s'] * len(actual_exchanges))
                exchange_filter = f"AND (m.exchange IS NULL OR m.exchange IN ({exchange_placeholders}))"
                params.extend(actual_exchanges)
            elif has_null:
                exchange_filter = "AND m.exchange IS NULL"
            elif actual_exchanges:
                exchange_placeholders = ','.join(['%s'] * len(actual_exchanges))
                exchange_filter = f"AND m.exchange IN ({exchange_placeholders})"
                params.extend(actual_exchanges)

        start_date_filter = ""
        if start_date_from:
            start_date_filter = "AND a.start_date >= %s"
            params.append(start_date_from)
        
        # Build advanced filters
        advanced_filters = []
        
        if min_cagr:
            advanced_filters.append(f"AND a.annualized_return_pct >= %s")
            params.append(float(min_cagr))
        
        if max_drawdown:
            advanced_filters.append(f"AND a.max_drawdown_pct <= %s")
            params.append(float(max_drawdown))
        
        if max_loss:
            advanced_filters.append(f"AND a.{loss_column} <= %s")
            params.append(float(max_loss))
        
        if min_sharpe:
            advanced_filters.append(f"AND a.sharpe_ratio >= %s")
            params.append(float(min_sharpe))
        
        if min_sortino:
            advanced_filters.append(f"AND a.sortino_ratio >= %s")
            params.append(float(min_sortino))
        
        if min_calmar:
            advanced_filters.append(f"AND a.calmar_ratio >= %s")
            params.append(float(min_calmar))
        
        advanced_filter_str = " ".join(advanced_filters)
        
        # Add symbols filter
        symbols_placeholder = ','.join(['%s'] * len(symbols_list))
        params.extend(symbols_list)
        
        # Get time series data
        cursor.execute(f"""
            SELECT 
                a.symbol,
                a.start_date,
                a.end_date,
                a.annualized_return_pct,
                a.total_return_pct,
                a.volatility_pct,
                a.max_drawdown_pct,
                a.{loss_column} as max_loss_pct,
                a.sharpe_ratio,
                a.sortino_ratio,
                a.calmar_ratio
            FROM {table} a
            JOIN asset_metadata m ON a.symbol = m.symbol
            WHERE a.holding_period_years = %s 
            {strategy_filter}
            {asset_type_filter}
            {treasury_exclusion}
            {exchange_filter}
            {start_date_filter}
            {advanced_filter_str}
            AND a.symbol IN ({symbols_placeholder})
            ORDER BY a.symbol, a.start_date
        """, params)
        
        results = cursor.fetchall()
        
        # Organize data by symbol\
        time_series_data = {}
        stats_data = {}
        
        for row in results:
            symbol = row[0]
            if symbol not in time_series_data:
                time_series_data[symbol] = {
                    'dates': [],
                    'cagr': [],
                    'total_return': [],
                    'volatility': [],
                    'drawdown': [],
                    'max_loss': [],
                    'sharpe': [],
                    'sortino': [],
                    'calmar': []
                }
                stats_data[symbol] = {
                    'cagr': [],
                    'total_return': [],
                    'volatility': [],
                    'drawdown': [],
                    'max_loss': [],
                    'sharpe': [],
                    'sortino': [],
                    'calmar': []
                }
            
            time_series_data[symbol]['dates'].append(str(row[1]))
            time_series_data[symbol]['cagr'].append(float(row[3]))
            time_series_data[symbol]['total_return'].append(float(row[4]))
            time_series_data[symbol]['volatility'].append(float(row[5]))
            time_series_data[symbol]['drawdown'].append(-float(row[6]))  # Negative for consistency
            time_series_data[symbol]['max_loss'].append(float(row[7]))
            time_series_data[symbol]['sharpe'].append(float(row[8]) if row[8] else 0)
            time_series_data[symbol]['sortino'].append(float(row[9]) if row[9] else 0)
            time_series_data[symbol]['calmar'].append(float(row[10]) if row[10] else 0)
            
            # Collect for stats
            stats_data[symbol]['cagr'].append(float(row[3]))
            stats_data[symbol]['total_return'].append(float(row[4]))
            stats_data[symbol]['volatility'].append(float(row[5]))
            stats_data[symbol]['drawdown'].append(-float(row[6]))
            stats_data[symbol]['max_loss'].append(float(row[7]))
            stats_data[symbol]['sharpe'].append(float(row[8]) if row[8] else 0)
            stats_data[symbol]['sortino'].append(float(row[9]) if row[9] else 0)
            stats_data[symbol]['calmar'].append(float(row[10]) if row[10] else 0)
        
        # Calculate statistics
        def calculate_percentiles(data):
            import numpy as np
            sorted_data = sorted(data)
            return {
                'min': round(float(np.min(sorted_data)), 2),
                'p5': round(float(np.percentile(sorted_data, 5)), 2),
                'p10': round(float(np.percentile(sorted_data, 10)), 2),
                'p25': round(float(np.percentile(sorted_data, 25)), 2),
                'median': round(float(np.percentile(sorted_data, 50)), 2),
                'p75': round(float(np.percentile(sorted_data, 75)), 2),
                'p90': round(float(np.percentile(sorted_data, 90)), 2),
                'p95': round(float(np.percentile(sorted_data, 95)), 2),
                'max': round(float(np.max(sorted_data)), 2)
            }
        
        statistics = {}
        for symbol in symbols_list:
            if symbol in stats_data and len(stats_data[symbol]['cagr']) > 0:
                statistics[symbol] = {
                    'cagr': calculate_percentiles(stats_data[symbol]['cagr']),
                    'total_return': calculate_percentiles(stats_data[symbol]['total_return']),
                    'volatility': calculate_percentiles(stats_data[symbol]['volatility']),
                    'drawdown': calculate_percentiles(stats_data[symbol]['drawdown']),
                    'max_loss': calculate_percentiles(stats_data[symbol]['max_loss']),
                    'sharpe': calculate_percentiles(stats_data[symbol]['sharpe']),
                    'sortino': calculate_percentiles(stats_data[symbol]['sortino']),
                    'calmar': calculate_percentiles(stats_data[symbol]['calmar'])
                }
        
        # Get names
        names = {}
        for symbol in symbols_list:
            cursor.execute("SELECT name FROM asset_metadata WHERE symbol = %s LIMIT 1", (symbol,))
            result = cursor.fetchone()
            names[symbol] = result[0] if result else symbol
        
        cursor.close()
        conn.close()
        
        return jsonify({
            'success': True,
            'strategy': strategy,
            'period': period,
            'loss_metric': 'Max Loss From Entry' if strategy == 'lumpsum' else 'Max Loss From Cost',
            'time_series': time_series_data,
            'statistics': statistics,
            'names': names,
            'symbols_order': symbols_list
        })
        
    except Exception as e:
        import traceback
        return jsonify({
            'success': False,
            'error': str(e),
            'traceback': traceback.format_exc()
        }), 500

# ADD this new endpoint to your api.py

# REPLACE your /api/exchanges/list endpoint with this corrected version

# REPLACE your /api/exchanges/list endpoint with this corrected version

@app.route('/api/exchanges/list', methods=['GET'])
def get_exchanges_list():
    """Get list of all available exchanges from database"""
    try:
        conn = get_db_connection()
        cursor = conn.cursor()
        
        # Build exclusion list properly
        exclusion_placeholders = ','.join(['%s'] * len(EXCLUDED_SYMBOLS))
        
        # Get distinct exchanges from asset_metadata, then join with exchanges table for names
        cursor.execute(f"""
            SELECT 
                am.exchange,
                COALESCE(e.name, am.exchange) as exchange_name,
                e.country_name
            FROM (
                SELECT DISTINCT exchange,
                    CASE 
                        WHEN exchange IS NULL THEN 0
                        WHEN exchange = 'NASDAQ' THEN 1
                        WHEN exchange = 'NYSE' THEN 2
                        ELSE 3
                    END as sort_order
                FROM asset_metadata
                WHERE symbol NOT IN ({exclusion_placeholders})
            ) AS am
            LEFT JOIN exchanges e ON am.exchange = e.exchange
            ORDER BY am.sort_order, am.exchange
        """, EXCLUDED_SYMBOLS)
        
        results = cursor.fetchall()
        exchanges = []
        
        for row in results:
            exchange_code = row[0]
            exchange_name = row[1]
            country_name = row[2]
            
            if exchange_code is None:
                # NULL exchange (crypto, commodities)
                exchanges.append({
                    'code': 'NULL',
                    'name': 'Non-Exchange Assets (Crypto, Commodities)'
                })
            else:
                # Build display name: "Exchange Name (Country)" or just "Exchange Name" if no country
                if country_name:
                    display_name = f"{exchange_name} ({country_name})"
                else:
                    display_name = exchange_name
                
                exchanges.append({
                    'code': exchange_code,
                    'name': display_name
                })
        
        cursor.close()
        conn.close()
        
        return jsonify({'success': True, 'exchanges': exchanges})
    except Exception as e:
        import traceback
        return jsonify({'success': False, 'error': str(e), 'traceback': traceback.format_exc()}), 500


@app.route('/api/health', methods=['GET'])
def health_check():
    return jsonify({'status': 'ok', 'message': 'API is running'})

if __name__ == '__main__':
    app.run(host='0.0.0.0', port=5000, debug=False)
