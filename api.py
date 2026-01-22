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

@app.route('/api/leaderboard', methods=['GET'])
def get_leaderboard():
    """Get leaderboard with winners podium and period breakdown"""
    strategy = request.args.get('strategy', 'lumpsum')
    period = int(request.args.get('period', 5))
    asset_type = request.args.get('asset_type', 'all')
    start_date_from = request.args.get('start_date_from', None)
    
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
        
        # Determine table and filters
        if strategy == 'lumpsum':
            table = 'asset_performance_buy_and_hold'
            strategy_filter = ""
            loss_column = 'max_loss_from_entry_pct'
            params = [period]
        else:
            table = 'asset_performance_dca'
            frequency_map = {'dca_daily': 'daily', 'dca_weekly': 'weekly', 'dca_monthly': 'monthly'}
            frequency = frequency_map.get(strategy, 'monthly')
            strategy_filter = "AND dca_frequency = %s"
            loss_column = 'max_loss_from_cost_pct'
            params = [period, frequency]
        
        asset_type_filter = ""
        if asset_type != 'all':
            asset_type_filter = "AND asset_type = %s"
            params.append(asset_type)
        
        # Add start date filter
        start_date_filter = ""
        if start_date_from:
            start_date_filter = "AND start_date >= %s"
            params.append(start_date_from)
        
        # Build advanced filters
        advanced_filters = []
        
        if min_cagr:
            advanced_filters.append(f"AND annualized_return_pct >= %s")
            params.append(float(min_cagr))
        
        if max_drawdown:
            # Drawdown is stored as positive (e.g., 30.5 for 30.5% drawdown)
            # User wants "max 30% drawdown" = filter to drawdown <= 30
            advanced_filters.append(f"AND max_drawdown_pct <= %s")
            params.append(float(max_drawdown))
        
        if max_loss:
            # Loss is stored as positive (e.g., 25.0 for 25% loss)
            # User wants "max 25% loss" = filter to loss <= 25
            advanced_filters.append(f"AND {loss_column} <= %s")
            params.append(float(max_loss))
        
        if min_sharpe:
            advanced_filters.append(f"AND sharpe_ratio >= %s")
            params.append(float(min_sharpe))
        
        if min_sortino:
            advanced_filters.append(f"AND sortino_ratio >= %s")
            params.append(float(min_sortino))
        
        if min_calmar:
            advanced_filters.append(f"AND calmar_ratio >= %s")
            params.append(float(min_calmar))
        
        advanced_filter_str = " ".join(advanced_filters)
        
        # Get period-by-period winners
        cursor.execute(f"""
            WITH ranked_periods AS (
                SELECT 
                    symbol, asset_type, start_date, end_date, annualized_return_pct,
                    ROW_NUMBER() OVER (PARTITION BY start_date ORDER BY annualized_return_pct DESC) as rank
                FROM {table}
                WHERE holding_period_years = %s {strategy_filter} {asset_type_filter} {start_date_filter} {advanced_filter_str}
            )
            SELECT symbol, asset_type, start_date, end_date, annualized_return_pct, rank
            FROM ranked_periods
            WHERE rank <= 3
            ORDER BY start_date, rank
        """, params)
        
        period_results = cursor.fetchall()
        scores = {}
        period_breakdown = []
        
        for row in period_results:
            symbol, asset_type_val, start_date, end_date, cagr, rank = row[0], row[1], str(row[2]), str(row[3]), float(row[4]), int(row[5])
            
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
                'cagr': round(cagr, 2)
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
            periods_grouped[key][pos] = {'symbol': item['symbol'], 'cagr': item['cagr']}
        
        periods_list = list(periods_grouped.values())
        
        cursor.close()
        conn.close()
        
        return jsonify({
            'success': True,
            'strategy': strategy,
            'period': period,
            'asset_type': asset_type,
            'start_date_from': start_date_from,
            'filters_applied': {
                'min_cagr': min_cagr,
                'max_drawdown': max_drawdown,
                'max_loss': max_loss,
                'min_sharpe': min_sharpe,
                'min_sortino': min_sortino,
                'min_calmar': min_calmar
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

@app.route('/api/leaderboard/stats', methods=['GET'])
def get_leaderboard_stats():
    """
    Get detailed statistical breakdown of top 3 winners
    Uses only basic filters (strategy, period, asset_type, start_date_from)
    Ignores advanced filters to show true performance distribution
    """
    strategy = request.args.get('strategy', 'lumpsum')
    period = int(request.args.get('period', 5))
    asset_type = request.args.get('asset_type', 'all')
    start_date_from = request.args.get('start_date_from', None)
    top_symbols = request.args.get('symbols', '')  # Comma-separated list of winner symbols
    
    if not top_symbols:
        return jsonify({'success': False, 'error': 'No symbols provided'}), 400
    
    symbols_list = [s.strip() for s in top_symbols.split(',')]
    
    try:
        conn = get_db_connection()
        cursor = conn.cursor()
        
        # Determine table and filters
        if strategy == 'lumpsum':
            table = 'asset_performance_buy_and_hold'
            strategy_filter = ""
            loss_column = 'max_loss_from_entry_pct'
            params = [period]
        else:
            table = 'asset_performance_dca'
            frequency_map = {'dca_daily': 'daily', 'dca_weekly': 'weekly', 'dca_monthly': 'monthly'}
            frequency = frequency_map.get(strategy, 'monthly')
            strategy_filter = "AND dca_frequency = %s"
            loss_column = 'max_loss_from_cost_pct'
            params = [period, frequency]
        
        asset_type_filter = ""
        if asset_type != 'all':
            asset_type_filter = "AND asset_type = %s"
            params.append(asset_type)
        
        start_date_filter = ""
        if start_date_from:
            start_date_filter = "AND start_date >= %s"
            params.append(start_date_from)
        
        # Query all periods for the top symbols
        symbols_placeholder = ','.join(['%s'] * len(symbols_list))
        params.extend(symbols_list)
        
        cursor.execute(f"""
            SELECT 
                symbol,
                annualized_return_pct,
                sharpe_ratio,
                sortino_ratio,
                max_drawdown_pct,
                {loss_column} as max_loss_pct,
                volatility_pct
            FROM {table}
            WHERE holding_period_years = %s 
            {strategy_filter} 
            {asset_type_filter} 
            {start_date_filter}
            AND symbol IN ({symbols_placeholder})
            ORDER BY symbol, annualized_return_pct
        """, params)
        
        results = cursor.fetchall()
        
        # Organize data by symbol
        stats_by_symbol = {}
        for row in results:
            symbol = row[0]
            if symbol not in stats_by_symbol:
                stats_by_symbol[symbol] = {
                    'cagr': [],
                    'sharpe': [],
                    'sortino': [],
                    'drawdown': [],
                    'max_loss': [],
                    'volatility': []
                }
            
            stats_by_symbol[symbol]['cagr'].append(float(row[1]))
            stats_by_symbol[symbol]['sharpe'].append(float(row[2]) if row[2] else 0)
            stats_by_symbol[symbol]['sortino'].append(float(row[3]) if row[3] else 0)
            stats_by_symbol[symbol]['drawdown'].append(float(row[4]))
            stats_by_symbol[symbol]['max_loss'].append(float(row[5]))
            stats_by_symbol[symbol]['volatility'].append(float(row[6]))
        
        # Calculate percentiles for each symbol
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
        for symbol, metrics in stats_by_symbol.items():
            detailed_stats[symbol] = {
                'cagr': calculate_percentiles(metrics['cagr']),
                'sharpe': calculate_percentiles(metrics['sharpe']),
                'sortino': calculate_percentiles(metrics['sortino']),
                'drawdown': calculate_percentiles(metrics['drawdown']),
                'max_loss': calculate_percentiles(metrics['max_loss']),
                'volatility': calculate_percentiles(metrics['volatility'])
            }
        
        # Get names for symbols
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
            'stats': detailed_stats,
            'names': names
        })
        
    except Exception as e:
        import traceback
        return jsonify({
            'success': False,
            'error': str(e),
            'traceback': traceback.format_exc()
        }), 500

@app.route('/api/health', methods=['GET'])
def health_check():
    return jsonify({'status': 'ok', 'message': 'API is running'})

if __name__ == '__main__':
    app.run(host='0.0.0.0', port=5000, debug=False)
