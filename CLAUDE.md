# MyStoreOfValue.com - Back-End API

## Project Overview
Financial analysis platform that calculates and serves asset performance metrics for cryptocurrencies, commodities, and indices. Helps investors assess long-term viability of different assets as stores of value.

**Website:** https://mystoreofvalue.com
**GitHub:** https://github.com/mikhwan89/mystoreofvalue.com
**Server:** GCP N1-standard-1 (1 core, 3.75GB RAM)

## Tech Stack
- **Language:** Python 3
- **Framework:** Flask with CORS
- **Database:** PostgreSQL (database: `mystoreofvalue`, user: `ikhwan`)
- **Data Source:** Financial Modeling Prep (FMP) API
- **Deployment:** Production server, runs on port 5000
- **Proxy:** Nginx proxies `/api/` requests to `localhost:5000`

## Architecture & Components

### Core Scripts
1. **api.py** - Flask REST API server (8 endpoints)
2. **fetch_asset_light.py** - Fetches EOD prices from FMP API
3. **normalize_prices_to_usd.py** - Converts non-USD prices using forex rates
4. **calculate_performance.py** - Computes buy-and-hold metrics (CAGR, Sharpe, etc.)
5. **calculate_dca_performance.py** - Computes DCA strategy metrics
6. **update_performance_monthly.py** - Monthly recalculation (buy-and-hold)
7. **update_dca_monthly.py** - Monthly recalculation (DCA)
8. **populate_*.py** - Metadata loaders (assets, exchanges, forex, holidays)

### Database Schema
**Price Tables:**
- `crypto_prices` - BTC, ETH daily prices
- `commodity_prices` - Gold, oil, commodities
- `index_prices` - S&P 500, NASDAQ, indices
- `forex_prices` - Currency pairs (EURUSD, GBPUSD, etc.)

**Performance Tables:**
- `asset_performance_buy_and_hold` - Lumpsum strategy metrics
- `asset_performance_dca` - Dollar-cost averaging metrics

**Metadata Tables:**
- `asset_metadata` - Asset info (symbol, name, type, currency, exchange)
- `exchanges` - Exchange details (72+ exchanges)
- `exchange_holidays` - Trading holiday calendars
- `comments` - User comments with moderation

**Key Metrics Stored:**
- CAGR (annualized_return_pct)
- Total return, volatility, max drawdown, max loss
- Sharpe ratio, Sortino ratio, Calmar ratio
- Data completeness percentage

### API Endpoints
- `GET /api/currencies` - Currency depreciation data
- `GET /api/leaderboard` - Asset rankings with filters
- `GET /api/leaderboard/stats` - Winner statistics
- `GET /api/assets/list` - Available assets for selection
- `GET /api/assets/details` - Time-series performance data
- `GET /api/exchanges/list` - Available exchanges
- `GET /api/comments` - User comments
- `POST /api/comments` - Submit comment (with spam detection)

### Automated Jobs (Cron)
**Daily Update (2:00 AM EST):**
```bash
./daily_update.sh
# Updates: exchanges → holidays → metadata → forex → prices → normalize
```

**Monthly Performance (1st-10th, 3:00 AM & 4:00 AM EST):**
```bash
python update_performance_monthly.py    # Buy-and-hold
python update_dca_monthly.py            # DCA strategies
```

**Logs:** `logs/daily_update_YYYYMMDD_HHMMSS.log` (30-day retention)

## Critical Constraints

### Data Handling
- **Crypto Assets:** BTC and ETH ONLY (no altcoins)
- **Stock Data:** NO LONGER TRACKED (removed due to storage/time constraints)
- **Forward-Fill Strategy:** Missing dates filled with last known price
- **USD Normalization:** All prices converted to USD for fair comparison
- **Holding Periods:** 3, 4, 5, 6, 7, 8, 9, 10 years analyzed
- **Historical Range:** January 1, 2010 to present

### Performance Calculation Rules
- **MUST** have exact data on start AND end dates
- **Minimum 70%** data completeness for holding period
- Risk-free rate: 2% annual (for Sharpe/Sortino)
- Volatility: Annualized (daily returns × sqrt(365))

### Code Patterns
- Batch inserts: 5,000 records per batch
- Upsert pattern: `ON CONFLICT DO UPDATE`
- Thread-safe counters for parallel processing
- Rate limiting: 2-second retry on 429, max 3 retries
- Fresh DB connections per request (no pooling yet - TODO)

## Deployment Rules

### CRITICAL - Production Safety
1. **NEVER deploy directly to production** - Use staging first (to be built)
2. **NEVER push --force** to main branch
3. **NEVER skip hooks** (--no-verify) without explicit instruction
4. **ALWAYS test on staging** before production deployment
5. **NEVER commit .env files** - Contains sensitive credentials

### Backup & Recovery
- Backups stored in GitHub via `backup_db.sh`
- No automated restore testing yet (TODO)
- No alerting on backup failures (TODO)

### Git Workflow (To Be Implemented)
- `main` branch → production
- `staging` branch → staging environment (TODO)
- Feature branches → merge to staging first

## Environment Configuration

### .env File (NEVER COMMIT)
```
FMP_API_KEY=msXI31HQCqZFeMqcOoMSeoAJw1YiBspD
DB_NAME=mystoreofvalue
DB_USER=ikhwan
DB_PASSWORD=a352256ikh!@#
DB_HOST=localhost
DB_PORT=5432
```

### Future: Environment Separation (TODO)
- `.env.production` - Production credentials
- `.env.staging` - Staging credentials
- Environment variable in scripts to switch contexts

## Known Technical Debt
1. ❌ No connection pooling (creates fresh connections per request)
2. ❌ No staging environment (all code runs in production)
3. ❌ No monitoring/alerting for cron failures or API errors
4. ❌ Hardcoded API base URL in front-end (should be configurable)
5. ❌ Stock-related code still exists but unused (should be cleaned up)
6. ❌ No automated backup verification
7. ❌ No centralized log management
8. ❌ No health check with detailed status

## Common Tasks

### Start API Server
```bash
cd /home/m_ikhwan_online/projects/mystoreofvalue.com
python api.py  # Runs on port 5000
```

### Manual Data Update
```bash
cd /home/m_ikhwan_online/projects/mystoreofvalue.com
./daily_update.sh  # Full daily update pipeline
```

### Check Logs
```bash
ls -lh logs/
tail -f logs/daily_update_YYYYMMDD_HHMMSS.log
```

### Database Access
```bash
psql -U ikhwan -d mystoreofvalue
# Password: a352256ikh!@#
```

### Git Operations
```bash
git status
git pull origin main  # Only after testing on staging
# Use /commit slash command for proper commits
```

## Testing Strategy (TODO)
- No unit tests currently
- No integration tests
- No staging environment
- Manual testing on production (risky!)

## Monitoring (TODO)
- No error tracking (e.g., Sentry)
- No uptime monitoring (e.g., UptimeRobot)
- No cron job failure alerts
- No API performance metrics

## Related Repositories
- **Front-End:** `/var/www/mystoreofvalue` (github.com/mikhwan89/mystoreofvalue-frontend)
- **Analytics:** `/home/m_ikhwan_online/projects/umami` (self-hosted Umami)

## Notes for Claude
- Always read files before editing
- Use staging environment once built (TODO)
- Confirm destructive operations (push, delete, force)
- Use `/commit` for git commits with proper messages
- Check cron schedules before modifying automation
- Be mindful of GCP instance limits (N1-standard-1 = low resources)
- Production database is live - handle with care!
