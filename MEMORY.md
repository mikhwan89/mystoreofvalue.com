# MyStoreOfValue.com - Session Memory

## Current Priority Tasks

### Completed (Staging Environment Setup)
- ✅ Staging environment infrastructure (directories, database, ports)
- ✅ Environment-based configuration (.env.production, .env.staging)
- ✅ Git branch strategy (main → production, staging → staging env)
- ✅ Nginx virtual host for staging subdomain

### High Priority Next Steps (IMMEDIATE - Complete Staging First!)
1. 🚀 **DNS configuration on GoDaddy** - Add A record for staging.mystoreofvalue.com
2. 🔒 **SSL certificate for staging** - Run certbot after DNS is configured
3. 📦 **Deploy front-end to staging** - Copy files to /var/www/mystoreofvalue-staging
4. ⚙️ **Configure staging API** - Ensure API runs on port 5001 with staging .env

### Next Priority (After Staging Works)
5. 🚨 **Google Secret Manager migration** - CRITICAL: Replace .env files with GCP Secret Manager
6. **Staging-specific cron jobs** - Add environment flags to automation scripts
7. **Monitoring & alerting** - Set up alerts for cron failures and API errors
8. **Database connection pooling** - Improve API performance and resource usage

### Project Context
- Production site: mystoreofvalue.com (financial analysis platform)
- GCP N1-standard-1 instance (limited resources - be mindful)
- PostgreSQL database: `mystoreofvalue`
- Main assets tracked: BTC, ETH, commodities, indices (NO stocks anymore)
- Staging environment now available for safe testing

## User Preferences
- Always test on staging before production
- Use `/commit` for git commits
- Explicit approval needed for pushes to main
- Clear communication before risky operations

## Important Patterns
- Batch inserts: 5,000 records max
- Forward-fill strategy for missing price data
- Minimum 70% data completeness for calculations
- Risk-free rate: 2% for Sharpe/Sortino calculations
