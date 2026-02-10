# MyStoreOfValue.com - Project TODO List

## ✅ Completed Tasks (4/21)

- [x] Set up staging environment infrastructure (directories, database, ports)
- [x] Implement environment-based configuration (.env.production, .env.staging)
- [x] Set up Git branch strategy (main, staging, production branches)
- [x] Set up nginx virtual host for staging subdomain

---

## 📋 Pending Tasks (17/21)

### 🎯 High Priority - Infrastructure & Operations

- [ ] **#15** Create staging-specific cron jobs or add environment flags
  - Modify `daily_update.sh` and `setup_*_cron.sh` to respect environment
  - Prevent staging jobs from affecting production data

- [ ] **#16** Add monitoring/alerting for failed cron jobs and API errors
  - Set up email/Slack notifications for failures
  - Monitor disk space, database health, API response times

- [ ] **#11** Add database connection pooling to Flask API
  - Implement connection pooling (SQLAlchemy or psycopg2 pool)
  - Improve performance and resource usage

### 🚀 New Features & Content

- [ ] **#5** Add weekly start date analysis (design batched initial fill strategy)
  - Calculate performance for different weekly purchase days
  - Help users optimize their DCA schedule

- [ ] **#6** Add educational content explaining metrics (Sharpe, Sortino, Calmar, CAGR, etc.)
  - Create glossary or info tooltips
  - Help users understand what each metric means

- [ ] **#7** Add use cases/personas for different investor profiles
  - Example scenarios: "Conservative Investor", "Bitcoin Maximalist", etc.
  - Show how different investors might use the tool

- [ ] **#8** Add comparison tools (What if I invested $X in year Y?)
  - Interactive calculator for historical investment scenarios
  - Show growth of hypothetical investments

### 📈 Marketing & Growth

- [ ] **#9** Implement SEO optimization (meta tags, semantic HTML, blog content)
  - Improve search engine visibility
  - Add structured data markup
  - Consider creating a blog for content marketing

- [ ] **#10** Build Twitter/X bot for organic promotion on crypto/Bitcoin Twitter
  - Automated posts with interesting stats
  - Engage with crypto community
  - Drive organic traffic

### 🔧 Technical Improvements

- [ ] **#12** Make API base URL configurable (env variable, not hardcoded localhost)
  - Currently hardcoded in front-end
  - Should respect environment (staging vs production)

- [ ] **#13** Implement proper cache-busting with build hashing (replace ?v=7)
  - Use webpack or similar for asset fingerprinting
  - Automatic cache invalidation on deploys

- [ ] **#14** Refactor front-end global state management to module pattern
  - Clean up global scope pollution
  - Improve maintainability

### 🧹 Code Quality & Maintenance

- [ ] **#18** Remove unused stock-related code and references from codebase
  - Clean up obsolete code
  - Reduce technical debt

- [ ] **#19** Add error handling and retry logic for API failures
  - Graceful degradation on external API failures
  - Retry with exponential backoff

- [ ] **#20** Implement logging rotation and centralized log management
  - Current: 30-day retention in `logs/`
  - Consider centralized logging solution

- [ ] **#21** Add health check endpoints with detailed status reporting
  - `/api/health` endpoint with database, API key, disk space checks
  - Useful for monitoring and debugging

### 🔐 DevOps & Reliability

- [ ] **#17** Set up automated backup verification and restore testing
  - Currently: backups run but never tested
  - Automate restore testing to ensure backups work

---

## 📊 Progress Tracker

**Overall Progress:** 4/21 tasks completed (19%)

**By Category:**
- Infrastructure & Operations: 4/7 completed (57%)
- New Features & Content: 0/4 completed (0%)
- Marketing & Growth: 0/2 completed (0%)
- Technical Improvements: 0/3 completed (0%)
- Code Quality: 0/3 completed (0%)
- DevOps: 0/2 completed (0%)

---

## 🎯 Recommended Next Steps

1. **#15** - Staging cron jobs (enable safe testing of automation)
2. **#16** - Monitoring/alerting (operational visibility)
3. **#11** - Connection pooling (performance improvement)

---

*Last Updated: 2026-02-10*
*Project: MyStoreOfValue.com - Financial Analysis Platform*
