#!/bin/bash

# Setup Monthly Performance Update Cron Jobs
# Runs on 1st-10th of each month at 3 AM Eastern Time

SCRIPT_DIR="/home/$(whoami)/projects/mystoreofvalue.com"

echo "========================================"
echo "Monthly Performance Update Cron Setup"
echo "========================================"

# Remove existing monthly performance cron jobs if any
crontab -l 2>/dev/null | grep -v "update_performance_monthly.py" | grep -v "update_dca_monthly.py" | crontab -

# Add new cron jobs
# Run on 1st-10th of each month at 3 AM Eastern
(crontab -l 2>/dev/null; echo "# Monthly buy-and-hold performance update (1st-10th of month at 3 AM EST)") | crontab -
(crontab -l 2>/dev/null; echo "0 3 1-10 * * cd $SCRIPT_DIR && /usr/bin/python3 update_performance_monthly.py >> logs/monthly_performance_\$(date +\%Y\%m\%d).log 2>&1") | crontab -

(crontab -l 2>/dev/null; echo "# Monthly DCA performance update (1st-10th of month at 4 AM EST)") | crontab -
(crontab -l 2>/dev/null; echo "0 4 1-10 * * cd $SCRIPT_DIR && /usr/bin/python3 update_dca_monthly.py >> logs/monthly_dca_\$(date +\%Y\%m\%d).log 2>&1") | crontab -

echo ""
echo "✓ Cron jobs installed successfully!"
echo ""
echo "Current crontab:"
echo "========================================"
crontab -l
echo "========================================"
echo ""
echo "Schedule:"
echo "  - Buy-and-Hold update: Daily at 3 AM on 1st-10th of each month"
echo "  - DCA update: Daily at 4 AM on 1st-10th of each month"
echo ""
echo "Logs will be saved to:"
echo "  $SCRIPT_DIR/logs/monthly_performance_YYYYMMDD.log"
echo "  $SCRIPT_DIR/logs/monthly_dca_YYYYMMDD.log"
echo ""
