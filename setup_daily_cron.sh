#!/bin/bash

# Setup Daily Update Cron Job
# This script sets up the cron job to run daily at 2 AM US Eastern Time

echo "=========================================="
echo "Setting up Daily Asset Price Update Cron Job"
echo "=========================================="

# Get the current user's home directory
USER_HOME=$(eval echo ~$USER)
SCRIPT_DIR="$USER_HOME/projects/mystoreofvalue.com"

# Make the daily update script executable
chmod +x "$SCRIPT_DIR/daily_update.sh"

# Check current timezone
echo ""
echo "Current system timezone:"
timedatectl | grep "Time zone"

echo ""
echo "Setting timezone to US/Eastern..."
sudo timedatectl set-timezone America/New_York

echo ""
echo "New timezone:"
timedatectl | grep "Time zone"

# Add cron job (runs at 2 AM Eastern Time daily)
CRON_JOB="0 2 * * * $SCRIPT_DIR/daily_update.sh"

# Check if cron job already exists
(crontab -l 2>/dev/null | grep -v "daily_update.sh"; echo "$CRON_JOB") | crontab -

echo ""
echo "✓ Cron job installed successfully!"
echo ""
echo "Current crontab:"
crontab -l
echo ""
echo "=========================================="
echo "Daily updates will run at 2 AM US Eastern Time"
echo "Logs will be saved to: $SCRIPT_DIR/logs/"
echo "=========================================="
echo ""
echo "To manually run a daily update:"
echo "  cd $SCRIPT_DIR && python3 fetch_asset_light.py --daily"
echo ""
echo "To run a full historical update:"
echo "  cd $SCRIPT_DIR && python3 fetch_asset_light.py"
echo ""
