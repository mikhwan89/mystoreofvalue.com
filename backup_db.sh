#!/bin/bash

# Database credentials
DB_NAME="mystoreofvalue"
DB_USER="ikhwan"
BACKUP_DIR="$HOME/projects/mystoreofvalue.com/backups"
TIMESTAMP=$(date +"%Y%m%d_%H%M%S")
BACKUP_FILE="$BACKUP_DIR/schema.sql"

# Create backup directory if it doesn't exist
mkdir -p "$BACKUP_DIR"

# Export database schema only (no data)
sudo -u postgres pg_dump --schema-only "$DB_NAME" > "$BACKUP_FILE"

# Navigate to git repository
cd "$HOME/projects/mystoreofvalue.com"

# Add and commit the backup
git add backups/schema.sql
git commit -m "Daily database schema backup - $TIMESTAMP"
git push origin main

echo "Backup completed and pushed to GitHub at $(date)"
