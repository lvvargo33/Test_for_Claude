#!/bin/bash
# Quick Monday morning collection script
# Just run: ./run_weekly.sh

echo "🌅 Monday Morning Wisconsin Business Collection"
echo "=============================================="
echo "🕒 Started at: $(date)"
echo ""

# Navigate to correct directory
cd /workspaces/Test_for_Claude/Business/wisconsin_data_collection

# Set up Google Cloud credentials
export GOOGLE_APPLICATION_CREDENTIALS="location-optimizer-1-449414f93a5a.json"

# Run the weekly collection
python3 weekly_dfi_collection.py

echo ""
echo "🎉 Monday collection completed at: $(date)"
echo "📊 Check your BigQuery table for new Wisconsin business prospects!"