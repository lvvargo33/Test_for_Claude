#!/bin/bash
# Install the weekly DFI collection cron job

echo "🔧 Installing Weekly DFI Collection Cron Job"
echo "=============================================="

# Read the current crontab
crontab -l > /tmp/current_cron 2>/dev/null || touch /tmp/current_cron

# Check if our job is already installed
if grep -q "weekly_dfi_collection.py" /tmp/current_cron; then
    echo "✅ Cron job already installed"
    echo "📅 Current schedule:"
    grep "weekly_dfi_collection.py" /tmp/current_cron
else
    echo "📅 Adding weekly DFI collection job..."
    
    # Add our cron job
    echo "" >> /tmp/current_cron
    echo "# Wisconsin DFI Business Data Collection - Every Monday 7:00 AM" >> /tmp/current_cron
    echo "0 7 * * 1 cd /workspaces/Test_for_Claude/Business/wisconsin_data_collection && export GOOGLE_APPLICATION_CREDENTIALS=\"/workspaces/Test_for_Claude/Business/wisconsin_data_collection/location-optimizer-1-449414f93a5a.json\" && /usr/bin/python3 /workspaces/Test_for_Claude/Business/wisconsin_data_collection/weekly_dfi_collection.py >> /workspaces/Test_for_Claude/Business/wisconsin_data_collection/logs/weekly_cron.log 2>&1" >> /tmp/current_cron
    
    # Install the new crontab
    crontab /tmp/current_cron
    
    echo "✅ Cron job installed successfully!"
fi

echo ""
echo "📋 Current crontab:"
crontab -l

echo ""
echo "🎉 Weekly DFI collection will run every Monday at 7:00 AM"
echo "📊 New Wisconsin business data will be automatically added to BigQuery"
echo "📝 Check logs at: /workspaces/Test_for_Claude/Business/wisconsin_data_collection/logs/"

# Clean up
rm -f /tmp/current_cron