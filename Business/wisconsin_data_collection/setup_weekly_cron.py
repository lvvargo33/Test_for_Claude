#!/usr/bin/env python3
"""
Setup weekly cron job for DFI data collection
Runs every Monday at 7:00 AM
"""

import os
import subprocess

def setup_weekly_cron():
    print('⏰ Setting Up Weekly DFI Collection')
    print('=' * 45)
    
    # Get the current directory path
    current_dir = '/workspaces/Test_for_Claude/Business/wisconsin_data_collection'
    script_path = f'{current_dir}/weekly_dfi_collection.py'
    
    # Cron entry for every Monday at 7:00 AM
    cron_entry = f'''# Wisconsin DFI Business Data Collection - Every Monday 7:00 AM
0 7 * * 1 cd {current_dir} && export GOOGLE_APPLICATION_CREDENTIALS="{current_dir}/location-optimizer-1-449414f93a5a.json" && /usr/bin/python3 {script_path} >> {current_dir}/logs/weekly_cron.log 2>&1
'''
    
    print(f'📅 Cron Schedule: Every Monday at 7:00 AM')
    print(f'📂 Script: {script_path}')
    print(f'📝 Logs: {current_dir}/logs/')
    
    print(f'\n📋 Cron Entry to Add:')
    print(cron_entry)
    
    # Create logs directory
    logs_dir = f'{current_dir}/logs'
    os.makedirs(logs_dir, exist_ok=True)
    print(f'✅ Created logs directory: {logs_dir}')
    
    # Write the cron entry to a file for manual installation
    cron_file = f'{current_dir}/dfi_weekly_cron.txt'
    with open(cron_file, 'w') as f:
        f.write(cron_entry)
    
    print(f'✅ Saved cron entry to: {cron_file}')
    
    print(f'\n🔧 Manual Installation Instructions:')
    print(f'1. Run: crontab -e')
    print(f'2. Add the contents of: {cron_file}')
    print(f'3. Save and exit')
    print(f'4. Verify with: crontab -l')
    
    print(f'\n📊 What This Will Do:')
    print(f'• Runs every Monday at 7:00 AM')
    print(f'• Collects Wisconsin businesses registered in the last 7 days')
    print(f'• Searches for restaurants, salons, fitness, auto, retail, etc.')
    print(f'• Adds new businesses to BigQuery (avoids duplicates)')
    print(f'• Logs all activity to {logs_dir}/')
    
    print(f'\n🧪 Test the Script Now:')
    print(f'cd {current_dir}')
    print(f'export GOOGLE_APPLICATION_CREDENTIALS="{current_dir}/location-optimizer-1-449414f93a5a.json"')
    print(f'python3 weekly_dfi_collection.py')
    
    return True

if __name__ == "__main__":
    setup_weekly_cron()