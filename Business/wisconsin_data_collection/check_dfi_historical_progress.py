#!/usr/bin/env python3
"""
Check DFI Historical Collection Progress
=======================================

Monitor the progress of the 5-year historical data collection.
"""

import os
import time
import subprocess
from datetime import datetime

def check_collection_progress():
    """Check the current progress of historical collection"""
    
    print("📊 DFI HISTORICAL COLLECTION PROGRESS MONITOR")
    print("=" * 60)
    print(f"🕒 Check time: {datetime.now().strftime('%Y-%m-%d %H:%M:%S')}")
    print()
    
    # Check if process is running
    try:
        result = subprocess.run(['ps', 'aux'], capture_output=True, text=True)
        process_lines = [line for line in result.stdout.split('\n') if 'collect_dfi_historical.py' in line and 'grep' not in line]
        
        if process_lines:
            print("✅ Historical collection is running")
            for line in process_lines:
                parts = line.split()
                if len(parts) >= 2:
                    pid = parts[1]
                    cpu = parts[2] if len(parts) > 2 else "N/A"
                    mem = parts[3] if len(parts) > 3 else "N/A"
                    print(f"   PID: {pid} | CPU: {cpu}% | Memory: {mem}%")
        else:
            print("❌ Historical collection process not found")
            return False
            
    except Exception as e:
        print(f"⚠️  Could not check process status: {e}")
    
    # Check log file for progress
    log_file = '/workspaces/Test_for_Claude/Business/wisconsin_data_collection/dfi_historical_output.log'
    
    if os.path.exists(log_file):
        print(f"\n📋 LOG FILE ANALYSIS:")
        
        # Get file size and modification time
        stat = os.stat(log_file)
        file_size_kb = stat.st_size / 1024
        mod_time = datetime.fromtimestamp(stat.st_mtime)
        
        print(f"   Log file size: {file_size_kb:.1f} KB")
        print(f"   Last modified: {mod_time.strftime('%Y-%m-%d %H:%M:%S')}")
        
        # Read log lines
        try:
            with open(log_file, 'r') as f:
                lines = f.readlines()
            
            print(f"   Total log lines: {len(lines):,}")
            
            # Count successful searches
            search_lines = [line for line in lines if 'Searching DFI registrations for' in line]
            found_lines = [line for line in lines if 'Found ' in line and 'business registrations' in line]
            
            print(f"   Searches started: {len(search_lines)}")
            print(f"   Searches completed: {len(found_lines)}")
            
            # Extract current progress info
            if lines:
                print(f"\n📈 RECENT ACTIVITY (last 5 lines):")
                for line in lines[-5:]:
                    if line.strip():
                        timestamp = line.split(' - ')[0] if ' - ' in line else ''
                        message = line.split(' - ', 2)[-1].strip() if ' - ' in line else line.strip()
                        print(f"   {timestamp.split()[-1] if timestamp else 'N/A'}: {message}")
            
            # Estimate progress based on keywords and batches
            if search_lines:
                # Extract unique date ranges to estimate batch progress
                date_ranges = set()
                for line in search_lines:
                    if 'from ' in line and ' to ' in line:
                        date_part = line.split('from ')[-1].split(' to ')
                        if len(date_part) == 2:
                            start_date = date_part[0].strip().strip("'")
                            end_date = date_part[1].strip().strip("'")
                            date_ranges.add(f"{start_date} to {end_date}")
                
                print(f"\n🎯 PROGRESS ESTIMATION:")
                print(f"   Unique date batches processed: {len(date_ranges)}")
                
                # Estimate total batches (5 years / 3 months = ~20 batches)
                estimated_total_batches = 20
                progress_percent = (len(date_ranges) / estimated_total_batches) * 100
                
                print(f"   Estimated total batches: {estimated_total_batches}")
                print(f"   Progress: {progress_percent:.1f}%")
                
                if len(date_ranges) > 0:
                    avg_time_per_batch = (datetime.now() - mod_time).total_seconds() / len(date_ranges)
                    remaining_batches = max(0, estimated_total_batches - len(date_ranges))
                    estimated_remaining_minutes = (remaining_batches * avg_time_per_batch) / 60
                    
                    print(f"   Estimated time remaining: {estimated_remaining_minutes:.0f} minutes")
                
                # Show date ranges being processed
                if date_ranges:
                    print(f"\n📅 DATE RANGES PROCESSED:")
                    for i, date_range in enumerate(sorted(date_ranges)[:3], 1):
                        print(f"   {i}. {date_range}")
                    if len(date_ranges) > 3:
                        print(f"   ... and {len(date_ranges) - 3} more")
            
        except Exception as e:
            print(f"   ⚠️  Could not analyze log file: {e}")
    
    else:
        print(f"\n⚠️  Log file not found: {log_file}")
    
    return True

def monitor_collection():
    """Continuous monitoring of collection progress"""
    
    print("🔄 Starting continuous monitoring (Ctrl+C to stop)")
    print("Checking every 30 seconds...")
    print()
    
    try:
        while True:
            check_collection_progress()
            print("\n" + "="*60)
            print("Waiting 30 seconds for next check...")
            time.sleep(30)
            
    except KeyboardInterrupt:
        print("\n\n⏹️  Monitoring stopped by user")

def main():
    """Main function"""
    
    print("🚀 DFI HISTORICAL COLLECTION PROGRESS CHECKER")
    print("=" * 60)
    print("Choose monitoring option:")
    print("1. Single progress check")
    print("2. Continuous monitoring (every 30 seconds)")
    print()
    
    # For automated use, just do single check
    choice = "1"
    
    if choice == "1":
        check_collection_progress()
    elif choice == "2":
        monitor_collection()
    else:
        print("Invalid choice")

if __name__ == "__main__":
    main()