#!/usr/bin/env python3
"""
Monitor DFI Historical Collection Completion
===========================================

Continuously monitor the 5-year historical collection and notify when complete.
"""

import os
import time
import subprocess
from datetime import datetime

def check_completion_status():
    """Check if the historical collection has completed"""
    
    # Check if process is still running
    try:
        result = subprocess.run(['ps', 'aux'], capture_output=True, text=True)
        process_lines = [line for line in result.stdout.split('\n') if 'collect_dfi_historical.py' in line and 'grep' not in line]
        
        if not process_lines:
            return True, "Process completed"
    except Exception as e:
        return False, f"Error checking process: {e}"
    
    # Check log file for completion indicators
    log_file = '/workspaces/Test_for_Claude/Business/wisconsin_data_collection/dfi_historical_output.log'
    
    if os.path.exists(log_file):
        try:
            with open(log_file, 'r') as f:
                lines = f.readlines()
            
            # Look for completion indicators
            completion_indicators = [
                "Successfully loaded",
                "Historical collection completed successfully",
                "5-year historical collection completed"
            ]
            
            for line in lines:
                for indicator in completion_indicators:
                    if indicator in line:
                        return True, f"Found completion indicator: {indicator}"
            
            # Check if we've reached the end date (2020)
            recent_lines = lines[-10:] if len(lines) > 10 else lines
            for line in recent_lines:
                if "2020" in line and "06/28/2020" in line:
                    return True, "Reached target start date (2020)"
                    
        except Exception as e:
            return False, f"Error reading log: {e}"
    
    return False, "Still running"

def get_current_progress():
    """Get current progress information"""
    
    log_file = '/workspaces/Test_for_Claude/Business/wisconsin_data_collection/dfi_historical_output.log'
    
    if not os.path.exists(log_file):
        return "No log file found"
    
    try:
        with open(log_file, 'r') as f:
            lines = f.readlines()
        
        if not lines:
            return "Empty log file"
        
        # Get last few lines for current status
        recent_lines = lines[-3:]
        
        current_status = []
        for line in recent_lines:
            if line.strip():
                timestamp = line.split(' - ')[0] if ' - ' in line else ''
                message = line.split(' - ', 2)[-1].strip() if ' - ' in line else line.strip()
                
                # Extract just the time
                time_part = timestamp.split()[-1] if timestamp else 'N/A'
                current_status.append(f"{time_part}: {message}")
        
        return "; ".join(current_status)
        
    except Exception as e:
        return f"Error reading progress: {e}"

def estimate_completion_time():
    """Estimate when collection will complete"""
    
    log_file = '/workspaces/Test_for_Claude/Business/wisconsin_data_collection/dfi_historical_output.log'
    
    if not os.path.exists(log_file):
        return "Cannot estimate - no log file"
    
    try:
        with open(log_file, 'r') as f:
            lines = f.readlines()
        
        # Count date ranges processed
        date_ranges = set()
        for line in lines:
            if 'Searching DFI registrations for' in line and 'from ' in line and ' to ' in line:
                date_part = line.split('from ')[-1].split(' to ')
                if len(date_part) == 2:
                    start_date = date_part[0].strip().strip("'")
                    end_date = date_part[1].strip().strip("'")
                    date_ranges.add(f"{start_date} to {end_date}")
        
        # Estimate progress
        estimated_total_batches = 20  # 5 years / 3 months = ~20 batches
        batches_completed = len(date_ranges)
        
        if batches_completed == 0:
            return "Just started"
        
        progress_percent = (batches_completed / estimated_total_batches) * 100
        
        # Estimate time based on current progress
        if len(lines) > 0:
            first_line = lines[0]
            if ' - ' in first_line:
                start_time_str = first_line.split(' - ')[0]
                try:
                    start_time = datetime.strptime(start_time_str, '%Y-%m-%d %H:%M:%S,%f')
                    elapsed_minutes = (datetime.now() - start_time).total_seconds() / 60
                    
                    if batches_completed > 0:
                        avg_time_per_batch = elapsed_minutes / batches_completed
                        remaining_batches = max(0, estimated_total_batches - batches_completed)
                        estimated_remaining_minutes = remaining_batches * avg_time_per_batch
                        
                        return f"{progress_percent:.0f}% complete, ~{estimated_remaining_minutes:.0f} min remaining"
                except:
                    pass
        
        return f"{progress_percent:.0f}% complete"
        
    except Exception as e:
        return f"Cannot estimate: {e}"

def main():
    """Monitor the collection until completion"""
    
    print("🔄 MONITORING DFI HISTORICAL COLLECTION")
    print("=" * 60)
    print("Checking every 60 seconds until completion...")
    print("Press Ctrl+C to stop monitoring")
    print()
    
    check_count = 0
    
    try:
        while True:
            check_count += 1
            current_time = datetime.now().strftime('%H:%M:%S')
            
            # Check completion status
            is_complete, status = check_completion_status()
            
            if is_complete:
                print(f"\n🎉 COLLECTION COMPLETED!")
                print(f"🕒 Completion time: {current_time}")
                print(f"📊 Status: {status}")
                
                # Get final summary
                progress = get_current_progress()
                print(f"📋 Final status: {progress}")
                
                print(f"\n✅ 5-year DFI historical data collection is complete!")
                print(f"🎯 Ready for historical trend analysis and client presentations")
                break
            
            else:
                # Show progress update
                progress = get_current_progress()
                estimate = estimate_completion_time()
                
                print(f"⏰ {current_time} - Check #{check_count}")
                print(f"   Status: {estimate}")
                print(f"   Current: {progress}")
                print()
                
                # Wait 60 seconds before next check
                time.sleep(60)
                
    except KeyboardInterrupt:
        print(f"\n\n⏹️  Monitoring stopped by user at {datetime.now().strftime('%H:%M:%S')}")
        print("Collection may still be running in background")

if __name__ == "__main__":
    main()