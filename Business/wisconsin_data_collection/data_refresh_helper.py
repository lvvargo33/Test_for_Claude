#!/usr/bin/env python3
"""
Data Refresh Helper - Interactive Guide
=======================================

Simple interactive script to help choose the right data refresh option.
"""

import subprocess
import sys
from datetime import datetime

def print_banner():
    """Print welcome banner"""
    print("🔄" * 40)
    print("   WISCONSIN BUSINESS DATA REFRESH")
    print("🔄" * 40)
    print(f"📅 {datetime.now().strftime('%A, %B %d, %Y at %H:%M')}")
    print()

def show_menu():
    """Display refresh options menu"""
    print("What would you like to do today?")
    print()
    print("📊 DATA REFRESH OPTIONS:")
    print("   1. Weekly Refresh (2-3 min)      - New business registrations")
    print("   2. Monthly Refresh (5-8 min)     - Weekly + data health checks")
    print("   3. Quarterly Refresh (10-15 min) - Monthly + current census")
    print("   4. Annual Refresh (20-30 min)    - Everything + historical data")
    print()
    print("🔍 INDIVIDUAL SOURCES:")
    print("   5. DFI Registrations Only         - Just business registrations")
    print("   6. Census Data Only               - Current year demographics")
    print("   7. Historical Census              - 2013-2023 demographic data")
    print("   8. Population Estimates           - County population data")
    print("   9. BLS Current Year               - Employment & unemployment data")
    print("   10. BLS Historical                - 2015-current employment data")
    print()
    print("🔧 UTILITIES:")
    print("   11. Check Data Status             - See when data was last updated")
    print("   0. Help/Documentation             - Show detailed guide")
    print()

def run_command(cmd_args):
    """Run the comprehensive refresh command"""
    cmd = ["python", "comprehensive_data_refresh.py"] + cmd_args
    print(f"🚀 Running: {' '.join(cmd)}")
    print("=" * 60)
    
    try:
        result = subprocess.run(cmd, check=True, capture_output=False)
        return True
    except subprocess.CalledProcessError as e:
        print(f"❌ Command failed with exit code {e.returncode}")
        return False
    except Exception as e:
        print(f"❌ Error running command: {str(e)}")
        return False

def show_help():
    """Show detailed help information"""
    print("📖 DETAILED REFRESH GUIDE")
    print("=" * 50)
    print()
    print("🔄 REFRESH TYPES:")
    print()
    print("WEEKLY (Every Monday):")
    print("  • New DFI business registrations from last 7 days")
    print("  • Restaurants, salons, fitness centers, retail stores")
    print("  • Quick prospect identification")
    print()
    print("MONTHLY (First Monday of month):")
    print("  • Everything from weekly")
    print("  • SBA loan data status check") 
    print("  • Business licenses status check")
    print("  • Data health monitoring")
    print()
    print("QUARTERLY (Jan/Apr/Jul/Oct):")
    print("  • Everything from monthly")
    print("  • Current year census demographics")
    print("  • Population growth trends")
    print()
    print("ANNUAL (January):")
    print("  • Everything from quarterly")
    print("  • Historical census data (2013-2023)")
    print("  • Complete population estimates")
    print("  • Full market intelligence refresh")
    print()
    print("📊 DATA SOURCES:")
    print("  • DFI Business Registrations: Weekly updates")
    print("  • Census Demographics: Annual updates (11 years historical)")
    print("  • Population Estimates: County-level growth data")
    print("  • BLS Employment Data: County-level employment & unemployment (2015-current)")
    print("  • SBA Loans: 2,904 records (2009-2023)")
    print("  • Business Licenses: Municipal data")
    print()

def main():
    """Main interactive function"""
    print_banner()
    
    while True:
        show_menu()
        
        try:
            choice = input("Enter your choice (1-11, 0 for help): ").strip()
            print()
            
            if choice == "1":
                print("🔄 Starting Weekly Refresh...")
                success = run_command([])
                
            elif choice == "2":
                print("🔄 Starting Monthly Refresh...")
                success = run_command(["--monthly"])
                
            elif choice == "3":
                print("🔄 Starting Quarterly Refresh...")
                print("⚠️  This will take 10-15 minutes")
                confirm = input("Continue? (y/N): ").lower()
                if confirm == 'y':
                    success = run_command(["--quarterly"])
                else:
                    continue
                
            elif choice == "4":
                print("🔄 Starting Annual Refresh...")
                print("⚠️  This will take 20-30 minutes and refresh ALL data")
                confirm = input("Continue? (y/N): ").lower()
                if confirm == 'y':
                    success = run_command(["--annual"])
                else:
                    continue
                
            elif choice == "5":
                print("🔄 Refreshing DFI Registrations Only...")
                success = run_command(["--dfi-only"])
                
            elif choice == "6":
                print("🔄 Refreshing Current Census Data...")
                success = run_command(["--census-only"])
                
            elif choice == "7":
                print("🔄 Refreshing Historical Census Data...")
                print("⚠️  This will take 15-20 minutes")
                confirm = input("Continue? (y/N): ").lower()
                if confirm == 'y':
                    success = run_command(["--historical-census"])
                else:
                    continue
                
            elif choice == "8":
                print("🔄 Refreshing Population Estimates...")
                success = run_command(["--population-estimates"])
                
            elif choice == "9":
                print("🔄 Refreshing BLS Current Year Data...")
                success = run_command(["--bls-current"])
                
            elif choice == "10":
                print("🔄 Refreshing BLS Historical Data...")
                print("⚠️  This will take 30-45 minutes")
                confirm = input("Continue? (y/N): ").lower()
                if confirm == 'y':
                    success = run_command(["--bls-historical"])
                else:
                    continue
                
            elif choice == "11":
                print("🔍 Checking Data Status...")
                success = run_command(["--check-status"])
                
            elif choice == "0":
                show_help()
                continue
                
            else:
                print("❌ Invalid choice. Please enter 1-11 or 0.")
                continue
            
            print()
            if success:
                print("✅ Operation completed successfully!")
            else:
                print("❌ Operation failed. Check error messages above.")
            
            print()
            another = input("Run another operation? (y/N): ").lower()
            if another != 'y':
                break
                
        except KeyboardInterrupt:
            print("\n\n👋 Exiting...")
            break
        except Exception as e:
            print(f"❌ Unexpected error: {str(e)}")
            break
    
    print("\n📊 Data refresh session completed!")
    print("💡 Tip: Set up recurring reminders for weekly/monthly refreshes")

if __name__ == "__main__":
    main()