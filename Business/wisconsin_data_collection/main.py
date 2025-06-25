"""
Location Optimizer - Main Runner
===============================

Main execution script for the improved Wisconsin data collection system.
Uses the new architecture with proper error handling and data validation.
"""

import argparse
import sys
import logging
import time
from datetime import datetime
from pathlib import Path

# Add the current directory to Python path for imports
sys.path.append(str(Path(__file__).parent))

from wisconsin_collector import WisconsinDataCollector
from setup_bigquery import BigQuerySetup
import pandas as pd

# Configure logging
logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(name)s - %(levelname)s - %(message)s',
    handlers=[
        logging.FileHandler('location_optimizer.log'),
        logging.StreamHandler()
    ]
)
logger = logging.getLogger(__name__)


def setup_infrastructure():
    """Setup BigQuery infrastructure"""
    logger.info("Setting up BigQuery infrastructure...")
    
    try:
        setup = BigQuerySetup()
        setup.setup_all_tables()
        print("✅ BigQuery infrastructure setup complete!")
        return True
    except Exception as e:
        logger.error(f"Error setting up infrastructure: {e}")
        print(f"❌ Infrastructure setup failed: {e}")
        return False


def collect_wisconsin_data(days_back: int = 90, include_demographics: bool = True):
    """Collect Wisconsin business data including demographics"""
    logger.info(f"Starting Wisconsin data collection for last {days_back} days")
    
    try:
        # Initialize collector
        collector = WisconsinDataCollector()
        
        # Run full collection including demographics
        summary = collector.run_full_wisconsin_collection(
            days_back=days_back,
            include_demographics=include_demographics,
            geographic_levels=['county', 'tract']  # Start with county and tract levels
        )
        
        # Display results
        print("\n" + "=" * 60)
        print("WISCONSIN DATA COLLECTION RESULTS")
        print("=" * 60)
        print(f"📊 Collection Date: {summary['start_time'].strftime('%Y-%m-%d %H:%M:%S')}")
        print(f"🏢 Business Registrations: {summary['businesses_collected']:,}")
        print(f"💰 SBA Loan Approvals: {summary['sba_loans_collected']:,}")
        print(f"📋 Business Licenses: {summary['licenses_collected']:,}")
        print(f"👥 Demographics Collected: {'YES' if summary['demographics_collected'] else 'NO'}")
        total_records = summary['businesses_collected'] + summary['sba_loans_collected'] + summary['licenses_collected']
        print(f"📈 Total Records: {total_records:,}")
        if 'processing_time' in summary:
            print(f"⏱️  Processing Time: {summary['processing_time']:.1f} seconds")
        print(f"✅ Success: {'YES' if summary['success'] else 'NO'}")
        
        if summary['errors']:
            print(f"⚠️  Errors: {len(summary['errors'])}")
            for error in summary['errors'][:3]:  # Show first 3 errors
                print(f"   • {error}")
        
        print("=" * 60)
        
        if summary['success']:
            print("\n🎉 Data collection successful!")
            print("\n📋 Next steps:")
            print("   1. Run analysis: python main.py --analyze")
            print("   2. Export prospects: python main.py --export-prospects")
            print("   3. Set up automated daily collection")
        else:
            print("\n❌ Data collection had issues. Check logs for details.")
        
        return summary
        
    except Exception as e:
        logger.error(f"Error in Wisconsin data collection: {e}")
        print(f"❌ Data collection failed: {e}")
        return None


def run_analysis():
    """Run opportunity analysis on collected data"""
    logger.info("Running Wisconsin opportunity analysis")
    
    try:
        from google.cloud import bigquery
        client = bigquery.Client(project="location-optimizer-1")
        
        # Analysis queries
        analyses = {
            "🔥 Hot SBA Prospects (Last 30 Days)": """
            SELECT 
              borrower_name,
              borrower_city,
              ROUND(loan_amount) as loan_amount,
              approval_date,
              franchise_name,
              DATE_DIFF(CURRENT_DATE(), approval_date, DAY) as days_since_approval
            FROM `location-optimizer-1.raw_business_data.sba_loan_approvals`
            WHERE borrower_state = 'WI'
              AND approval_date >= DATE_SUB(CURRENT_DATE(), INTERVAL 30 DAY)
              AND loan_amount >= 100000
            ORDER BY approval_date DESC, loan_amount DESC
            LIMIT 10
            """,
            
            "🏪 New Business Registrations (Last 14 Days)": """
            SELECT 
              business_name,
              business_type,
              city,
              registration_date,
              confidence_score
            FROM `location-optimizer-1.raw_business_data.business_entities`
            WHERE state = 'WI'
              AND registration_date >= DATE_SUB(CURRENT_DATE(), INTERVAL 14 DAY)
              AND business_type IN ('restaurant', 'retail', 'franchise', 'personal_services')
            ORDER BY registration_date DESC, confidence_score DESC
            LIMIT 10
            """,
            
            "📊 Market Activity Summary by City": """
            SELECT 
              city,
              COUNT(*) as total_new_businesses,
              COUNT(CASE WHEN business_type = 'restaurant' THEN 1 END) as restaurants,
              COUNT(CASE WHEN business_type = 'retail' THEN 1 END) as retail,
              COUNT(CASE WHEN business_type = 'franchise' THEN 1 END) as franchises,
              ROUND(AVG(confidence_score), 1) as avg_confidence_score
            FROM `location-optimizer-1.raw_business_data.business_entities`
            WHERE state = 'WI'
              AND registration_date >= DATE_SUB(CURRENT_DATE(), INTERVAL 90 DAY)
            GROUP BY city
            HAVING COUNT(*) >= 3
            ORDER BY COUNT(*) DESC
            LIMIT 10
            """,
            
            "💡 Franchise Opportunities": """
            SELECT 
              franchise_name,
              COUNT(*) as recent_loans,
              ROUND(AVG(loan_amount)) as avg_loan_amount,
              ROUND(SUM(loan_amount)) as total_funding,
              STRING_AGG(DISTINCT borrower_city ORDER BY borrower_city) as cities
            FROM `location-optimizer-1.raw_business_data.sba_loan_approvals`
            WHERE borrower_state = 'WI'
              AND approval_date >= DATE_SUB(CURRENT_DATE(), INTERVAL 180 DAY)
              AND franchise_name IS NOT NULL
              AND franchise_name != ''
            GROUP BY franchise_name
            HAVING COUNT(*) >= 2
            ORDER BY COUNT(*) DESC, AVG(loan_amount) DESC
            LIMIT 8
            """
        }
        
        print("\n" + "=" * 80)
        print("WISCONSIN BUSINESS OPPORTUNITY ANALYSIS")
        print("=" * 80)
        
        for title, query in analyses.items():
            print(f"\n{title}")
            print("-" * 50)
            
            try:
                results = client.query(query).result()
                
                # Convert to DataFrame for better display
                df = results.to_dataframe()
                
                if len(df) > 0:
                    # Display results in a formatted table
                    print(df.to_string(index=False, max_cols=6))
                else:
                    print("   No data found for this analysis")
                    
            except Exception as e:
                print(f"   Error running analysis: {str(e)}")
        
        print("\n" + "=" * 80)
        print("💡 Analysis complete! Use these insights for client outreach.")
        
        return True
        
    except Exception as e:
        logger.error(f"Error in analysis: {e}")
        print(f"❌ Analysis failed: {e}")
        return False


def export_prospects(filename: str = "wisconsin_prospects.csv"):
    """Export prospect list for outreach"""
    logger.info(f"Exporting prospect list to {filename}")
    
    try:
        from google.cloud import bigquery
        client = bigquery.Client(project="location-optimizer-1")
        
        export_query = """
        WITH hot_prospects AS (
          -- High-value SBA loan recipients
          SELECT 
            'SBA_LOAN' as source,
            borrower_name as business_name,
            borrower_city as city,
            borrower_state as state,
            borrower_zip as zip_code,
            'Phone: Congratulations on SBA approval! Free location analysis offer' as contact_approach,
            loan_amount as value_indicator,
            CAST(approval_date AS STRING) as key_date,
            DATE_DIFF(CURRENT_DATE(), approval_date, DAY) as days_since_trigger,
            1 as priority_score,
            CASE 
              WHEN loan_amount >= 300000 THEN 'HIGH'
              WHEN loan_amount >= 150000 THEN 'MEDIUM'
              ELSE 'QUALIFIED'
            END as lead_quality
          FROM `location-optimizer-1.raw_business_data.sba_loan_approvals`
          WHERE borrower_state = 'WI'
            AND approval_date >= DATE_SUB(CURRENT_DATE(), INTERVAL 90 DAY)
            AND loan_amount >= 75000
          
          UNION ALL
          
          -- New franchise/restaurant registrations
          SELECT 
            'NEW_BUSINESS' as source,
            business_name,
            city,
            state,
            zip_code,
            'Email: Welcome to Wisconsin! Complimentary market insights available' as contact_approach,
            confidence_score as value_indicator,
            CAST(registration_date AS STRING) as key_date,
            DATE_DIFF(CURRENT_DATE(), registration_date, DAY) as days_since_trigger,
            2 as priority_score,
            CASE 
              WHEN business_type = 'franchise' AND confidence_score >= 85 THEN 'HIGH'
              WHEN business_type IN ('restaurant', 'retail') AND confidence_score >= 80 THEN 'MEDIUM'
              WHEN confidence_score >= 70 THEN 'QUALIFIED'
              ELSE 'STANDARD'
            END as lead_quality
          FROM `location-optimizer-1.raw_business_data.business_entities`
          WHERE state = 'WI'
            AND registration_date >= DATE_SUB(CURRENT_DATE(), INTERVAL 45 DAY)
            AND business_type IN ('restaurant', 'retail', 'franchise', 'personal_services')
            AND confidence_score >= 70
        )
        SELECT 
          source,
          business_name,
          city,
          state,
          zip_code,
          contact_approach,
          ROUND(value_indicator, 0) as value_indicator,
          key_date,
          days_since_trigger,
          lead_quality,
          priority_score
        FROM hot_prospects
        ORDER BY priority_score ASC, value_indicator DESC, days_since_trigger ASC
        LIMIT 100
        """
        
        # Execute query and export
        df = client.query(export_query).to_dataframe()
        
        if len(df) > 0:
            # Save to CSV
            df.to_csv(filename, index=False)
            
            print(f"📊 Exported {len(df)} prospects to {filename}")
            print("\n🎯 Top 5 Prospects:")
            print("=" * 50)
            
            # Show top 5 prospects
            top_prospects = df.head()
            for i, row in top_prospects.iterrows():
                print(f"\n{i+1}. {row['business_name']} ({row['city']}, {row['state']})")
                print(f"   Source: {row['source']}")
                print(f"   Quality: {row['lead_quality']}")
                print(f"   Days Since: {row['days_since_trigger']}")
                print(f"   Approach: {row['contact_approach']}")
            
            print(f"\n💡 Full prospect list saved to: {filename}")
            
        else:
            print("📊 No prospects found matching criteria")
        
        return True
        
    except Exception as e:
        logger.error(f"Error exporting prospects: {e}")
        print(f"❌ Export failed: {e}")
        return False


def run_daily_collection():
    """Run daily collection routine (for automation)"""
    logger.info("Running daily collection routine")
    
    # Collect last 7 days of data (with overlap for reliability)
    summary = collect_wisconsin_data(days_back=7)
    
    if summary and summary['success']:
        # If successful, update prospect list
        export_prospects(f"daily_prospects_{datetime.now().strftime('%Y%m%d')}.csv")
        
        total_daily_records = summary['businesses_collected'] + summary['sba_loans_collected'] + summary['licenses_collected']
        print(f"✅ Daily collection complete: {total_daily_records} records")
        return True
    else:
        print("❌ Daily collection failed")
        return False


def main():
    """Main function with command line interface"""
    parser = argparse.ArgumentParser(
        description='Location Optimizer - Wisconsin Business Data Collection',
        formatter_class=argparse.RawDescriptionHelpFormatter,
        epilog="""
Examples:
  python main.py --setup                    # Setup BigQuery infrastructure
  python main.py --collect                  # Collect Wisconsin data (90 days)
  python main.py --collect --days-back 30   # Collect last 30 days
  python main.py --analyze                  # Run opportunity analysis
  python main.py --export-prospects         # Export prospect list
  python main.py --daily                    # Run daily collection routine
        """
    )
    
    parser.add_argument('--setup', action='store_true',
                       help='Setup BigQuery infrastructure')
    parser.add_argument('--collect', action='store_true',
                       help='Collect Wisconsin business data')
    parser.add_argument('--analyze', action='store_true',
                       help='Run opportunity analysis')
    parser.add_argument('--export-prospects', action='store_true',
                       help='Export prospect list to CSV')
    parser.add_argument('--daily', action='store_true',
                       help='Run daily collection routine')
    parser.add_argument('--days-back', type=int, default=90,
                       help='Number of days to look back (default: 90)')
    parser.add_argument('--output-file', default='wisconsin_prospects.csv',
                       help='Output filename for prospects (default: wisconsin_prospects.csv)')
    
    args = parser.parse_args()
    
    # If no arguments provided, show help
    if not any([args.setup, args.collect, args.analyze, args.export_prospects, args.daily]):
        parser.print_help()
        print("\n🚀 Quick start: python main.py --setup --collect --analyze")
        sys.exit(1)
    
    print("🎯 Location Optimizer - Wisconsin Data Collection")
    print("=" * 60)
    print(f"📅 Started: {datetime.now().strftime('%Y-%m-%d %H:%M:%S')}")
    print("=" * 60)
    
    success = True
    
    try:
        if args.setup:
            success &= setup_infrastructure()
        
        if args.collect:
            summary = collect_wisconsin_data(args.days_back)
            success &= (summary is not None and summary['success'])
        
        if args.analyze:
            success &= run_analysis()
        
        if args.export_prospects:
            success &= export_prospects(args.output_file)
        
        if args.daily:
            success &= run_daily_collection()
        
        # Final status
        print("\n" + "=" * 60)
        if success:
            print("🎉 All operations completed successfully!")
            print("\n📋 Recommended next steps:")
            print("   • Review generated prospect lists")
            print("   • Set up automated daily collection")
            print("   • Begin client outreach campaigns")
        else:
            print("⚠️  Some operations failed. Check logs for details.")
            sys.exit(1)
            
    except KeyboardInterrupt:
        print("\n\n⏹️  Operation cancelled by user")
        sys.exit(1)
    except Exception as e:
        logger.error(f"Unexpected error in main: {e}")
        print(f"\n❌ Unexpected error: {e}")
        sys.exit(1)


if __name__ == "__main__":
    main()