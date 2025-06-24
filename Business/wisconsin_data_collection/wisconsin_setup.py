#!/usr/bin/env python3
"""
Wisconsin Data Collection Setup Script - FIXED VERSION
=====================================

Quick setup script to initialize Wisconsin business data collection
for the Location Optimizer franchise business.

Usage:
    python wisconsin_setup.py --setup-tables
    python wisconsin_setup.py --collect-data
    python wisconsin_setup.py --run-analysis

Requirements:
    pip install google-cloud-bigquery pandas requests beautifulsoup4
"""

import argparse
import sys
from google.cloud import bigquery
from wisconsin_data_ingestion import WisconsinDataCollector
import logging

# Configure logging
logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)

def setup_bigquery_tables(project_id: str = "location-optimizer-1"):
    """Create the required BigQuery tables for Wisconsin data"""
    
    logger.info("Setting up BigQuery tables for Wisconsin data collection")
    client = bigquery.Client(project=project_id)
    
    # Fixed setup queries with proper partition syntax
    setup_queries = [
        # State registrations table - simplified without partitioning for now
        f"""
        CREATE TABLE IF NOT EXISTS `{project_id}.raw_business_licenses.state_registrations` (
          business_id STRING NOT NULL,
          business_name STRING,
          owner_name STRING,
          business_type STRING,
          naics_code STRING,
          registration_date DATE,
          status STRING,
          address_full STRING,
          city STRING,
          state STRING,
          zip_code STRING,
          county STRING,
          phone STRING,
          email STRING,
          business_description STRING,
          entity_type STRING,
          source_state STRING,
          source_url STRING,
          data_extraction_date TIMESTAMP,
          created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP(),
          updated_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP()
        )
        CLUSTER BY state, business_type, naics_code
        """,
        
        # SBA loans table - simplified without partitioning for now
        f"""
        CREATE TABLE IF NOT EXISTS `{project_id}.raw_sba_data.loan_approvals` (
          loan_id STRING NOT NULL,
          borrower_name STRING,
          borrower_address STRING,
          borrower_city STRING,
          borrower_state STRING,
          borrower_zip STRING,
          naics_code STRING,
          business_type STRING,
          loan_amount NUMERIC,
          approval_date DATE,
          approval_fiscal_year INT64,
          jobs_supported INT64,
          franchise_code STRING,
          franchise_name STRING,
          lender_name STRING,
          program_type STRING,
          loan_status STRING,
          data_source STRING,
          created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP(),
          updated_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP()
        )
        CLUSTER BY borrower_state, naics_code, franchise_name
        """
    ]
    
    for i, query in enumerate(setup_queries):
        try:
            job = client.query(query)
            job.result()  # Wait for completion
            logger.info(f"Table {i+1} created successfully")
        except Exception as e:
            logger.error(f"Error creating table {i+1}: {str(e)}")
            
    logger.info("BigQuery table setup complete!")

def run_data_collection(days_back: int = 90):
    """Run the Wisconsin data collection process"""
    
    logger.info(f"Starting Wisconsin data collection for last {days_back} days")
    
    # Initialize collector
    collector = WisconsinDataCollector()
    
    # Run full collection
    results = collector.run_full_wisconsin_collection(days_back=days_back)
    
    # Print results
    print("\n" + "="*50)
    print("WISCONSIN DATA COLLECTION RESULTS")
    print("="*50)
    print(f"Business Registrations: {results['businesses']}")
    print(f"SBA Loan Approvals: {results['sba_loans']}")
    print(f"Business Licenses: {results['licenses']}")
    print(f"Total Records: {results['total_records']}")
    print(f"Success: {'YES' if results['success'] else 'NO'}")
    print("="*50)
    
    if results['success']:
        print("\nData collection successful!")
        print("\nNext steps:")
        print("1. Run analysis queries to identify opportunities")
        print("2. Export prospect lists for outreach")
        print("3. Set up automated daily collection")
    else:
        print("\nData collection had issues. Check logs for details.")
    
    return results

def run_opportunity_analysis(project_id: str = "location-optimizer-1"):
    """Run analysis queries and show key insights"""
    
    logger.info("Running Wisconsin opportunity analysis")
    client = bigquery.Client(project=project_id)
    
    # Key insight queries
    insight_queries = {
        "Hot SBA Prospects": f"""
        SELECT 
          borrower_name,
          borrower_city,
          loan_amount,
          approval_date,
          franchise_name
        FROM `{project_id}.raw_sba_data.loan_approvals`
        WHERE borrower_state = 'WI'
          AND DATE(approval_date) >= DATE_SUB(CURRENT_DATE(), INTERVAL 30 DAY)
          AND loan_amount >= 100000
        ORDER BY approval_date DESC, loan_amount DESC
        LIMIT 10
        """,
        
        "New Business Registrations": f"""
        SELECT 
          business_name,
          business_type,
          city,
          registration_date
        FROM `{project_id}.raw_business_licenses.state_registrations`
        WHERE state = 'WI'
          AND DATE(registration_date) >= DATE_SUB(CURRENT_DATE(), INTERVAL 14 DAY)
          AND (business_type LIKE '%RESTAURANT%' OR business_type LIKE '%RETAIL%' OR business_type LIKE '%FRANCHISE%')
        ORDER BY registration_date DESC
        LIMIT 10
        """,
        
        "Market Summary": f"""
        SELECT 
          city,
          COUNT(*) as total_new_businesses,
          COUNT(CASE WHEN business_type LIKE '%RESTAURANT%' THEN 1 END) as restaurants,
          COUNT(CASE WHEN business_type LIKE '%RETAIL%' THEN 1 END) as retail
        FROM `{project_id}.raw_business_licenses.state_registrations`
        WHERE state = 'WI'
          AND DATE(registration_date) >= DATE_SUB(CURRENT_DATE(), INTERVAL 90 DAY)
        GROUP BY city
        HAVING COUNT(*) >= 3
        ORDER BY COUNT(*) DESC
        LIMIT 10
        """
    }
    
    print("\n" + "="*60)
    print("WISCONSIN OPPORTUNITY ANALYSIS")
    print("="*60)
    
    for title, query in insight_queries.items():
        try:
            print(f"\n{title.upper()}")
            print("-" * 40)
            
            results = client.query(query).result()
            
            for i, row in enumerate(results):
                if i >= 5:  # Limit display to 5 rows
                    break
                print("  ", dict(row))
                
        except Exception as e:
            print(f"Error running {title}: {str(e)}")
    
    print("\n" + "="*60)

def export_prospect_list(project_id: str = "location-optimizer-1", filename: str = "wisconsin_prospects.csv"):
    """Export a CSV file of top prospects for outreach"""
    
    logger.info(f"Exporting prospect list to {filename}")
    client = bigquery.Client(project=project_id)
    
    export_query = f"""
    WITH hot_prospects AS (
      -- SBA prospects
      SELECT 
        'SBA_LOAN' as source,
        borrower_name as business_name,
        borrower_city as city,
        borrower_zip as zip_code,
        'Phone: Congratulations on SBA approval!' as contact_approach,
        loan_amount as value_indicator,
        approval_date as key_date,
        1 as priority
      FROM `{project_id}.raw_sba_data.loan_approvals`
      WHERE borrower_state = 'WI'
        AND DATE(approval_date) >= DATE_SUB(CURRENT_DATE(), INTERVAL 60 DAY)
        AND loan_amount >= 75000
      
      UNION ALL
      
      -- New business registrations
      SELECT 
        'NEW_BUSINESS' as source,
        business_name,
        city,
        zip_code,
        'Email: Welcome to Wisconsin! Free location market insights' as contact_approach,
        NULL as value_indicator,
        registration_date as key_date,
        2 as priority
      FROM `{project_id}.raw_business_licenses.state_registrations`
      WHERE state = 'WI'
        AND DATE(registration_date) >= DATE_SUB(CURRENT_DATE(), INTERVAL 30 DAY)
        AND (business_type LIKE '%RESTAURANT%' OR business_type LIKE '%RETAIL%' OR business_type LIKE '%FRANCHISE%')
    )
    SELECT 
      source,
      business_name,
      city,
      zip_code,
      contact_approach,
      value_indicator,
      key_date,
      DATE_DIFF(CURRENT_DATE(), DATE(key_date), DAY) as days_since_trigger,
      CASE 
        WHEN priority = 1 AND value_indicator >= 200000 THEN 'HIGH'
        WHEN priority = 1 AND value_indicator >= 100000 THEN 'MEDIUM'
        WHEN priority = 2 THEN 'QUALIFIED'
        ELSE 'STANDARD'
      END as lead_quality
    FROM hot_prospects
    ORDER BY priority, value_indicator DESC, key_date DESC
    LIMIT 100
    """
    
    try:
        # Execute query and save to CSV
        import pandas as pd
        
        df = client.query(export_query).to_dataframe()
        df.to_csv(filename, index=False)
        
        print(f"Exported {len(df)} prospects to {filename}")
        print(f"\nTop 5 prospects:")
        print(df.head().to_string())
        
    except Exception as e:
        logger.error(f"Error exporting prospects: {str(e)}")

def main():
    """Main function to handle command line arguments"""
    
    parser = argparse.ArgumentParser(description='Wisconsin Business Data Collection Setup')
    parser.add_argument('--setup-tables', action='store_true', 
                       help='Create BigQuery tables')
    parser.add_argument('--collect-data', action='store_true',
                       help='Run data collection process')
    parser.add_argument('--run-analysis', action='store_true',
                       help='Run opportunity analysis')
    parser.add_argument('--export-prospects', action='store_true',
                       help='Export prospect list to CSV')
    parser.add_argument('--days-back', type=int, default=90,
                       help='Number of days to look back for data collection')
    parser.add_argument('--project-id', default='location-optimizer-1',
                       help='BigQuery project ID')
    
    args = parser.parse_args()
    
    # If no arguments provided, show help
    if not any([args.setup_tables, args.collect_data, args.run_analysis, args.export_prospects]):
        parser.print_help()
        sys.exit(1)
    
    print("Wisconsin Business Data Collection System")
    print("=" * 50)
    
    try:
        if args.setup_tables:
            setup_bigquery_tables(args.project_id)
        
        if args.collect_data:
            run_data_collection(args.days_back)
        
        if args.run_analysis:
            run_opportunity_analysis(args.project_id)
        
        if args.export_prospects:
            export_prospect_list(args.project_id)
            
    except Exception as e:
        logger.error(f"Script execution failed: {str(e)}")
        sys.exit(1)

if __name__ == "__main__":
    main()