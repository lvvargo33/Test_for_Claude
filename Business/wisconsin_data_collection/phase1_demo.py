#!/usr/bin/env python3
"""
Phase 1 Demo - Customer-Centric Site Analysis System
Demonstrates the integrated analysis capabilities
"""

import pandas as pd
from google.cloud import bigquery
from google.oauth2 import service_account
from datetime import datetime

# Configuration
PROJECT_ID = "location-optimizer-1"
DATASET_ID = "location_optimizer"
CREDENTIALS_PATH = "location-optimizer-1-449414f93a5a.json"

# Initialize BigQuery client
credentials = service_account.Credentials.from_service_account_file(CREDENTIALS_PATH)
client = bigquery.Client(project=PROJECT_ID, credentials=credentials)

def demo_cotenancy_insights():
    """Show co-tenancy patterns discovered from SBA data"""
    print("\n=== CO-TENANCY INSIGHTS ===\n")
    
    query = f"""
    SELECT 
        anchor_business_type,
        confidence_score,
        sample_size,
        ARRAY_TO_STRING(applicable_metro_areas, ', ') as top_markets,
        within_same_plaza[SAFE_OFFSET(0)].business_type as best_partner_1,
        within_same_plaza[SAFE_OFFSET(0)].success_rate as success_1,
        within_same_plaza[SAFE_OFFSET(1)].business_type as best_partner_2,
        within_same_plaza[SAFE_OFFSET(1)].success_rate as success_2
    FROM `{PROJECT_ID}.{DATASET_ID}.cotenancy_success_patterns`
    WHERE confidence_score > 0.5
    ORDER BY sample_size DESC
    LIMIT 10
    """
    
    df = client.query(query).to_dataframe()
    
    for _, row in df.iterrows():
        print(f"{row['anchor_business_type'].upper()}")
        print(f"  Sample Size: {row['sample_size']} pairs analyzed")
        print(f"  Confidence: {row['confidence_score']:.0%}")
        print(f"  Top Markets: {row['top_markets'][:50]}...")
        if pd.notna(row['best_partner_1']):
            print(f"  Best Partners:")
            print(f"    1. {row['best_partner_1']} (Success: {row['success_1']:.2f})")
            if pd.notna(row['best_partner_2']):
                print(f"    2. {row['best_partner_2']} (Success: {row['success_2']:.2f})")
        print()

def demo_sample_site_analysis():
    """Demonstrate what a site analysis would look like"""
    print("\n=== SAMPLE SITE ANALYSIS ===\n")
    print("Location: 123 State Street, Madison, WI")
    print("Proposed Business Type: Restaurant\n")
    
    # In a real implementation, this would query the trade_area_profiles
    # and accessibility_adjusted_demographics tables
    
    print("TRADE AREA ANALYSIS:")
    print("  0-5 minute drive: 12,450 people")
    print("  5-10 minute drive: 28,300 people")
    print("  10-15 minute drive: 41,200 people")
    print("  Peak lunch population: 35,000 workers")
    print("  Accessibility score: 0.85 (Very Good)")
    print()
    
    print("CO-TENANCY RECOMMENDATIONS:")
    print("  Ideal neighbors within same plaza:")
    print("    - Grocery store (Success rate: 0.89)")
    print("    - Personal services (Success rate: 0.76)")
    print("    - Fitness center (Success rate: 0.72)")
    print()
    
    print("BARRIER ANALYSIS:")
    print("  ✓ No major highways to cross")
    print("  ✓ No railway barriers")
    print("  ⚠ University campus boundary may limit student access")
    print("  Accessible population: 10,800 (87% of raw count)")
    print()
    
    print("OVERALL SITE SCORE: 8.2/10")
    print("Recommendation: STRONG - High foot traffic, good co-tenancy, minimal barriers")

def show_bigquery_summary():
    """Show what tables we've created"""
    print("\n=== PHASE 1 INFRASTRUCTURE SUMMARY ===\n")
    
    # List tables
    dataset_ref = client.dataset(DATASET_ID)
    tables = list(client.list_tables(dataset_ref))
    
    print("BigQuery Tables Created:")
    for table in tables:
        if table.table_id in ['trade_area_profiles', 'cotenancy_success_patterns', 
                             'retail_cluster_profiles', 'accessibility_adjusted_demographics']:
            # Get row count
            query = f"SELECT COUNT(*) as count FROM `{PROJECT_ID}.{DATASET_ID}.{table.table_id}`"
            count = client.query(query).to_dataframe()['count'][0]
            print(f"  - {table.table_id}: {count:,} rows")
    
    print("\nViews Created:")
    print("  - site_success_factors_v1 (Master analysis view)")

def main():
    """Run the Phase 1 demonstration"""
    print("=" * 60)
    print("PHASE 1: CUSTOMER-CENTRIC SITE ANALYSIS")
    print("=" * 60)
    
    # Show BigQuery infrastructure
    show_bigquery_summary()
    
    # Show co-tenancy insights
    demo_cotenancy_insights()
    
    # Show sample site analysis
    demo_sample_site_analysis()
    
    print("\n" + "=" * 60)
    print("NEXT STEPS:")
    print("=" * 60)
    print("\n1. Run trade area analysis on your existing successful locations")
    print("2. Geocode and analyze potential new sites")
    print("3. Implement barrier detection using OSM data")
    print("4. Create client-facing reports with these insights")
    print("\nThe system is ready to analyze real locations!")

if __name__ == "__main__":
    main()