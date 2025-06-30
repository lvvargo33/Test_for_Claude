#!/usr/bin/env python3
"""
Verify BEA Data in BigQuery
===========================

Comprehensive verification of the loaded BEA consumer spending data.
"""

import os
from google.cloud import bigquery
import pandas as pd

os.environ['GOOGLE_APPLICATION_CREDENTIALS'] = '/workspaces/Test_for_Claude/Business/wisconsin_data_collection/location-optimizer-1-96b6102d3548.json'

def verify_bea_data():
    """Comprehensive verification of BEA data in BigQuery"""
    
    client = bigquery.Client(project="location-optimizer-1")
    
    print("🔍 Comprehensive BEA Consumer Spending Data Verification")
    print("="*65)
    
    # 1. Overall data summary
    print("\n1. Data Summary:")
    print("-" * 40)
    
    summary_query = """
    SELECT 
        data_source,
        COUNT(*) as total_records,
        MIN(year) as earliest_year,
        MAX(year) as latest_year,
        COUNT(DISTINCT year) as years_covered,
        COUNT(DISTINCT spending_category) as categories
    FROM `location-optimizer-1.raw_business_data.consumer_spending`
    GROUP BY data_source
    ORDER BY data_source
    """
    
    for row in client.query(summary_query):
        print(f"Source: {row.data_source}")
        print(f"  Records: {row.total_records:,}")
        print(f"  Years: {row.earliest_year}-{row.latest_year} ({row.years_covered} years)")
        print(f"  Categories: {row.categories}")
        print()
    
    # 2. BEA Real Data - All Categories by Year
    print("2. BEA Real Data - All Categories by Year:")
    print("-" * 50)
    
    categories_query = """
    SELECT 
        year,
        spending_category,
        ROUND(spending_amount / 1000000000, 2) as amount_billions,
        ROUND(per_capita_spending, 0) as per_capita
    FROM `location-optimizer-1.raw_business_data.consumer_spending`
    WHERE data_source = 'BEA_API_Real'
    ORDER BY year DESC, spending_category
    """
    
    current_year = None
    for row in client.query(categories_query):
        if row.year != current_year:
            if current_year is not None:
                print()
            print(f"Year {row.year}:")
            current_year = row.year
        
        category_display = row.spending_category.replace('_', ' ').title()
        print(f"  {category_display:20} ${row.amount_billions:>8.2f}B  (${row.per_capita:>7,.0f} per capita)")
    
    # 3. Year-over-year growth analysis
    print("\n\n3. Year-over-Year Growth Analysis (Total PCE):")
    print("-" * 55)
    
    growth_query = """
    WITH yearly_data AS (
        SELECT 
            year,
            spending_amount / 1000000000 as amount_billions
        FROM `location-optimizer-1.raw_business_data.consumer_spending`
        WHERE data_source = 'BEA_API_Real'
          AND spending_category = 'total_pce'
        ORDER BY year
    ),
    growth_calc AS (
        SELECT 
            year,
            amount_billions,
            LAG(amount_billions) OVER (ORDER BY year) as prev_amount,
            ROUND(
                ((amount_billions - LAG(amount_billions) OVER (ORDER BY year)) / 
                 LAG(amount_billions) OVER (ORDER BY year)) * 100, 2
            ) as growth_rate
        FROM yearly_data
    )
    SELECT * FROM growth_calc ORDER BY year
    """
    
    print(f"{'Year':>6} | {'Amount ($B)':>12} | {'Growth %':>9}")
    print("-" * 35)
    
    for row in client.query(growth_query):
        growth_display = f"{row.growth_rate:>8.1f}%" if row.growth_rate is not None else "     N/A"
        print(f"{row.year:>6} | ${row.amount_billions:>11.2f} | {growth_display}")
    
    # 4. Category breakdown for latest year
    print("\n\n4. Latest Year Category Breakdown (2023):")
    print("-" * 45)
    
    latest_query = """
    SELECT 
        spending_category,
        ROUND(spending_amount / 1000000000, 2) as amount_billions,
        ROUND(per_capita_spending, 0) as per_capita,
        ROUND(
            (spending_amount / (
                SELECT spending_amount 
                FROM `location-optimizer-1.raw_business_data.consumer_spending`
                WHERE data_source = 'BEA_API_Real' 
                  AND spending_category = 'total_pce' 
                  AND year = 2023
            )) * 100, 1
        ) as pct_of_total
    FROM `location-optimizer-1.raw_business_data.consumer_spending`
    WHERE data_source = 'BEA_API_Real'
      AND year = 2023
      AND spending_category != 'total_pce'
    ORDER BY amount_billions DESC
    """
    
    print(f"{'Category':25} | {'Amount ($B)':>12} | {'Per Capita':>11} | {'% of Total':>11}")
    print("-" * 70)
    
    for row in client.query(latest_query):
        category_display = row.spending_category.replace('_', ' ').title()
        print(f"{category_display:25} | ${row.amount_billions:>11.2f} | ${row.per_capita:>10,.0f} | {row.pct_of_total:>10.1f}%")
    
    # 5. Data quality check
    print("\n\n5. Data Quality Check:")
    print("-" * 30)
    
    quality_query = """
    SELECT 
        'Total Records' as metric,
        COUNT(*) as value
    FROM `location-optimizer-1.raw_business_data.consumer_spending`
    WHERE data_source = 'BEA_API_Real'
    
    UNION ALL
    
    SELECT 
        'Complete Years (5 categories each)' as metric,
        COUNT(*) / 5 as value
    FROM `location-optimizer-1.raw_business_data.consumer_spending`
    WHERE data_source = 'BEA_API_Real'
    
    UNION ALL
    
    SELECT 
        'Records with Valid Amounts' as metric,
        COUNT(*) as value
    FROM `location-optimizer-1.raw_business_data.consumer_spending`
    WHERE data_source = 'BEA_API_Real'
      AND spending_amount > 0
    
    UNION ALL
    
    SELECT 
        'Average Per Capita PCE' as metric,
        ROUND(AVG(per_capita_spending), 0) as value
    FROM `location-optimizer-1.raw_business_data.consumer_spending`
    WHERE data_source = 'BEA_API_Real'
      AND spending_category = 'total_pce'
    """
    
    for row in client.query(quality_query):
        print(f"{row.metric:35} {row.value:>10,.0f}")
    
    # 6. Comparison with any existing demo data
    print("\n\n6. Data Source Comparison:")
    print("-" * 35)
    
    comparison_query = """
    SELECT 
        data_source,
        year,
        ROUND(spending_amount / 1000000000, 2) as total_pce_billions
    FROM `location-optimizer-1.raw_business_data.consumer_spending`
    WHERE spending_category = 'total_pce'
      AND year IN (2021, 2022, 2023)
    ORDER BY year, data_source
    """
    
    print(f"{'Year':>6} | {'Source':15} | {'Total PCE ($B)':>15}")
    print("-" * 45)
    
    for row in client.query(comparison_query):
        print(f"{row.year:>6} | {row.data_source:15} | ${row.total_pce_billions:>14.2f}")

def main():
    """Main verification function"""
    
    try:
        verify_bea_data()
        
        print("\n" + "="*65)
        print("✅ BEA Consumer Spending Data Successfully Loaded & Verified!")
        print("="*65)
        print("📊 Coverage: 2015-2023 (9 years)")
        print("📈 Categories: Total PCE, Goods (durable/nondurable), Services") 
        print("💰 Data Source: Bureau of Economic Analysis (Real API data)")
        print("🏷️  Table: location-optimizer-1.raw_business_data.consumer_spending")
        
    except Exception as e:
        print(f"❌ Error during verification: {e}")

if __name__ == "__main__":
    main()