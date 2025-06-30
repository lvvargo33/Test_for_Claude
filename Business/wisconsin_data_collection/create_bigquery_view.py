#!/usr/bin/env python3
"""
Create integrated BigQuery view
"""

import os
from google.cloud import bigquery

# Set up environment
if 'GOOGLE_APPLICATION_CREDENTIALS' not in os.environ:
    os.environ['GOOGLE_APPLICATION_CREDENTIALS'] = 'location-optimizer-1-96b6102d3548.json'

def main():
    client = bigquery.Client(project='location-optimizer-1')
    
    # Create business_intelligence dataset if it doesn't exist
    dataset_id = "location-optimizer-1.business_intelligence"
    try:
        client.get_dataset(dataset_id)
        print(f"Dataset {dataset_id} already exists")
    except:
        dataset = bigquery.Dataset(dataset_id)
        dataset.location = "US"
        dataset.description = "Business intelligence views and processed data"
        client.create_dataset(dataset)
        print(f"Created dataset {dataset_id}")
    
    # Create the integrated view
    view_sql = """
    CREATE OR REPLACE VIEW `location-optimizer-1.business_intelligence.industry_benchmarks_integrated` AS
    WITH census_data AS (
        SELECT 
            naics_code,
            naics_title as industry_name,
            geo_level,
            state_fips,
            county_fips,
            county_name,
            establishments_total,
            employees_total,
            revenue_total,
            payroll_annual_total,
            revenue_per_establishment,
            revenue_per_employee,
            payroll_as_pct_of_revenue,
            census_year,
            data_collection_date
        FROM `location-optimizer-1.raw_business_data.census_economic_benchmarks`
        WHERE state_fips = '55'  -- Wisconsin
    ),
    benchmark_data AS (
        SELECT 
            naics_code,
            industry_name,
            benchmark_type,
            metric_name,
            benchmark_value,
            benchmark_unit,
            percentile_25,
            percentile_50,
            percentile_75,
            profit_margin_pct,
            labor_cost_pct,
            rent_cost_pct,
            initial_investment_low,
            initial_investment_high,
            data_year,
            data_source
        FROM `location-optimizer-1.raw_business_data.industry_benchmarks`
    )
    SELECT 
        COALESCE(c.naics_code, b.naics_code) as naics_code,
        COALESCE(c.industry_name, b.industry_name) as industry_name,
        c.geo_level,
        c.county_fips,
        c.county_name,
        -- Census metrics
        c.establishments_total,
        c.employees_total,
        c.revenue_total as revenue_total_thousands,
        c.revenue_per_establishment,
        c.revenue_per_employee,
        c.payroll_as_pct_of_revenue,
        -- Benchmark metrics
        b.benchmark_value as national_benchmark_value,
        b.metric_name as benchmark_metric,
        b.percentile_50 as national_median,
        b.profit_margin_pct as typical_profit_margin,
        b.labor_cost_pct as typical_labor_cost_pct,
        b.initial_investment_low,
        b.initial_investment_high,
        -- Comparative metrics
        CASE 
            WHEN c.revenue_per_establishment > b.percentile_50 THEN 'Above National Median'
            WHEN c.revenue_per_establishment < b.percentile_50 THEN 'Below National Median'
            ELSE 'At National Median'
        END as wisconsin_vs_national,
        -- Metadata
        c.census_year,
        b.data_year as benchmark_year,
        c.data_collection_date as last_updated
    FROM census_data c
    FULL OUTER JOIN benchmark_data b
        ON c.naics_code = b.naics_code
        AND b.metric_name = 'Average Annual Revenue'
    ORDER BY naics_code, geo_level, county_fips
    """
    
    # Execute the view creation
    client.query(view_sql).result()
    print('✅ Integrated benchmarks view created successfully!')
    
    # Test the view
    test_query = """
    SELECT 
        naics_code,
        industry_name,
        geo_level,
        COUNT(*) as records,
        ROUND(AVG(revenue_per_establishment), 0) as avg_revenue_per_establishment
    FROM `location-optimizer-1.business_intelligence.industry_benchmarks_integrated`
    WHERE naics_code IN ('722', '72', '44-45', '54')
    GROUP BY naics_code, industry_name, geo_level
    ORDER BY naics_code, geo_level
    """
    
    print('\nSample data from integrated view:')
    results = client.query(test_query).result()
    for row in results:
        revenue = row.avg_revenue_per_establishment or 0
        print(f'{row.naics_code} ({row.geo_level}): {row.records} records, Avg Revenue/Est: ${revenue:,.0f}k')

if __name__ == "__main__":
    main()