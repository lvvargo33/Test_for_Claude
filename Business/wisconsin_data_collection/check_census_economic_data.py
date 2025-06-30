#!/usr/bin/env python3
"""
Check and summarize Census Economic data in BigQuery
"""

import os
import logging
from google.cloud import bigquery
from datetime import datetime
import pandas as pd

# Set up environment
if 'GOOGLE_APPLICATION_CREDENTIALS' not in os.environ:
    os.environ['GOOGLE_APPLICATION_CREDENTIALS'] = 'location-optimizer-1-96b6102d3548.json'


def main():
    """Check Census Economic data in BigQuery"""
    
    logging.basicConfig(level=logging.INFO)
    logger = logging.getLogger(__name__)
    
    try:
        client = bigquery.Client(project="location-optimizer-1")
        
        logger.info("=" * 80)
        logger.info("Census Economic Data Summary in BigQuery")
        logger.info("=" * 80)
        
        # Query 1: Overall summary
        query1 = """
        SELECT 
            COUNT(*) as total_records,
            COUNT(DISTINCT census_year) as years_covered,
            MIN(census_year) as earliest_year,
            MAX(census_year) as latest_year,
            COUNT(DISTINCT naics_code) as industries,
            COUNT(DISTINCT county_fips) as counties,
            ROUND(AVG(data_quality_score), 1) as avg_quality_score
        FROM `location-optimizer-1.raw_business_data.census_economic_benchmarks`
        WHERE state_fips = '55'
        """
        
        results = client.query(query1).result()
        for row in results:
            logger.info(f"\nOverall Summary:")
            logger.info(f"  Total Records: {row.total_records:,}")
            logger.info(f"  Years Covered: {row.years_covered} ({row.earliest_year}-{row.latest_year})")
            logger.info(f"  Industries: {row.industries}")
            logger.info(f"  Counties: {row.counties}")
            logger.info(f"  Avg Data Quality: {row.avg_quality_score}%")
        
        # Query 2: Top industries by revenue
        query2 = """
        SELECT 
            naics_code,
            naics_title,
            census_year,
            ROUND(SUM(revenue_total), 0) as total_revenue_thousands,
            SUM(establishments_total) as total_establishments,
            SUM(employees_total) as total_employees,
            ROUND(AVG(revenue_per_employee), 0) as avg_revenue_per_employee
        FROM `location-optimizer-1.raw_business_data.census_economic_benchmarks`
        WHERE state_fips = '55' 
            AND geo_level = 'state'
            AND revenue_total IS NOT NULL
        GROUP BY naics_code, naics_title, census_year
        ORDER BY total_revenue_thousands DESC
        LIMIT 10
        """
        
        logger.info(f"\nTop 10 Industries by Revenue (State Level):")
        results = client.query(query2).result()
        for i, row in enumerate(results, 1):
            logger.info(f"\n{i}. {row.naics_code} - {row.naics_title} ({row.census_year})")
            logger.info(f"   Revenue: ${row.total_revenue_thousands:,.0f}k")
            logger.info(f"   Establishments: {row.total_establishments:,.0f}")
            logger.info(f"   Employees: {row.total_employees:,.0f}")
            logger.info(f"   Revenue/Employee: ${row.avg_revenue_per_employee:,.0f}k")
        
        # Query 3: County comparison
        query3 = """
        SELECT 
            county_name,
            COUNT(DISTINCT naics_code) as industries_tracked,
            ROUND(SUM(revenue_total), 0) as total_revenue_thousands,
            SUM(establishments_total) as total_establishments,
            ROUND(AVG(revenue_per_establishment), 0) as avg_revenue_per_establishment
        FROM `location-optimizer-1.raw_business_data.census_economic_benchmarks`
        WHERE state_fips = '55' 
            AND geo_level = 'county'
            AND revenue_total IS NOT NULL
        GROUP BY county_name
        ORDER BY total_revenue_thousands DESC
        """
        
        logger.info(f"\nCounty Economic Summary:")
        results = client.query(query3).result()
        for row in results:
            logger.info(f"\n{row.county_name}:")
            logger.info(f"  Industries: {row.industries_tracked}")
            logger.info(f"  Total Revenue: ${row.total_revenue_thousands:,.0f}k")
            logger.info(f"  Establishments: {row.total_establishments:,.0f}")
            logger.info(f"  Avg Revenue/Establishment: ${row.avg_revenue_per_establishment:,.0f}k")
        
        # Query 4: Year-over-year trends
        query4 = """
        WITH yearly_data AS (
            SELECT 
                census_year,
                COUNT(*) as records,
                COUNT(DISTINCT naics_code) as industries,
                ROUND(SUM(revenue_total), 0) as total_revenue,
                SUM(establishments_total) as total_establishments
            FROM `location-optimizer-1.raw_business_data.census_economic_benchmarks`
            WHERE state_fips = '55' 
                AND geo_level = 'state'
            GROUP BY census_year
        )
        SELECT *
        FROM yearly_data
        ORDER BY census_year DESC
        """
        
        logger.info(f"\nData Coverage by Year:")
        results = client.query(query4).result()
        for row in results:
            logger.info(f"\n{row.census_year}:")
            logger.info(f"  Records: {row.records}")
            logger.info(f"  Industries: {row.industries}")
            if row.total_revenue:
                logger.info(f"  Total Revenue: ${row.total_revenue:,.0f}k")
            if row.total_establishments:
                logger.info(f"  Total Establishments: {row.total_establishments:,.0f}")
        
        # Query 5: Restaurant industry deep dive
        query5 = """
        SELECT 
            census_year,
            naics_code,
            naics_title,
            geo_level,
            COUNT(*) as records,
            ROUND(AVG(revenue_per_establishment), 0) as avg_revenue_per_establishment,
            ROUND(AVG(employees_avg_per_establishment), 1) as avg_employees_per_establishment,
            ROUND(AVG(payroll_as_pct_of_revenue), 1) as avg_payroll_pct
        FROM `location-optimizer-1.raw_business_data.census_economic_benchmarks`
        WHERE state_fips = '55' 
            AND naics_code IN ('72', '722', '7225')
        GROUP BY census_year, naics_code, naics_title, geo_level
        ORDER BY census_year DESC, naics_code, geo_level
        """
        
        logger.info(f"\nRestaurant Industry Analysis:")
        results = client.query(query5).result()
        current_year = None
        for row in results:
            if row.census_year != current_year:
                logger.info(f"\n--- Year {row.census_year} ---")
                current_year = row.census_year
            logger.info(f"\n{row.naics_code} - {row.naics_title} ({row.geo_level}):")
            logger.info(f"  Records: {row.records}")
            if row.avg_revenue_per_establishment:
                logger.info(f"  Avg Revenue/Establishment: ${row.avg_revenue_per_establishment:,.0f}k")
            if row.avg_employees_per_establishment:
                logger.info(f"  Avg Employees/Establishment: {row.avg_employees_per_establishment:.1f}")
            if row.avg_payroll_pct:
                logger.info(f"  Payroll as % of Revenue: {row.avg_payroll_pct:.1f}%")
        
        logger.info("\n" + "=" * 80)
        logger.info("Analysis Complete")
        
    except Exception as e:
        logger.error(f"Error: {e}")
        return 1
    
    return 0


if __name__ == "__main__":
    main()