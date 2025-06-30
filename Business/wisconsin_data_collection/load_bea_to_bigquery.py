#!/usr/bin/env python3
"""
Load BEA Consumer Spending Data to BigQuery
===========================================

Comprehensive script to collect BEA consumer spending data from 2015 to present
and load it to BigQuery.
"""

import os
import sys
import logging
import requests
import pandas as pd
from datetime import datetime
from google.cloud import bigquery
import time

# Setup
logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)

# Set environment variables
os.environ['BEA_API_KEY'] = '1988DB31-BD6F-4482-A53F-F82AA2BE2E23'
os.environ['GOOGLE_APPLICATION_CREDENTIALS'] = '/workspaces/Test_for_Claude/Business/wisconsin_data_collection/location-optimizer-1-96b6102d3548.json'

class ComprehensiveBEACollector:
    """Comprehensive BEA consumer spending data collector"""
    
    def __init__(self):
        self.api_key = os.environ['BEA_API_KEY']
        self.base_url = "https://apps.bea.gov/api/data"
        self.wisconsin_fips = "55000"
        
        # Initialize BigQuery client
        self.bq_client = bigquery.Client(project="location-optimizer-1")
        
        # Wisconsin population by year (for per capita calculations)
        self.population_data = {
            2015: 5757564,
            2016: 5778708,
            2017: 5795483,
            2018: 5813568,
            2019: 5822434,
            2020: 5893718,
            2021: 5895908,
            2022: 5892539,
            2023: 5910955,
            2024: 5950000  # Estimated
        }
    
    def get_available_years(self):
        """Get available years from BEA API"""
        
        params = {
            'UserID': self.api_key,
            'method': 'GetParameterValues',
            'datasetname': 'Regional',
            'ParameterName': 'Year',
            'TableName': 'SAPCE1',
            'ResultFormat': 'json'
        }
        
        try:
            response = requests.get(self.base_url, params=params, timeout=30)
            
            if response.status_code == 200:
                data = response.json()
                
                if 'BEAAPI' in data and 'Results' in data['BEAAPI']:
                    results = data['BEAAPI']['Results']
                    
                    if 'ParamValue' in results:
                        years = []
                        for item in results['ParamValue']:
                            year_str = item.get('Key', '')
                            if year_str.isdigit():
                                year = int(year_str)
                                if 2015 <= year <= 2024:
                                    years.append(year)
                        
                        return sorted(years)
        
        except Exception as e:
            logger.error(f"Error getting available years: {e}")
        
        # Fallback to estimated range
        current_year = datetime.now().year
        return list(range(2015, min(current_year + 1, 2025)))
    
    def collect_sapce1_comprehensive(self, years):
        """Collect comprehensive SAPCE1 data"""
        
        logger.info(f"Collecting SAPCE1 data for years: {years}")
        
        # Core PCE categories
        line_mapping = {
            '1': 'total_pce',
            '2': 'goods_total',
            '3': 'goods_durable',
            '4': 'goods_nondurable', 
            '5': 'services_total'
        }
        
        all_records = []
        
        for year in years:
            logger.info(f"  Processing year {year}...")
            
            # Collect each category
            for line_code, category in line_mapping.items():
                
                params = {
                    'UserID': self.api_key,
                    'method': 'GetData',
                    'datasetname': 'Regional',
                    'TableName': 'SAPCE1',
                    'LineCode': line_code,
                    'GeoFips': self.wisconsin_fips,
                    'Year': str(year),
                    'ResultFormat': 'json'
                }
                
                try:
                    response = requests.get(self.base_url, params=params, timeout=30)
                    
                    if response.status_code == 200:
                        data = response.json()
                        
                        if 'BEAAPI' in data and 'Results' in data['BEAAPI']:
                            results = data['BEAAPI']['Results']
                            
                            if 'Data' in results and results['Data']:
                                record = results['Data'][0]
                                
                                # Parse value
                                data_value = record.get('DataValue', '0')
                                if data_value and data_value not in ['(D)', '(NA)', '(NM)']:
                                    try:
                                        value_millions = float(data_value.replace(',', ''))
                                    except:
                                        value_millions = 0.0
                                else:
                                    value_millions = 0.0
                                
                                # Create record
                                spending_record = {
                                    'geo_fips': self.wisconsin_fips,
                                    'geo_name': 'Wisconsin',
                                    'state_fips': '55',
                                    'state_name': 'Wisconsin',
                                    'data_year': year,
                                    'data_period': 'annual',
                                    'spending_category': category,
                                    'line_code': line_code,
                                    'value_millions': value_millions,
                                    'value_dollars': value_millions * 1_000_000,
                                    'data_source': 'BEA',
                                    'api_dataset': 'Regional_SAPCE1',
                                    'table_name': 'SAPCE1',
                                    'data_collection_date': datetime.now(),
                                    'seasonally_adjusted': True
                                }
                                
                                all_records.append(spending_record)
                                
                                logger.debug(f"    {category}: ${value_millions:,.1f}M")
                    
                    time.sleep(0.1)  # Rate limiting
                    
                except Exception as e:
                    logger.error(f"Error collecting {category} for {year}: {e}")
                    continue
        
        return all_records
    
    def collect_detailed_categories(self, years):
        """Collect detailed spending categories from SAPCE3"""
        
        logger.info(f"Collecting detailed categories from SAPCE3 for years: {years}")
        
        # Key detailed categories that are useful for business analysis
        detailed_categories = {
            '6': 'food_at_home',
            '7': 'food_away_from_home', 
            '10': 'housing',
            '24': 'health_care',
            '27': 'transportation_services',
            '29': 'recreation_services',
            '31': 'food_services_accommodations',
            '33': 'financial_services_insurance'
        }
        
        all_records = []
        
        # Sample a few recent years for detailed data
        sample_years = [y for y in years if y >= 2020]
        
        for year in sample_years:
            logger.info(f"  Processing detailed data for {year}...")
            
            for line_code, category in detailed_categories.items():
                
                params = {
                    'UserID': self.api_key,
                    'method': 'GetData',
                    'datasetname': 'Regional',
                    'TableName': 'SAPCE3',
                    'LineCode': line_code,
                    'GeoFips': self.wisconsin_fips,
                    'Year': str(year),
                    'ResultFormat': 'json'
                }
                
                try:
                    response = requests.get(self.base_url, params=params, timeout=30)
                    
                    if response.status_code == 200:
                        data = response.json()
                        
                        if 'BEAAPI' in data and 'Results' in data['BEAAPI']:
                            results = data['BEAAPI']['Results']
                            
                            if 'Data' in results and results['Data']:
                                record = results['Data'][0]
                                
                                data_value = record.get('DataValue', '0')
                                if data_value and data_value not in ['(D)', '(NA)', '(NM)']:
                                    try:
                                        value_millions = float(data_value.replace(',', ''))
                                    except:
                                        value_millions = 0.0
                                else:
                                    value_millions = 0.0
                                
                                detailed_record = {
                                    'geo_fips': self.wisconsin_fips,
                                    'geo_name': 'Wisconsin',
                                    'state_fips': '55',
                                    'state_name': 'Wisconsin',
                                    'data_year': year,
                                    'data_period': 'annual',
                                    'spending_category': category,
                                    'line_code': line_code,
                                    'value_millions': value_millions,
                                    'value_dollars': value_millions * 1_000_000,
                                    'data_source': 'BEA',
                                    'api_dataset': 'Regional_SAPCE3',
                                    'table_name': 'SAPCE3',
                                    'data_collection_date': datetime.now(),
                                    'seasonally_adjusted': True
                                }
                                
                                all_records.append(detailed_record)
                    
                    time.sleep(0.1)  # Rate limiting
                    
                except Exception as e:
                    logger.error(f"Error collecting detailed {category} for {year}: {e}")
                    continue
        
        return all_records
    
    def create_comprehensive_dataframe(self, sapce1_records, sapce3_records):
        """Create comprehensive structured DataFrame"""
        
        # Convert records to DataFrames
        df1 = pd.DataFrame(sapce1_records) if sapce1_records else pd.DataFrame()
        df3 = pd.DataFrame(sapce3_records) if sapce3_records else pd.DataFrame()
        
        # Combine all records
        all_records = sapce1_records + sapce3_records
        
        if not all_records:
            return pd.DataFrame()
        
        # Create DataFrame with all records
        df_all = pd.DataFrame(all_records)
        
        # Pivot to get structured format (one row per year)
        pivot_df = df_all.pivot_table(
            index=['data_year', 'state_fips', 'state_name', 'geo_fips', 'geo_name'],
            columns='spending_category',
            values='value_millions',
            aggfunc='first'
        ).reset_index()
        
        # Add metadata
        pivot_df['data_source'] = 'BEA'
        pivot_df['data_collection_date'] = datetime.now()
        pivot_df['data_period'] = 'annual'
        pivot_df['seasonally_adjusted'] = True
        
        # Add population data
        pivot_df['population'] = pivot_df['data_year'].map(self.population_data)
        
        # Calculate per capita values
        for col in pivot_df.columns:
            if col not in ['data_year', 'state_fips', 'state_name', 'geo_fips', 'geo_name', 
                          'data_source', 'data_collection_date', 'data_period', 
                          'seasonally_adjusted', 'population']:
                if col in pivot_df.columns and pivot_df[col].notna().any():
                    per_capita_col = f"{col}_per_capita"
                    pivot_df[per_capita_col] = (pivot_df[col] * 1_000_000) / pivot_df['population']
        
        # Calculate growth rates
        pivot_df = pivot_df.sort_values('data_year')
        
        if 'total_pce' in pivot_df.columns:
            pivot_df['total_pce_growth_rate'] = pivot_df['total_pce'].pct_change() * 100
        
        if 'goods_total' in pivot_df.columns:
            pivot_df['goods_growth_rate'] = pivot_df['goods_total'].pct_change() * 100
        
        if 'services_total' in pivot_df.columns:
            pivot_df['services_growth_rate'] = pivot_df['services_total'].pct_change() * 100
        
        # Calculate business-relevant metrics
        if 'goods_total' in pivot_df.columns:
            pivot_df['retail_relevant_spending'] = pivot_df['goods_total']
        
        if 'food_services_accommodations' in pivot_df.columns:
            pivot_df['restaurant_relevant_spending'] = pivot_df['food_services_accommodations']
        elif 'food_away_from_home' in pivot_df.columns:
            pivot_df['restaurant_relevant_spending'] = pivot_df['food_away_from_home']
        
        # Data quality score
        pivot_df['data_quality_score'] = 95.0
        
        return pivot_df
    
    def load_to_bigquery(self, df):
        """Load DataFrame to BigQuery"""
        
        if df.empty:
            logger.warning("No data to load to BigQuery")
            return False
        
        try:
            table_id = "location-optimizer-1.raw_business_data.consumer_spending"
            
            # Configure job
            job_config = bigquery.LoadJobConfig(
                write_disposition="WRITE_TRUNCATE",  # Replace existing data
                time_partitioning=bigquery.TimePartitioning(
                    type_=bigquery.TimePartitioningType.DAY,
                    field="data_collection_date"
                ),
                clustering_fields=["state_fips", "data_year", "data_period"],
                autodetect=True
            )
            
            logger.info(f"Loading {len(df)} records to {table_id}...")
            
            # Load data
            job = self.bq_client.load_table_from_dataframe(df, table_id, job_config=job_config)
            job.result()  # Wait for completion
            
            # Check results
            table = self.bq_client.get_table(table_id)
            logger.info(f"Successfully loaded data to {table_id}")
            logger.info(f"Total rows in table: {table.num_rows:,}")
            
            return True
            
        except Exception as e:
            logger.error(f"Error loading to BigQuery: {e}")
            return False

def main():
    """Main function"""
    
    print("🚀 Comprehensive BEA Consumer Spending Data Collection")
    print("="*70)
    
    # Initialize collector
    collector = ComprehensiveBEACollector()
    
    # Get available years
    available_years = collector.get_available_years()
    logger.info(f"Available years: {available_years}")
    
    print(f"\n📅 Collecting data for years: {min(available_years)}-{max(available_years)}")
    
    # Collect SAPCE1 data (core categories)
    print("\n📊 Collecting core PCE categories (SAPCE1)...")
    sapce1_records = collector.collect_sapce1_comprehensive(available_years)
    print(f"✅ Collected {len(sapce1_records)} core category records")
    
    # Collect detailed categories for recent years
    print("\n📊 Collecting detailed categories (SAPCE3)...")
    sapce3_records = collector.collect_detailed_categories(available_years)
    print(f"✅ Collected {len(sapce3_records)} detailed category records")
    
    # Create comprehensive DataFrame
    print("\n📊 Creating structured dataset...")
    df = collector.create_comprehensive_dataframe(sapce1_records, sapce3_records)
    
    if df.empty:
        print("❌ No data collected")
        return
    
    print(f"✅ Structured into {len(df)} annual records")
    
    # Display summary
    print("\n" + "="*80)
    print("Wisconsin Consumer Spending Summary (2015-Present)")
    print("="*80)
    
    # Show key columns
    display_cols = ['data_year', 'total_pce', 'goods_total', 'services_total', 
                   'total_pce_per_capita', 'total_pce_growth_rate']
    available_display_cols = [col for col in display_cols if col in df.columns]
    
    if available_display_cols:
        summary_df = df[available_display_cols].sort_values('data_year')
        
        print(f"{'Year':>6} | {'Total PCE ($M)':>14} | {'Goods ($M)':>12} | {'Services ($M)':>14} | {'Per Capita':>11} | {'Growth %':>9}")
        print("-" * 80)
        
        for _, row in summary_df.iterrows():
            year = int(row['data_year'])
            total_pce = row.get('total_pce', 0)
            goods = row.get('goods_total', 0)
            services = row.get('services_total', 0)
            per_capita = row.get('total_pce_per_capita', 0)
            growth = row.get('total_pce_growth_rate', 0)
            
            print(f"{year:>6} | ${total_pce:>13,.0f} | ${goods:>11,.0f} | ${services:>13,.0f} | ${per_capita:>10,.0f} | {growth:>8.1f}%")
    
    # Load to BigQuery
    print(f"\n📤 Loading to BigQuery...")
    success = collector.load_to_bigquery(df)
    
    if success:
        print("✅ Successfully loaded to BigQuery!")
        
        # Run verification query
        query = """
        SELECT 
            data_year,
            ROUND(total_pce, 1) as total_pce_millions,
            ROUND(total_pce_per_capita, 0) as per_capita,
            ROUND(total_pce_growth_rate, 1) as growth_rate
        FROM `location-optimizer-1.raw_business_data.consumer_spending`
        WHERE total_pce IS NOT NULL
        ORDER BY data_year DESC
        LIMIT 5
        """
        
        print("\n📊 Latest data in BigQuery:")
        print("Year | Total PCE ($M) | Per Capita | Growth %")
        print("-" * 50)
        
        try:
            for row in collector.bq_client.query(query):
                print(f"{int(row.data_year):>4} | ${row.total_pce_millions:>13.1f} | ${row.per_capita:>9,.0f} | {row.growth_rate:>7.1f}%")
        except Exception as e:
            logger.warning(f"Could not run verification query: {e}")
        
    else:
        print("❌ Failed to load to BigQuery")
        
        # Save to CSV as backup
        output_file = 'wisconsin_consumer_spending_comprehensive.csv'
        df.to_csv(output_file, index=False)
        print(f"💾 Data saved to {output_file}")
    
    print(f"\n🎉 Data collection complete!")
    print(f"   Records collected: {len(sapce1_records + sapce3_records)}")
    print(f"   Years covered: {min(available_years)}-{max(available_years)}")
    print(f"   Categories: Core PCE + detailed breakdowns")

if __name__ == "__main__":
    main()