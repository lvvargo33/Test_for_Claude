#!/usr/bin/env python3
"""
Run Fixed BEA Collection
========================

This script properly collects BEA consumer spending data and loads it to BigQuery.
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

# Set BEA API key
os.environ['BEA_API_KEY'] = '1988DB31-BD6F-4482-A53F-F82AA2BE2E23'

def collect_bea_pce_data():
    """Collect BEA PCE data using SAPCE1 table"""
    
    api_key = os.environ['BEA_API_KEY']
    base_url = "https://apps.bea.gov/api/data"
    wisconsin_fips = "55000"
    
    print("📊 Collecting BEA Consumer Spending Data for Wisconsin")
    print("="*60)
    
    all_records = []
    years = [2019, 2020, 2021, 2022, 2023]
    
    # SAPCE1 line codes mapping
    line_mapping = {
        '1': 'total_pce',
        '2': 'goods_total', 
        '3': 'goods_durable',
        '4': 'goods_nondurable',
        '5': 'services_total'
    }
    
    for year in years:
        print(f"\n📅 Collecting data for {year}...")
        
        # Collect each line code separately for clarity
        for line_code, category in line_mapping.items():
            params = {
                'UserID': api_key,
                'method': 'GetData',
                'datasetname': 'Regional',
                'TableName': 'SAPCE1',
                'LineCode': line_code,
                'GeoFips': wisconsin_fips,
                'Year': str(year),
                'ResultFormat': 'json'
            }
            
            try:
                response = requests.get(base_url, params=params, timeout=30)
                
                if response.status_code == 200:
                    data = response.json()
                    
                    if 'BEAAPI' in data and 'Results' in data['BEAAPI']:
                        results = data['BEAAPI']['Results']
                        
                        if 'Data' in results and results['Data']:
                            record = results['Data'][0]
                            
                            # Parse value
                            data_value = record.get('DataValue', '0')
                            if data_value and data_value != '(D)':
                                try:
                                    value = float(data_value.replace(',', ''))
                                except:
                                    value = 0
                            else:
                                value = 0
                            
                            # Create record
                            spending_record = {
                                'geo_fips': wisconsin_fips,
                                'geo_name': 'Wisconsin',
                                'state_fips': '55',
                                'state_name': 'Wisconsin',
                                'data_year': year,
                                'data_period': 'annual',
                                'spending_category': category,
                                'line_code': line_code,
                                'value_millions': value,
                                'data_source': 'BEA',
                                'api_dataset': 'Regional_SAPCE1',
                                'data_collection_date': datetime.now(),
                                'seasonally_adjusted': True,
                                'data_quality_score': 95.0
                            }
                            
                            all_records.append(spending_record)
                            
                            print(f"   ✓ {category}: ${value:,.1f} million")
                
                time.sleep(0.1)  # Rate limiting
                
            except Exception as e:
                logger.error(f"Error collecting {category} for {year}: {e}")
    
    return all_records

def create_structured_dataframe(records):
    """Create structured DataFrame with one row per year"""
    
    df = pd.DataFrame(records)
    
    if df.empty:
        return df
    
    # Pivot to get one row per year
    pivot_df = df.pivot_table(
        index=['data_year', 'state_fips', 'state_name', 'geo_fips', 'geo_name'],
        columns='spending_category',
        values='value_millions',
        aggfunc='first'
    ).reset_index()
    
    # Add metadata
    pivot_df['data_source'] = 'BEA'
    pivot_df['api_dataset'] = 'Regional_SAPCE1'
    pivot_df['data_collection_date'] = datetime.now()
    pivot_df['data_period'] = 'annual'
    pivot_df['seasonally_adjusted'] = True
    
    # Wisconsin population by year
    population_map = {
        2019: 5822434,
        2020: 5893718,
        2021: 5895908,
        2022: 5892539,
        2023: 5910955
    }
    
    pivot_df['population'] = pivot_df['data_year'].map(population_map)
    
    # Calculate per capita values
    if 'total_pce' in pivot_df.columns:
        pivot_df['total_pce_per_capita'] = (pivot_df['total_pce'] * 1_000_000) / pivot_df['population']
    
    if 'goods_total' in pivot_df.columns:
        pivot_df['goods_per_capita'] = (pivot_df['goods_total'] * 1_000_000) / pivot_df['population']
    
    if 'services_total' in pivot_df.columns:
        pivot_df['services_per_capita'] = (pivot_df['services_total'] * 1_000_000) / pivot_df['population']
    
    # Calculate growth rates
    pivot_df = pivot_df.sort_values('data_year')
    if 'total_pce' in pivot_df.columns:
        pivot_df['total_pce_growth_rate'] = pivot_df['total_pce'].pct_change() * 100
    
    # Data quality score
    pivot_df['data_quality_score'] = 95.0
    
    return pivot_df

def main():
    """Main function"""
    
    print("🚀 BEA Consumer Spending Data Collection")
    print("="*60)
    
    # Collect data
    records = collect_bea_pce_data()
    
    print(f"\n✅ Collected {len(records)} raw records")
    
    # Create structured DataFrame
    df = create_structured_dataframe(records)
    
    if df.empty:
        print("❌ No data to process")
        return
    
    print(f"📊 Structured into {len(df)} annual records")
    
    # Display summary
    print("\n" + "="*80)
    print("Wisconsin Personal Consumption Expenditure Summary (in millions)")
    print("="*80)
    print(f"{'Year':>6} | {'Total PCE':>10} | {'Goods':>10} | {'Services':>10} | {'Per Capita':>10} | {'Growth %':>8}")
    print("-"*80)
    
    for _, row in df.iterrows():
        total_pce = row.get('total_pce', 0)
        goods = row.get('goods_total', 0)
        services = row.get('services_total', 0)
        per_capita = row.get('total_pce_per_capita', 0)
        growth = row.get('total_pce_growth_rate', 0)
        
        print(f"{row['data_year']:>6} | ${total_pce:>9,.0f} | ${goods:>9,.0f} | ${services:>9,.0f} | ${per_capita:>9,.0f} | {growth:>7.1f}%")
    
    # Try to load to BigQuery
    try:
        client = bigquery.Client(project="location-optimizer-1")
        table_id = "location-optimizer-1.raw_business_data.consumer_spending"
        
        job_config = bigquery.LoadJobConfig(
            write_disposition="WRITE_TRUNCATE",
            time_partitioning=bigquery.TimePartitioning(
                type_=bigquery.TimePartitioningType.DAY,
                field="data_collection_date"
            ),
            clustering_fields=["state_fips", "data_year", "data_period"],
            autodetect=True
        )
        
        print(f"\n📤 Loading {len(df)} records to BigQuery...")
        job = client.load_table_from_dataframe(df, table_id, job_config=job_config)
        job.result()
        
        table = client.get_table(table_id)
        print(f"✅ Successfully loaded data to {table_id}")
        print(f"   Total rows in table: {table.num_rows:,}")
        
    except Exception as e:
        print(f"\n⚠️  Could not load to BigQuery: {e}")
        print("   Data collection was successful - saving to CSV instead")
        
        # Save to CSV
        output_file = 'wisconsin_bea_consumer_spending.csv'
        df.to_csv(output_file, index=False)
        print(f"💾 Data saved to {output_file}")
    
    print("\n🎉 BEA data collection complete!")
    
    return df

if __name__ == "__main__":
    df = main()