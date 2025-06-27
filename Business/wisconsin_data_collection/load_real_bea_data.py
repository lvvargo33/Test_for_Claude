#!/usr/bin/env python3
"""
Load Real BEA Consumer Spending Data
===================================

Collects real consumer spending data from BEA API and loads to BigQuery.
"""

import os
import logging
import requests
import pandas as pd
from datetime import datetime
from google.cloud import bigquery
import time

# Set BEA API key
os.environ['BEA_API_KEY'] = '1988DB31-BD6F-4482-A53F-F82AA2BE2E23'

logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)

class RealBEACollector:
    """Real BEA data collector using API"""
    
    def __init__(self):
        self.api_key = os.environ['BEA_API_KEY']
        self.base_url = "https://apps.bea.gov/api/data"
        self.wisconsin_fips = "55000"
        
    def collect_wisconsin_pce_data(self, years=None):
        """Collect Wisconsin Personal Consumption Expenditure data"""
        
        if years is None:
            years = [2019, 2020, 2021, 2022, 2023]  # Last 5 years
        
        print(f"📊 Collecting Wisconsin PCE data for years: {years}")
        
        # BEA PCE line codes mapping
        line_codes = {
            '1': 'total_pce',
            '2': 'goods_total', 
            '3': 'goods_durable',
            '4': 'goods_nondurable',
            '5': 'services_total',
            '6': 'housing_utilities',
            '7': 'healthcare',
            '8': 'transportation',
            '9': 'recreation',
            '10': 'food_beverages',
            '11': 'restaurants_hotels',
            '12': 'financial_insurance',
            '13': 'other_services'
        }
        
        all_data = []
        
        for year in years:
            print(f"   Collecting {year} data...")
            
            for line_code, category in line_codes.items():
                try:
                    params = {
                        'UserID': self.api_key,
                        'method': 'GetData',
                        'datasetname': 'Regional',
                        'TableName': 'CAINC30',  # Personal consumption expenditures
                        'LineCode': line_code,
                        'GeoFips': self.wisconsin_fips,
                        'Year': str(year),
                        'ResultFormat': 'json'
                    }
                    
                    response = requests.get(self.base_url, params=params, timeout=15)
                    
                    if response.status_code == 200:
                        data = response.json()
                        
                        if 'BEAAPI' in data and 'Results' in data['BEAAPI']:
                            results = data['BEAAPI']['Results']
                            
                            if 'Data' in results and results['Data']:
                                for record in results['Data']:
                                    spending_record = {
                                        'state': 'WI',
                                        'state_fips': self.wisconsin_fips,
                                        'year': int(year),
                                        'spending_category': category,
                                        'line_code': line_code,
                                        'spending_amount': self._parse_value(record.get('DataValue', '0')),
                                        'data_source': 'BEA_API_Real',
                                        'api_dataset': record.get('TableName', 'CAINC30'),
                                        'geo_name': record.get('GeoName', 'Wisconsin'),
                                        'collection_date': datetime.utcnow(),
                                        'data_extraction_date': datetime.utcnow()
                                    }
                                    
                                    # Calculate per capita (WI population ~5.9M)
                                    wi_population = 5900000
                                    spending_record['per_capita_spending'] = spending_record['spending_amount'] / wi_population
                                    
                                    all_data.append(spending_record)
                            else:
                                print(f"      ⚠️  No data for {category} {year}")
                        else:
                            print(f"      ❌ API error for {category} {year}: Invalid response structure")
                    else:
                        print(f"      ❌ API error for {category} {year}: Status {response.status_code}")
                        
                    # Rate limiting
                    time.sleep(0.2)
                    
                except Exception as e:
                    print(f"      ❌ Error collecting {category} {year}: {e}")
                    continue
        
        return all_data
    
    def _parse_value(self, value_str):
        """Parse BEA value string to float"""
        try:
            if isinstance(value_str, str):
                # Remove commas and handle thousands/millions indicators
                cleaned = value_str.replace(',', '').replace('(D)', '0')
                
                # Handle BEA's notation (values are typically in millions)
                if cleaned and cleaned != '0':
                    return float(cleaned) * 1000000  # Convert millions to actual dollars
                
            return float(value_str) if value_str else 0.0
        except:
            return 0.0

def load_real_bea_data():
    """Load real BEA data to BigQuery"""
    
    print("💰 Loading Real BEA Consumer Spending Data")
    print("="*60)
    
    # Initialize collector
    collector = RealBEACollector()
    
    # Collect data
    data = collector.collect_wisconsin_pce_data()
    
    if not data:
        print("❌ No data collected from BEA API")
        return 0
    
    print(f"📊 Collected {len(data)} spending records")
    
    # Convert to DataFrame
    df = pd.DataFrame(data)
    
    # Load to BigQuery
    client = bigquery.Client(project="location-optimizer-1")
    table_id = "location-optimizer-1.raw_business_data.consumer_spending"
    
    job_config = bigquery.LoadJobConfig(
        write_disposition="WRITE_TRUNCATE",  # Replace demo data
        time_partitioning=bigquery.TimePartitioning(
            type_=bigquery.TimePartitioningType.DAY,
            field="data_extraction_date"
        ),
        clustering_fields=["state", "year", "spending_category"],
        autodetect=True
    )
    
    try:
        # Load data to BigQuery
        logger.info(f"Loading {len(df)} records to {table_id}")
        job = client.load_table_from_dataframe(df, table_id, job_config=job_config)
        job.result()  # Wait for job to complete
        
        # Verify load
        table = client.get_table(table_id)
        logger.info(f"✅ Successfully loaded {table.num_rows} rows to {table_id}")
        
        print(f"\n✅ SUCCESS: Loaded {len(df)} real BEA records to BigQuery")
        print(f"   Table: {table_id}")
        print(f"   Total rows: {table.num_rows:,}")
        
        # Show sample of loaded data
        query = f"""
        SELECT 
            year,
            spending_category,
            ROUND(spending_amount / 1000000000, 2) as spending_billions,
            ROUND(per_capita_spending, 0) as per_capita,
            data_source
        FROM `{table_id}`
        WHERE spending_category IN ('total_pce', 'food_beverages', 'restaurants_hotels')
        ORDER BY year DESC, spending_category
        LIMIT 10
        """
        
        print("\n📊 Sample Real BEA Data:")
        print("   Year | Category           | Amount ($B) | Per Capita | Source")
        print("   " + "-"*70)
        for row in client.query(query):
            category = row.spending_category.replace('_', ' ').title()[:18]
            print(f"   {row.year} | {category:<18} | ${row.spending_billions:>7.1f}B | ${row.per_capita:>6,.0f} | {row.data_source}")
            
        return len(df)
        
    except Exception as e:
        logger.error(f"Failed to load data to BigQuery: {e}")
        print(f"\n❌ ERROR: Failed to load data - {e}")
        return 0

def main():
    """Main function"""
    
    print("🚀 Real BEA Data Collection and Loading")
    print("="*60)
    
    records_loaded = load_real_bea_data()
    
    if records_loaded > 0:
        print(f"\n🎉 Successfully loaded {records_loaded} real BEA consumer spending records!")
        print("   Data source changed from demo to BEA_API_Real")
        print("   Coverage: 2019-2023 Wisconsin consumer spending")
    else:
        print("\n❌ Failed to load real BEA data")

if __name__ == "__main__":
    main()