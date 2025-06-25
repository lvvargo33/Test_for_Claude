#!/usr/bin/env python3
"""
Collect Census Data 2013-2023
=============================

Collects ACS 5-year estimates and available population estimates
for Wisconsin counties from 2013-2023.
"""

import os
import sys
import requests
import json
import time
from datetime import datetime
from google.cloud import bigquery
import pandas as pd

# Set credentials
os.environ['GOOGLE_APPLICATION_CREDENTIALS'] = '/workspaces/Test_for_Claude/Business/wisconsin_data_collection/location-optimizer-1-449414f93a5a.json'

API_KEY = "dd75feaae49ed1a1884869cf57289ceacb0962f5"

def collect_acs_year(year):
    """Collect ACS data for a specific year"""
    print(f"Collecting ACS {year} data...")
    
    # Key demographic variables
    variables = [
        'B01003_001E',  # Total population
        'B01002_001E',  # Median age
        'B19013_001E',  # Median household income
        'B23025_005E',  # Unemployment count
        'B23025_002E',  # Labor force
        'B15003_022E',  # Bachelor degree count
        'B15003_001E',  # Total education population
        'B25001_001E',  # Total housing units
        'B25003_002E',  # Owner occupied units
        'B25003_001E',  # Total occupied units
        'NAME'          # County name
    ]
    
    url = f"https://api.census.gov/data/{year}/acs/acs5"
    params = {
        'get': ','.join(variables),
        'for': 'county:*',
        'in': 'state:55',
        'key': API_KEY
    }
    
    try:
        response = requests.get(url, params=params, timeout=30)
        response.raise_for_status()
        data = response.json()
        
        if len(data) <= 1:
            return []
        
        headers = data[0]
        records = []
        
        for row in data[1:]:
            record = {
                'acs_year': year,
                'state_fips': '55',
                'county_fips': f"55{row[-1]}",
                'geographic_level': 'county',
                'data_extraction_date': datetime.now(),
                'data_source': f'ACS_{year}_5YR'
            }
            
            # Map variables to record
            for i, header in enumerate(headers):
                if header == 'NAME':
                    record['county_name'] = row[i]
                elif header == 'B01003_001E' and row[i] not in [None, '', '-']:
                    record['total_population'] = int(row[i])
                elif header == 'B01002_001E' and row[i] not in [None, '', '-']:
                    record['median_age'] = float(row[i])
                elif header == 'B19013_001E' and row[i] not in [None, '', '-']:
                    record['median_household_income'] = int(row[i])
                elif header == 'B23025_005E' and row[i] not in [None, '', '-']:
                    record['unemployment_count'] = int(row[i])
                elif header == 'B23025_002E' and row[i] not in [None, '', '-']:
                    record['labor_force'] = int(row[i])
                elif header == 'B15003_022E' and row[i] not in [None, '', '-']:
                    record['bachelor_degree_count'] = int(row[i])
                elif header == 'B15003_001E' and row[i] not in [None, '', '-']:
                    record['total_education_pop'] = int(row[i])
                elif header == 'B25001_001E' and row[i] not in [None, '', '-']:
                    record['total_housing_units'] = int(row[i])
                elif header == 'B25003_002E' and row[i] not in [None, '', '-']:
                    record['owner_occupied_units'] = int(row[i])
                elif header == 'B25003_001E' and row[i] not in [None, '', '-']:
                    record['total_occupied_units'] = int(row[i])
            
            # Calculate derived metrics
            if record.get('labor_force') and record.get('unemployment_count'):
                record['unemployment_rate'] = (record['unemployment_count'] / record['labor_force']) * 100
            
            if record.get('total_education_pop') and record.get('bachelor_degree_count'):
                record['bachelor_degree_pct'] = (record['bachelor_degree_count'] / record['total_education_pop']) * 100
            
            if record.get('total_occupied_units') and record.get('owner_occupied_units'):
                record['owner_occupied_pct'] = (record['owner_occupied_units'] / record['total_occupied_units']) * 100
            
            records.append(record)
        
        print(f"  Collected {len(records)} county records for {year}")
        return records
        
    except Exception as e:
        print(f"  Error collecting {year}: {str(e)}")
        return []

def collect_pep_2019():
    """Collect 2019 Population Estimates Program data"""
    print("Collecting 2019 Population Estimates...")
    
    url = "https://api.census.gov/data/2019/pep/population"
    params = {
        'get': 'NAME,POP,DENSITY',
        'for': 'county:*',
        'in': 'state:55',
        'key': API_KEY
    }
    
    try:
        response = requests.get(url, params=params, timeout=30)
        response.raise_for_status()
        data = response.json()
        
        if len(data) <= 1:
            return {}
        
        pep_data = {}
        for row in data[1:]:
            county_fips = f"55{row[-1]}"
            pep_data[county_fips] = {
                'population_2019': int(row[1]) if row[1] not in [None, '', '-'] else None,
                'population_density_2019': float(row[2]) if row[2] not in [None, '', '-'] else None
            }
        
        print(f"  Collected PEP data for {len(pep_data)} counties")
        return pep_data
        
    except Exception as e:
        print(f"  Error collecting PEP 2019: {str(e)}")
        return {}

def store_to_bigquery(records):
    """Store records to BigQuery"""
    if not records:
        print("No records to store")
        return
    
    try:
        client = bigquery.Client(project="location-optimizer-1")
        table_id = "location-optimizer-1.raw_business_data.census_demographics"
        
        # Convert to DataFrame
        df = pd.DataFrame(records)
        
        # Configure load job
        job_config = bigquery.LoadJobConfig(
            write_disposition=bigquery.WriteDisposition.WRITE_APPEND,
            schema_update_options=[bigquery.SchemaUpdateOption.ALLOW_FIELD_ADDITION]
        )
        
        # Load data
        job = client.load_table_from_dataframe(df, table_id, job_config=job_config)
        job.result()  # Wait for completion
        
        print(f"Successfully stored {len(records)} records to BigQuery")
        
    except Exception as e:
        print(f"Error storing to BigQuery: {str(e)}")

def main():
    """Main collection function"""
    print("Historical Census Data Collection (2013-2023)")
    print("=" * 60)
    
    all_records = []
    
    # Collect ACS data for each year
    years = list(range(2013, 2024))  # 2013-2023
    
    for year in years:
        records = collect_acs_year(year)
        if records:
            all_records.extend(records)
        time.sleep(1)  # Rate limiting
    
    # Collect 2019 PEP data and merge
    pep_data = collect_pep_2019()
    
    # Merge PEP data with 2019 ACS records
    for record in all_records:
        if record['acs_year'] == 2019 and record['county_fips'] in pep_data:
            record.update(pep_data[record['county_fips']])
    
    print(f"\nTotal records collected: {len(all_records)}")
    
    # Show summary by year
    year_counts = {}
    for record in all_records:
        year = record['acs_year']
        year_counts[year] = year_counts.get(year, 0) + 1
    
    print("\nRecords by year:")
    for year in sorted(year_counts.keys()):
        print(f"  {year}: {year_counts[year]} counties")
    
    # Store to BigQuery
    if all_records:
        print(f"\nStoring {len(all_records)} records to BigQuery...")
        store_to_bigquery(all_records)
    
    # Save sample to file
    sample_file = "sample_historical_census.json"
    if all_records:
        with open(sample_file, 'w') as f:
            json.dump(all_records[:5], f, indent=2, default=str)
        print(f"Sample data saved to {sample_file}")

if __name__ == "__main__":
    main()