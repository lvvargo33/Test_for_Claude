#!/usr/bin/env python3
"""
Fixed BEA PCE Data Collector
============================

Properly collects Personal Consumption Expenditure data from BEA using SAPCE tables.
"""

import os
import requests
import pandas as pd
import logging
import time
from datetime import datetime
from typing import List, Dict, Optional

# Set up logging
logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)

# Set BEA API key
os.environ['BEA_API_KEY'] = '1988DB31-BD6F-4482-A53F-F82AA2BE2E23'

class BEAPCECollector:
    """BEA PCE data collector using SAPCE tables"""
    
    def __init__(self):
        self.api_key = os.environ['BEA_API_KEY']
        self.base_url = "https://apps.bea.gov/api/data"
        self.wisconsin_fips = "55000"
        
    def get_line_codes_for_table(self, table_name: str) -> Dict[str, str]:
        """Get available line codes for a specific table"""
        
        params = {
            'UserID': self.api_key,
            'method': 'GetParameterValues',
            'datasetname': 'Regional',
            'ParameterName': 'LineCode',
            'TableName': table_name,
            'ResultFormat': 'json'
        }
        
        try:
            response = requests.get(self.base_url, params=params, timeout=30)
            
            if response.status_code == 200:
                data = response.json()
                
                if 'BEAAPI' in data and 'Results' in data['BEAAPI']:
                    results = data['BEAAPI']['Results']
                    
                    if 'ParamValue' in results:
                        line_codes = {}
                        for item in results['ParamValue']:
                            code = item.get('Key', '')
                            desc = item.get('Description', item.get('Desc', ''))
                            if code and desc:
                                line_codes[code] = desc
                        return line_codes
        except Exception as e:
            logger.error(f"Error getting line codes: {e}")
        
        return {}
    
    def collect_sapce1_data(self, years: List[int]) -> List[Dict]:
        """Collect SAPCE1 - PCE by major type of product"""
        
        logger.info("Collecting SAPCE1 - PCE by major type of product")
        
        # Get line codes for SAPCE1
        line_codes = self.get_line_codes_for_table('SAPCE1')
        logger.info(f"Found {len(line_codes)} line codes for SAPCE1")
        
        # If no line codes found, use default set
        if not line_codes:
            line_codes = {
                '1': 'Total personal consumption expenditures',
                '2': 'Goods',
                '3': 'Durable goods',
                '4': 'Nondurable goods',
                '5': 'Services'
            }
        
        all_records = []
        
        for year in years:
            logger.info(f"  Collecting SAPCE1 data for {year}")
            
            # Get all line codes at once
            params = {
                'UserID': self.api_key,
                'method': 'GetData',
                'datasetname': 'Regional',
                'TableName': 'SAPCE1',
                'LineCode': 'ALL',
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
                            for record in results['Data']:
                                # Parse the record
                                line_code = record.get('LineCode', '')
                                data_value = record.get('DataValue', '0')
                                
                                # Skip if no data
                                if data_value in ['(D)', '(NA)', None]:
                                    continue
                                
                                # Convert value (in millions)
                                try:
                                    value_millions = float(data_value.replace(',', ''))
                                except:
                                    continue
                                
                                # Get description
                                description = line_codes.get(line_code, f'Line {line_code}')
                                
                                record_data = {
                                    'state': 'WI',
                                    'state_fips': '55',
                                    'state_name': 'Wisconsin',
                                    'data_year': year,
                                    'table_name': 'SAPCE1',
                                    'line_code': line_code,
                                    'category': description,
                                    'value_millions': value_millions,
                                    'data_source': 'BEA',
                                    'collection_date': datetime.now()
                                }
                                
                                all_records.append(record_data)
                            
                            logger.info(f"    Found {len(results['Data'])} records")
                        else:
                            logger.warning(f"    No data for {year}")
                
                time.sleep(0.2)  # Rate limiting
                
            except Exception as e:
                logger.error(f"Error collecting SAPCE1 data for {year}: {e}")
        
        return all_records
    
    def collect_sapce3_data(self, years: List[int]) -> List[Dict]:
        """Collect SAPCE3 - PCE by state by type of product"""
        
        logger.info("Collecting SAPCE3 - PCE by state by type of product (detailed)")
        
        # Get line codes for SAPCE3
        line_codes = self.get_line_codes_for_table('SAPCE3')
        logger.info(f"Found {len(line_codes)} line codes for SAPCE3")
        
        all_records = []
        
        for year in years:
            logger.info(f"  Collecting SAPCE3 data for {year}")
            
            params = {
                'UserID': self.api_key,
                'method': 'GetData',
                'datasetname': 'Regional',
                'TableName': 'SAPCE3',
                'LineCode': 'ALL',
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
                            for record in results['Data']:
                                line_code = record.get('LineCode', '')
                                data_value = record.get('DataValue', '0')
                                
                                if data_value in ['(D)', '(NA)', None]:
                                    continue
                                
                                try:
                                    value_millions = float(data_value.replace(',', ''))
                                except:
                                    continue
                                
                                description = line_codes.get(line_code, f'Line {line_code}')
                                
                                record_data = {
                                    'state': 'WI',
                                    'state_fips': '55',
                                    'state_name': 'Wisconsin',
                                    'data_year': year,
                                    'table_name': 'SAPCE3',
                                    'line_code': line_code,
                                    'category': description,
                                    'value_millions': value_millions,
                                    'data_source': 'BEA',
                                    'collection_date': datetime.now()
                                }
                                
                                all_records.append(record_data)
                            
                            logger.info(f"    Found {len(results['Data'])} records")
                
                time.sleep(0.2)  # Rate limiting
                
            except Exception as e:
                logger.error(f"Error collecting SAPCE3 data for {year}: {e}")
        
        return all_records
    
    def create_structured_dataframe(self, sapce1_records: List[Dict], sapce3_records: List[Dict]) -> pd.DataFrame:
        """Create structured DataFrame from SAPCE records"""
        
        # Convert to DataFrames
        df1 = pd.DataFrame(sapce1_records) if sapce1_records else pd.DataFrame()
        df3 = pd.DataFrame(sapce3_records) if sapce3_records else pd.DataFrame()
        
        # Combine data - SAPCE1 for major categories, SAPCE3 for detailed
        all_data = []
        
        # Get unique years
        years = set()
        if not df1.empty:
            years.update(df1['data_year'].unique())
        if not df3.empty:
            years.update(df3['data_year'].unique())
        
        for year in sorted(years):
            year_record = {
                'state': 'WI',
                'state_fips': '55',
                'state_name': 'Wisconsin',
                'data_year': year,
                'data_source': 'BEA',
                'data_collection_date': datetime.now()
            }
            
            # Add SAPCE1 data (major categories)
            if not df1.empty:
                year_df1 = df1[df1['data_year'] == year]
                
                for _, row in year_df1.iterrows():
                    category = row['category'].lower()
                    
                    if 'total' in category:
                        year_record['total_pce'] = row['value_millions']
                    elif category == 'goods':
                        year_record['goods_total'] = row['value_millions']
                    elif 'durable goods' in category:
                        year_record['goods_durable'] = row['value_millions']
                    elif 'nondurable goods' in category:
                        year_record['goods_nondurable'] = row['value_millions']
                    elif category == 'services':
                        year_record['services_total'] = row['value_millions']
            
            # Add SAPCE3 data (detailed categories)
            if not df3.empty:
                year_df3 = df3[df3['data_year'] == year]
                
                for _, row in year_df3.iterrows():
                    category = row['category'].lower()
                    
                    # Map to standard categories
                    if 'food' in category and 'accommodations' in category:
                        year_record['food_services_accommodations'] = row['value_millions']
                    elif 'health' in category:
                        year_record['health_care'] = row['value_millions']
                    elif 'housing' in category:
                        year_record['housing_utilities'] = row['value_millions']
                    elif 'transportation' in category and 'services' in category:
                        year_record['transportation_services'] = row['value_millions']
                    elif 'recreation' in category:
                        year_record['recreation_services'] = row['value_millions']
                    elif 'financial' in category:
                        year_record['financial_insurance'] = row['value_millions']
            
            all_data.append(year_record)
        
        # Create final DataFrame
        final_df = pd.DataFrame(all_data)
        
        # Add population data for per capita calculations
        population_by_year = {
            2019: 5822434,
            2020: 5893718,
            2021: 5895908,
            2022: 5892539,
            2023: 5910955
        }
        
        final_df['population'] = final_df['data_year'].map(population_by_year)
        
        # Calculate per capita values
        if 'total_pce' in final_df.columns:
            final_df['total_pce_per_capita'] = (final_df['total_pce'] * 1_000_000) / final_df['population']
        
        return final_df

def main():
    """Main function to collect and display BEA PCE data"""
    
    print("🚀 BEA PCE Data Collection (Using Correct Tables)")
    print("="*60)
    
    # Initialize collector
    collector = BEAPCECollector()
    
    # Collect data for recent years
    years = [2021, 2022, 2023]
    
    print(f"\n📊 Collecting PCE data for years: {years}")
    
    # Collect SAPCE1 data (major categories)
    sapce1_data = collector.collect_sapce1_data(years)
    print(f"\n✅ Collected {len(sapce1_data)} SAPCE1 records")
    
    # Collect SAPCE3 data (detailed categories)
    sapce3_data = collector.collect_sapce3_data(years)
    print(f"✅ Collected {len(sapce3_data)} SAPCE3 records")
    
    # Create structured DataFrame
    df = collector.create_structured_dataframe(sapce1_data, sapce3_data)
    
    print(f"\n📊 Structured into {len(df)} annual records")
    
    # Display results
    print("\n" + "="*80)
    print("Wisconsin Personal Consumption Expenditure Summary")
    print("="*80)
    
    for _, row in df.iterrows():
        print(f"\nYear: {row['data_year']}")
        print("-" * 40)
        
        if 'total_pce' in row and pd.notna(row['total_pce']):
            print(f"Total PCE: ${row['total_pce']:,.1f} million")
            if 'total_pce_per_capita' in row:
                print(f"Per Capita: ${row['total_pce_per_capita']:,.0f}")
        
        if 'goods_total' in row and pd.notna(row['goods_total']):
            print(f"Goods Total: ${row['goods_total']:,.1f} million")
            
            if 'goods_durable' in row and pd.notna(row['goods_durable']):
                print(f"  - Durable: ${row['goods_durable']:,.1f} million")
            if 'goods_nondurable' in row and pd.notna(row['goods_nondurable']):
                print(f"  - Nondurable: ${row['goods_nondurable']:,.1f} million")
        
        if 'services_total' in row and pd.notna(row['services_total']):
            print(f"Services Total: ${row['services_total']:,.1f} million")
        
        # Show some detailed categories if available
        detailed_cats = ['food_services_accommodations', 'health_care', 'housing_utilities']
        for cat in detailed_cats:
            if cat in row and pd.notna(row[cat]):
                cat_name = cat.replace('_', ' ').title()
                print(f"{cat_name}: ${row[cat]:,.1f} million")
    
    # Save to CSV for inspection
    output_file = 'wisconsin_pce_data.csv'
    df.to_csv(output_file, index=False)
    print(f"\n💾 Data saved to {output_file}")
    
    return df

if __name__ == "__main__":
    df = main()