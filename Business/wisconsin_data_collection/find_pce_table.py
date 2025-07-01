#!/usr/bin/env python3
"""
Find PCE Table in BEA
=====================

Find the correct table for Personal Consumption Expenditure data.
"""

import os
import requests

# Set BEA API key
os.environ['BEA_API_KEY'] = '1988DB31-BD6F-4482-A53F-F82AA2BE2E23'

def find_pce_tables():
    """Find tables related to PCE"""
    
    api_key = os.environ['BEA_API_KEY']
    base_url = "https://apps.bea.gov/api/data"
    
    print("🔍 Finding BEA Tables for Personal Consumption Expenditure (PCE)")
    print("="*60)
    
    # Get available tables for Regional dataset
    params = {
        'UserID': api_key,
        'method': 'GetParameterValues',
        'datasetname': 'Regional',
        'ParameterName': 'TableName',
        'ResultFormat': 'json'
    }
    
    response = requests.get(base_url, params=params)
    
    if response.status_code == 200:
        data = response.json()
        
        if 'BEAAPI' in data and 'Results' in data['BEAAPI']:
            results = data['BEAAPI']['Results']
            
            if 'ParamValue' in results:
                tables = results['ParamValue']
                
                # Look for PCE-related tables
                print("\nPCE-related tables:")
                print("-"*80)
                
                pce_tables = []
                for table in tables:
                    table_name = table.get('Key', '')
                    desc = table.get('Description', table.get('Desc', ''))
                    
                    # Look for PCE, consumption, or spending keywords
                    if any(keyword in desc.lower() for keyword in ['pce', 'consumption', 'spending', 'expenditure']):
                        pce_tables.append((table_name, desc))
                        print(f"{table_name:15} | {desc[:60]}")
                
                # Also check NIPA dataset for PCE data
                print("\n\nChecking NIPA dataset for PCE tables:")
                print("-"*80)
                
                params = {
                    'UserID': api_key,
                    'method': 'GetParameterValues',
                    'datasetname': 'NIPA',
                    'ParameterName': 'TableName',
                    'ResultFormat': 'json'
                }
                
                response = requests.get(base_url, params=params)
                
                if response.status_code == 200:
                    data = response.json()
                    
                    if 'BEAAPI' in data and 'Results' in data['BEAAPI']:
                        results = data['BEAAPI']['Results']
                        
                        if 'ParamValue' in results:
                            tables = results['ParamValue']
                            
                            for table in tables[:20]:  # Show first 20
                                table_name = table.get('Key', '')
                                desc = table.get('Description', table.get('Desc', ''))
                                
                                if 'consumption' in desc.lower() or 'pce' in desc.lower():
                                    print(f"{table_name:15} | {desc[:60]}")
                
                # Test SAPCE1 table which might have state PCE data
                print("\n\nTesting SAPCE1 table (State Annual PCE):")
                print("-"*80)
                
                params = {
                    'UserID': api_key,
                    'method': 'GetData',
                    'datasetname': 'Regional',
                    'TableName': 'SAPCE1',
                    'LineCode': '1',
                    'GeoFips': '55000',
                    'Year': '2023',
                    'ResultFormat': 'json'
                }
                
                response = requests.get(base_url, params=params)
                
                if response.status_code == 200:
                    data = response.json()
                    
                    if 'BEAAPI' in data and 'Results' in data['BEAAPI']:
                        results = data['BEAAPI']['Results']
                        
                        if 'Data' in results and results['Data']:
                            print("✅ SAPCE1 table has data!")
                            record = results['Data'][0]
                            print(f"   Value: {record.get('DataValue')}")
                            print(f"   Unit: {record.get('UnitMultiplier', 'N/A')}")
                            print(f"   Description: {record.get('Description', 'N/A')}")
                        else:
                            print("❌ No data found in SAPCE1")
                else:
                    print(f"❌ Error accessing SAPCE1: {response.status_code}")

if __name__ == "__main__":
    find_pce_tables()