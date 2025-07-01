#!/usr/bin/env python3
"""
Debug SAPCE Data
================

Debug what data we're getting from SAPCE tables.
"""

import os
import requests
import json

# Set BEA API key
os.environ['BEA_API_KEY'] = '1988DB31-BD6F-4482-A53F-F82AA2BE2E23'

def debug_sapce_data():
    """Debug SAPCE data retrieval"""
    
    api_key = os.environ['BEA_API_KEY']
    base_url = "https://apps.bea.gov/api/data"
    
    print("🔍 Debugging SAPCE Data")
    print("="*60)
    
    # Test SAPCE1 with specific line codes
    print("\n1. Testing SAPCE1 with specific line codes:")
    
    test_line_codes = ['1', '2', '3', '4', '5']  # Basic PCE categories
    
    for line_code in test_line_codes:
        params = {
            'UserID': api_key,
            'method': 'GetData',
            'datasetname': 'Regional',
            'TableName': 'SAPCE1',
            'LineCode': line_code,
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
                    record = results['Data'][0]
                    print(f"   Line {line_code}: Value={record.get('DataValue')}, "
                          f"LineCode={record.get('LineCode')}, "
                          f"GeoName={record.get('GeoName')}")
                    
                    # Print all fields to see structure
                    if line_code == '1':
                        print(f"   All fields: {list(record.keys())}")
    
    # Test SAPCE1 with ALL
    print("\n2. Testing SAPCE1 with LineCode='ALL':")
    
    params = {
        'UserID': api_key,
        'method': 'GetData',
        'datasetname': 'Regional',
        'TableName': 'SAPCE1',
        'LineCode': 'ALL',
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
                print(f"   Found {len(results['Data'])} records")
                
                # Show first 10 records
                for i, record in enumerate(results['Data'][:10]):
                    print(f"   Record {i+1}: LineCode={record.get('LineCode')}, "
                          f"Value={record.get('DataValue')}, "
                          f"Code={record.get('Code')}")
    
    # Get line code descriptions properly
    print("\n3. Getting line code descriptions for SAPCE1:")
    
    params = {
        'UserID': api_key,
        'method': 'GetParameterValues',
        'datasetname': 'Regional',
        'ParameterName': 'LineCode',
        'TableName': 'SAPCE1',
        'ResultFormat': 'json'
    }
    
    response = requests.get(base_url, params=params)
    
    if response.status_code == 200:
        data = response.json()
        
        if 'BEAAPI' in data and 'Results' in data['BEAAPI']:
            results = data['BEAAPI']['Results']
            
            if 'ParamValue' in results:
                line_codes = results['ParamValue']
                
                # Show first 20 that start with single digits
                count = 0
                for code_info in line_codes:
                    key = code_info.get('Key', '')
                    desc = code_info.get('Description', code_info.get('Desc', ''))
                    
                    if key and key.isdigit() and int(key) <= 20:
                        print(f"   Code {key}: {desc[:70]}")
                        count += 1
                        
                        if count >= 20:
                            break

if __name__ == "__main__":
    debug_sapce_data()