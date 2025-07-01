#!/usr/bin/env python3
"""
Test BEA API Debug Script
========================

Debug BEA API calls to identify why we're getting limited data.
"""

import os
import requests
import json
from datetime import datetime

# Set BEA API key
os.environ['BEA_API_KEY'] = '1988DB31-BD6F-4482-A53F-F82AA2BE2E23'

def test_bea_api_calls():
    """Test different BEA API call configurations"""
    
    api_key = os.environ['BEA_API_KEY']
    base_url = "https://apps.bea.gov/api/data"
    
    print("🔍 Testing BEA API Configurations")
    print("="*60)
    
    # Test 1: Using LineCode='ALL' (current implementation)
    print("\n1. Testing with LineCode='ALL':")
    params = {
        'UserID': api_key,
        'method': 'GetData',
        'datasetname': 'Regional',
        'TableName': 'CAINC30',
        'LineCode': 'ALL',
        'GeoFips': '55000',  # Wisconsin
        'Year': '2023',
        'ResultFormat': 'json'
    }
    
    response = requests.get(base_url, params=params)
    print(f"   Status: {response.status_code}")
    
    if response.status_code == 200:
        data = response.json()
        if 'BEAAPI' in data and 'Results' in data['BEAAPI']:
            results = data['BEAAPI']['Results']
            if 'Data' in results:
                print(f"   Records returned: {len(results['Data'])}")
                # Show first few records
                for i, record in enumerate(results['Data'][:3]):
                    print(f"   Record {i+1}: LineCode={record.get('LineCode')}, "
                          f"DataValue={record.get('DataValue')}, "
                          f"LineName={record.get('LineName', 'N/A')[:50]}")
            else:
                print(f"   No data in results: {results}")
        else:
            print(f"   API Error: {data}")
    
    # Test 2: Get parameter metadata first
    print("\n2. Testing GetParameterValues for line codes:")
    params = {
        'UserID': api_key,
        'method': 'GetParameterValues',
        'datasetname': 'Regional',
        'ParameterName': 'LineCode',
        'TableName': 'CAINC30',
        'ResultFormat': 'json'
    }
    
    response = requests.get(base_url, params=params)
    print(f"   Status: {response.status_code}")
    
    available_line_codes = []
    if response.status_code == 200:
        data = response.json()
        if 'BEAAPI' in data and 'Results' in data['BEAAPI']:
            results = data['BEAAPI']['Results']
            if 'ParamValue' in results:
                print(f"   Available line codes: {len(results['ParamValue'])}")
                for code in results['ParamValue'][:10]:  # Show first 10
                    print(f"   - {code.get('Key')}: {code.get('Description', 'N/A')[:60]}")
                    available_line_codes.append(code.get('Key'))
    
    # Test 3: Try individual line codes
    print("\n3. Testing individual line codes:")
    test_codes = ['1', '2', '3', '10', '11']  # Sample codes
    
    for code in test_codes:
        params = {
            'UserID': api_key,
            'method': 'GetData',
            'datasetname': 'Regional',
            'TableName': 'CAINC30',
            'LineCode': code,
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
                    print(f"   LineCode {code}: {record.get('DataValue')} - "
                          f"{record.get('LineName', 'N/A')[:50]}")
    
    # Test 4: Try with multiple years
    print("\n4. Testing with multiple years (comma-separated):")
    params = {
        'UserID': api_key,
        'method': 'GetData',
        'datasetname': 'Regional',
        'TableName': 'CAINC30',
        'LineCode': '1',  # Total PCE
        'GeoFips': '55000',
        'Year': '2021,2022,2023',
        'ResultFormat': 'json'
    }
    
    response = requests.get(base_url, params=params)
    print(f"   Status: {response.status_code}")
    
    if response.status_code == 200:
        data = response.json()
        if 'BEAAPI' in data and 'Results' in data['BEAAPI']:
            results = data['BEAAPI']['Results']
            if 'Data' in results:
                print(f"   Records returned: {len(results['Data'])}")
                for record in results['Data']:
                    print(f"   Year {record.get('TimePeriod')}: {record.get('DataValue')}")
    
    # Test 5: Check if we need different GeoFips format
    print("\n5. Testing different GeoFips formats:")
    geo_formats = ['55000', '55', '055000', 'STATE:55']
    
    for geo in geo_formats:
        params = {
            'UserID': api_key,
            'method': 'GetData',
            'datasetname': 'Regional',
            'TableName': 'CAINC30',
            'LineCode': '1',
            'GeoFips': geo,
            'Year': '2023',
            'ResultFormat': 'json'
        }
        
        response = requests.get(base_url, params=params)
        if response.status_code == 200:
            data = response.json()
            if 'BEAAPI' in data and 'Results' in data['BEAAPI']:
                results = data['BEAAPI']['Results']
                if 'Data' in results and results['Data']:
                    print(f"   GeoFips '{geo}': Success - {results['Data'][0].get('GeoName')}")
                else:
                    print(f"   GeoFips '{geo}': No data")

if __name__ == "__main__":
    test_bea_api_calls()