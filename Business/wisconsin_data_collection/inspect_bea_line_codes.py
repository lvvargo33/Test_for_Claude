#!/usr/bin/env python3
"""
Inspect BEA Line Codes
======================

Get the actual line code descriptions from BEA API.
"""

import os
import requests

# Set BEA API key
os.environ['BEA_API_KEY'] = '1988DB31-BD6F-4482-A53F-F82AA2BE2E23'

def get_line_descriptions():
    """Get the actual line descriptions from BEA"""
    
    api_key = os.environ['BEA_API_KEY']
    base_url = "https://apps.bea.gov/api/data"
    
    print("🔍 Getting BEA CAINC30 Line Descriptions")
    print("="*60)
    
    # Get metadata about line codes
    params = {
        'UserID': api_key,
        'method': 'GetParameterValues',
        'datasetname': 'Regional',
        'ParameterName': 'LineCode',
        'TableName': 'CAINC30',
        'ResultFormat': 'json'
    }
    
    response = requests.get(base_url, params=params)
    
    if response.status_code == 200:
        data = response.json()
        
        if 'BEAAPI' in data and 'Results' in data['BEAAPI']:
            results = data['BEAAPI']['Results']
            
            if 'ParamValue' in results:
                line_codes = results['ParamValue']
                
                # Display first 30 line codes
                print(f"\nFound {len(line_codes)} line codes. First 30:")
                print("-"*80)
                print(f"{'Code':>6} | {'Description'}")
                print("-"*80)
                
                for i, code_info in enumerate(line_codes[:30]):
                    code = code_info.get('Key', '')
                    desc = code_info.get('Description', code_info.get('Desc', 'N/A'))
                    
                    # Clean up description
                    if desc and len(desc) > 70:
                        desc = desc[:67] + "..."
                    
                    print(f"{code:>6} | {desc}")
                
                # Now get actual data to see the order
                print("\n\nActual data order from API (2023):")
                print("-"*80)
                
                params = {
                    'UserID': api_key,
                    'method': 'GetData',
                    'datasetname': 'Regional',
                    'TableName': 'CAINC30',
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
                        
                        if 'Data' in results:
                            records = results['Data']
                            
                            print(f"Order | {'Value':>12} | Line Description")
                            print("-"*80)
                            
                            for i, record in enumerate(records):
                                value = record.get('DataValue', '0')
                                # The records should have line descriptions
                                desc = record.get('Description', record.get('LineName', f'Record {i+1}'))
                                
                                if len(desc) > 60:
                                    desc = desc[:57] + "..."
                                
                                print(f"{i+1:>5} | {value:>12} | {desc}")

if __name__ == "__main__":
    get_line_descriptions()