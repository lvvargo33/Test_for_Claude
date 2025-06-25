#!/usr/bin/env python3
"""
Test BLS API Series IDs
=======================

Test actual BLS API endpoints and series ID formats to ensure correct data collection.
"""

import requests
import json

def test_bls_api():
    """Test BLS API with known series IDs"""
    
    # Test with known working series IDs
    test_series = [
        # National unemployment rate
        "LNS14000000",
        
        # Wisconsin state unemployment rate  
        "LASST550000000000003",
        
        # Dane County (Madison) LAUS unemployment rate
        "LAUCN550250000000003",
        
        # Milwaukee County LAUS unemployment rate
        "LAUCN550790000000003"
    ]
    
    data = {
        'seriesid': test_series,
        'startyear': '2022',
        'endyear': '2023'
    }
    
    response = requests.post(
        "https://api.bls.gov/publicAPI/v2/timeseries/data/",
        json=data,
        timeout=30
    )
    
    print(f"Response status: {response.status_code}")
    
    if response.status_code == 200:
        result = response.json()
        print(f"API Status: {result.get('status')}")
        print(f"Message: {result.get('message', 'No message')}")
        
        if 'Results' in result and 'series' in result['Results']:
            print(f"\nFound {len(result['Results']['series'])} series:")
            
            for series in result['Results']['series']:
                series_id = series['seriesID']
                data_count = len(series.get('data', []))
                print(f"  {series_id}: {data_count} data points")
                
                # Show sample data
                if data_count > 0:
                    sample = series['data'][0]
                    print(f"    Sample: {sample.get('year')}-{sample.get('period')} = {sample.get('value')}")
        
        return result
    else:
        print(f"Error: {response.text}")
        return None

def test_wisconsin_county_series():
    """Test Wisconsin county series ID formats"""
    
    # Test different series ID formats for Wisconsin counties
    test_formats = [
        # LAUS format variations
        "LAUCN550250000000003",  # Dane County unemployment rate (standard format)
        "LAUCN550250000000004",  # Dane County unemployment level
        "LAUCN550250000000005",  # Dane County employment level
        "LAUCN550250000000006",  # Dane County labor force
        
        # QCEW format - need to find correct format
        # These might not work - need to research QCEW series IDs
        "ENUCS55025540",         # Attempt 1
        "ENS5502500000000540",   # Attempt 2
    ]
    
    print("\nTesting Wisconsin County Series IDs:")
    print("=" * 50)
    
    for series_id in test_formats:
        data = {
            'seriesid': [series_id],
            'startyear': '2022',
            'endyear': '2023'
        }
        
        response = requests.post(
            "https://api.bls.gov/publicAPI/v2/timeseries/data/",
            json=data,
            timeout=10
        )
        
        if response.status_code == 200:
            result = response.json()
            if result.get('status') == 'REQUEST_SUCCEEDED':
                series_data = result.get('Results', {}).get('series', [])
                if series_data and len(series_data[0].get('data', [])) > 0:
                    print(f"✅ {series_id}: WORKING - {len(series_data[0]['data'])} data points")
                else:
                    print(f"❌ {series_id}: No data returned")
            else:
                print(f"❌ {series_id}: {result.get('message', 'Request failed')}")
        else:
            print(f"❌ {series_id}: HTTP {response.status_code}")

def main():
    """Main test function"""
    print("🧪 BLS API Series ID Testing")
    print("=" * 40)
    
    # Test basic API functionality
    print("Testing basic BLS API...")
    result = test_bls_api()
    
    if result:
        print("\n✅ Basic API test successful!")
        
        # Test Wisconsin-specific series
        test_wisconsin_county_series()
    else:
        print("\n❌ Basic API test failed!")

if __name__ == "__main__":
    main()