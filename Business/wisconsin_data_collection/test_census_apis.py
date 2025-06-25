#!/usr/bin/env python3
"""
Test Census APIs to verify data availability
"""

import requests
import json
from datetime import datetime

API_KEY = "dd75feaae49ed1a1884869cf57289ceacb0962f5"

def test_acs_years():
    """Test which ACS years are available"""
    print("Testing ACS 5-year estimates availability...")
    print("=" * 60)
    
    years_to_test = list(range(2013, 2024))
    available_years = []
    
    for year in years_to_test:
        url = f"https://api.census.gov/data/{year}/acs/acs5"
        params = {
            'get': 'B01003_001E,NAME',  # Total population
            'for': 'county:025',  # Dane County
            'in': 'state:55',    # Wisconsin
            'key': API_KEY
        }
        
        try:
            response = requests.get(url, params=params, timeout=10)
            if response.status_code == 200:
                data = response.json()
                if len(data) > 1:
                    available_years.append(year)
                    print(f"✓ {year}: Available - {data[1][1]} - Population: {data[1][0]}")
            else:
                print(f"✗ {year}: Not available (Status: {response.status_code})")
        except Exception as e:
            print(f"✗ {year}: Error - {str(e)}")
    
    print(f"\nAvailable ACS years: {available_years}")
    return available_years

def test_pep_years():
    """Test Population Estimates Program data availability"""
    print("\n\nTesting Population Estimates Program (PEP) availability...")
    print("=" * 60)
    
    # Test different PEP endpoints
    pep_endpoints = [
        {'year': 2019, 'endpoint': '/2019/pep/population'},
        {'year': 2022, 'endpoint': '/2022/pep/population'},
        {'year': 2023, 'endpoint': '/2023/pep/population'},
    ]
    
    for config in pep_endpoints:
        url = f"https://api.census.gov/data{config['endpoint']}"
        params = {
            'get': 'NAME,POP',
            'for': 'county:025',
            'in': 'state:55',
            'key': API_KEY
        }
        
        try:
            response = requests.get(url, params=params, timeout=10)
            if response.status_code == 200:
                data = response.json()
                print(f"✓ {config['year']} PEP: Available")
                print(f"  Variables: {data[0]}")
                if len(data) > 1:
                    print(f"  Sample: {data[1]}")
            else:
                print(f"✗ {config['year']} PEP: Not available (Status: {response.status_code})")
        except Exception as e:
            print(f"✗ {config['year']} PEP: Error - {str(e)}")

def test_recent_population_data():
    """Test what recent population data is available"""
    print("\n\nTesting recent population estimates (2020-2023)...")
    print("=" * 60)
    
    # Test vintage 2023 estimates
    url = "https://api.census.gov/data/2023/pep/population"
    params = {
        'get': 'NAME,POP_2020,POP_2021,POP_2022,POP_2023,NPOPCHG_2022,BIRTHS2022,DEATHS2022',
        'for': 'county:*',
        'in': 'state:55',
        'key': API_KEY
    }
    
    try:
        response = requests.get(url, params=params, timeout=10)
        if response.status_code == 200:
            data = response.json()
            print(f"✓ 2023 Vintage Population Estimates: Available")
            print(f"  Counties found: {len(data) - 1}")
            print(f"  Available fields: {data[0]}")
            
            # Show sample data for first 3 counties
            print("\n  Sample data:")
            for i in range(1, min(4, len(data))):
                county_data = dict(zip(data[0], data[i]))
                print(f"    {county_data['NAME']}:")
                print(f"      2020: {county_data.get('POP_2020', 'N/A')}")
                print(f"      2021: {county_data.get('POP_2021', 'N/A')}")
                print(f"      2022: {county_data.get('POP_2022', 'N/A')}")
                print(f"      2023: {county_data.get('POP_2023', 'N/A')}")
        else:
            print(f"✗ 2023 Vintage not available (Status: {response.status_code})")
    except Exception as e:
        print(f"✗ 2023 Vintage error: {str(e)}")

def main():
    """Run all tests"""
    print(f"Census API Testing - {datetime.now().strftime('%Y-%m-%d %H:%M:%S')}")
    print("=" * 80)
    
    # Test ACS availability
    acs_years = test_acs_years()
    
    # Test PEP availability
    test_pep_years()
    
    # Test recent population data
    test_recent_population_data()
    
    print("\n" + "=" * 80)
    print("Summary:")
    print(f"- ACS 5-year estimates available: {min(acs_years)} - {max(acs_years)}")
    print("- Population estimates available through vintage 2023 API")
    print("- Can collect population data from 2013-2023")

if __name__ == "__main__":
    main()