#!/usr/bin/env python3
"""Test QCEW data availability through different BLS endpoints"""

import requests
import json

def test_qcew_availability():
    """Test if QCEW data is available through the timeseries API"""
    
    # According to BLS documentation, QCEW data might need different endpoints
    # Let's test some known working QCEW series from their examples
    
    test_series = [
        # From BLS QCEW documentation examples
        "ENUN5102050510133",      # Example from BLS docs
        "ENUN06001050510133",     # Another example
        
        # Try CEW prefix (Census of Employment and Wages)
        "CEWN550250105101",       # Dane County attempt with CEW
        "CEWN550250105",          # Shorter version
        
        # Try with different area type codes
        "ENU5C5025105000000",     # With area type C for county
        "ENUCS55025105",          # Shorter with CS prefix
    ]
    
    print("Testing QCEW Data Availability:")
    print("=" * 60)
    
    for series_id in test_series:
        data = {
            'seriesid': [series_id],
            'startyear': '2023',
            'endyear': '2023', 
            'registrationkey': 'c177d400482b4df282ff74850f23a7d9'
        }
        
        try:
            response = requests.post(
                "https://api.bls.gov/publicAPI/v2/timeseries/data/",
                json=data,
                timeout=10
            )
            
            if response.status_code == 200:
                result = response.json()
                status = result.get('status')
                
                if status == 'REQUEST_SUCCEEDED':
                    series_data = result.get('Results', {}).get('series', [])
                    if series_data and len(series_data[0].get('data', [])) > 0:
                        data_count = len(series_data[0]['data'])
                        print(f"✅ {series_id}: WORKING - {data_count} data points")
                    else:
                        print(f"❌ {series_id}: No data")
                else:
                    print(f"❌ {series_id}: {result.get('message', [])}")
            else:
                print(f"❌ {series_id}: HTTP {response.status_code}")
                
        except Exception as e:
            print(f"❌ {series_id}: Error - {str(e)}")

    # Also check what data IS available for Wisconsin
    print("\n\nChecking Wisconsin state-level series:")
    wi_series = [
        "SMS55000000000000001",   # State employment
        "LASST550000000000003",   # State unemployment rate (known working)
        "CEUS5500000001",         # CE series for Wisconsin
    ]
    
    for series_id in wi_series:
        data = {
            'seriesid': [series_id],
            'startyear': '2023',
            'endyear': '2023',
            'registrationkey': 'c177d400482b4df282ff74850f23a7d9'
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
                    print(f"✅ {series_id}: {len(series_data[0]['data'])} data points")

if __name__ == "__main__":
    test_qcew_availability()