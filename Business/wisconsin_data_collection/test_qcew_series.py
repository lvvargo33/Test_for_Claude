#!/usr/bin/env python3
"""Test QCEW series ID formats for Wisconsin counties"""

import requests
import json

def test_qcew_formats():
    """Test different QCEW series ID formats"""
    
    # Test different QCEW series ID formats
    test_series = [
        # Format attempts for Dane County (55025)
        "ENU55025105000000",      # Employment, all industries, private
        "ENU5502510000000",       # Shorter format
        "ENUC55025105000000",     # With 'C' for county
        "ENU55025105",            # Minimal format
        "EWU55025105000000",      # Try EWU prefix (wages)
        
        # Try state level to verify format
        "ENU5500000510",          # Wisconsin state level
        "ENUSS00000510",          # US format for comparison
    ]
    
    print("Testing QCEW Series ID Formats:")
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
                        sample = series_data[0]['data'][0]
                        print(f"✅ {series_id}: WORKING - {data_count} data points")
                        print(f"   Sample: {sample.get('year')}-{sample.get('period')} = {sample.get('value')}")
                    else:
                        print(f"❌ {series_id}: No data returned")
                else:
                    messages = result.get('message', [])
                    print(f"❌ {series_id}: {messages}")
            else:
                print(f"❌ {series_id}: HTTP {response.status_code}")
                
        except Exception as e:
            print(f"❌ {series_id}: Error - {str(e)}")

if __name__ == "__main__":
    test_qcew_formats()