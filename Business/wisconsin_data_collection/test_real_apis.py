#!/usr/bin/env python3
"""
Test Real API Connections
=========================

Test LoopNet and BEA API connections with provided keys.
"""

import os
import logging
import requests
import json
from datetime import datetime

logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)

def test_loopnet_api():
    """Test LoopNet API connection with subscription check"""
    
    print("🏢 Testing LoopNet API Connection (Updated)")
    print("="*50)
    
    # Set up API key
    loopnet_key = '076f3014a8msh724b957d1c5e85ep125678jsn4335aaddbc1a'
    
    # Common LoopNet API endpoints through RapidAPI
    endpoints_to_try = [
        {
            'name': 'LoopNet Properties Search',
            'url': 'https://loopnet-com.p.rapidapi.com/properties/search',
            'host': 'loopnet-com.p.rapidapi.com'
        },
        {
            'name': 'LoopNet Property Details',
            'url': 'https://loopnet-com.p.rapidapi.com/property/details',
            'host': 'loopnet-com.p.rapidapi.com'
        },
        {
            'name': 'Alternative LoopNet API',
            'url': 'https://loopnet1.p.rapidapi.com/search',
            'host': 'loopnet1.p.rapidapi.com'
        }
    ]
    
    for endpoint in endpoints_to_try:
        print(f"\n📍 Testing: {endpoint['name']}")
        
        headers = {
            'X-RapidAPI-Key': loopnet_key,
            'X-RapidAPI-Host': endpoint['host']
        }
        
        # Test with simple Milwaukee search
        params = {
            'location': 'Milwaukee, WI',
            'propertyType': 'office',
            'limit': 3
        }
        
        try:
            response = requests.get(endpoint['url'], headers=headers, params=params, timeout=15)
            
            print(f"   Status Code: {response.status_code}")
            
            if response.status_code == 200:
                print("   ✅ SUCCESS: API connection working!")
                try:
                    data = response.json()
                    print(f"   📊 Response structure: {list(data.keys())[:5]}")
                    
                    # Check if we got property data
                    if isinstance(data, dict):
                        if 'properties' in data:
                            print(f"   🏢 Found {len(data.get('properties', []))} properties")
                        elif 'results' in data:
                            print(f"   🏢 Found {len(data.get('results', []))} results")
                        else:
                            print(f"   📄 Response keys: {list(data.keys())}")
                    
                    return True, data
                    
                except json.JSONDecodeError:
                    print(f"   📄 Response (first 200 chars): {response.text[:200]}")
                    return True, response.text
                    
            elif response.status_code == 403:
                print(f"   ❌ Forbidden: {response.text[:100]}")
                if "not subscribed" in response.text.lower():
                    print("   ➡️  Still not subscribed to this specific endpoint")
                else:
                    print("   ➡️  Different access issue")
                    
            elif response.status_code == 401:
                print(f"   ❌ Unauthorized: {response.text[:100]}")
                print("   ➡️  API key may be invalid")
                
            else:
                print(f"   ⚠️  Status {response.status_code}: {response.text[:100]}")
                
        except requests.exceptions.Timeout:
            print("   ⏰ Request timed out")
        except Exception as e:
            print(f"   ❌ Error: {e}")
    
    return False, None

def test_bea_api():
    """Test BEA API connection"""
    
    print("\n💰 Testing BEA API Connection")
    print("="*50)
    
    bea_key = '1988DB31-BD6F-4482-A53F-F82AA2BE2E23'
    
    # Test BEA Regional Data API
    base_url = "https://apps.bea.gov/api/data"
    
    # Test with Wisconsin personal consumption expenditures
    params = {
        'UserID': bea_key,
        'method': 'GetData',
        'datasetname': 'Regional',
        'TableName': 'CAINC30',  # Personal consumption expenditures
        'LineCode': '1',  # Total PCE
        'GeoFips': '55000',  # Wisconsin FIPS
        'Year': '2022',
        'ResultFormat': 'json'
    }
    
    try:
        print("📊 Testing Wisconsin PCE data request...")
        response = requests.get(base_url, params=params, timeout=15)
        
        print(f"Status Code: {response.status_code}")
        
        if response.status_code == 200:
            try:
                data = response.json()
                print("✅ SUCCESS: BEA API connection working!")
                
                # Check response structure
                if 'BEAAPI' in data:
                    bea_data = data['BEAAPI']
                    print(f"📊 Response structure: {list(bea_data.keys())}")
                    
                    if 'Results' in bea_data:
                        results = bea_data['Results']
                        if 'Data' in results:
                            data_records = results['Data']
                            print(f"📈 Found {len(data_records)} data records")
                            
                            # Show sample data
                            if data_records:
                                sample = data_records[0]
                                print(f"📄 Sample record keys: {list(sample.keys())}")
                                print(f"📍 Sample data: {sample}")
                        else:
                            print(f"📋 Results keys: {list(results.keys())}")
                    else:
                        print(f"🔍 BEAAPI keys: {list(bea_data.keys())}")
                
                return True, data
                
            except json.JSONDecodeError:
                print(f"📄 Non-JSON response: {response.text[:200]}")
                return False, response.text
                
        else:
            print(f"❌ Failed with status {response.status_code}")
            print(f"📄 Response: {response.text[:300]}")
            return False, response.text
            
    except Exception as e:
        print(f"❌ Error testing BEA API: {e}")
        return False, str(e)

def analyze_adhoc_vs_automated():
    """Analyze ad hoc vs automated approach for county GIS and SBA data"""
    
    print("\n🤔 Ad Hoc vs Automated Data Collection Analysis")
    print("="*80)
    
    analysis = {
        'county_gis': {
            'ad_hoc_pros': [
                '✅ Faster initial implementation',
                '✅ Can target specific counties as needed',
                '✅ Less complex infrastructure',
                '✅ Good for testing data quality first'
            ],
            'ad_hoc_cons': [
                '❌ Manual effort for each county',
                '❌ Data gets stale over time',
                '❌ Inconsistent update schedules',
                '❌ Harder to scale to all 72 counties'
            ],
            'automated_pros': [
                '✅ Consistent, fresh data',
                '✅ Scalable to all counties',
                '✅ Scheduled updates',
                '✅ Better for production analysis'
            ],
            'automated_cons': [
                '❌ More complex setup',
                '❌ Need to handle API changes',
                '❌ Rate limiting considerations',
                '❌ Error handling complexity'
            ],
            'recommendation': 'HYBRID: Start ad hoc for top 5 counties, automate the successful ones'
        },
        
        'sba_reports': {
            'ad_hoc_pros': [
                '✅ Can target specific reports needed',
                '✅ Manual quality control',
                '✅ Can handle PDF format changes',
                '✅ Lower risk of being blocked'
            ],
            'ad_hoc_cons': [
                '❌ Reports update annually/semi-annually',
                '❌ Manual effort to check for updates',
                '❌ May miss new reports',
                '❌ Time-consuming extraction'
            ],
            'automated_pros': [
                '✅ Never miss report updates',
                '✅ Consistent data extraction',
                '✅ Can monitor multiple report types',
                '✅ Better for ongoing analysis'
            ],
            'automated_cons': [
                '❌ PDF parsing can be brittle',
                '❌ Website structure changes break automation',
                '❌ May get blocked by anti-bot measures',
                '❌ Complex error handling needed'
            ],
            'recommendation': 'AD HOC: Reports change infrequently, manual extraction is more reliable'
        }
    }
    
    print("🗺️  COUNTY GIS DATA APPROACH:")
    print("   Recommendation: HYBRID (Ad hoc → Automate successful counties)")
    print("   Reasoning:")
    print("   • Start with top 5 counties manually to test data quality")
    print("   • Automate counties with reliable APIs once validated")
    print("   • Keep smaller counties ad hoc (updates less critical)")
    
    print("\n📈 SBA ADVOCACY REPORTS APPROACH:")
    print("   Recommendation: AD HOC (Manual extraction)")
    print("   Reasoning:")
    print("   • Reports update only annually/semi-annually")
    print("   • PDF format changes frequently break automation")
    print("   • Manual extraction ensures data quality")
    print("   • Can be done quarterly as a batch process")
    
    return analysis

def main():
    """Test all APIs and provide recommendations"""
    
    print("🧪 TESTING REAL API CONNECTIONS")
    print("="*80)
    
    # Test LoopNet API
    loopnet_success, loopnet_data = test_loopnet_api()
    
    # Test BEA API
    bea_success, bea_data = test_bea_api()
    
    # Analyze ad hoc vs automated approaches
    analysis = analyze_adhoc_vs_automated()
    
    # Summary
    print("\n📊 API TEST SUMMARY")
    print("="*50)
    print(f"🏢 LoopNet API: {'✅ Working' if loopnet_success else '❌ Issues'}")
    print(f"💰 BEA API: {'✅ Working' if bea_success else '❌ Issues'}")
    
    print("\n🎯 RECOMMENDED APPROACH")
    print("="*50)
    print("• County GIS: 🔄 HYBRID (Ad hoc → Automate)")
    print("• SBA Reports: 📋 AD HOC (Manual quarterly)")
    print("• LoopNet: 🔄 API Integration (if working)")
    print("• BEA: 🔄 API Integration (if working)")

if __name__ == "__main__":
    main()