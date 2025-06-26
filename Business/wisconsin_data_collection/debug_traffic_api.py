"""
Debug Wisconsin DOT Traffic API
===============================

Debug the actual API response structure to fix field mapping.
"""

import requests
import json

def debug_traffic_api():
    print("DEBUGGING WISCONSIN DOT TRAFFIC API")
    print("=" * 50)
    
    # Wisconsin DOT traffic counts endpoint
    url = "https://data-wisdot.opendata.arcgis.com/datasets/WisDOT::traffic-counts.geojson"
    
    try:
        print(f"Requesting: {url}")
        response = requests.get(url, timeout=30)
        response.raise_for_status()
        
        data = response.json()
        
        print(f"Response status: {response.status_code}")
        print(f"Response type: {type(data)}")
        
        if 'features' in data:
            print(f"Total features: {len(data['features'])}")
            
            if data['features']:
                # Look at first feature
                first_feature = data['features'][0]
                print(f"\nFirst feature structure:")
                print(f"- Geometry type: {first_feature.get('geometry', {}).get('type')}")
                print(f"- Properties keys: {list(first_feature.get('properties', {}).keys())}")
                
                # Show sample properties
                properties = first_feature.get('properties', {})
                print(f"\nSample properties:")
                for key, value in list(properties.items())[:15]:  # First 15 fields
                    print(f"  {key}: {value}")
                
                # Look for traffic-related fields
                traffic_fields = []
                for key in properties.keys():
                    key_lower = key.lower()
                    if any(term in key_lower for term in ['traffic', 'aadt', 'volume', 'count', 'route', 'highway', 'road']):
                        traffic_fields.append((key, properties[key]))
                
                if traffic_fields:
                    print(f"\nTraffic-related fields:")
                    for key, value in traffic_fields:
                        print(f"  {key}: {value}")
                else:
                    print(f"\nNo obvious traffic fields found. All fields:")
                    for key, value in properties.items():
                        print(f"  {key}: {value}")
        
        else:
            print("No 'features' key in response")
            print(f"Response keys: {list(data.keys()) if isinstance(data, dict) else 'Not a dict'}")
        
    except Exception as e:
        print(f"Error: {e}")

if __name__ == "__main__":
    debug_traffic_api()