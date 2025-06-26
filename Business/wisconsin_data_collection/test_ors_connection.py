#!/usr/bin/env python3
"""Test OpenRouteService API connection"""

import requests
import os

# Test the API key
ORS_API_KEY = "5b3ce3597851110001cf6248d33b6d517e6840ddaebe04692fec12ec"

# Test isochrone generation for Madison, WI  
url = "https://api.openrouteservice.org/v2/isochrones"
headers = {
    'Authorization': ORS_API_KEY,
    'Content-Type': 'application/json'
}

# Test with GET request instead
params = {
    'locations': '-89.4012,43.0731',
    'range': '300',
    'profile': 'driving-car',
    'api_key': ORS_API_KEY
}

# Try GET request
response_get = requests.get("https://api.openrouteservice.org/v2/isochrones/driving-car", params=params)
print(f"GET Status: {response_get.status_code}")
if response_get.status_code != 200:
    print("GET response:", response_get.text)

# Also try POST with corrected format
body = {
    "locations": [[-89.4012, 43.0731]],  # Madison, WI (lon, lat) 
    "range": [300, 600, 900],  # 5, 10, 15 minutes in seconds
    "range_type": "time"
}

try:
    # Try POST to the driving-car specific endpoint
    url_post = "https://api.openrouteservice.org/v2/isochrones/driving-car"
    response = requests.post(url_post, json=body, headers=headers)
    print(f"POST Status Code: {response.status_code}")
    
    if response.status_code == 200:
        data = response.json()
        print("✅ OpenRouteService API working!")
        print(f"Generated {len(data['features'])} isochrones")
        for i, feature in enumerate(data['features']):
            range_seconds = feature['properties']['value']
            range_minutes = range_seconds / 60
            print(f"  Isochrone {i+1}: {range_minutes} minutes")
    else:
        print(f"❌ API Error: {response.status_code}")
        print(response.text)
        
except Exception as e:
    print(f"❌ Connection Error: {e}")