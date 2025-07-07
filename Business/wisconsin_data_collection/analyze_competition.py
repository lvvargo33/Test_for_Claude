#!/usr/bin/env python3
"""
Competitive Analysis for Section 2.1
Analyze competition around 5264 Anton Dr, Fitchburg, WI
"""

import pandas as pd
import numpy as np
from math import radians, cos, sin, asin, sqrt
import json

def haversine(lon1, lat1, lon2, lat2):
    """Calculate distance between two points using haversine formula"""
    lon1, lat1, lon2, lat2 = map(radians, [lon1, lat1, lon2, lat2])
    dlon = lon2 - lon1 
    dlat = lat2 - lat1 
    a = sin(dlat/2)**2 + cos(lat1) * cos(lat2) * sin(dlon/2)**2
    c = 2 * asin(sqrt(a)) 
    r = 3959  # Radius of earth in miles
    return c * r

def analyze_competition():
    print('🔍 SECTION 2.1 COMPETITIVE ANALYSIS')
    print('=' * 70)
    
    # Target site coordinates - 5264 Anton Dr, Fitchburg, WI
    site_lat = 43.0265
    site_lng = -89.4698
    site_name = "5264 Anton Dr, Fitchburg"
    
    print(f'Target Site: {site_name}')
    print(f'Coordinates: {site_lat}, {site_lng}')
    
    # Load Google Places data
    df = pd.read_csv('google_places_phase1_20250627_212804.csv')
    print(f'\n📊 Loaded {len(df)} businesses from Google Places data')
    
    # Calculate distances
    df['distance_miles'] = df.apply(lambda row: haversine(
        site_lng, site_lat, 
        row['geometry_location_lng'], row['geometry_location_lat']
    ), axis=1)
    
    # Get businesses within 5 miles
    nearby = df[df['distance_miles'] <= 5.0].copy().sort_values('distance_miles')
    print(f'Businesses within 5 miles: {len(nearby)}')
    
    # Filter for restaurants
    restaurants = nearby[
        (nearby['business_category'] == 'Restaurant') |
        (nearby['types'].str.contains('restaurant', case=False, na=False)) |
        (nearby['types'].str.contains('food', case=False, na=False))
    ].copy()
    
    print(f'\n🍽️ RESTAURANTS WITHIN 5 MILES: {len(restaurants)}')
    
    # Restaurant details
    if len(restaurants) > 0:
        print(f'\n📋 RESTAURANT COMPETITOR PROFILES:')
        for i, (idx, rest) in enumerate(restaurants.iterrows(), 1):
            print(f'\n{i}. {rest["name"]}')
            print(f'   📍 Distance: {rest["distance_miles"]:.2f} miles')
            print(f'   🏠 Address: {rest.get("vicinity", "N/A")}')
            print(f'   ⭐ Rating: {rest.get("rating", "N/A")} stars')
            print(f'   👥 Reviews: {rest.get("user_ratings_total", "N/A")}')
            if pd.notna(rest.get("price_level")):
                price = "$" * int(rest.get("price_level", 1))
                print(f'   💰 Price Level: {price}')
            print(f'   🏢 Category: {rest.get("business_category", "N/A")}')
    
    # Competitive density analysis
    print(f'\n📏 COMPETITIVE DENSITY ANALYSIS:')
    density_results = {}
    
    for radius in [1, 3, 5]:
        radius_restaurants = restaurants[restaurants['distance_miles'] <= radius]
        area_sq_miles = 3.14159 * radius * radius
        density = len(radius_restaurants) / area_sq_miles
        
        density_results[radius] = {
            'count': len(radius_restaurants),
            'density': density,
            'avg_rating': radius_restaurants['rating'].mean() if len(radius_restaurants) > 0 else 0,
            'avg_reviews': radius_restaurants['user_ratings_total'].mean() if len(radius_restaurants) > 0 else 0
        }
        
        print(f'\n{radius}-Mile Radius:')
        print(f'  Total Restaurants: {len(radius_restaurants)}')
        print(f'  Density: {density:.2f} restaurants per sq mile')
        
        if len(radius_restaurants) > 0:
            print(f'  Average Rating: {density_results[radius]["avg_rating"]:.1f} stars')
            print(f'  Average Reviews: {density_results[radius]["avg_reviews"]:.0f}')
    
    # Search for Indian restaurants specifically
    print(f'\n🇮🇳 INDIAN RESTAURANT ANALYSIS:')
    indian_keywords = ['indian', 'curry', 'tandoor', 'biryani', 'masala', 'bollywood']
    
    indian_restaurants = nearby[
        nearby['name'].str.lower().str.contains('|'.join(indian_keywords), na=False) |
        nearby['types'].str.lower().str.contains('|'.join(indian_keywords), na=False)
    ]
    
    print(f'Indian restaurants within 5 miles: {len(indian_restaurants)}')
    
    for radius in [1, 3, 5]:
        indian_in_radius = indian_restaurants[indian_restaurants['distance_miles'] <= radius]
        print(f'{radius}-mile radius: {len(indian_in_radius)} Indian restaurants')
    
    # Ethnic cuisine analysis
    print(f'\n🌍 ETHNIC CUISINE COMPETITION:')
    ethnic_cuisines = {
        'Chinese': ['chinese', 'china', 'wok'],
        'Thai': ['thai', 'thailand', 'pad thai'],
        'Japanese': ['japanese', 'sushi', 'hibachi', 'ramen'],
        'Mexican': ['mexican', 'taco', 'burrito'],
        'Italian': ['italian', 'pizza', 'pasta'],
        'Mediterranean': ['mediterranean', 'greek', 'gyro'],
        'Vietnamese': ['vietnamese', 'pho'],
        'Korean': ['korean', 'bbq']
    }
    
    ethnic_summary = {}
    for cuisine, keywords in ethnic_cuisines.items():
        matches = nearby[
            nearby['name'].str.lower().str.contains('|'.join(keywords), na=False) |
            nearby['types'].str.lower().str.contains('|'.join(keywords), na=False)
        ]
        
        # Filter to 3-mile radius for ethnic competition
        matches_3mi = matches[matches['distance_miles'] <= 3.0]
        
        ethnic_summary[cuisine] = len(matches_3mi)
        if len(matches_3mi) > 0:
            print(f'{cuisine}: {len(matches_3mi)} restaurants (3-mile radius)')
            for _, rest in matches_3mi.iterrows():
                print(f'  • {rest["name"]} - {rest["distance_miles"]:.1f}mi - {rest.get("rating", "N/A")}⭐')
    
    return density_results, ethnic_summary

if __name__ == "__main__":
    analyze_competition()