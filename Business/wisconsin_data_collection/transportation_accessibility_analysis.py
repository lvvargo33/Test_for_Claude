#!/usr/bin/env python3
"""
Transportation Accessibility Analysis
====================================

Analyzes transportation accessibility including highways, public transit, and traffic patterns.
Uses OpenStreetMap data for road networks and integrates with Census transportation data.
"""

import logging
import pandas as pd
import numpy as np
import requests
import json
from typing import Dict, List, Tuple, Optional, Any
from dataclasses import dataclass
from google.cloud import bigquery
import math
from datetime import datetime
import time

@dataclass
class HighwayAccess:
    """Highway accessibility information"""
    highway_name: str
    highway_type: str  # 'interstate', 'us_highway', 'state_highway'
    distance_miles: float
    access_point: str  # 'on_ramp', 'exit', 'intersection'
    access_lat: float
    access_lon: float

@dataclass
class PublicTransitAccess:
    """Public transit accessibility information"""
    transit_type: str  # 'bus_stop', 'train_station', 'subway_station'
    stop_name: str
    distance_miles: float
    routes: List[str]
    frequency: str  # 'high', 'medium', 'low'

@dataclass
class TransportationAccessibilityResult:
    """Results of transportation accessibility analysis"""
    site_latitude: float
    site_longitude: float
    highway_accesses: List[HighwayAccess]
    transit_accesses: List[PublicTransitAccess]
    nearest_interstate_miles: float
    nearest_major_highway_miles: float
    transit_accessibility_score: float  # 0-100
    highway_accessibility_score: float  # 0-100
    overall_accessibility_score: float  # 0-100
    commute_score: float  # Based on Census data
    traffic_level: str  # 'Low', 'Medium', 'High'
    accessibility_rating: str  # 'Excellent', 'Good', 'Fair', 'Poor'

class TransportationAccessibilityAnalyzer:
    """Analyzes transportation accessibility for site selection"""
    
    def __init__(self, credentials_path: str = "location-optimizer-1-449414f93a5a.json"):
        self.client = bigquery.Client.from_service_account_json(credentials_path)
        self.logger = self._setup_logging()
        
        # Analysis parameters
        self.search_radius_miles = 10
        self.highway_search_radius = 25  # Larger radius for highways
        self.transit_search_radius = 2   # Smaller radius for transit
        
        # Overpass API for OpenStreetMap data
        self.overpass_url = "http://overpass-api.de/api/interpreter"
        
    def _setup_logging(self) -> logging.Logger:
        """Setup logging"""
        logger = logging.getLogger('transportation_analyzer')
        logger.setLevel(logging.INFO)
        
        if not logger.handlers:
            handler = logging.FileHandler('location_optimizer.log')
            formatter = logging.Formatter('%(asctime)s - %(name)s - %(levelname)s - %(message)s')
            handler.setFormatter(formatter)
            logger.addHandler(handler)
            
        return logger
    
    def _haversine_distance(self, lat1: float, lon1: float, lat2: float, lon2: float) -> float:
        """Calculate distance between two points in miles"""
        R = 3959  # Earth's radius in miles
        
        lat1_rad = math.radians(lat1)
        lat2_rad = math.radians(lat2)
        delta_lat = math.radians(lat2 - lat1)
        delta_lon = math.radians(lon2 - lon1)
        
        a = (math.sin(delta_lat / 2) ** 2 + 
             math.cos(lat1_rad) * math.cos(lat2_rad) * math.sin(delta_lon / 2) ** 2)
        c = 2 * math.atan2(math.sqrt(a), math.sqrt(1 - a))
        
        return R * c
    
    def _query_overpass(self, query: str) -> Optional[Dict]:
        """Query Overpass API for OpenStreetMap data"""
        try:
            response = requests.post(self.overpass_url, data=query, timeout=30)
            response.raise_for_status()
            return response.json()
        except Exception as e:
            self.logger.warning(f"Overpass API query failed: {str(e)}")
            return None
    
    def find_highway_access(self, latitude: float, longitude: float) -> List[HighwayAccess]:
        """Find nearby highway access points"""
        self.logger.info(f"Finding highway access near {latitude}, {longitude}")
        
        # Convert miles to degrees (approximate)
        radius_deg = self.highway_search_radius / 69.0
        
        # Overpass query for highways
        query = f"""
        [out:json][timeout:25];
        (
          way["highway"~"^(motorway|trunk|primary)$"]
            (around:{self.highway_search_radius * 1609},{latitude},{longitude});
          way["ref"~"^(I-|US |WI )"]
            (around:{self.highway_search_radius * 1609},{latitude},{longitude});
        );
        out geom;
        """
        
        result = self._query_overpass(query)
        highway_accesses = []
        
        if result and 'elements' in result:
            for element in result['elements']:
                if 'tags' in element and 'geometry' in element:
                    tags = element['tags']
                    highway_type = tags.get('highway', '')
                    ref = tags.get('ref', '')
                    name = tags.get('name', ref)
                    
                    # Determine highway type
                    if highway_type == 'motorway' or (ref and ref.startswith('I-')):
                        hw_type = 'interstate'
                    elif highway_type == 'trunk' or (ref and ref.startswith('US ')):
                        hw_type = 'us_highway'
                    else:
                        hw_type = 'state_highway'
                    
                    # Find closest point on the highway
                    if element['geometry']:
                        closest_point = min(
                            element['geometry'],
                            key=lambda p: self._haversine_distance(latitude, longitude, p['lat'], p['lon'])
                        )
                        
                        distance = self._haversine_distance(
                            latitude, longitude, closest_point['lat'], closest_point['lon']
                        )
                        
                        if distance <= self.highway_search_radius:
                            access = HighwayAccess(
                                highway_name=name or f"Highway {ref}",
                                highway_type=hw_type,
                                distance_miles=round(distance, 2),
                                access_point='intersection',
                                access_lat=closest_point['lat'],
                                access_lon=closest_point['lon']
                            )
                            highway_accesses.append(access)
        
        # Sort by distance and remove duplicates
        highway_accesses.sort(key=lambda x: x.distance_miles)
        seen_highways = set()
        unique_accesses = []
        
        for access in highway_accesses:
            if access.highway_name not in seen_highways:
                seen_highways.add(access.highway_name)
                unique_accesses.append(access)
        
        return unique_accesses[:10]  # Return top 10
    
    def find_public_transit_access(self, latitude: float, longitude: float) -> List[PublicTransitAccess]:
        """Find nearby public transit access points"""
        self.logger.info(f"Finding public transit near {latitude}, {longitude}")
        
        # Overpass query for public transit
        query = f"""
        [out:json][timeout:25];
        (
          node["public_transport"~"stop_position|platform"]
            (around:{self.transit_search_radius * 1609},{latitude},{longitude});
          node["highway"="bus_stop"]
            (around:{self.transit_search_radius * 1609},{latitude},{longitude});
          node["railway"~"station|halt"]
            (around:{self.transit_search_radius * 1609},{latitude},{longitude});
        );
        out;
        """
        
        result = self._query_overpass(query)
        transit_accesses = []
        
        if result and 'elements' in result:
            for element in result['elements']:
                if 'tags' in element:
                    tags = element['tags']
                    lat = element['lat']
                    lon = element['lon']
                    
                    # Determine transit type
                    if 'railway' in tags:
                        transit_type = 'train_station'
                    elif tags.get('highway') == 'bus_stop' or tags.get('public_transport'):
                        transit_type = 'bus_stop'
                    else:
                        continue
                    
                    name = tags.get('name', f"Transit Stop {element.get('id', '')}")
                    distance = self._haversine_distance(latitude, longitude, lat, lon)
                    
                    # Extract route information if available
                    routes = []
                    if 'route_ref' in tags:
                        routes = tags['route_ref'].split(';')
                    
                    # Estimate frequency based on transit type and location
                    frequency = self._estimate_transit_frequency(transit_type, tags)
                    
                    access = PublicTransitAccess(
                        transit_type=transit_type,
                        stop_name=name,
                        distance_miles=round(distance, 2),
                        routes=routes,
                        frequency=frequency
                    )
                    transit_accesses.append(access)
        
        # Sort by distance
        transit_accesses.sort(key=lambda x: x.distance_miles)
        return transit_accesses[:15]  # Return top 15
    
    def _estimate_transit_frequency(self, transit_type: str, tags: Dict[str, str]) -> str:
        """Estimate transit frequency based on type and tags"""
        if transit_type == 'train_station':
            return 'medium'  # Most train stations have regular service
        
        # For bus stops, use heuristics based on available tags
        if 'operator' in tags:
            operator = tags['operator'].lower()
            if any(term in operator for term in ['metro', 'transit', 'city']):
                return 'high'
            elif 'county' in operator:
                return 'medium'
        
        return 'low'  # Default for unknown frequency
    
    def get_census_commute_data(self, latitude: float, longitude: float) -> Dict[str, float]:
        """Get Census commute data for the area"""
        # Query Census data for nearby areas
        query = """
        SELECT 
            AVG(mean_commute_time) as avg_commute_time,
            AVG(public_transport_pct) as public_transport_pct,
            AVG(long_commute_pct) as long_commute_pct
        FROM `location-optimizer-1.raw_business_data.census_demographics`
        WHERE geographic_level = 'tract'
        AND ABS(latitude - @site_lat) < 0.1
        AND ABS(longitude - @site_lon) < 0.1
        """
        
        job_config = bigquery.QueryJobConfig(
            query_parameters=[
                bigquery.ScalarQueryParameter("site_lat", "FLOAT64", latitude),
                bigquery.ScalarQueryParameter("site_lon", "FLOAT64", longitude)
            ]
        )
        
        try:
            result = self.client.query(query, job_config=job_config).to_dataframe()
            if not result.empty:
                return {
                    'avg_commute_time': float(result['avg_commute_time'].iloc[0] or 0),
                    'public_transport_pct': float(result['public_transport_pct'].iloc[0] or 0),
                    'long_commute_pct': float(result['long_commute_pct'].iloc[0] or 0)
                }
        except Exception as e:
            self.logger.warning(f"Failed to get Census commute data: {str(e)}")
        
        return {'avg_commute_time': 0, 'public_transport_pct': 0, 'long_commute_pct': 0}
    
    def analyze_transportation_accessibility(self, latitude: float, longitude: float) -> TransportationAccessibilityResult:
        """
        Analyze transportation accessibility for a given location
        
        Args:
            latitude: Site latitude
            longitude: Site longitude
            
        Returns:
            Transportation accessibility analysis results
        """
        self.logger.info(f"Analyzing transportation accessibility for {latitude}, {longitude}")
        
        # Find highway and transit access
        highway_accesses = self.find_highway_access(latitude, longitude)
        transit_accesses = self.find_public_transit_access(latitude, longitude)
        
        # Calculate key metrics
        nearest_interstate = min(
            (h.distance_miles for h in highway_accesses if h.highway_type == 'interstate'),
            default=float('inf')
        )
        
        nearest_major_highway = min(
            (h.distance_miles for h in highway_accesses if h.highway_type in ['interstate', 'us_highway']),
            default=float('inf')
        )
        
        # Calculate accessibility scores
        highway_score = self._calculate_highway_score(highway_accesses, nearest_interstate, nearest_major_highway)
        transit_score = self._calculate_transit_score(transit_accesses)
        
        # Get commute data
        commute_data = self.get_census_commute_data(latitude, longitude)
        commute_score = self._calculate_commute_score(commute_data)
        
        # Calculate overall score
        overall_score = (highway_score * 0.5 + transit_score * 0.3 + commute_score * 0.2)
        
        # Assess traffic level based on commute data
        traffic_level = self._assess_traffic_level(commute_data)
        
        # Overall accessibility rating
        accessibility_rating = self._get_accessibility_rating(overall_score)
        
        return TransportationAccessibilityResult(
            site_latitude=latitude,
            site_longitude=longitude,
            highway_accesses=highway_accesses,
            transit_accesses=transit_accesses,
            nearest_interstate_miles=nearest_interstate if nearest_interstate != float('inf') else 0,
            nearest_major_highway_miles=nearest_major_highway if nearest_major_highway != float('inf') else 0,
            transit_accessibility_score=transit_score,
            highway_accessibility_score=highway_score,
            overall_accessibility_score=overall_score,
            commute_score=commute_score,
            traffic_level=traffic_level,
            accessibility_rating=accessibility_rating
        )
    
    def _calculate_highway_score(self, accesses: List[HighwayAccess], 
                               nearest_interstate: float, nearest_major: float) -> float:
        """Calculate highway accessibility score (0-100)"""
        score = 0
        
        # Points for interstate access
        if nearest_interstate <= 2:
            score += 40
        elif nearest_interstate <= 5:
            score += 30
        elif nearest_interstate <= 10:
            score += 20
        elif nearest_interstate <= 20:
            score += 10
        
        # Points for major highway access
        if nearest_major <= 1:
            score += 30
        elif nearest_major <= 3:
            score += 20
        elif nearest_major <= 7:
            score += 10
        
        # Points for multiple highway options
        interstate_count = sum(1 for a in accesses if a.highway_type == 'interstate')
        major_count = sum(1 for a in accesses if a.highway_type in ['interstate', 'us_highway'])
        
        if interstate_count >= 2:
            score += 15
        elif interstate_count >= 1:
            score += 10
        
        if major_count >= 3:
            score += 15
        elif major_count >= 2:
            score += 10
        
        return min(score, 100)
    
    def _calculate_transit_score(self, accesses: List[PublicTransitAccess]) -> float:
        """Calculate public transit accessibility score (0-100)"""
        if not accesses:
            return 0
        
        score = 0
        
        # Points for nearby transit
        nearest_distance = min(a.distance_miles for a in accesses)
        if nearest_distance <= 0.25:
            score += 40
        elif nearest_distance <= 0.5:
            score += 30
        elif nearest_distance <= 1:
            score += 20
        elif nearest_distance <= 2:
            score += 10
        
        # Points for transit quality
        high_freq_count = sum(1 for a in accesses if a.frequency == 'high')
        train_count = sum(1 for a in accesses if a.transit_type == 'train_station')
        
        if high_freq_count >= 2:
            score += 25
        elif high_freq_count >= 1:
            score += 15
        
        if train_count >= 1:
            score += 20
        
        # Points for multiple options
        if len(accesses) >= 5:
            score += 15
        elif len(accesses) >= 3:
            score += 10
        elif len(accesses) >= 2:
            score += 5
        
        return min(score, 100)
    
    def _calculate_commute_score(self, commute_data: Dict[str, float]) -> float:
        """Calculate commute score based on Census data (0-100)"""
        score = 100  # Start with perfect score
        
        # Deduct points for long commute times
        avg_commute = commute_data.get('avg_commute_time', 0)
        if avg_commute > 35:
            score -= 30
        elif avg_commute > 30:
            score -= 20
        elif avg_commute > 25:
            score -= 10
        
        # Deduct points for high percentage of long commutes
        long_commute_pct = commute_data.get('long_commute_pct', 0)
        if long_commute_pct > 20:
            score -= 20
        elif long_commute_pct > 15:
            score -= 10
        
        # Add points for public transport availability
        public_transport_pct = commute_data.get('public_transport_pct', 0)
        if public_transport_pct > 15:
            score += 10
        elif public_transport_pct > 10:
            score += 5
        
        return max(0, min(score, 100))
    
    def _assess_traffic_level(self, commute_data: Dict[str, float]) -> str:
        """Assess traffic level based on commute data"""
        avg_commute = commute_data.get('avg_commute_time', 0)
        long_commute_pct = commute_data.get('long_commute_pct', 0)
        
        if avg_commute > 30 or long_commute_pct > 20:
            return 'High'
        elif avg_commute > 25 or long_commute_pct > 15:
            return 'Medium'
        else:
            return 'Low'
    
    def _get_accessibility_rating(self, overall_score: float) -> str:
        """Get overall accessibility rating"""
        if overall_score >= 80:
            return 'Excellent'
        elif overall_score >= 65:
            return 'Good'
        elif overall_score >= 45:
            return 'Fair'
        else:
            return 'Poor'
    
    def generate_transportation_report(self, latitude: float, longitude: float) -> str:
        """Generate a comprehensive transportation accessibility report"""
        analysis = self.analyze_transportation_accessibility(latitude, longitude)
        
        report = f"""
TRANSPORTATION ACCESSIBILITY ANALYSIS
=====================================
Location: {latitude:.4f}, {longitude:.4f}
Analysis Date: {datetime.now().strftime('%Y-%m-%d %H:%M:%S')}

OVERALL ACCESSIBILITY: {analysis.accessibility_rating}
Overall Score: {analysis.overall_accessibility_score:.1f}/100

HIGHWAY ACCESSIBILITY:
• Score: {analysis.highway_accessibility_score:.1f}/100
• Nearest Interstate: {analysis.nearest_interstate_miles:.1f} miles
• Nearest Major Highway: {analysis.nearest_major_highway_miles:.1f} miles

PUBLIC TRANSIT ACCESSIBILITY:
• Score: {analysis.transit_accessibility_score:.1f}/100
• Transit Options: {len(analysis.transit_accesses)}

COMMUTE CHARACTERISTICS:
• Commute Score: {analysis.commute_score:.1f}/100
• Traffic Level: {analysis.traffic_level}

NEARBY HIGHWAYS:
"""
        
        for i, highway in enumerate(analysis.highway_accesses[:5], 1):
            report += f"""
{i}. {highway.highway_name}
   Type: {highway.highway_type.replace('_', ' ').title()}
   Distance: {highway.distance_miles} miles
"""
        
        if analysis.transit_accesses:
            report += "\nNEARBY PUBLIC TRANSIT:\n"
            for i, transit in enumerate(analysis.transit_accesses[:5], 1):
                report += f"""
{i}. {transit.stop_name}
   Type: {transit.transit_type.replace('_', ' ').title()}
   Distance: {transit.distance_miles} miles
   Frequency: {transit.frequency.title()}
"""
        else:
            report += "\nNo public transit found within search radius.\n"
        
        return report


def main():
    """Test the transportation accessibility analyzer"""
    analyzer = TransportationAccessibilityAnalyzer()
    
    # Test with Madison, WI coordinates
    madison_lat, madison_lon = 43.0731, -89.4012
    
    print("🚗 Transportation Accessibility Analysis Test")
    print("=" * 50)
    
    # Generate report
    report = analyzer.generate_transportation_report(madison_lat, madison_lon)
    print(report)


if __name__ == "__main__":
    main()