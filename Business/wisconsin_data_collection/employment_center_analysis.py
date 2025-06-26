#!/usr/bin/env python3
"""
Employment Center Proximity Analysis
====================================

Analyzes proximity to major employment centers, business districts, and high-employment areas.
Combines BLS employment data with OSM business data to identify employment clusters.
"""

import logging
import pandas as pd
import numpy as np
from typing import Dict, List, Tuple, Optional, Any
from dataclasses import dataclass
from google.cloud import bigquery
import math
from datetime import datetime

@dataclass
class EmploymentCenter:
    """Represents a major employment center"""
    name: str
    latitude: float
    longitude: float
    total_employment: int
    avg_weekly_wage: float
    major_employers: List[str]
    business_types: List[str]
    employment_density: float  # employees per square mile
    center_type: str  # 'business_district', 'industrial_park', 'major_employer', 'mixed'

@dataclass
class EmploymentProximityResult:
    """Results of employment center proximity analysis"""
    site_latitude: float
    site_longitude: float
    nearby_centers: List[Dict[str, Any]]
    total_employment_1mile: int
    total_employment_3mile: int
    total_employment_5mile: int
    avg_wage_nearby: float
    employment_score: float  # 0-100 scoring proximity to employment
    dominant_industries: List[str]
    commute_accessibility: str  # 'Excellent', 'Good', 'Fair', 'Poor'

class EmploymentCenterAnalyzer:
    """Analyzes employment centers and proximity for site selection"""
    
    def __init__(self, credentials_path: str = "location-optimizer-1-449414f93a5a.json"):
        self.client = bigquery.Client.from_service_account_json(credentials_path)
        self.logger = self._setup_logging()
        
        # Employment center identification thresholds
        self.min_employment_for_center = 500  # minimum employees to qualify as center
        self.business_cluster_radius = 0.5  # miles for business clustering
        self.major_employer_threshold = 1000  # employees for major employer status
        
    def _setup_logging(self) -> logging.Logger:
        """Setup logging"""
        logger = logging.getLogger('employment_analyzer')
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
    
    def identify_employment_centers(self, state: str = 'WI') -> List[EmploymentCenter]:
        """
        Identify major employment centers using BLS and OSM data
        
        Args:
            state: State code to analyze
            
        Returns:
            List of identified employment centers
        """
        self.logger.info(f"Identifying employment centers for {state}")
        
        # Get BLS employment data by county
        employment_query = """
        SELECT 
            county_fips,
            county_name,
            AVG(CASE WHEN measure_type = 'employment_level' THEN value END) as avg_employment,
            AVG(CASE WHEN measure_type = 'labor_force' THEN value END) as labor_force,
            AVG(CASE WHEN measure_type = 'unemployment_rate' THEN value END) as unemployment_rate
        FROM `location-optimizer-1.raw_business_data.bls_laus_data`
        WHERE year >= 2022
        GROUP BY county_fips, county_name
        HAVING avg_employment > @min_employment
        ORDER BY avg_employment DESC
        """
        
        job_config = bigquery.QueryJobConfig(
            query_parameters=[
                bigquery.ScalarQueryParameter("min_employment", "INT64", self.min_employment_for_center)
            ]
        )
        
        employment_df = self.client.query(employment_query, job_config=job_config).to_dataframe()
        
        # Get business clusters from OSM data
        business_query = """
        SELECT 
            address_city as city,
            address_state as state,
            COUNT(*) as business_count,
            AVG(latitude) as center_lat,
            AVG(longitude) as center_lon,
            ARRAY_AGG(DISTINCT business_type) as business_types,
            ARRAY_AGG(DISTINCT name ORDER BY name LIMIT 10) as sample_businesses
        FROM `location-optimizer-1.raw_business_data.osm_businesses`
        WHERE address_state = @state
        GROUP BY address_city, address_state
        HAVING business_count >= 20
        ORDER BY business_count DESC
        """
        
        job_config = bigquery.QueryJobConfig(
            query_parameters=[
                bigquery.ScalarQueryParameter("state", "STRING", state)
            ]
        )
        
        business_df = self.client.query(business_query, job_config=job_config).to_dataframe()
        
        # Combine data to identify employment centers
        employment_centers = []
        
        # Add major business districts (high business density areas)
        for _, row in business_df.iterrows():
            # Estimate employment based on business count and types
            estimated_employment = self._estimate_employment_from_businesses(
                row['business_count'], 
                row['business_types']
            )
            
            if estimated_employment >= self.min_employment_for_center:
                center = EmploymentCenter(
                    name=f"{row['city']} Business District",
                    latitude=row['center_lat'],
                    longitude=row['center_lon'],
                    total_employment=estimated_employment,
                    avg_weekly_wage=self._estimate_wages_from_business_types(row['business_types']),
                    major_employers=row['sample_businesses'][:5],
                    business_types=row['business_types'],
                    employment_density=estimated_employment / (math.pi * self.business_cluster_radius ** 2),
                    center_type='business_district'
                )
                employment_centers.append(center)
        
        # Add county employment centers based on BLS data
        for _, row in employment_df.iterrows():
            # Get approximate center coordinates for county
            county_coords = self._get_county_center_coordinates(row['county_name'], state)
            
            if county_coords:
                center = EmploymentCenter(
                    name=f"{row['county_name']} Employment Center",
                    latitude=county_coords[0],
                    longitude=county_coords[1],
                    total_employment=int(row['avg_employment']),
                    avg_weekly_wage=self._estimate_county_weekly_wage(row['unemployment_rate']),
                    major_employers=[],  # Would need additional data source
                    business_types=['mixed'],
                    employment_density=row['avg_employment'] / 100,  # Rough estimate per sq mile
                    center_type='county_center'
                )
                employment_centers.append(center)
        
        self.logger.info(f"Identified {len(employment_centers)} employment centers")
        return employment_centers
    
    def _estimate_employment_from_businesses(self, business_count: int, business_types) -> int:
        """Estimate employment based on business count and types"""
        # Average employees per business by type
        employment_multipliers = {
            'retail': 8,
            'restaurant': 15,
            'automotive': 12,
            'healthcare': 25,
            'professional_services': 10,
            'personal_services': 6,
            'fitness': 8,
            'hospitality': 20
        }
        
        # Convert to list if it's a numpy array or other format
        if business_types is None:
            business_types_list = []
        elif hasattr(business_types, 'tolist'):
            business_types_list = business_types.tolist()
        elif isinstance(business_types, (list, tuple)):
            business_types_list = list(business_types)
        else:
            business_types_list = [str(business_types)]
        
        if not business_types_list:
            return business_count * 10  # Default multiplier
        
        # Calculate weighted average based on business types
        total_multiplier = 0
        for btype in business_types_list:
            if btype:  # Check for non-null values
                total_multiplier += employment_multipliers.get(btype, 10)
        
        avg_multiplier = total_multiplier / len(business_types_list) if business_types_list else 10
        return int(business_count * avg_multiplier)
    
    def _estimate_wages_from_business_types(self, business_types) -> float:
        """Estimate average wages based on business types"""
        wage_estimates = {
            'retail': 600,
            'restaurant': 400,
            'automotive': 800,
            'healthcare': 1200,
            'professional_services': 1400,
            'personal_services': 500,
            'fitness': 600,
            'hospitality': 500
        }
        
        # Convert to list if it's a numpy array or other format
        if business_types is None:
            business_types_list = []
        elif hasattr(business_types, 'tolist'):
            business_types_list = business_types.tolist()
        elif isinstance(business_types, (list, tuple)):
            business_types_list = list(business_types)
        else:
            business_types_list = [str(business_types)]
        
        if not business_types_list:
            return 700  # Default wage
        
        total_wages = sum(wage_estimates.get(btype, 700) for btype in business_types_list if btype)
        return total_wages / len(business_types_list) if business_types_list else 700
    
    def _estimate_county_weekly_wage(self, unemployment_rate: Optional[float]) -> float:
        """Estimate county weekly wage based on unemployment rate and regional data"""
        # Wisconsin average weekly wage estimates (rough approximation)
        base_wage = 850  # Approximate Wisconsin average
        
        if unemployment_rate:
            # Lower unemployment often correlates with higher wages
            if unemployment_rate < 3.0:
                return base_wage * 1.2  # Higher wages in low unemployment areas
            elif unemployment_rate < 4.0:
                return base_wage * 1.1
            elif unemployment_rate > 6.0:
                return base_wage * 0.9  # Lower wages in high unemployment areas
        
        return base_wage
    
    def _get_county_center_coordinates(self, county_name: str, state: str) -> Optional[Tuple[float, float]]:
        """Get approximate center coordinates for a county"""
        # Wisconsin county centers (approximate)
        wisconsin_county_centers = {
            'Milwaukee County': (43.0389, -87.9065),
            'Dane County': (43.0731, -89.4012),
            'Waukesha County': (43.0118, -88.2315),
            'Brown County': (44.5133, -88.0133),
            'Racine County': (42.7261, -87.7829),
            'Kenosha County': (42.5847, -87.8212),
            'Rock County': (42.6919, -89.0187),
            'Winnebago County': (44.0267, -88.5426),
            'La Crosse County': (43.8014, -91.0990),
            'Eau Claire County': (44.6582, -91.2985)
        }
        
        return wisconsin_county_centers.get(county_name)
    
    def analyze_employment_proximity(self, latitude: float, longitude: float, 
                                   employment_centers: Optional[List[EmploymentCenter]] = None) -> EmploymentProximityResult:
        """
        Analyze employment center proximity for a given location
        
        Args:
            latitude: Site latitude
            longitude: Site longitude
            employment_centers: Pre-identified employment centers (if None, will identify them)
            
        Returns:
            Employment proximity analysis results
        """
        if employment_centers is None:
            employment_centers = self.identify_employment_centers()
        
        # Calculate distances to all employment centers
        nearby_centers = []
        
        for center in employment_centers:
            distance = self._haversine_distance(latitude, longitude, center.latitude, center.longitude)
            
            if distance <= 10:  # Within 10 miles
                center_info = {
                    'name': center.name,
                    'distance_miles': round(distance, 2),
                    'employment': center.total_employment,
                    'avg_weekly_wage': center.avg_weekly_wage,
                    'center_type': center.center_type,
                    'business_types': center.business_types,
                    'employment_density': center.employment_density
                }
                nearby_centers.append(center_info)
        
        # Sort by distance
        nearby_centers.sort(key=lambda x: x['distance_miles'])
        
        # Calculate employment within different radii
        employment_1mile = sum(c['employment'] for c in nearby_centers if c['distance_miles'] <= 1)
        employment_3mile = sum(c['employment'] for c in nearby_centers if c['distance_miles'] <= 3)
        employment_5mile = sum(c['employment'] for c in nearby_centers if c['distance_miles'] <= 5)
        
        # Calculate average wage for nearby centers
        wage_centers = [c for c in nearby_centers if c['avg_weekly_wage'] > 0]
        avg_wage_nearby = (sum(c['avg_weekly_wage'] for c in wage_centers) / len(wage_centers)
                          if wage_centers else 0)
        
        # Calculate employment accessibility score (0-100)
        employment_score = self._calculate_employment_score(
            employment_1mile, employment_3mile, employment_5mile, nearby_centers
        )
        
        # Identify dominant industries
        all_business_types = []
        for center in nearby_centers[:5]:  # Top 5 closest centers
            all_business_types.extend(center['business_types'])
        
        dominant_industries = list(set(all_business_types))[:5]
        
        # Assess commute accessibility
        commute_accessibility = self._assess_commute_accessibility(nearby_centers)
        
        return EmploymentProximityResult(
            site_latitude=latitude,
            site_longitude=longitude,
            nearby_centers=nearby_centers[:10],  # Top 10 closest
            total_employment_1mile=employment_1mile,
            total_employment_3mile=employment_3mile,
            total_employment_5mile=employment_5mile,
            avg_wage_nearby=avg_wage_nearby,
            employment_score=employment_score,
            dominant_industries=dominant_industries,
            commute_accessibility=commute_accessibility
        )
    
    def _calculate_employment_score(self, emp_1mi: int, emp_3mi: int, emp_5mi: int, 
                                  centers: List[Dict]) -> float:
        """Calculate employment accessibility score (0-100)"""
        score = 0
        
        # Points for employment within 1 mile (most important)
        score += min(emp_1mi / 100, 40)  # Up to 40 points for 10,000+ jobs within 1 mile
        
        # Points for employment within 3 miles
        score += min(emp_3mi / 200, 30)  # Up to 30 points for 6,000+ jobs within 3 miles
        
        # Points for employment within 5 miles
        score += min(emp_5mi / 500, 20)  # Up to 20 points for 10,000+ jobs within 5 miles
        
        # Bonus for multiple employment centers (diversity)
        if len(centers) >= 3:
            score += 10
        elif len(centers) >= 2:
            score += 5
        
        return min(score, 100)
    
    def _assess_commute_accessibility(self, centers: List[Dict]) -> str:
        """Assess commute accessibility based on nearby employment centers"""
        if not centers:
            return 'Poor'
        
        closest_major_center = next((c for c in centers if c['employment'] > 1000), None)
        
        if closest_major_center:
            distance = closest_major_center['distance_miles']
            if distance <= 2:
                return 'Excellent'
            elif distance <= 5:
                return 'Good'
            elif distance <= 10:
                return 'Fair'
        
        return 'Poor'
    
    def generate_employment_report(self, latitude: float, longitude: float) -> str:
        """Generate a comprehensive employment proximity report"""
        analysis = self.analyze_employment_proximity(latitude, longitude)
        
        report = f"""
EMPLOYMENT CENTER PROXIMITY ANALYSIS
=====================================
Location: {latitude:.4f}, {longitude:.4f}
Analysis Date: {datetime.now().strftime('%Y-%m-%d %H:%M:%S')}

EMPLOYMENT ACCESSIBILITY SCORE: {analysis.employment_score:.1f}/100
Commute Accessibility: {analysis.commute_accessibility}

EMPLOYMENT WITHIN RADIUS:
• 1 mile: {analysis.total_employment_1mile:,} jobs
• 3 miles: {analysis.total_employment_3mile:,} jobs  
• 5 miles: {analysis.total_employment_5mile:,} jobs

AVERAGE NEARBY WAGE: ${analysis.avg_wage_nearby:.0f}/week

DOMINANT INDUSTRIES:
{chr(10).join(f'• {industry}' for industry in analysis.dominant_industries)}

NEARBY EMPLOYMENT CENTERS:
"""
        
        for i, center in enumerate(analysis.nearby_centers[:5], 1):
            report += f"""
{i}. {center['name']}
   Distance: {center['distance_miles']} miles
   Employment: {center['employment']:,} jobs
   Avg Wage: ${center['avg_weekly_wage']:.0f}/week
   Type: {center['center_type']}
"""
        
        return report


def main():
    """Test the employment center analyzer"""
    analyzer = EmploymentCenterAnalyzer()
    
    # Test with Madison, WI coordinates
    madison_lat, madison_lon = 43.0731, -89.4012
    
    print("🏭 Employment Center Analysis Test")
    print("=" * 50)
    
    # Generate report
    report = analyzer.generate_employment_report(madison_lat, madison_lon)
    print(report)


if __name__ == "__main__":
    main()