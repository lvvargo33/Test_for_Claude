"""
OSM Competitive Analysis System - Phase 2
=========================================

Provides competitive analysis capabilities using OSM business data:
- Map competitors around potential sites
- Analyze business density patterns  
- Identify market saturation areas
"""

import logging
import math
import os
from datetime import datetime
from typing import List, Dict, Tuple, Optional
from dataclasses import dataclass
from google.cloud import bigquery
import json


@dataclass
class CompetitorSite:
    """Represents a competitor business location"""
    osm_id: str
    name: str
    business_type: str
    latitude: float
    longitude: float
    address_city: str
    address_street: Optional[str]
    franchise_indicator: bool
    brand: Optional[str]
    distance_miles: Optional[float] = None


@dataclass
class BusinessDensity:
    """Business density metrics for an area"""
    area_name: str
    center_lat: float
    center_lon: float
    radius_miles: float
    total_businesses: int
    businesses_per_sq_mile: float
    franchise_percentage: float
    dominant_business_types: Dict[str, int]
    competition_score: float  # 0-100, higher = more competitive


@dataclass
class MarketSaturation:
    """Market saturation analysis results"""
    area_name: str
    business_type: str
    saturation_level: str  # 'Low', 'Medium', 'High', 'Saturated'
    saturation_score: float  # 0-100
    total_competitors: int
    franchise_competitors: int
    recommended_action: str
    opportunity_score: float  # 0-100, higher = better opportunity


class OSMCompetitiveAnalysis:
    """Competitive analysis using OSM business data"""
    
    def __init__(self, project_id: str = "location-optimizer-1"):
        """
        Initialize competitive analysis system
        
        Args:
            project_id: Google Cloud project ID
        """
        self.client = bigquery.Client(project=project_id)
        self.project_id = project_id
        self.dataset_id = "raw_business_data"
        self.logger = logging.getLogger(self.__class__.__name__)
        
        # Business type mappings for competitive analysis
        self.competitive_groups = {
            'food_beverage': ['food_beverage', 'restaurant'],
            'retail': ['retail'],
            'automotive': ['automotive'],
            'healthcare': ['healthcare'],
            'personal_services': ['personal_services'],
            'professional_services': ['professional_services'],
            'fitness': ['fitness'],
            'hospitality': ['hospitality']
        }
    
    def haversine_distance(self, lat1: float, lon1: float, lat2: float, lon2: float) -> float:
        """
        Calculate distance between two points using Haversine formula
        
        Args:
            lat1, lon1: First point coordinates
            lat2, lon2: Second point coordinates
            
        Returns:
            Distance in miles
        """
        # Convert to radians
        lat1, lon1, lat2, lon2 = map(math.radians, [lat1, lon1, lat2, lon2])
        
        # Haversine formula
        dlat = lat2 - lat1
        dlon = lon2 - lon1
        a = math.sin(dlat/2)**2 + math.cos(lat1) * math.cos(lat2) * math.sin(dlon/2)**2
        c = 2 * math.asin(math.sqrt(a))
        
        # Earth's radius in miles
        earth_radius_miles = 3959
        return earth_radius_miles * c
    
    def find_competitors_around_site(self, target_lat: float, target_lon: float, 
                                   radius_miles: float = 3.0,
                                   business_types: List[str] = None,
                                   include_franchises_only: bool = False) -> List[CompetitorSite]:
        """
        Find competitors within radius of a potential site
        
        Args:
            target_lat: Target site latitude
            target_lon: Target site longitude
            radius_miles: Search radius in miles
            business_types: List of business types to include (None for all)
            include_franchises_only: Only include franchise businesses
            
        Returns:
            List of competitor sites with distance calculations
        """
        # Build query filters
        filters = []
        if business_types:
            type_filter = "', '".join(business_types)
            filters.append(f"business_type IN ('{type_filter}')")
        
        if include_franchises_only:
            filters.append("franchise_indicator = true")
        
        where_clause = "WHERE " + " AND ".join(filters) if filters else ""
        
        query = f"""
        SELECT 
            osm_id,
            name,
            business_type,
            latitude,
            longitude,
            address_city,
            address_street,
            franchise_indicator,
            brand
        FROM `{self.project_id}.{self.dataset_id}.osm_businesses`
        {where_clause}
        """
        
        try:
            results = self.client.query(query).result()
            competitors = []
            
            for row in results:
                if row.latitude and row.longitude:
                    distance = self.haversine_distance(
                        target_lat, target_lon, 
                        row.latitude, row.longitude
                    )
                    
                    # Only include businesses within radius
                    if distance <= radius_miles:
                        competitor = CompetitorSite(
                            osm_id=row.osm_id,
                            name=row.name,
                            business_type=row.business_type,
                            latitude=row.latitude,
                            longitude=row.longitude,
                            address_city=row.address_city,
                            address_street=row.address_street,
                            franchise_indicator=row.franchise_indicator,
                            brand=row.brand,
                            distance_miles=distance
                        )
                        competitors.append(competitor)
            
            # Sort by distance
            competitors.sort(key=lambda x: x.distance_miles)
            
            self.logger.info(f"Found {len(competitors)} competitors within {radius_miles} miles")
            return competitors
            
        except Exception as e:
            self.logger.error(f"Error finding competitors: {e}")
            return []
    
    def analyze_business_density(self, center_lat: float, center_lon: float,
                               radius_miles: float = 5.0,
                               area_name: str = "Analysis Area") -> BusinessDensity:
        """
        Analyze business density in a circular area
        
        Args:
            center_lat: Center latitude
            center_lon: Center longitude
            radius_miles: Analysis radius in miles
            area_name: Name for the analysis area
            
        Returns:
            BusinessDensity analysis results
        """
        # Get all businesses in the area
        all_businesses = self.find_competitors_around_site(
            center_lat, center_lon, radius_miles
        )
        
        if not all_businesses:
            return BusinessDensity(
                area_name=area_name,
                center_lat=center_lat,
                center_lon=center_lon,
                radius_miles=radius_miles,
                total_businesses=0,
                businesses_per_sq_mile=0,
                franchise_percentage=0,
                dominant_business_types={},
                competition_score=0
            )
        
        # Calculate area in square miles
        area_sq_miles = math.pi * (radius_miles ** 2)
        density = len(all_businesses) / area_sq_miles
        
        # Calculate franchise percentage
        franchise_count = sum(1 for b in all_businesses if b.franchise_indicator)
        franchise_percentage = (franchise_count / len(all_businesses)) * 100
        
        # Count business types
        type_counts = {}
        for business in all_businesses:
            btype = business.business_type
            type_counts[btype] = type_counts.get(btype, 0) + 1
        
        # Sort business types by count
        dominant_types = dict(sorted(type_counts.items(), key=lambda x: x[1], reverse=True)[:5])
        
        # Calculate competition score (higher density = higher competition)
        # Scale: 0-20 businesses/sq mi = 0-100 score
        max_density_for_scoring = 20
        competition_score = min(100, (density / max_density_for_scoring) * 100)
        
        return BusinessDensity(
            area_name=area_name,
            center_lat=center_lat,
            center_lon=center_lon,
            radius_miles=radius_miles,
            total_businesses=len(all_businesses),
            businesses_per_sq_mile=density,
            franchise_percentage=franchise_percentage,
            dominant_business_types=dominant_types,
            competition_score=competition_score
        )
    
    def assess_market_saturation(self, center_lat: float, center_lon: float,
                               target_business_type: str,
                               radius_miles: float = 3.0,
                               area_name: str = "Market Area") -> MarketSaturation:
        """
        Assess market saturation for a specific business type
        
        Args:
            center_lat: Center latitude
            center_lon: Center longitude
            target_business_type: Business type to analyze
            radius_miles: Analysis radius
            area_name: Name for the market area
            
        Returns:
            MarketSaturation analysis results
        """
        # Get competing business types
        competing_types = self.competitive_groups.get(target_business_type, [target_business_type])
        
        competitors = self.find_competitors_around_site(
            center_lat, center_lon, radius_miles, competing_types
        )
        
        total_competitors = len(competitors)
        franchise_competitors = sum(1 for c in competitors if c.franchise_indicator)
        
        # Calculate area population estimate (simplified)
        area_sq_miles = math.pi * (radius_miles ** 2)
        
        # Saturation scoring based on competitor density
        competitor_density = total_competitors / area_sq_miles if area_sq_miles > 0 else 0
        
        # Business type specific thresholds (competitors per sq mile)
        saturation_thresholds = {
            'food_beverage': {'low': 2, 'medium': 5, 'high': 10},
            'retail': {'low': 3, 'medium': 8, 'high': 15},
            'automotive': {'low': 1, 'medium': 3, 'high': 6},
            'healthcare': {'low': 1, 'medium': 2, 'high': 4},
            'personal_services': {'low': 2, 'medium': 5, 'high': 10},
            'professional_services': {'low': 3, 'medium': 7, 'high': 12},
            'fitness': {'low': 0.5, 'medium': 1.5, 'high': 3},
            'hospitality': {'low': 0.5, 'medium': 1, 'high': 2}
        }
        
        thresholds = saturation_thresholds.get(target_business_type, 
                                             saturation_thresholds['retail'])
        
        # Determine saturation level
        if competitor_density <= thresholds['low']:
            saturation_level = 'Low'
            saturation_score = (competitor_density / thresholds['low']) * 25
            recommended_action = 'Strong opportunity - enter market'
        elif competitor_density <= thresholds['medium']:
            saturation_level = 'Medium'
            saturation_score = 25 + ((competitor_density - thresholds['low']) / 
                                   (thresholds['medium'] - thresholds['low'])) * 25
            recommended_action = 'Moderate opportunity - differentiate offering'
        elif competitor_density <= thresholds['high']:
            saturation_level = 'High'
            saturation_score = 50 + ((competitor_density - thresholds['medium']) / 
                                   (thresholds['high'] - thresholds['medium'])) * 35
            recommended_action = 'Challenging market - focus on unique value proposition'
        else:
            saturation_level = 'Saturated'
            saturation_score = min(100, 85 + (competitor_density - thresholds['high']) * 3)
            recommended_action = 'Avoid market - oversaturated'
        
        # Calculate opportunity score (inverse of saturation)
        opportunity_score = 100 - saturation_score
        
        return MarketSaturation(
            area_name=area_name,
            business_type=target_business_type,
            saturation_level=saturation_level,
            saturation_score=saturation_score,
            total_competitors=total_competitors,
            franchise_competitors=franchise_competitors,
            recommended_action=recommended_action,
            opportunity_score=opportunity_score
        )
    
    def generate_site_analysis_report(self, target_lat: float, target_lon: float,
                                    target_business_type: str,
                                    site_name: str = "Potential Site",
                                    analysis_radius: float = 3.0) -> str:
        """
        Generate comprehensive site analysis report
        
        Args:
            target_lat: Site latitude
            target_lon: Site longitude
            target_business_type: Proposed business type
            site_name: Name for the site
            analysis_radius: Analysis radius in miles
            
        Returns:
            Formatted analysis report
        """
        # Get competitive analysis
        competitors = self.find_competitors_around_site(
            target_lat, target_lon, analysis_radius,
            self.competitive_groups.get(target_business_type, [target_business_type])
        )
        
        # Business density analysis
        density = self.analyze_business_density(
            target_lat, target_lon, analysis_radius, site_name
        )
        
        # Market saturation assessment
        saturation = self.assess_market_saturation(
            target_lat, target_lon, target_business_type, analysis_radius, site_name
        )
        
        # Generate report
        report = []
        report.append(f"🎯 COMPETITIVE ANALYSIS REPORT")
        report.append("=" * 60)
        report.append(f"Site: {site_name}")
        report.append(f"Location: {target_lat:.6f}, {target_lon:.6f}")
        report.append(f"Business Type: {target_business_type}")
        report.append(f"Analysis Radius: {analysis_radius} miles")
        report.append(f"Analysis Date: {datetime.now().strftime('%Y-%m-%d %H:%M:%S')}")
        
        # Competitor overview
        report.append(f"\n🏢 DIRECT COMPETITORS:")
        report.append(f"   Total Competitors: {len(competitors)}")
        if competitors:
            report.append(f"   Closest Competitor: {competitors[0].distance_miles:.2f} miles")
            report.append(f"   Franchise Competition: {sum(1 for c in competitors if c.franchise_indicator)}")
            
            # Top 5 closest competitors
            report.append(f"\n📍 CLOSEST COMPETITORS:")
            for i, comp in enumerate(competitors[:5], 1):
                franchise_mark = "🏢" if comp.franchise_indicator else "🏪"
                report.append(f"   {i}. {comp.name} {franchise_mark}")
                report.append(f"      Distance: {comp.distance_miles:.2f} miles")
                report.append(f"      Location: {comp.address_street}, {comp.address_city}")
                if comp.brand:
                    report.append(f"      Brand: {comp.brand}")
        else:
            report.append("   No direct competitors found in analysis area")
        
        # Business density analysis
        report.append(f"\n📊 BUSINESS DENSITY ANALYSIS:")
        report.append(f"   Total Businesses: {density.total_businesses}")
        report.append(f"   Density: {density.businesses_per_sq_mile:.1f} businesses/sq mile")
        report.append(f"   Franchise Percentage: {density.franchise_percentage:.1f}%")
        report.append(f"   Competition Score: {density.competition_score:.1f}/100")
        
        if density.dominant_business_types:
            report.append(f"\n🏪 DOMINANT BUSINESS TYPES:")
            for btype, count in density.dominant_business_types.items():
                percentage = (count / density.total_businesses) * 100
                report.append(f"   {btype}: {count} ({percentage:.1f}%)")
        
        # Market saturation assessment
        report.append(f"\n⚖️ MARKET SATURATION ASSESSMENT:")
        report.append(f"   Saturation Level: {saturation.saturation_level}")
        report.append(f"   Saturation Score: {saturation.saturation_score:.1f}/100")
        report.append(f"   Opportunity Score: {saturation.opportunity_score:.1f}/100")
        report.append(f"   Recommendation: {saturation.recommended_action}")
        
        # Overall recommendation
        if saturation.opportunity_score >= 75:
            overall_rating = "🟢 EXCELLENT"
        elif saturation.opportunity_score >= 50:
            overall_rating = "🟡 GOOD"
        elif saturation.opportunity_score >= 25:
            overall_rating = "🟠 FAIR"
        else:
            overall_rating = "🔴 POOR"
        
        report.append(f"\n🎯 OVERALL SITE RATING: {overall_rating}")
        report.append(f"   Opportunity Score: {saturation.opportunity_score:.1f}/100")
        
        return "\n".join(report)
    
    def analyze_city_business_clusters(self, city_name: str, business_type: str = None) -> Dict:
        """
        Analyze business clustering patterns within a city
        
        Args:
            city_name: City to analyze
            business_type: Specific business type (None for all)
            
        Returns:
            Clustering analysis results
        """
        # Build query
        where_conditions = [f"address_city = '{city_name}'"]
        if business_type:
            where_conditions.append(f"business_type = '{business_type}'")
        
        query = f"""
        SELECT 
            name,
            business_type,
            latitude,
            longitude,
            address_street,
            franchise_indicator,
            brand
        FROM `{self.project_id}.{self.dataset_id}.osm_businesses`
        WHERE {' AND '.join(where_conditions)}
        AND latitude IS NOT NULL 
        AND longitude IS NOT NULL
        """
        
        try:
            results = self.client.query(query).result()
            businesses = list(results)
            
            if not businesses:
                return {"error": f"No businesses found in {city_name}"}
            
            # Simple clustering analysis
            clusters = {}
            for business in businesses:
                # Group by street for basic clustering
                street = business.address_street or "Unknown Street"
                if street not in clusters:
                    clusters[street] = []
                clusters[street].append({
                    'name': business.name,
                    'type': business.business_type,
                    'franchise': business.franchise_indicator,
                    'brand': business.brand
                })
            
            # Find streets with multiple businesses
            business_corridors = {street: businesses for street, businesses 
                                in clusters.items() if len(businesses) >= 3}
            
            return {
                "city": city_name,
                "total_businesses": len(businesses),
                "total_streets": len(clusters),
                "business_corridors": business_corridors,
                "corridor_count": len(business_corridors)
            }
            
        except Exception as e:
            self.logger.error(f"Error analyzing city clusters: {e}")
            return {"error": str(e)}


def main():
    """Test the competitive analysis system"""
    logging.basicConfig(level=logging.INFO)
    
    print("🎯 OSM Competitive Analysis System - Phase 2")
    print("=" * 60)
    
    # Set up credentials
    credentials_path = "/workspaces/Test_for_Claude/Business/wisconsin_data_collection/location-optimizer-1-449414f93a5a.json"
    os.environ['GOOGLE_APPLICATION_CREDENTIALS'] = credentials_path
    
    analyzer = OSMCompetitiveAnalysis()
    
    # Test with Madison downtown area
    madison_lat = 43.0731
    madison_lon = -89.4012
    
    print("\n📍 Testing: Madison Downtown Analysis")
    print("=" * 40)
    
    # Generate comprehensive site analysis
    report = analyzer.generate_site_analysis_report(
        target_lat=madison_lat,
        target_lon=madison_lon,
        target_business_type='food_beverage',
        site_name="Madison Downtown - Restaurant Site",
        analysis_radius=2.0
    )
    
    print(report)
    
    # Test city clustering analysis
    print(f"\n\n🏙️ Madison Business Clustering Analysis")
    print("=" * 40)
    
    clustering = analyzer.analyze_city_business_clusters("Madison", "food_beverage")
    
    if "error" not in clustering:
        print(f"City: {clustering['city']}")
        print(f"Total Businesses: {clustering['total_businesses']}")
        print(f"Business Corridors: {clustering['corridor_count']}")
        
        if clustering['business_corridors']:
            print(f"\n🛣️ Top Business Corridors:")
            for street, businesses in list(clustering['business_corridors'].items())[:3]:
                print(f"   {street}: {len(businesses)} businesses")
                for biz in businesses[:3]:
                    franchise = "🏢" if biz['franchise'] else "🏪"
                    print(f"     - {biz['name']} {franchise}")
    else:
        print(f"Error: {clustering['error']}")


if __name__ == "__main__":
    main()