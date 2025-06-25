"""
Market Opportunity Scanner
==========================

Scans Wisconsin cities and regions to identify optimal business opportunities
based on competitive analysis, demographics, and market gaps.
"""

import logging
import os
from datetime import datetime
from typing import List, Dict, Tuple
from dataclasses import dataclass, asdict
from google.cloud import bigquery
from osm_competitive_analysis import OSMCompetitiveAnalysis, MarketSaturation
import json


@dataclass
class MarketOpportunity:
    """Represents a market opportunity"""
    city: str
    county: str
    business_type: str
    opportunity_score: float  # 0-100
    competition_level: str
    total_competitors: int
    franchise_competition: int
    market_gap_indicator: bool
    population_estimate: int
    recommendation: str
    analysis_date: str


@dataclass
class CityProfile:
    """City business profile for opportunity analysis"""
    city: str
    county: str
    total_businesses: int
    business_types: Dict[str, int]
    franchise_percentage: float
    business_density: float
    dominant_industries: List[str]


class MarketOpportunityScanner:
    """Scanner for identifying optimal business opportunities"""
    
    def __init__(self, project_id: str = "location-optimizer-1"):
        """Initialize market opportunity scanner"""
        self.analyzer = OSMCompetitiveAnalysis(project_id)
        self.client = bigquery.Client(project=project_id)
        self.project_id = project_id
        self.dataset_id = "raw_business_data"
        self.logger = logging.getLogger(self.__class__.__name__)
        
        # Business type opportunity thresholds
        self.opportunity_thresholds = {
            'food_beverage': {'min_pop': 5000, 'max_competitors_per_1k': 2.0},
            'retail': {'min_pop': 3000, 'max_competitors_per_1k': 3.0},
            'automotive': {'min_pop': 8000, 'max_competitors_per_1k': 1.0},
            'healthcare': {'min_pop': 10000, 'max_competitors_per_1k': 0.8},
            'personal_services': {'min_pop': 4000, 'max_competitors_per_1k': 2.5},
            'professional_services': {'min_pop': 5000, 'max_competitors_per_1k': 2.0},
            'fitness': {'min_pop': 8000, 'max_competitors_per_1k': 0.5},
            'hospitality': {'min_pop': 15000, 'max_competitors_per_1k': 0.3}
        }
    
    def get_city_profiles(self) -> List[CityProfile]:
        """
        Get business profiles for all Wisconsin cities
        
        Returns:
            List of city business profiles
        """
        query = f"""
        SELECT 
            address_city as city,
            COUNT(*) as total_businesses,
            SUM(CASE WHEN franchise_indicator = true THEN 1 ELSE 0 END) as franchise_count,
            business_type,
            COUNT(*) as type_count
        FROM `{self.project_id}.{self.dataset_id}.osm_businesses`
        WHERE address_city IS NOT NULL
        GROUP BY address_city, business_type
        ORDER BY address_city, type_count DESC
        """
        
        try:
            results = self.client.query(query).result()
            
            # Group by city
            city_data = {}
            for row in results:
                city = row.city
                if city not in city_data:
                    city_data[city] = {
                        'total_businesses': 0,
                        'franchise_count': 0,
                        'business_types': {}
                    }
                
                city_data[city]['business_types'][row.business_type] = row.type_count
                city_data[city]['total_businesses'] += row.type_count
                city_data[city]['franchise_count'] += row.franchise_count
            
            # Convert to CityProfile objects
            profiles = []
            for city, data in city_data.items():
                franchise_percentage = (data['franchise_count'] / data['total_businesses']) * 100
                
                # Get dominant industries (top 3)
                sorted_types = sorted(data['business_types'].items(), 
                                    key=lambda x: x[1], reverse=True)
                dominant_industries = [btype for btype, count in sorted_types[:3]]
                
                # Estimate business density (simplified)
                business_density = data['total_businesses'] / 10  # Rough estimate
                
                profile = CityProfile(
                    city=city,
                    county="Unknown",  # Would need county mapping
                    total_businesses=data['total_businesses'],
                    business_types=data['business_types'],
                    franchise_percentage=franchise_percentage,
                    business_density=business_density,
                    dominant_industries=dominant_industries
                )
                profiles.append(profile)
            
            self.logger.info(f"Generated profiles for {len(profiles)} cities")
            return profiles
            
        except Exception as e:
            self.logger.error(f"Error getting city profiles: {e}")
            return []
    
    def identify_market_gaps(self, city_profile: CityProfile, 
                           min_population: int = 5000) -> List[str]:
        """
        Identify business types that are underrepresented in a city
        
        Args:
            city_profile: City business profile
            min_population: Minimum population threshold
            
        Returns:
            List of business types with market gaps
        """
        # Estimate population based on business count (rough heuristic)
        estimated_population = city_profile.total_businesses * 50
        
        if estimated_population < min_population:
            return []  # City too small
        
        market_gaps = []
        
        for business_type, thresholds in self.opportunity_thresholds.items():
            if estimated_population < thresholds['min_pop']:
                continue
            
            current_count = city_profile.business_types.get(business_type, 0)
            expected_count = (estimated_population / 1000) * thresholds['max_competitors_per_1k']
            
            # Market gap if significantly under-served
            if current_count < (expected_count * 0.7):
                market_gaps.append(business_type)
        
        return market_gaps
    
    def scan_wisconsin_opportunities(self, business_type: str = None,
                                   min_opportunity_score: float = 60.0) -> List[MarketOpportunity]:
        """
        Scan Wisconsin for market opportunities
        
        Args:
            business_type: Specific business type to analyze (None for all)
            min_opportunity_score: Minimum opportunity score threshold
            
        Returns:
            List of market opportunities
        """
        city_profiles = self.get_city_profiles()
        opportunities = []
        
        analysis_date = datetime.now().strftime('%Y-%m-%d')
        
        for profile in city_profiles:
            # Skip very small cities
            if profile.total_businesses < 20:
                continue
            
            # Estimate city center coordinates (simplified)
            # In practice, you'd want to geocode city names
            city_coords = self._get_city_coordinates(profile.city)
            if not city_coords:
                continue
            
            lat, lon = city_coords
            
            # Determine business types to analyze
            if business_type:
                types_to_analyze = [business_type]
            else:
                market_gaps = self.identify_market_gaps(profile)
                types_to_analyze = market_gaps[:3]  # Top 3 opportunities
            
            for btype in types_to_analyze:
                try:
                    # Analyze market saturation
                    saturation = self.analyzer.assess_market_saturation(
                        lat, lon, btype, radius_miles=5.0, area_name=profile.city
                    )
                    
                    # Only include high-opportunity markets
                    if saturation.opportunity_score >= min_opportunity_score:
                        # Estimate population
                        estimated_population = profile.total_businesses * 50
                        
                        opportunity = MarketOpportunity(
                            city=profile.city,
                            county="Wisconsin",  # Simplified
                            business_type=btype,
                            opportunity_score=saturation.opportunity_score,
                            competition_level=saturation.saturation_level,
                            total_competitors=saturation.total_competitors,
                            franchise_competition=saturation.franchise_competitors,
                            market_gap_indicator=btype in self.identify_market_gaps(profile),
                            population_estimate=estimated_population,
                            recommendation=saturation.recommended_action,
                            analysis_date=analysis_date
                        )
                        opportunities.append(opportunity)
                
                except Exception as e:
                    self.logger.warning(f"Error analyzing {profile.city} for {btype}: {e}")
                    continue
        
        # Sort by opportunity score
        opportunities.sort(key=lambda x: x.opportunity_score, reverse=True)
        
        self.logger.info(f"Found {len(opportunities)} market opportunities")
        return opportunities
    
    def _get_city_coordinates(self, city_name: str) -> Tuple[float, float]:
        """
        Get approximate coordinates for a city (simplified implementation)
        
        Args:
            city_name: Name of the city
            
        Returns:
            Tuple of (latitude, longitude) or None if not found
        """
        # Get coordinates from existing business data
        query = f"""
        SELECT AVG(latitude) as avg_lat, AVG(longitude) as avg_lon
        FROM `{self.project_id}.{self.dataset_id}.osm_businesses`
        WHERE address_city = '{city_name}'
        AND latitude IS NOT NULL 
        AND longitude IS NOT NULL
        """
        
        try:
            results = self.client.query(query).result()
            for row in results:
                if row.avg_lat and row.avg_lon:
                    return (row.avg_lat, row.avg_lon)
        except Exception as e:
            self.logger.warning(f"Could not get coordinates for {city_name}: {e}")
        
        return None
    
    def generate_opportunity_report(self, opportunities: List[MarketOpportunity],
                                  business_type: str = None) -> str:
        """
        Generate formatted opportunity report
        
        Args:
            opportunities: List of market opportunities
            business_type: Business type filter (None for all)
            
        Returns:
            Formatted report string
        """
        if business_type:
            opportunities = [op for op in opportunities if op.business_type == business_type]
        
        report = []
        report.append("🎯 WISCONSIN MARKET OPPORTUNITY REPORT")
        report.append("=" * 60)
        report.append(f"Analysis Date: {datetime.now().strftime('%Y-%m-%d %H:%M:%S')}")
        report.append(f"Total Opportunities: {len(opportunities)}")
        
        if business_type:
            report.append(f"Business Type: {business_type}")
        
        if not opportunities:
            report.append("\nNo market opportunities found matching criteria.")
            return "\n".join(report)
        
        # Summary statistics
        avg_opportunity_score = sum(op.opportunity_score for op in opportunities) / len(opportunities)
        high_opportunity_count = sum(1 for op in opportunities if op.opportunity_score >= 80)
        
        report.append(f"\n📊 SUMMARY STATISTICS:")
        report.append(f"   Average Opportunity Score: {avg_opportunity_score:.1f}/100")
        report.append(f"   High-Opportunity Markets (80+): {high_opportunity_count}")
        report.append(f"   Market Gap Opportunities: {sum(1 for op in opportunities if op.market_gap_indicator)}")
        
        # Top opportunities by business type
        if not business_type:
            report.append(f"\n🏆 TOP OPPORTUNITIES BY BUSINESS TYPE:")
            type_groups = {}
            for op in opportunities:
                if op.business_type not in type_groups:
                    type_groups[op.business_type] = []
                type_groups[op.business_type].append(op)
            
            for btype, ops in type_groups.items():
                best_op = max(ops, key=lambda x: x.opportunity_score)
                report.append(f"   {btype}: {best_op.city} ({best_op.opportunity_score:.1f}/100)")
        
        # Detailed top opportunities
        report.append(f"\n🎯 TOP MARKET OPPORTUNITIES:")
        for i, op in enumerate(opportunities[:10], 1):
            gap_indicator = "🔍" if op.market_gap_indicator else ""
            report.append(f"\n   {i}. {op.city} - {op.business_type} {gap_indicator}")
            report.append(f"      Opportunity Score: {op.opportunity_score:.1f}/100")
            report.append(f"      Competition Level: {op.competition_level}")
            report.append(f"      Current Competitors: {op.total_competitors}")
            report.append(f"      Franchise Competition: {op.franchise_competition}")
            report.append(f"      Est. Population: {op.population_estimate:,}")
            report.append(f"      Recommendation: {op.recommendation}")
        
        # Geographic distribution
        report.append(f"\n🗺️ GEOGRAPHIC DISTRIBUTION:")
        city_counts = {}
        for op in opportunities[:20]:  # Top 20
            if op.city not in city_counts:
                city_counts[op.city] = 0
            city_counts[op.city] += 1
        
        top_cities = sorted(city_counts.items(), key=lambda x: x[1], reverse=True)[:5]
        for city, count in top_cities:
            report.append(f"   {city}: {count} opportunities")
        
        return "\n".join(report)
    
    def save_opportunities_to_json(self, opportunities: List[MarketOpportunity], 
                                 filename: str = None) -> str:
        """
        Save opportunities to JSON file
        
        Args:
            opportunities: List of market opportunities
            filename: Output filename (auto-generated if None)
            
        Returns:
            Filename of saved file
        """
        if not filename:
            timestamp = datetime.now().strftime('%Y%m%d_%H%M%S')
            filename = f"wisconsin_market_opportunities_{timestamp}.json"
        
        # Convert to dictionaries
        data = {
            'analysis_date': datetime.now().isoformat(),
            'total_opportunities': len(opportunities),
            'opportunities': [asdict(op) for op in opportunities]
        }
        
        try:
            with open(filename, 'w') as f:
                json.dump(data, f, indent=2)
            
            self.logger.info(f"Saved {len(opportunities)} opportunities to {filename}")
            return filename
        
        except Exception as e:
            self.logger.error(f"Error saving opportunities: {e}")
            return None


def main():
    """Test the market opportunity scanner"""
    logging.basicConfig(level=logging.INFO)
    
    print("🔍 Wisconsin Market Opportunity Scanner")
    print("=" * 50)
    
    # Set up credentials
    credentials_path = "/workspaces/Test_for_Claude/Business/wisconsin_data_collection/location-optimizer-1-449414f93a5a.json"
    os.environ['GOOGLE_APPLICATION_CREDENTIALS'] = credentials_path
    
    scanner = MarketOpportunityScanner()
    
    # Scan for food & beverage opportunities
    print("\n🍕 Scanning for Food & Beverage Opportunities...")
    food_opportunities = scanner.scan_wisconsin_opportunities(
        business_type='food_beverage',
        min_opportunity_score=70.0
    )
    
    if food_opportunities:
        report = scanner.generate_opportunity_report(food_opportunities, 'food_beverage')
        print(report)
        
        # Save to file
        filename = scanner.save_opportunities_to_json(food_opportunities)
        if filename:
            print(f"\n💾 Opportunities saved to: {filename}")
    else:
        print("No high-opportunity food & beverage markets found.")


if __name__ == "__main__":
    main()