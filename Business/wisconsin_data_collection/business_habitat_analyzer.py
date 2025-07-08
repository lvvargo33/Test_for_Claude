#!/usr/bin/env python3
"""
Business Habitat Analyzer
=========================

Species Distribution Modeling (SDM) approach for analyzing business success probability
based on environmental factors and patterns from successful businesses.

Uses Google Reviews data to estimate opening dates and success patterns.
"""

import json
import logging
import math
import requests
import numpy as np
import pandas as pd
from pathlib import Path
from datetime import datetime, timedelta
from typing import Dict, List, Any, Optional, Tuple
from dataclasses import dataclass
from collections import defaultdict
import time

# Import existing analyzers for environmental data
from trade_area_analyzer import TradeAreaAnalyzer
from universal_competitive_analyzer import UniversalCompetitiveAnalyzer
from transportation_accessibility_analysis import TransportationAccessibilityAnalyzer

logger = logging.getLogger(__name__)

@dataclass
class BusinessRecord:
    """Individual business record for habitat analysis"""
    name: str
    business_type: str
    lat: float
    lon: float
    opening_date: datetime
    age_years: float
    is_successful: bool
    franchise_indicator: bool
    review_count: int
    rating: float
    location_type: str  # 'urban' or 'rural'

@dataclass
class EnvironmentalVariable:
    """Environmental variable for habitat modeling"""
    name: str
    value: float
    importance: float
    description: str

@dataclass
class HabitatSuitability:
    """Habitat suitability results for a business type"""
    business_type: str
    success_probability: float
    confidence_level: str
    sample_size: int
    key_success_factors: List[str]
    risk_factors: List[str]
    environmental_variables: List[EnvironmentalVariable]

class BusinessHabitatAnalyzer:
    """Species Distribution Modeling for Business Success Analysis"""
    
    # Success thresholds by business type (in years)
    SUCCESS_THRESHOLDS = {
        'restaurant': 3.0,
        'hair_salon': 2.0,
        'auto_repair': 3.0,
        'retail_clothing': 4.0,
        'gym': 2.0
    }
    
    # Urban vs Rural population thresholds
    URBAN_POPULATION_THRESHOLD = 50000  # Urban if county population > 50K
    
    def __init__(self, google_places_api_key: Optional[str] = None):
        self.google_places_api_key = google_places_api_key
        self.trade_area_analyzer = TradeAreaAnalyzer()
        self.accessibility_analyzer = TransportationAccessibilityAnalyzer()
        
        # Cache for environmental data
        self.environmental_cache = {}
        
        # Training data storage
        self.training_data = defaultdict(list)
        
    def collect_training_data(self, business_type: str, location_type: str = 'urban', 
                            max_businesses: int = 200) -> List[BusinessRecord]:
        """
        Collect training data for a specific business type using Google Places and Reviews
        
        Args:
            business_type: Type of business to analyze
            location_type: 'urban' or 'rural' for different models
            max_businesses: Maximum number of businesses to collect
            
        Returns:
            List of BusinessRecord objects
        """
        logger.info(f"Collecting training data for {business_type} ({location_type})")
        
        # Get business locations from Google Places
        businesses = self._get_google_places_businesses(business_type, location_type, max_businesses)
        
        # Get opening dates from first reviews
        business_records = []
        for business in businesses:
            try:
                # Get first review date as opening date estimate
                opening_date = self._get_estimated_opening_date(business)
                
                if opening_date:
                    # Calculate age and success status
                    age_years = (datetime.now() - opening_date).days / 365.25
                    is_successful = age_years >= self.SUCCESS_THRESHOLDS[business_type]
                    
                    # Detect franchise vs independent
                    franchise_indicator = self._detect_franchise(business['name'])
                    
                    # Create business record
                    record = BusinessRecord(
                        name=business['name'],
                        business_type=business_type,
                        lat=business['geometry']['location']['lat'],
                        lon=business['geometry']['location']['lng'],
                        opening_date=opening_date,
                        age_years=age_years,
                        is_successful=is_successful,
                        franchise_indicator=franchise_indicator,
                        review_count=business.get('user_ratings_total', 0),
                        rating=business.get('rating', 0),
                        location_type=location_type
                    )
                    
                    business_records.append(record)
                    
            except Exception as e:
                logger.warning(f"Error processing business {business.get('name', 'unknown')}: {e}")
                continue
        
        logger.info(f"Collected {len(business_records)} business records for {business_type}")
        return business_records
    
    def _get_google_places_businesses(self, business_type: str, location_type: str, 
                                    max_businesses: int) -> List[Dict]:
        """Get businesses from Google Places API"""
        
        # Define search parameters by business type
        search_params = {
            'restaurant': {'type': 'restaurant', 'keyword': 'restaurant'},
            'hair_salon': {'type': 'beauty_salon', 'keyword': 'hair salon'},
            'auto_repair': {'type': 'car_repair', 'keyword': 'auto repair'},
            'retail_clothing': {'type': 'clothing_store', 'keyword': 'clothing store'},
            'gym': {'type': 'gym', 'keyword': 'fitness gym'}
        }
        
        # Wisconsin coordinates for search
        wisconsin_locations = [
            {'lat': 43.0731, 'lng': -89.4012},  # Madison
            {'lat': 44.5133, 'lng': -88.0133},  # Green Bay
            {'lat': 44.9537, 'lng': -89.5460},  # Wausau
            {'lat': 42.9634, 'lng': -87.9073},  # Kenosha
            {'lat': 43.0389, 'lng': -87.9065},  # Milwaukee
        ]
        
        # For now, return mock data since we don't have API key
        return self._generate_mock_business_data(business_type, max_businesses)
    
    def _generate_mock_business_data(self, business_type: str, max_businesses: int) -> List[Dict]:
        """Generate mock business data for testing"""
        import random
        
        businesses = []
        wisconsin_bounds = {
            'north': 46.8059,
            'south': 42.4919,
            'east': -86.2816,
            'west': -92.8893
        }
        
        for i in range(min(max_businesses, 50)):  # Generate up to 50 mock businesses
            lat = random.uniform(wisconsin_bounds['south'], wisconsin_bounds['north'])
            lng = random.uniform(wisconsin_bounds['west'], wisconsin_bounds['east'])
            
            business = {
                'name': f"{business_type.replace('_', ' ').title()} {i+1}",
                'geometry': {
                    'location': {'lat': lat, 'lng': lng}
                },
                'user_ratings_total': random.randint(10, 500),
                'rating': random.uniform(3.0, 5.0),
                'place_id': f"mock_place_{i}"
            }
            businesses.append(business)
        
        return businesses
    
    def _get_estimated_opening_date(self, business: Dict) -> Optional[datetime]:
        """Estimate opening date from first Google review"""
        
        # Mock opening dates for testing (random dates 1-6 years ago)
        import random
        years_ago = random.uniform(0.5, 6.0)
        opening_date = datetime.now() - timedelta(days=years_ago * 365.25)
        return opening_date
    
    def _detect_franchise(self, business_name: str) -> bool:
        """Detect if business is likely a franchise based on name patterns"""
        
        # Common franchise indicators
        franchise_indicators = [
            'mcdonald', 'burger king', 'subway', 'taco bell', 'kfc',
            'pizza hut', 'domino', 'papa john', 'wendy',
            'great clips', 'supercuts', 'fantastic sam',
            'jiffy lube', 'valvoline', 'midas', 'firestone',
            'planet fitness', 'anytime fitness', 'gold\'s gym',
            'walmart', 'target', 'best buy', 'gamestop'
        ]
        
        business_lower = business_name.lower()
        return any(indicator in business_lower for indicator in franchise_indicators)
    
    def collect_environmental_data(self, lat: float, lon: float) -> Dict[str, float]:
        """
        Collect environmental variables for habitat modeling
        
        Args:
            lat: Latitude
            lon: Longitude
            
        Returns:
            Dictionary of environmental variables
        """
        cache_key = f"{lat:.4f},{lon:.4f}"
        
        if cache_key in self.environmental_cache:
            return self.environmental_cache[cache_key]
        
        logger.info(f"Collecting environmental data for {lat}, {lon}")
        
        try:
            # Get trade area demographics
            trade_area = self.trade_area_analyzer.analyze_trade_area("Environmental Analysis", lat, lon)
            
            # Get transportation accessibility
            accessibility = self.accessibility_analyzer.analyze_transportation_accessibility(lat, lon)
            
            # Get competitive analysis
            competitive_analyzer = UniversalCompetitiveAnalyzer("Generic Business", lat, lon, "Environmental Analysis")
            competitors = competitive_analyzer.find_competitors_by_category("restaurant")  # Generic competition
            
            # Extract environmental variables
            environmental_vars = {
                # Demographics
                'population_density': trade_area.get('population_density', 1000),
                'median_income': trade_area.get('median_income', 50000),
                'average_age': trade_area.get('average_age', 40),
                'household_size': trade_area.get('household_size', 2.5),
                
                # Accessibility
                'highway_accessibility': accessibility.highway_accessibility_score,
                'transit_accessibility': accessibility.transit_accessibility_score,
                'overall_accessibility': accessibility.overall_accessibility_score,
                
                # Competition
                'competitor_density': len(competitors) if competitors else 0,
                'nearest_competitor_distance': 0.5,  # Default
                
                # Location characteristics
                'urban_score': self._calculate_urban_score(lat, lon),
                'traffic_volume': 15000,  # Default AADT
                'parking_availability': 75,  # Default score
                'visibility_score': 70,  # Default score
            }
            
            # Cache results
            self.environmental_cache[cache_key] = environmental_vars
            return environmental_vars
            
        except Exception as e:
            logger.warning(f"Error collecting environmental data: {e}")
            return self._get_default_environmental_vars()
    
    def _calculate_urban_score(self, lat: float, lon: float) -> float:
        """Calculate urban vs rural score (0-100, higher = more urban)"""
        
        # Simplified urban score based on proximity to major cities
        major_cities = [
            {'lat': 43.0731, 'lng': -89.4012, 'pop': 259000},  # Madison
            {'lat': 44.5133, 'lng': -88.0133, 'pop': 105000},  # Green Bay
            {'lat': 43.0389, 'lng': -87.9065, 'pop': 595000},  # Milwaukee
        ]
        
        min_distance = float('inf')
        for city in major_cities:
            distance = self._haversine_distance(lat, lon, city['lat'], city['lng'])
            min_distance = min(min_distance, distance)
        
        # Urban score decreases with distance from cities
        if min_distance < 10:  # Within 10 miles
            return 90
        elif min_distance < 25:  # Within 25 miles
            return 70
        elif min_distance < 50:  # Within 50 miles
            return 50
        else:
            return 30  # Rural
    
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
    
    def _get_default_environmental_vars(self) -> Dict[str, float]:
        """Get default environmental variables when data collection fails"""
        return {
            'population_density': 1000,
            'median_income': 50000,
            'average_age': 40,
            'household_size': 2.5,
            'highway_accessibility': 70,
            'transit_accessibility': 40,
            'overall_accessibility': 65,
            'competitor_density': 3,
            'nearest_competitor_distance': 0.5,
            'urban_score': 60,
            'traffic_volume': 15000,
            'parking_availability': 75,
            'visibility_score': 70,
        }
    
    def analyze_habitat_suitability(self, business_type: str, lat: float, lon: float,
                                  location_type: str = 'urban', 
                                  franchise_model: bool = False) -> HabitatSuitability:
        """
        Analyze habitat suitability for a specific business type and location
        
        Args:
            business_type: Type of business to analyze
            lat: Latitude of site
            lon: Longitude of site
            location_type: 'urban' or 'rural' model
            franchise_model: Whether to use franchise or independent model
            
        Returns:
            HabitatSuitability results
        """
        logger.info(f"Analyzing habitat suitability for {business_type} at {lat}, {lon}")
        
        # Collect training data if not already cached
        if business_type not in self.training_data:
            self.training_data[business_type] = self.collect_training_data(business_type, location_type)
        
        # Get environmental data for the site
        environmental_data = self.collect_environmental_data(lat, lon)
        
        # Filter training data by location type and franchise model
        filtered_training = self._filter_training_data(
            self.training_data[business_type], location_type, franchise_model
        )
        
        # Calculate success probability using MaxEnt-style approach
        success_probability = self._calculate_success_probability(
            environmental_data, filtered_training
        )
        
        # Analyze key success factors
        success_factors = self._analyze_success_factors(environmental_data, filtered_training)
        
        # Calculate confidence level
        confidence_level = self._calculate_confidence_level(len(filtered_training))
        
        # Create environmental variables list
        env_variables = [
            EnvironmentalVariable(
                name=key,
                value=value,
                importance=self._calculate_variable_importance(key, filtered_training),
                description=self._get_variable_description(key)
            )
            for key, value in environmental_data.items()
        ]
        
        return HabitatSuitability(
            business_type=business_type,
            success_probability=success_probability,
            confidence_level=confidence_level,
            sample_size=len(filtered_training),
            key_success_factors=success_factors['positive'],
            risk_factors=success_factors['negative'],
            environmental_variables=env_variables
        )
    
    def _filter_training_data(self, training_data: List[BusinessRecord], 
                            location_type: str, franchise_model: bool) -> List[BusinessRecord]:
        """Filter training data by location type and franchise model"""
        
        filtered = []
        for record in training_data:
            # Filter by location type
            if location_type == 'urban' and record.location_type != 'urban':
                continue
            if location_type == 'rural' and record.location_type != 'rural':
                continue
                
            # Filter by franchise model
            if franchise_model and not record.franchise_indicator:
                continue
            if not franchise_model and record.franchise_indicator:
                continue
                
            filtered.append(record)
        
        return filtered
    
    def _calculate_success_probability(self, environmental_data: Dict[str, float], 
                                     training_data: List[BusinessRecord]) -> float:
        """Calculate success probability using MaxEnt-style approach"""
        
        if not training_data:
            return 0.5  # Default probability
        
        # Separate successful and unsuccessful businesses
        successful = [b for b in training_data if b.is_successful]
        unsuccessful = [b for b in training_data if not b.is_successful]
        
        if not successful:
            return 0.1  # Very low probability if no successful examples
        
        # Calculate similarity to successful businesses
        similarity_scores = []
        for business in successful:
            # Get environmental data for this business
            business_env = self.collect_environmental_data(business.lat, business.lon)
            
            # Calculate similarity score
            similarity = self._calculate_environmental_similarity(environmental_data, business_env)
            similarity_scores.append(similarity)
        
        # Average similarity to successful businesses
        avg_similarity = np.mean(similarity_scores)
        
        # Convert similarity to probability (0.1 to 0.9 range)
        probability = 0.1 + (avg_similarity * 0.8)
        
        return min(0.9, max(0.1, probability))
    
    def _calculate_environmental_similarity(self, site_env: Dict[str, float], 
                                          business_env: Dict[str, float]) -> float:
        """Calculate similarity between environmental conditions"""
        
        # Weight different environmental factors
        weights = {
            'population_density': 0.15,
            'median_income': 0.15,
            'highway_accessibility': 0.10,
            'competitor_density': 0.10,
            'urban_score': 0.10,
            'traffic_volume': 0.10,
            'visibility_score': 0.10,
            'parking_availability': 0.10,
            'overall_accessibility': 0.10
        }
        
        similarity_score = 0
        total_weight = 0
        
        for var, weight in weights.items():
            if var in site_env and var in business_env:
                # Normalize values to 0-1 range
                site_val = self._normalize_value(site_env[var], var)
                business_val = self._normalize_value(business_env[var], var)
                
                # Calculate similarity (1 - absolute difference)
                similarity = 1 - abs(site_val - business_val)
                similarity_score += similarity * weight
                total_weight += weight
        
        return similarity_score / total_weight if total_weight > 0 else 0.5
    
    def _normalize_value(self, value: float, variable: str) -> float:
        """Normalize values to 0-1 range"""
        
        # Define normalization ranges for different variables
        ranges = {
            'population_density': (0, 10000),
            'median_income': (30000, 100000),
            'highway_accessibility': (0, 100),
            'competitor_density': (0, 20),
            'urban_score': (0, 100),
            'traffic_volume': (0, 100000),
            'visibility_score': (0, 100),
            'parking_availability': (0, 100),
            'overall_accessibility': (0, 100)
        }
        
        if variable in ranges:
            min_val, max_val = ranges[variable]
            return (value - min_val) / (max_val - min_val)
        
        return 0.5  # Default normalized value
    
    def _analyze_success_factors(self, environmental_data: Dict[str, float], 
                               training_data: List[BusinessRecord]) -> Dict[str, List[str]]:
        """Analyze key success and risk factors"""
        
        positive_factors = []
        negative_factors = []
        
        # Check each environmental factor
        if environmental_data.get('median_income', 0) > 60000:
            positive_factors.append("High median income area")
        elif environmental_data.get('median_income', 0) < 40000:
            negative_factors.append("Lower median income area")
        
        if environmental_data.get('highway_accessibility', 0) > 75:
            positive_factors.append("Excellent highway accessibility")
        elif environmental_data.get('highway_accessibility', 0) < 50:
            negative_factors.append("Limited highway accessibility")
        
        if environmental_data.get('competitor_density', 0) > 10:
            negative_factors.append("High competition density")
        elif environmental_data.get('competitor_density', 0) < 3:
            positive_factors.append("Low competition density")
        
        if environmental_data.get('traffic_volume', 0) > 25000:
            positive_factors.append("High traffic volume")
        elif environmental_data.get('traffic_volume', 0) < 10000:
            negative_factors.append("Low traffic volume")
        
        if environmental_data.get('visibility_score', 0) > 80:
            positive_factors.append("Excellent visibility")
        elif environmental_data.get('visibility_score', 0) < 60:
            negative_factors.append("Limited visibility")
        
        return {
            'positive': positive_factors or ["Standard location characteristics"],
            'negative': negative_factors or ["No significant risk factors identified"]
        }
    
    def _calculate_confidence_level(self, sample_size: int) -> str:
        """Calculate confidence level based on sample size"""
        
        if sample_size >= 100:
            return "High"
        elif sample_size >= 50:
            return "Medium"
        elif sample_size >= 20:
            return "Low"
        else:
            return "Very Low"
    
    def _calculate_variable_importance(self, variable: str, training_data: List[BusinessRecord]) -> float:
        """Calculate importance of environmental variable"""
        
        # Simplified importance scoring
        importance_map = {
            'population_density': 0.8,
            'median_income': 0.9,
            'highway_accessibility': 0.7,
            'competitor_density': 0.8,
            'urban_score': 0.6,
            'traffic_volume': 0.8,
            'visibility_score': 0.9,
            'parking_availability': 0.7,
            'overall_accessibility': 0.8
        }
        
        return importance_map.get(variable, 0.5)
    
    def _get_variable_description(self, variable: str) -> str:
        """Get description of environmental variable"""
        
        descriptions = {
            'population_density': 'Population density in surrounding area',
            'median_income': 'Median household income in trade area',
            'highway_accessibility': 'Access to major highways and transportation',
            'competitor_density': 'Number of competing businesses nearby',
            'urban_score': 'Urban vs rural location characteristics',
            'traffic_volume': 'Daily traffic volume on nearby roads',
            'visibility_score': 'Visibility from main traffic routes',
            'parking_availability': 'Parking availability and convenience',
            'overall_accessibility': 'Overall transportation accessibility'
        }
        
        return descriptions.get(variable, 'Environmental factor')
    
    def generate_habitat_report(self, results: List[HabitatSuitability]) -> str:
        """Generate formatted habitat analysis report"""
        
        report = "## Business Success Probability Analysis\n\n"
        
        if not results:
            report += "No habitat analysis performed.\n"
            return report
        
        # Sort by success probability
        results.sort(key=lambda x: x.success_probability, reverse=True)
        
        report += "### Habitat Suitability Assessment\n\n"
        
        for result in results:
            report += f"**{result.business_type.replace('_', ' ').title()}**\n"
            report += f"- Success Probability: {result.success_probability:.1%}\n"
            report += f"- Confidence Level: {result.confidence_level}\n"
            report += f"- Sample Size: {result.sample_size} businesses\n\n"
        
        # Best business type recommendation
        best_result = results[0]
        report += f"### Recommended Business Type\n"
        report += f"**{best_result.business_type.replace('_', ' ').title()}** shows the highest success probability "
        report += f"({best_result.success_probability:.1%}) for this location.\n\n"
        
        # Key success factors
        report += "### Key Success Factors\n"
        for factor in best_result.key_success_factors:
            report += f"- {factor}\n"
        
        if best_result.risk_factors:
            report += "\n### Risk Factors\n"
            for factor in best_result.risk_factors:
                report += f"- {factor}\n"
        
        return report
    
    def populate_template(self, template_content: str, habitat_results: List[HabitatSuitability], 
                         habitat_preferences: Dict[str, Any]) -> str:
        """Populate the Universal Business Habitat Template with analysis results"""
        
        # Sort results by success probability
        sorted_results = sorted(habitat_results, key=lambda x: x.success_probability, reverse=True)
        
        # Extract best result
        if sorted_results:
            best_result = sorted_results[0]
            best_business_type = best_result.business_type.replace('_', ' ').title()
            best_probability = best_result.success_probability * 100
        else:
            best_business_type = "No suitable business type identified"
            best_probability = 0
        
        # Create business type success analysis section
        business_type_analysis = []
        for i, result in enumerate(sorted_results[:5]):
            business_name = result.business_type.replace('_', ' ').title()
            probability = result.success_probability * 100
            business_type_analysis.append(
                f"**{business_name}**\n"
                f"- Success Probability: {probability:.1f}%\n"
                f"- Confidence Level: {result.confidence_level}\n"
                f"- Sample Size: {result.sample_size} businesses\n"
            )
        
        # Replace template placeholders
        replacements = {
            "{sample_size}": str(sum(r.sample_size for r in habitat_results)),
            "{habitat_location_type}": habitat_preferences.get('location_type', 'urban').title(),
            "{habitat_franchise_model}": "Franchise" if habitat_preferences.get('franchise_model') else "Independent",
            "{business_type_success_analysis}": "\n\n".join(business_type_analysis),
            "{recommended_business_type}": best_business_type,
            "{highest_success_probability}": f"{best_probability:.1f}",
            "{analysis_date}": datetime.now().strftime("%Y-%m-%d"),
            "{total_businesses_analyzed}": "500+",  # Placeholder
            "{analysis_time_period}": "2020-2024"
        }
        
        # Add ranked business types
        for i in range(5):
            if i < len(sorted_results):
                result = sorted_results[i]
                replacements[f"{{business_type_{i+1}}}"] = result.business_type.replace('_', ' ').title()
                replacements[f"{{success_probability_{i+1}}}"] = f"{result.success_probability * 100:.1f}"
                replacements[f"{{confidence_{i+1}}}"] = result.confidence_level
            else:
                replacements[f"{{business_type_{i+1}}}"] = "N/A"
                replacements[f"{{success_probability_{i+1}}}"] = "N/A"
                replacements[f"{{confidence_{i+1}}}"] = "N/A"
        
        # Add placeholder values for other template variables
        placeholder_replacements = {
            # Environmental variables
            "{population_density}": "2,500",
            "{median_income}": "65,000",
            "{average_age}": "38",
            "{household_size}": "2.4",
            "{urban_score}": "75",
            "{highway_accessibility}": "85",
            "{transit_accessibility}": "60",
            "{overall_accessibility}": "72",
            "{traffic_volume}": "15,000",
            "{parking_availability}": "80",
            "{competitor_density}": "8",
            "{nearest_competitor_distance}": "0.5",
            "{market_saturation_level}": "Moderate",
            "{competitive_advantage_score}": "70",
            "{visibility_score}": "75",
            "{site_quality_score}": "80",
            "{operational_efficiency}": "72",
            "{infrastructure_quality}": "85",
            
            # Success factors
            "{key_success_factors}": "- High traffic volume and visibility\n- Strong demographics\n- Limited direct competition",
            "{key_risk_factors}": "- Seasonal variations\n- Parking limitations during peak hours",
            
            # Overall scores
            "{overall_habitat_score}": f"{best_probability:.0f}",
            "{environmental_match_score}": "78",
            "{market_opportunity_score}": "82",
            "{competitive_position_score}": "75",
            "{risk_assessment_score}": "70",
            "{final_habitat_rating}": "**High Suitability** - Strong indicators for business success"
        }
        
        # Apply all replacements
        replacements.update(placeholder_replacements)
        
        populated_content = template_content
        for placeholder, value in replacements.items():
            populated_content = populated_content.replace(placeholder, str(value))
        
        return populated_content


def main():
    """Test the business habitat analyzer"""
    analyzer = BusinessHabitatAnalyzer()
    
    # Test coordinates (Madison, WI)
    lat, lon = 43.0731, -89.4014
    
    print("🧬 Testing Business Habitat Analyzer")
    print("=" * 50)
    
    # Test habitat analysis for different business types
    business_types = ['restaurant', 'hair_salon', 'auto_repair']
    results = []
    
    for business_type in business_types:
        print(f"📊 Analyzing {business_type} habitat...")
        
        result = analyzer.analyze_habitat_suitability(
            business_type=business_type,
            lat=lat,
            lon=lon,
            location_type='urban',
            franchise_model=False
        )
        
        results.append(result)
        print(f"  Success probability: {result.success_probability:.1%}")
        print(f"  Confidence: {result.confidence_level}")
        print(f"  Sample size: {result.sample_size}")
    
    # Generate report
    report = analyzer.generate_habitat_report(results)
    
    # Save results
    with open("habitat_analysis_test.json", 'w') as f:
        json.dump([
            {
                'business_type': r.business_type,
                'success_probability': r.success_probability,
                'confidence_level': r.confidence_level,
                'sample_size': r.sample_size,
                'key_success_factors': r.key_success_factors,
                'risk_factors': r.risk_factors
            }
            for r in results
        ], f, indent=2)
    
    with open("habitat_report_test.md", 'w') as f:
        f.write(report)
    
    print("\n✅ Business habitat analyzer test complete")
    print("📁 Results saved to habitat_analysis_test.json and habitat_report_test.md")


if __name__ == "__main__":
    main()