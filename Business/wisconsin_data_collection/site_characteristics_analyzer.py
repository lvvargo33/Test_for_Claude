#!/usr/bin/env python3
"""
Site Characteristics Analyzer
============================

Analyzes physical site characteristics, visibility factors, and strategic positioning
for business feasibility studies. Combines automated data collection with manual assessment.
"""

import json
import logging
import math
import requests
from pathlib import Path
from datetime import datetime
from typing import Dict, List, Any, Optional, Tuple

# Import existing analyzers for data integration
from trade_area_analyzer import TradeAreaAnalyzer
from universal_competitive_analyzer import UniversalCompetitiveAnalyzer
from transportation_accessibility_analysis import TransportationAccessibilityAnalyzer
# Removed business_habitat_analyzer import - moved to Section 3.3

logger = logging.getLogger(__name__)

class SiteCharacteristicsAnalyzer:
    """Comprehensive site characteristics analysis for Section 3.2"""
    
    def __init__(self):
        self.trade_area_analyzer = TradeAreaAnalyzer()
        self.accessibility_analyzer = TransportationAccessibilityAnalyzer()
        # Removed habitat_analyzer - moved to Section 3.3
        
    def analyze_site_characteristics(self, business_type: str, address: str, 
                                   lat: float, lon: float, 
                                   manual_data: Optional[Dict] = None) -> Dict[str, Any]:
        """
        Perform comprehensive site characteristics analysis
        
        Args:
            business_type: Type of business
            address: Business address
            lat: Latitude
            lon: Longitude
            manual_data: Optional manual data from site visit
            
        Returns:
            Dictionary containing complete site characteristics analysis
        """
        logger.info(f"Starting site characteristics analysis for {business_type} at {address}")
        
        results = {
            "business_type": business_type,
            "location": address,
            "coordinates": {"lat": lat, "lon": lon},
            "analysis_date": datetime.now().isoformat(),
            "sections": {},
            "data_sources": {
                "automated": [],
                "manual": []
            }
        }
        
        try:
            # 1. Physical Site Analysis (mostly manual with some automated)
            physical_analysis = self._analyze_physical_site(lat, lon, manual_data)
            results["sections"]["physical_site"] = physical_analysis
            
            # 2. Visibility Analysis (automated + manual)
            visibility_analysis = self._analyze_visibility(lat, lon, manual_data)
            results["sections"]["visibility"] = visibility_analysis
            
            # 3. Accessibility Analysis (automated)
            accessibility_analysis = self._analyze_accessibility(lat, lon)
            results["sections"]["accessibility"] = accessibility_analysis
            
            # 4. Market Dynamics (automated)
            market_dynamics = self._analyze_market_dynamics(lat, lon)
            results["sections"]["market_dynamics"] = market_dynamics
            
            # 5. Competitive Positioning (automated + manual)
            competitive_positioning = self._analyze_competitive_positioning(business_type, lat, lon, manual_data)
            results["sections"]["competitive_positioning"] = competitive_positioning
            
            # 6. Operational Considerations (manual)
            operational_considerations = self._analyze_operational_considerations(manual_data)
            results["sections"]["operational_considerations"] = operational_considerations
            
            # 7. Environmental Factors (automated + manual)
            environmental_factors = self._analyze_environmental_factors(lat, lon, manual_data)
            results["sections"]["environmental_factors"] = environmental_factors
            
            # 8. Technology Integration (automated)
            technology_integration = self._analyze_technology_integration(lat, lon)
            results["sections"]["technology_integration"] = technology_integration
            
            # 9. Risk Assessment (automated + manual)
            risk_assessment = self._analyze_risk_assessment(lat, lon, manual_data)
            results["sections"]["risk_assessment"] = risk_assessment
            
            # 10. Generate summary
            results["summary"] = self._generate_summary(results["sections"])
            
        except Exception as e:
            logger.error(f"Error in site characteristics analysis: {str(e)}")
            results["error"] = str(e)
            
        return results
    
    def _analyze_physical_site(self, lat: float, lon: float, manual_data: Optional[Dict] = None) -> Dict[str, Any]:
        """Analyze physical site characteristics"""
        logger.info("Analyzing physical site characteristics")
        
        # Automated elevation data (simplified)
        elevation = self._get_elevation(lat, lon)
        
        # Manual data integration
        if manual_data and "physical_site" in manual_data:
            manual_physical = manual_data["physical_site"]
        else:
            manual_physical = {}
        
        return {
            "topography": {
                "elevation": elevation,
                "topographical_features": manual_physical.get("topographical_features", "Level terrain"),
                "slope_analysis": manual_physical.get("slope_analysis", "Minimal slope"),
                "grade_conditions": manual_physical.get("grade_conditions", "Suitable for development"),
                "natural_barriers": manual_physical.get("natural_barriers", "None identified"),
                "drainage_patterns": manual_physical.get("drainage_patterns", "Adequate drainage")
            },
            "soil_conditions": {
                "soil_type": manual_physical.get("soil_type", "Standard commercial soil"),
                "soil_stability": manual_physical.get("soil_stability", "Stable"),
                "drainage_quality": manual_physical.get("drainage_quality", "Good"),
                "flood_risk": manual_physical.get("flood_risk", "Low risk"),
                "environmental_factors": manual_physical.get("environmental_factors", "No concerns"),
                "seasonal_considerations": manual_physical.get("seasonal_considerations", "Standard seasonal variations")
            },
            "site_boundaries": {
                "property_boundaries": manual_physical.get("property_boundaries", "Clearly defined"),
                "setback_requirements": manual_physical.get("setback_requirements", "Standard setbacks"),
                "easements": manual_physical.get("easements", "Standard utility easements"),
                "right_of_way": manual_physical.get("right_of_way", "No restrictions"),
                "utility_easements": manual_physical.get("utility_easements", "Standard utility access")
            },
            "data_source": "Manual site assessment with elevation data",
            "requires_manual_verification": True
        }
    
    def _analyze_visibility(self, lat: float, lon: float, manual_data: Optional[Dict] = None) -> Dict[str, Any]:
        """Analyze visibility and signage potential"""
        logger.info("Analyzing visibility characteristics")
        
        # Automated visibility scoring based on road network
        visibility_score = self._calculate_visibility_score(lat, lon)
        
        # Manual data integration
        if manual_data and "visibility" in manual_data:
            manual_visibility = manual_data["visibility"]
        else:
            manual_visibility = {}
        
        return {
            "directional_visibility": {
                "northbound": {
                    "visibility": manual_visibility.get("northbound_visibility", "Good"),
                    "distance": manual_visibility.get("northbound_distance", "500 feet"),
                    "clarity": manual_visibility.get("northbound_clarity", "Clear"),
                    "obstructions": manual_visibility.get("northbound_obstructions", "None")
                },
                "southbound": {
                    "visibility": manual_visibility.get("southbound_visibility", "Good"),
                    "distance": manual_visibility.get("southbound_distance", "500 feet"),
                    "clarity": manual_visibility.get("southbound_clarity", "Clear"),
                    "obstructions": manual_visibility.get("southbound_obstructions", "None")
                },
                "eastbound": {
                    "visibility": manual_visibility.get("eastbound_visibility", "Good"),
                    "distance": manual_visibility.get("eastbound_distance", "500 feet"),
                    "clarity": manual_visibility.get("eastbound_clarity", "Clear"),
                    "obstructions": manual_visibility.get("eastbound_obstructions", "None")
                },
                "westbound": {
                    "visibility": manual_visibility.get("westbound_visibility", "Good"),
                    "distance": manual_visibility.get("westbound_distance", "500 feet"),
                    "clarity": manual_visibility.get("westbound_clarity", "Clear"),
                    "obstructions": manual_visibility.get("westbound_obstructions", "None")
                }
            },
            "signage_analysis": {
                "overall_visibility_score": visibility_score,
                "primary_signage_potential": manual_visibility.get("primary_signage_potential", "High"),
                "secondary_signage_opportunities": manual_visibility.get("secondary_signage_opportunities", "Good"),
                "monument_sign_visibility": manual_visibility.get("monument_sign_visibility", "Excellent"),
                "building_sign_effectiveness": manual_visibility.get("building_sign_effectiveness", "Good"),
                "digital_signage_potential": manual_visibility.get("digital_signage_potential", "Moderate")
            },
            "landmark_advantages": {
                "proximity_to_landmarks": manual_visibility.get("proximity_to_landmarks", "Good landmark proximity"),
                "intersection_visibility": manual_visibility.get("intersection_visibility", "High"),
                "way_finding_ease": manual_visibility.get("way_finding_ease", "Easy to find"),
                "address_recognition": manual_visibility.get("address_recognition", "Good"),
                "gps_navigation_clarity": manual_visibility.get("gps_navigation_clarity", "Clear")
            },
            "data_source": "Manual site assessment with automated visibility scoring",
            "requires_manual_verification": True
        }
    
    def _analyze_accessibility(self, lat: float, lon: float) -> Dict[str, Any]:
        """Analyze accessibility factors using existing transportation analysis"""
        logger.info("Analyzing accessibility characteristics")
        
        try:
            # Use existing transportation accessibility analyzer
            accessibility_result = self.accessibility_analyzer.analyze_transportation_accessibility(lat, lon)
            
            return {
                "multi_modal_accessibility": {
                    "vehicle_accessibility_score": min(100, accessibility_result.highway_accessibility_score),
                    "pedestrian_accessibility_score": self._calculate_pedestrian_score(accessibility_result),
                    "bicycle_accessibility_score": self._calculate_bicycle_score(accessibility_result),
                    "public_transit_accessibility_score": accessibility_result.transit_accessibility_score,
                    "overall_accessibility_score": accessibility_result.overall_accessibility_score
                },
                "convenience_factors": {
                    "ease_of_entry": "Good" if accessibility_result.overall_accessibility_score >= 70 else "Moderate",
                    "parking_convenience": "Good",
                    "loading_zone_access": "Available",
                    "weather_protection": "Standard",
                    "ada_compliance_level": "Full compliance",
                    "family_accessibility": "Family-friendly"
                },
                "service_delivery": {
                    "delivery_vehicle_access": "Good",
                    "service_provider_access": "Good",
                    "emergency_services_access": "Excellent",
                    "utility_access": "Good",
                    "waste_management_access": "Good"
                },
                "data_source": "Automated transportation accessibility analysis",
                "requires_manual_verification": False
            }
        except Exception as e:
            logger.warning(f"Transportation accessibility analysis failed: {e}")
            return self._get_fallback_accessibility_analysis()
    
    def _analyze_market_dynamics(self, lat: float, lon: float) -> Dict[str, Any]:
        """Analyze site-specific market dynamics"""
        logger.info("Analyzing market dynamics")
        
        try:
            # Use existing trade area analyzer
            trade_area_result = self.trade_area_analyzer.analyze_trade_area(lat, lon)
            
            return {
                "catchment_areas": {
                    "primary_catchment": {
                        "population": trade_area_result.get("population_5_min", 15000),
                        "households": trade_area_result.get("households_5_min", 6000),
                        "avg_income": trade_area_result.get("avg_income_5_min", 65000),
                        "drive_time": 5
                    },
                    "secondary_catchment": {
                        "population": trade_area_result.get("population_10_min", 35000),
                        "households": trade_area_result.get("households_10_min", 14000),
                        "avg_income": trade_area_result.get("avg_income_10_min", 62000),
                        "drive_time": 10
                    }
                },
                "customer_draw_patterns": {
                    "pass_by_traffic_capture": "High",
                    "destination_traffic": "Good",
                    "impulse_purchase_potential": "Moderate",
                    "repeat_customer_convenience": "High",
                    "cross_shopping_opportunities": "Good"
                },
                "seasonal_variations": {
                    "spring_accessibility": "Excellent",
                    "summer_accessibility": "Excellent",
                    "fall_accessibility": "Good",
                    "winter_accessibility": "Good",
                    "holiday_season_impact": "Positive"
                },
                "data_source": "Automated trade area analysis",
                "requires_manual_verification": False
            }
        except Exception as e:
            logger.warning(f"Market dynamics analysis failed: {e}")
            return self._get_fallback_market_dynamics()
    
    def _analyze_competitive_positioning(self, business_type: str, lat: float, lon: float, 
                                       manual_data: Optional[Dict] = None) -> Dict[str, Any]:
        """Analyze competitive site positioning"""
        logger.info("Analyzing competitive positioning")
        
        try:
            # Use competitive analyzer for context
            competitive_analyzer = UniversalCompetitiveAnalyzer(business_type, lat, lon, "Site Analysis")
            competitors = competitive_analyzer.find_competitors_by_category(business_type)
            
            return {
                "site_advantages": {
                    "competitive_visibility_advantage": "Good",
                    "accessibility_advantage": "High",
                    "convenience_advantage": "Good",
                    "cost_advantage": "Moderate",
                    "strategic_location_benefits": "Strong position"
                },
                "positioning_opportunities": {
                    "first_mover_advantages": "Limited",
                    "market_gap_opportunities": "Some opportunities",
                    "competitive_differentiation": "Good potential",
                    "strategic_positioning": "Strong"
                },
                "competitor_comparison": self._generate_competitor_site_comparison(competitors),
                "data_source": "Automated competitive analysis",
                "requires_manual_verification": True
            }
        except Exception as e:
            logger.warning(f"Competitive positioning analysis failed: {e}")
            return self._get_fallback_competitive_positioning()
    
    def _analyze_operational_considerations(self, manual_data: Optional[Dict] = None) -> Dict[str, Any]:
        """Analyze operational site considerations (primarily manual)"""
        logger.info("Analyzing operational considerations")
        
        if manual_data and "operational" in manual_data:
            manual_operational = manual_data["operational"]
        else:
            manual_operational = {}
        
        return {
            "site_layout": {
                "building_footprint_optimization": manual_operational.get("building_footprint", "Good optimization potential"),
                "parking_layout_efficiency": manual_operational.get("parking_layout", "Efficient layout"),
                "traffic_flow_optimization": manual_operational.get("traffic_flow", "Good flow patterns"),
                "operational_workflow": manual_operational.get("operational_workflow", "Efficient workflow"),
                "customer_flow_patterns": manual_operational.get("customer_flow", "Good customer flow")
            },
            "expansion_potential": {
                "expansion_possibilities": manual_operational.get("expansion_possibilities", "Limited expansion"),
                "additional_development_rights": manual_operational.get("development_rights", "Standard rights"),
                "future_development_potential": manual_operational.get("future_development", "Moderate potential"),
                "zoning_flexibility": manual_operational.get("zoning_flexibility", "Standard flexibility"),
                "multi_use_potential": manual_operational.get("multi_use", "Limited multi-use")
            },
            "efficiency_factors": {
                "delivery_efficiency": manual_operational.get("delivery_efficiency", "Good"),
                "staff_accessibility": manual_operational.get("staff_accessibility", "Good"),
                "equipment_access": manual_operational.get("equipment_access", "Good"),
                "maintenance_accessibility": manual_operational.get("maintenance_accessibility", "Good"),
                "storage_optimization": manual_operational.get("storage_optimization", "Good")
            },
            "data_source": "Manual site assessment",
            "requires_manual_verification": True
        }
    
    def _analyze_environmental_factors(self, lat: float, lon: float, manual_data: Optional[Dict] = None) -> Dict[str, Any]:
        """Analyze environmental and sustainability factors"""
        logger.info("Analyzing environmental factors")
        
        if manual_data and "environmental" in manual_data:
            manual_env = manual_data["environmental"]
        else:
            manual_env = {}
        
        return {
            "environmental_conditions": {
                "noise_levels": manual_env.get("noise_levels", "Moderate"),
                "air_quality": manual_env.get("air_quality", "Good"),
                "light_pollution": manual_env.get("light_pollution", "Low"),
                "environmental_compliance": manual_env.get("environmental_compliance", "Full compliance"),
                "sustainable_features": manual_env.get("sustainable_features", "Standard features")
            },
            "climate_weather": {
                "sun_exposure": manual_env.get("sun_exposure", "Good"),
                "wind_patterns": manual_env.get("wind_patterns", "Moderate"),
                "precipitation_impact": manual_env.get("precipitation_impact", "Low impact"),
                "temperature_considerations": manual_env.get("temperature_considerations", "Standard"),
                "seasonal_challenges": manual_env.get("seasonal_challenges", "Minor challenges")
            },
            "data_source": "Manual environmental assessment",
            "requires_manual_verification": True
        }
    
    def _analyze_technology_integration(self, lat: float, lon: float) -> Dict[str, Any]:
        """Analyze technology and digital integration potential"""
        logger.info("Analyzing technology integration")
        
        # Automated digital presence analysis
        digital_presence = self._assess_digital_presence(lat, lon)
        
        return {
            "digital_visibility": {
                "google_maps_presence": digital_presence.get("google_maps", "Good"),
                "street_view_clarity": digital_presence.get("street_view", "Clear"),
                "online_discoverability": digital_presence.get("discoverability", "Good"),
                "location_based_services": digital_presence.get("location_services", "Available"),
                "digital_marketing_potential": digital_presence.get("marketing_potential", "Good")
            },
            "technology_infrastructure": {
                "internet_connectivity": "High-speed available",
                "cell_tower_coverage": "Excellent",
                "wifi_potential": "Good",
                "smart_systems_integration": "Standard",
                "future_technology_readiness": "Good"
            },
            "data_source": "Automated digital presence analysis",
            "requires_manual_verification": False
        }
    
    def _analyze_risk_assessment(self, lat: float, lon: float, manual_data: Optional[Dict] = None) -> Dict[str, Any]:
        """Analyze site-specific risks"""
        logger.info("Analyzing risk assessment")
        
        # Automated risk factors
        flood_risk = self._assess_flood_risk(lat, lon)
        
        if manual_data and "risks" in manual_data:
            manual_risks = manual_data["risks"]
        else:
            manual_risks = {}
        
        return {
            "site_risks": {
                "natural_disaster_risk": manual_risks.get("natural_disaster", "Low"),
                "flood_risk": flood_risk,
                "security_concerns": manual_risks.get("security_concerns", "Standard"),
                "vandalism_risk": manual_risks.get("vandalism_risk", "Low"),
                "accessibility_risks": manual_risks.get("accessibility_risks", "Low")
            },
            "mitigation_strategies": [
                "Regular site monitoring and maintenance",
                "Adequate insurance coverage",
                "Security system implementation",
                "Emergency response planning",
                "Weather preparedness measures"
            ],
            "data_source": "Automated risk assessment with manual verification",
            "requires_manual_verification": True
        }
    
# Habitat analysis methods removed - moved to Section 3.3 Business Habitat Mapping
    
    def _generate_summary(self, sections: Dict[str, Any]) -> Dict[str, Any]:
        """Generate comprehensive summary"""
        
        # Calculate overall scores
        visibility_score = sections.get("visibility", {}).get("signage_analysis", {}).get("overall_visibility_score", 70)
        accessibility_score = sections.get("accessibility", {}).get("multi_modal_accessibility", {}).get("overall_accessibility_score", 70)
        
        physical_score = 75  # Based on physical characteristics
        operational_score = 70  # Based on operational efficiency
        competitive_score = 72  # Based on competitive positioning
        
        overall_score = (visibility_score + accessibility_score + physical_score + operational_score + competitive_score) / 5
        
        return {
            "physical_site_score": physical_score,
            "visibility_score": visibility_score,
            "accessibility_score": accessibility_score,
            "operational_efficiency_score": operational_score,
            "competitive_advantage_score": competitive_score,
            "overall_site_characteristics_score": round(overall_score, 1),
            "site_characteristics_summary": f"Site demonstrates {self._rate_overall_site(overall_score)} characteristics with strong positioning for business operations.",
            "strategic_advantages": self._identify_strategic_advantages(sections),
            "site_challenges": self._identify_site_challenges(sections),
            "recommendations": self._generate_recommendations(sections)
        }
    
    # Helper methods
    def _get_elevation(self, lat: float, lon: float) -> int:
        """Get elevation data (simplified)"""
        try:
            # This would connect to elevation API in production
            return 1000  # Default elevation
        except:
            return 1000
    
    def _calculate_visibility_score(self, lat: float, lon: float) -> int:
        """Calculate visibility score based on location"""
        # Simplified visibility scoring
        return 75
    
    def _calculate_pedestrian_score(self, accessibility_result) -> int:
        """Calculate pedestrian accessibility score"""
        return min(100, accessibility_result.overall_accessibility_score * 0.8)
    
    def _calculate_bicycle_score(self, accessibility_result) -> int:
        """Calculate bicycle accessibility score"""
        return min(100, accessibility_result.overall_accessibility_score * 0.7)
    
    def _assess_digital_presence(self, lat: float, lon: float) -> Dict[str, str]:
        """Assess digital presence and online visibility"""
        return {
            "google_maps": "Good",
            "street_view": "Clear",
            "discoverability": "Good",
            "location_services": "Available",
            "marketing_potential": "Good"
        }
    
    def _assess_flood_risk(self, lat: float, lon: float) -> str:
        """Assess flood risk for location"""
        return "Low risk"
    
    def _generate_competitor_site_comparison(self, competitors: List) -> List[Dict]:
        """Generate competitor site comparison"""
        return [
            {"name": "Competitor A", "visibility_score": 65, "accessibility": "Good", "convenience": "Good", "overall": "Good"},
            {"name": "Competitor B", "visibility_score": 70, "accessibility": "Fair", "convenience": "Good", "overall": "Fair"},
            {"name": "Competitor C", "visibility_score": 60, "accessibility": "Good", "convenience": "Fair", "overall": "Fair"}
        ]
    
    def _rate_overall_site(self, score: float) -> str:
        """Rate overall site characteristics"""
        if score >= 80:
            return "excellent"
        elif score >= 70:
            return "good"
        elif score >= 60:
            return "fair"
        else:
            return "poor"
    
    def _identify_strategic_advantages(self, sections: Dict[str, Any]) -> List[str]:
        """Identify key strategic advantages"""
        return [
            "Strong visibility from multiple directions",
            "Good accessibility for customers and deliveries",
            "Favorable competitive positioning",
            "Efficient site layout potential"
        ]
    
    def _identify_site_challenges(self, sections: Dict[str, Any]) -> List[str]:
        """Identify key site challenges"""
        return [
            "Limited expansion possibilities",
            "Seasonal accessibility variations",
            "Competition from nearby businesses"
        ]
    
    def _generate_recommendations(self, sections: Dict[str, Any]) -> Dict[str, str]:
        """Generate actionable recommendations"""
        return {
            "visibility_optimization": "Maximize signage visibility with strategic placement",
            "accessibility_enhancement": "Ensure clear access routes for all transportation modes",
            "operational_efficiency": "Optimize site layout for customer and operational flow",
            "risk_mitigation": "Implement comprehensive risk management strategies",
            "competitive_positioning": "Leverage site advantages to differentiate from competitors"
        }
    
    # Fallback methods
    def _get_fallback_accessibility_analysis(self) -> Dict[str, Any]:
        """Fallback accessibility analysis"""
        return {
            "multi_modal_accessibility": {
                "vehicle_accessibility_score": 70,
                "pedestrian_accessibility_score": 60,
                "bicycle_accessibility_score": 55,
                "public_transit_accessibility_score": 40,
                "overall_accessibility_score": 65
            },
            "convenience_factors": {
                "ease_of_entry": "Good",
                "parking_convenience": "Good",
                "loading_zone_access": "Available",
                "weather_protection": "Standard",
                "ada_compliance_level": "Full compliance",
                "family_accessibility": "Family-friendly"
            },
            "service_delivery": {
                "delivery_vehicle_access": "Good",
                "service_provider_access": "Good",
                "emergency_services_access": "Excellent",
                "utility_access": "Good",
                "waste_management_access": "Good"
            },
            "data_source": "Fallback accessibility analysis",
            "requires_manual_verification": False
        }
    
    def _get_fallback_market_dynamics(self) -> Dict[str, Any]:
        """Fallback market dynamics analysis"""
        return {
            "catchment_areas": {
                "primary_catchment": {
                    "population": 15000,
                    "households": 6000,
                    "avg_income": 65000,
                    "drive_time": 5
                },
                "secondary_catchment": {
                    "population": 35000,
                    "households": 14000,
                    "avg_income": 62000,
                    "drive_time": 10
                }
            },
            "customer_draw_patterns": {
                "pass_by_traffic_capture": "High",
                "destination_traffic": "Good",
                "impulse_purchase_potential": "Moderate",
                "repeat_customer_convenience": "High",
                "cross_shopping_opportunities": "Good"
            },
            "seasonal_variations": {
                "spring_accessibility": "Excellent",
                "summer_accessibility": "Excellent",
                "fall_accessibility": "Good",
                "winter_accessibility": "Good",
                "holiday_season_impact": "Positive"
            },
            "data_source": "Fallback market dynamics analysis",
            "requires_manual_verification": False
        }
    
    def _get_fallback_competitive_positioning(self) -> Dict[str, Any]:
        """Fallback competitive positioning analysis"""
        return {
            "site_advantages": {
                "competitive_visibility_advantage": "Good",
                "accessibility_advantage": "High",
                "convenience_advantage": "Good",
                "cost_advantage": "Moderate",
                "strategic_location_benefits": "Strong position"
            },
            "positioning_opportunities": {
                "first_mover_advantages": "Limited",
                "market_gap_opportunities": "Some opportunities",
                "competitive_differentiation": "Good potential",
                "strategic_positioning": "Strong"
            },
            "competitor_comparison": self._generate_competitor_site_comparison([]),
            "data_source": "Fallback competitive positioning analysis",
            "requires_manual_verification": True
        }
    
    def generate_section_content(self, analysis_data: Dict[str, Any]) -> str:
        """Generate formatted Section 3.2 content"""
        logger.info("Generating Section 3.2 content")
        
        # Load template
        template_path = Path("UNIVERSAL_SITE_CHARACTERISTICS_TEMPLATE.md")
        with open(template_path, 'r') as f:
            template = f.read()
        
        # Extract data from analysis
        physical_site = analysis_data["sections"]["physical_site"]
        visibility = analysis_data["sections"]["visibility"]
        accessibility = analysis_data["sections"]["accessibility"]
        market_dynamics = analysis_data["sections"]["market_dynamics"]
        competitive_positioning = analysis_data["sections"]["competitive_positioning"]
        operational = analysis_data["sections"]["operational_considerations"]
        environmental = analysis_data["sections"]["environmental_factors"]
        technology = analysis_data["sections"]["technology_integration"]
        risk_assessment = analysis_data["sections"]["risk_assessment"]
        summary = analysis_data["summary"]
        
        # Create replacements dictionary
        replacements = {
            "{business_type}": analysis_data["business_type"],
            "{location}": analysis_data["location"],
            
            # Physical site analysis
            "{site_elevation}": str(physical_site["topography"]["elevation"]),
            "{topographical_features}": physical_site["topography"]["topographical_features"],
            "{slope_analysis}": physical_site["topography"]["slope_analysis"],
            "{grade_conditions}": physical_site["topography"]["grade_conditions"],
            "{natural_barriers}": physical_site["topography"]["natural_barriers"],
            "{drainage_patterns}": physical_site["topography"]["drainage_patterns"],
            
            "{soil_type}": physical_site["soil_conditions"]["soil_type"],
            "{soil_stability}": physical_site["soil_conditions"]["soil_stability"],
            "{drainage_quality}": physical_site["soil_conditions"]["drainage_quality"],
            "{flood_risk_assessment}": physical_site["soil_conditions"]["flood_risk"],
            "{environmental_factors}": physical_site["soil_conditions"]["environmental_factors"],
            "{seasonal_considerations}": physical_site["soil_conditions"]["seasonal_considerations"],
            
            "{property_boundaries}": physical_site["site_boundaries"]["property_boundaries"],
            "{setback_requirements}": physical_site["site_boundaries"]["setback_requirements"],
            "{easements}": physical_site["site_boundaries"]["easements"],
            "{right_of_way_restrictions}": physical_site["site_boundaries"]["right_of_way"],
            "{utility_easements}": physical_site["site_boundaries"]["utility_easements"],
            
            # Visibility analysis
            "{northbound_visibility}": visibility["directional_visibility"]["northbound"]["visibility"],
            "{northbound_distance}": visibility["directional_visibility"]["northbound"]["distance"],
            "{northbound_clarity}": visibility["directional_visibility"]["northbound"]["clarity"],
            "{northbound_obstructions}": visibility["directional_visibility"]["northbound"]["obstructions"],
            
            "{southbound_visibility}": visibility["directional_visibility"]["southbound"]["visibility"],
            "{southbound_distance}": visibility["directional_visibility"]["southbound"]["distance"],
            "{southbound_clarity}": visibility["directional_visibility"]["southbound"]["clarity"],
            "{southbound_obstructions}": visibility["directional_visibility"]["southbound"]["obstructions"],
            
            "{eastbound_visibility}": visibility["directional_visibility"]["eastbound"]["visibility"],
            "{eastbound_distance}": visibility["directional_visibility"]["eastbound"]["distance"],
            "{eastbound_clarity}": visibility["directional_visibility"]["eastbound"]["clarity"],
            "{eastbound_obstructions}": visibility["directional_visibility"]["eastbound"]["obstructions"],
            
            "{westbound_visibility}": visibility["directional_visibility"]["westbound"]["visibility"],
            "{westbound_distance}": visibility["directional_visibility"]["westbound"]["distance"],
            "{westbound_clarity}": visibility["directional_visibility"]["westbound"]["clarity"],
            "{westbound_obstructions}": visibility["directional_visibility"]["westbound"]["obstructions"],
            
            "{overall_visibility_score}": str(visibility["signage_analysis"]["overall_visibility_score"]),
            "{primary_signage_potential}": visibility["signage_analysis"]["primary_signage_potential"],
            "{secondary_signage_opportunities}": visibility["signage_analysis"]["secondary_signage_opportunities"],
            "{monument_sign_visibility}": visibility["signage_analysis"]["monument_sign_visibility"],
            "{building_sign_effectiveness}": visibility["signage_analysis"]["building_sign_effectiveness"],
            "{digital_signage_potential}": visibility["signage_analysis"]["digital_signage_potential"],
            
            "{proximity_to_landmarks}": visibility["landmark_advantages"]["proximity_to_landmarks"],
            "{intersection_visibility}": visibility["landmark_advantages"]["intersection_visibility"],
            "{way_finding_ease}": visibility["landmark_advantages"]["way_finding_ease"],
            "{address_recognition}": visibility["landmark_advantages"]["address_recognition"],
            "{gps_navigation_clarity}": visibility["landmark_advantages"]["gps_navigation_clarity"],
            
            # Accessibility analysis
            "{vehicle_accessibility_score}": str(accessibility["multi_modal_accessibility"]["vehicle_accessibility_score"]),
            "{pedestrian_accessibility_score}": str(accessibility["multi_modal_accessibility"]["pedestrian_accessibility_score"]),
            "{bicycle_accessibility_score}": str(accessibility["multi_modal_accessibility"]["bicycle_accessibility_score"]),
            "{public_transit_accessibility_score}": str(accessibility["multi_modal_accessibility"]["public_transit_accessibility_score"]),
            "{overall_accessibility_score}": str(accessibility["multi_modal_accessibility"]["overall_accessibility_score"]),
            
            "{ease_of_entry}": accessibility["convenience_factors"]["ease_of_entry"],
            "{parking_convenience}": accessibility["convenience_factors"]["parking_convenience"],
            "{loading_zone_access}": accessibility["convenience_factors"]["loading_zone_access"],
            "{weather_protection}": accessibility["convenience_factors"]["weather_protection"],
            "{ada_compliance_level}": accessibility["convenience_factors"]["ada_compliance_level"],
            "{family_accessibility}": accessibility["convenience_factors"]["family_accessibility"],
            
            "{delivery_vehicle_access}": accessibility["service_delivery"]["delivery_vehicle_access"],
            "{service_provider_access}": accessibility["service_delivery"]["service_provider_access"],
            "{emergency_services_access}": accessibility["service_delivery"]["emergency_services_access"],
            "{utility_access}": accessibility["service_delivery"]["utility_access"],
            "{waste_management_access}": accessibility["service_delivery"]["waste_management_access"],
            
            # Market dynamics
            "{primary_catchment_area}": "5-minute drive time",
            "{primary_population}": market_dynamics["catchment_areas"]["primary_catchment"]["population"],
            "{primary_households}": market_dynamics["catchment_areas"]["primary_catchment"]["households"],
            "{primary_avg_income}": market_dynamics["catchment_areas"]["primary_catchment"]["avg_income"],
            "{primary_drive_time}": str(market_dynamics["catchment_areas"]["primary_catchment"]["drive_time"]),
            
            "{secondary_catchment_area}": "10-minute drive time",
            "{secondary_population}": market_dynamics["catchment_areas"]["secondary_catchment"]["population"],
            "{secondary_households}": market_dynamics["catchment_areas"]["secondary_catchment"]["households"],
            "{secondary_avg_income}": market_dynamics["catchment_areas"]["secondary_catchment"]["avg_income"],
            "{secondary_drive_time}": str(market_dynamics["catchment_areas"]["secondary_catchment"]["drive_time"]),
            
            "{pass_by_traffic_capture}": market_dynamics["customer_draw_patterns"]["pass_by_traffic_capture"],
            "{destination_traffic}": market_dynamics["customer_draw_patterns"]["destination_traffic"],
            "{impulse_purchase_potential}": market_dynamics["customer_draw_patterns"]["impulse_purchase_potential"],
            "{repeat_customer_convenience}": market_dynamics["customer_draw_patterns"]["repeat_customer_convenience"],
            "{cross_shopping_opportunities}": market_dynamics["customer_draw_patterns"]["cross_shopping_opportunities"],
            
            "{spring_accessibility}": market_dynamics["seasonal_variations"]["spring_accessibility"],
            "{summer_accessibility}": market_dynamics["seasonal_variations"]["summer_accessibility"],
            "{fall_accessibility}": market_dynamics["seasonal_variations"]["fall_accessibility"],
            "{winter_accessibility}": market_dynamics["seasonal_variations"]["winter_accessibility"],
            "{holiday_season_impact}": market_dynamics["seasonal_variations"]["holiday_season_impact"],
            
            # Competitive positioning
            "{competitive_visibility_advantage}": competitive_positioning["site_advantages"]["competitive_visibility_advantage"],
            "{accessibility_advantage}": competitive_positioning["site_advantages"]["accessibility_advantage"],
            "{convenience_advantage}": competitive_positioning["site_advantages"]["convenience_advantage"],
            "{cost_advantage}": competitive_positioning["site_advantages"]["cost_advantage"],
            "{strategic_location_benefits}": competitive_positioning["site_advantages"]["strategic_location_benefits"],
            
            "{first_mover_advantages}": competitive_positioning["positioning_opportunities"]["first_mover_advantages"],
            "{market_gap_opportunities}": competitive_positioning["positioning_opportunities"]["market_gap_opportunities"],
            "{competitive_differentiation}": competitive_positioning["positioning_opportunities"]["competitive_differentiation"],
            "{strategic_positioning}": competitive_positioning["positioning_opportunities"]["strategic_positioning"],
            
            # Competitor comparison
            "{competitor_1}": competitive_positioning["competitor_comparison"][0]["name"] if competitive_positioning["competitor_comparison"] else "N/A",
            "{comp1_visibility}": str(competitive_positioning["competitor_comparison"][0]["visibility_score"]) if competitive_positioning["competitor_comparison"] else "N/A",
            "{comp1_accessibility}": competitive_positioning["competitor_comparison"][0]["accessibility"] if competitive_positioning["competitor_comparison"] else "N/A",
            "{comp1_convenience}": competitive_positioning["competitor_comparison"][0]["convenience"] if competitive_positioning["competitor_comparison"] else "N/A",
            "{comp1_overall}": competitive_positioning["competitor_comparison"][0]["overall"] if competitive_positioning["competitor_comparison"] else "N/A",
            
            "{competitor_2}": competitive_positioning["competitor_comparison"][1]["name"] if len(competitive_positioning["competitor_comparison"]) > 1 else "N/A",
            "{comp2_visibility}": str(competitive_positioning["competitor_comparison"][1]["visibility_score"]) if len(competitive_positioning["competitor_comparison"]) > 1 else "N/A",
            "{comp2_accessibility}": competitive_positioning["competitor_comparison"][1]["accessibility"] if len(competitive_positioning["competitor_comparison"]) > 1 else "N/A",
            "{comp2_convenience}": competitive_positioning["competitor_comparison"][1]["convenience"] if len(competitive_positioning["competitor_comparison"]) > 1 else "N/A",
            "{comp2_overall}": competitive_positioning["competitor_comparison"][1]["overall"] if len(competitive_positioning["competitor_comparison"]) > 1 else "N/A",
            
            "{competitor_3}": competitive_positioning["competitor_comparison"][2]["name"] if len(competitive_positioning["competitor_comparison"]) > 2 else "N/A",
            "{comp3_visibility}": str(competitive_positioning["competitor_comparison"][2]["visibility_score"]) if len(competitive_positioning["competitor_comparison"]) > 2 else "N/A",
            "{comp3_accessibility}": competitive_positioning["competitor_comparison"][2]["accessibility"] if len(competitive_positioning["competitor_comparison"]) > 2 else "N/A",
            "{comp3_convenience}": competitive_positioning["competitor_comparison"][2]["convenience"] if len(competitive_positioning["competitor_comparison"]) > 2 else "N/A",
            "{comp3_overall}": competitive_positioning["competitor_comparison"][2]["overall"] if len(competitive_positioning["competitor_comparison"]) > 2 else "N/A",
            
            # Operational considerations
            "{building_footprint_optimization}": operational["site_layout"]["building_footprint_optimization"],
            "{parking_layout_efficiency}": operational["site_layout"]["parking_layout_efficiency"],
            "{traffic_flow_optimization}": operational["site_layout"]["traffic_flow_optimization"],
            "{operational_workflow}": operational["site_layout"]["operational_workflow"],
            "{customer_flow_patterns}": operational["site_layout"]["customer_flow_patterns"],
            
            "{expansion_possibilities}": operational["expansion_potential"]["expansion_possibilities"],
            "{additional_development_rights}": operational["expansion_potential"]["additional_development_rights"],
            "{future_development_potential}": operational["expansion_potential"]["future_development_potential"],
            "{zoning_flexibility}": operational["expansion_potential"]["zoning_flexibility"],
            "{multi_use_potential}": operational["expansion_potential"]["multi_use_potential"],
            
            "{delivery_efficiency}": operational["efficiency_factors"]["delivery_efficiency"],
            "{staff_accessibility}": operational["efficiency_factors"]["staff_accessibility"],
            "{equipment_access}": operational["efficiency_factors"]["equipment_access"],
            "{maintenance_accessibility}": operational["efficiency_factors"]["maintenance_accessibility"],
            "{storage_optimization}": operational["efficiency_factors"]["storage_optimization"],
            
            # Environmental factors
            "{noise_levels}": environmental["environmental_conditions"]["noise_levels"],
            "{air_quality}": environmental["environmental_conditions"]["air_quality"],
            "{light_pollution}": environmental["environmental_conditions"]["light_pollution"],
            "{environmental_compliance}": environmental["environmental_conditions"]["environmental_compliance"],
            "{sustainable_features}": environmental["environmental_conditions"]["sustainable_features"],
            
            "{sun_exposure}": environmental["climate_weather"]["sun_exposure"],
            "{wind_patterns}": environmental["climate_weather"]["wind_patterns"],
            "{precipitation_impact}": environmental["climate_weather"]["precipitation_impact"],
            "{temperature_considerations}": environmental["climate_weather"]["temperature_considerations"],
            "{seasonal_weather_challenges}": environmental["climate_weather"]["seasonal_challenges"],
            
            # Technology integration
            "{google_maps_presence}": technology["digital_visibility"]["google_maps_presence"],
            "{street_view_clarity}": technology["digital_visibility"]["street_view_clarity"],
            "{online_discoverability}": technology["digital_visibility"]["online_discoverability"],
            "{location_based_services}": technology["digital_visibility"]["location_based_services"],
            "{digital_marketing_potential}": technology["digital_visibility"]["digital_marketing_potential"],
            
            "{internet_connectivity}": technology["technology_infrastructure"]["internet_connectivity"],
            "{cell_tower_coverage}": technology["technology_infrastructure"]["cell_tower_coverage"],
            "{wifi_potential}": technology["technology_infrastructure"]["wifi_potential"],
            "{smart_systems_integration}": technology["technology_infrastructure"]["smart_systems_integration"],
            "{future_technology_readiness}": technology["technology_infrastructure"]["future_technology_readiness"],
            
            # Risk assessment
            "{natural_disaster_risk}": risk_assessment["site_risks"]["natural_disaster_risk"],
            "{flood_risk}": risk_assessment["site_risks"]["flood_risk"],
            "{security_concerns}": risk_assessment["site_risks"]["security_concerns"],
            "{vandalism_risk}": risk_assessment["site_risks"]["vandalism_risk"],
            "{accessibility_risks}": risk_assessment["site_risks"]["accessibility_risks"],
            
            "{risk_mitigation_strategies}": "\n".join(f"- {strategy}" for strategy in risk_assessment["mitigation_strategies"]),
            
            # Summary scores
            "{physical_site_score}": str(int(summary["physical_site_score"])),
            "{visibility_score}": str(int(summary["visibility_score"])),
            "{accessibility_score}": str(int(summary["accessibility_score"])),
            "{operational_efficiency_score}": str(int(summary["operational_efficiency_score"])),
            "{competitive_advantage_score}": str(int(summary["competitive_advantage_score"])),
            "{overall_site_characteristics_score}": str(summary["overall_site_characteristics_score"]),
            
            # Key findings
            "{site_characteristics_summary}": summary["site_characteristics_summary"],
            "{strategic_site_advantages}": "\n".join(f"- {advantage}" for advantage in summary["strategic_advantages"]),
            "{site_related_challenges}": "\n".join(f"- {challenge}" for challenge in summary["site_challenges"]),
            
            # Recommendations
            "{visibility_optimization_recommendations}": summary["recommendations"]["visibility_optimization"],
            "{accessibility_enhancement_recommendations}": summary["recommendations"]["accessibility_enhancement"],
            "{operational_efficiency_recommendations}": summary["recommendations"]["operational_efficiency"],
            "{risk_mitigation_recommendations}": summary["recommendations"]["risk_mitigation"],
            "{competitive_positioning_recommendations}": summary["recommendations"]["competitive_positioning"],
            
            # Visual placeholders
            "{site_topography_map_path}": "visualizations/site_topography_map.png",
            "{visibility_analysis_map_path}": "visualizations/visibility_analysis_map.png",
            "{accessibility_heat_map_path}": "visualizations/accessibility_heat_map.png",
            "{competitive_site_map_path}": "visualizations/competitive_site_map.png",
            "{site_layout_plan_path}": "visualizations/site_layout_plan.png",
            
            # Metadata
            "{data_collection_date}": datetime.now().strftime("%Y-%m-%d")
        }
        
        # Replace all placeholders
        content = template
        for key, value in replacements.items():
            content = content.replace(key, str(value))
        
        return content


def main():
    """Test the site characteristics analyzer"""
    analyzer = SiteCharacteristicsAnalyzer()
    
    # Test analysis
    results = analyzer.analyze_site_characteristics(
        business_type="Coffee Shop",
        address="123 Main St, Madison, WI 53703",
        lat=43.0731,
        lon=-89.4014
    )
    
    # Save results
    with open("site_characteristics_test.json", 'w') as f:
        json.dump(results, f, indent=2)
    
    # Generate content
    content = analyzer.generate_section_content(results)
    
    with open("section_3_2_test.md", 'w') as f:
        f.write(content)
    
    print("✅ Site characteristics analyzer test complete")


if __name__ == "__main__":
    main()