#!/usr/bin/env python3
"""
Market Saturation Analyzer
=========================

Analyzes market saturation levels by integrating business density, population metrics,
and competitive landscape data to determine market capacity and opportunities.
"""

import json
import logging
from pathlib import Path
from datetime import datetime
from typing import Dict, List, Any, Optional, Tuple
import math

# Import existing analyzers and collectors
from osm_competitive_analysis import OSMCompetitiveAnalysis
from trade_area_analyzer import TradeAreaAnalyzer
from market_opportunity_scanner import MarketOpportunityScanner
from census_collector import CensusDataCollector
from universal_competitive_analyzer import UniversalCompetitiveAnalyzer

logger = logging.getLogger(__name__)

class MarketSaturationAnalyzer:
    """Comprehensive market saturation analysis for business feasibility studies"""
    
    # Industry-specific saturation thresholds (businesses per sq mile)
    SATURATION_THRESHOLDS = {
        "restaurant": {"low": 2, "medium": 5, "high": 10, "saturated": 15},
        "food_beverage": {"low": 2, "medium": 5, "high": 10, "saturated": 15},
        "retail": {"low": 3, "medium": 8, "high": 15, "saturated": 20},
        "auto_repair": {"low": 1, "medium": 2, "high": 4, "saturated": 6},
        "healthcare": {"low": 1, "medium": 2, "high": 4, "saturated": 6},
        "personal_service": {"low": 2, "medium": 4, "high": 8, "saturated": 12},
        "professional_service": {"low": 3, "medium": 6, "high": 12, "saturated": 18},
        "default": {"low": 2, "medium": 5, "high": 10, "saturated": 15}
    }
    
    # Businesses per 1,000 residents benchmarks
    PER_CAPITA_BENCHMARKS = {
        "restaurant": {"low": 0.5, "adequate": 1.0, "high": 2.0, "saturated": 3.0},
        "food_beverage": {"low": 0.5, "adequate": 1.0, "high": 2.0, "saturated": 3.0},
        "retail": {"low": 1.0, "adequate": 2.0, "high": 4.0, "saturated": 6.0},
        "auto_repair": {"low": 0.2, "adequate": 0.4, "high": 0.8, "saturated": 1.2},
        "healthcare": {"low": 0.3, "adequate": 0.6, "high": 1.0, "saturated": 1.5},
        "personal_service": {"low": 0.4, "adequate": 0.8, "high": 1.5, "saturated": 2.5},
        "professional_service": {"low": 0.5, "adequate": 1.5, "high": 3.0, "saturated": 5.0},
        "default": {"low": 0.5, "adequate": 1.0, "high": 2.0, "saturated": 3.0}
    }
    
    def __init__(self):
        self.osm_analyzer = OSMCompetitiveAnalysis()
        self.trade_area_analyzer = TradeAreaAnalyzer()
        self.opportunity_scanner = MarketOpportunityScanner()
        self.census_collector = CensusDataCollector("Wisconsin")
        # UniversalCompetitiveAnalyzer will be initialized per analysis
        self.competitive_analyzer = None
        
    def analyze_market_saturation(self, business_type: str, address: str, 
                                 lat: float, lon: float) -> Dict[str, Any]:
        """
        Perform comprehensive market saturation analysis
        
        Args:
            business_type: Type of business (e.g., "restaurant", "auto_repair")
            address: Business address
            lat: Latitude
            lon: Longitude
            
        Returns:
            Dictionary containing complete saturation analysis
        """
        logger.info(f"Starting market saturation analysis for {business_type} at {address}")
        
        # Initialize competitive analyzer for this analysis
        self.competitive_analyzer = UniversalCompetitiveAnalyzer(
            business_type=business_type,
            site_lat=lat,
            site_lng=lon,
            site_address=address
        )
        
        results = {
            "business_type": business_type,
            "location": address,
            "coordinates": {"lat": lat, "lon": lon},
            "analysis_date": datetime.now().isoformat(),
            "sections": {}
        }
        
        try:
            # 1. Business Density Analysis
            density_analysis = self._analyze_business_density(business_type, lat, lon)
            results["sections"]["density_analysis"] = density_analysis
            
            # 2. Population-Based Metrics
            population_metrics = self._analyze_population_metrics(lat, lon, density_analysis)
            results["sections"]["population_metrics"] = population_metrics
            
            # 3. Market Gap Identification
            market_gaps = self._identify_market_gaps(business_type, lat, lon, density_analysis)
            results["sections"]["market_gaps"] = market_gaps
            
            # 4. Competitive Landscape
            competitive_landscape = self._analyze_competitive_landscape(business_type, lat, lon)
            results["sections"]["competitive_landscape"] = competitive_landscape
            
            # 5. Saturation Indicators
            saturation_indicators = self._calculate_saturation_indicators(
                density_analysis, population_metrics, competitive_landscape, market_gaps
            )
            results["sections"]["saturation_indicators"] = saturation_indicators
            
            # 6. Market Entry Assessment
            market_entry = self._assess_market_entry(
                business_type, saturation_indicators, market_gaps
            )
            results["sections"]["market_entry_assessment"] = market_entry
            
            # 7. Generate summary
            results["summary"] = self._generate_summary(results["sections"])
            
        except Exception as e:
            logger.error(f"Error in market saturation analysis: {str(e)}")
            results["error"] = str(e)
            
        return results
    
    def _analyze_business_density(self, business_type: str, lat: float, lon: float) -> Dict[str, Any]:
        """Analyze business density at various radii"""
        logger.info("Analyzing business density")
        
        # Get competitive data using available methods
        competitive_data = {}
        
        # Load data first
        self.competitive_analyzer.load_data()
        
        # Get competitors by category
        competitors_by_category = self.competitive_analyzer.find_competitors_by_category(5.0)
        
        # Convert to our expected format
        all_competitors = []
        for category, df in competitors_by_category.items():
            for _, row in df.iterrows():
                all_competitors.append({
                    "name": row.get("name", "Unknown"),
                    "category": category,
                    "distance": row.get("distance", 0),
                    "is_direct_competitor": category == "direct_competitors"
                })
        
        # Sort by distance and create distance-based groups
        all_competitors.sort(key=lambda x: x["distance"])
        
        competitors_1mi = [c for c in all_competitors if c["distance"] <= 1.0]
        competitors_3mi = [c for c in all_competitors if c["distance"] <= 3.0]
        competitors_5mi = [c for c in all_competitors if c["distance"] <= 5.0]
        
        competitive_data = {
            "competitors_1_mile": competitors_1mi,
            "competitors_3_mile": competitors_3mi,
            "competitors_5_mile": competitors_5mi
        }
        
        # Calculate areas for density calculations (in square miles)
        area_1mi = math.pi * 1**2
        area_3mi = math.pi * 3**2
        area_5mi = math.pi * 5**2
        
        # Extract competitor counts
        competitors_1mi = competitive_data.get("competitors_1_mile", [])
        competitors_3mi = competitive_data.get("competitors_3_mile", [])
        competitors_5mi = competitive_data.get("competitors_5_mile", [])
        
        density_data = {
            "1_mile": {
                "count": len(competitors_1mi),
                "area_sq_miles": round(area_1mi, 2),
                "density_per_sq_mile": round(len(competitors_1mi) / area_1mi, 2)
            },
            "3_mile": {
                "count": len(competitors_3mi),
                "area_sq_miles": round(area_3mi, 2),
                "density_per_sq_mile": round(len(competitors_3mi) / area_3mi, 2)
            },
            "5_mile": {
                "count": len(competitors_5mi),
                "area_sq_miles": round(area_5mi, 2),
                "density_per_sq_mile": round(len(competitors_5mi) / area_5mi, 2)
            }
        }
        
        # Determine saturation level based on 3-mile density
        density_3mi = density_data["3_mile"]["density_per_sq_mile"]
        thresholds = self._get_thresholds(business_type)
        
        if density_3mi < thresholds["low"]:
            saturation_level = "Low Saturation"
            saturation_score = 25
        elif density_3mi < thresholds["medium"]:
            saturation_level = "Medium Saturation"
            saturation_score = 50
        elif density_3mi < thresholds["high"]:
            saturation_level = "High Saturation"
            saturation_score = 75
        else:
            saturation_level = "Over-Saturated"
            saturation_score = 90
            
        density_data["saturation_level"] = saturation_level
        density_data["saturation_score"] = saturation_score
        density_data["thresholds"] = thresholds
        
        return density_data
    
    def _analyze_population_metrics(self, lat: float, lon: float, 
                                   density_analysis: Dict[str, Any]) -> Dict[str, Any]:
        """Analyze population-based market metrics"""
        logger.info("Analyzing population metrics")
        
        # Get trade area analysis
        trade_area_data = self.trade_area_analyzer.analyze_trade_area(
            "target_location", "Target Location", "business", lat, lon
        )
        
        # Extract population data for different radii
        pop_data = {
            "trade_area_population": 0,
            "trade_area_households": 0,
            "avg_household_income": 0,
            "drive_time_populations": {}
        }
        
        # Get 3-mile radius population (primary trade area)
        if "isochrones" in trade_area_data and "10_min" in trade_area_data["isochrones"]:
            demographics = trade_area_data["isochrones"]["10_min"].get("demographics", {})
            pop_data["trade_area_population"] = demographics.get("total_population", 0)
            pop_data["trade_area_households"] = demographics.get("households", 0)
            pop_data["avg_household_income"] = demographics.get("median_income", 0)
        
        # Calculate drive-time populations
        for time, key in [(5, "5_min"), (10, "10_min"), (15, "15_min")]:
            if key in trade_area_data.get("isochrones", {}):
                pop_data["drive_time_populations"][f"{time}_minutes"] = \
                    trade_area_data["isochrones"][key].get("demographics", {}).get("total_population", 0)
        
        # Calculate businesses per 1,000 residents
        if pop_data["trade_area_population"] > 0:
            business_count = density_analysis["3_mile"]["count"]
            pop_data["businesses_per_1000"] = round(
                (business_count / pop_data["trade_area_population"]) * 1000, 2
            )
        else:
            pop_data["businesses_per_1000"] = 0
            
        # Assess per capita saturation
        per_capita = pop_data["businesses_per_1000"]
        benchmarks = self._get_per_capita_benchmarks(density_analysis.get("business_type", "default"))
        
        if per_capita < benchmarks["low"]:
            pop_data["per_capita_assessment"] = "Underserved"
        elif per_capita < benchmarks["adequate"]:
            pop_data["per_capita_assessment"] = "Below Average"
        elif per_capita < benchmarks["high"]:
            pop_data["per_capita_assessment"] = "Adequate"
        elif per_capita < benchmarks["saturated"]:
            pop_data["per_capita_assessment"] = "Above Average"
        else:
            pop_data["per_capita_assessment"] = "Oversaturated"
            
        pop_data["per_capita_benchmarks"] = benchmarks
        
        return pop_data
    
    def _identify_market_gaps(self, business_type: str, lat: float, lon: float,
                            density_analysis: Dict[str, Any]) -> Dict[str, Any]:
        """Identify market gaps and underserved areas"""
        logger.info("Identifying market gaps")
        
        # Use market opportunity scanner
        opportunities = self.opportunity_scanner.scan_market_opportunities(
            "Wisconsin", [{"name": "Target City", "lat": lat, "lon": lon}]
        )
        
        gaps_data = {
            "service_gap_analysis": [],
            "underserved_areas": [],
            "market_opportunities": [],
            "opportunity_score": 0
        }
        
        # Calculate opportunity score (inverse of saturation)
        saturation_score = density_analysis.get("saturation_score", 50)
        gaps_data["opportunity_score"] = max(0, 100 - saturation_score)
        
        # Identify service gaps based on density
        density_3mi = density_analysis["3_mile"]["density_per_sq_mile"]
        thresholds = density_analysis["thresholds"]
        
        if density_3mi < thresholds["low"]:
            gaps_data["service_gap_analysis"].append(
                f"Significant service gap identified - only {density_3mi:.1f} {business_type} "
                f"businesses per square mile (benchmark: {thresholds['low']}-{thresholds['medium']})"
            )
            gaps_data["market_opportunities"].append(
                "High opportunity for market entry with limited competition"
            )
        elif density_3mi < thresholds["medium"]:
            gaps_data["service_gap_analysis"].append(
                f"Moderate service availability - {density_3mi:.1f} businesses per square mile"
            )
            gaps_data["market_opportunities"].append(
                "Room for differentiated offerings or specialized services"
            )
        else:
            gaps_data["service_gap_analysis"].append(
                f"Market well-served with {density_3mi:.1f} businesses per square mile"
            )
            gaps_data["market_opportunities"].append(
                "Focus on unique value propositions or underserved niches required"
            )
            
        # Identify underserved areas (placeholder for more sophisticated analysis)
        if density_3mi < thresholds["medium"]:
            gaps_data["underserved_areas"].append(
                "Residential areas beyond 2-mile radius have limited access"
            )
            gaps_data["underserved_areas"].append(
                "Growing neighborhoods to the north and west lack nearby options"
            )
            
        return gaps_data
    
    def _analyze_competitive_landscape(self, business_type: str, lat: float, lon: float) -> Dict[str, Any]:
        """Analyze the competitive landscape"""
        logger.info("Analyzing competitive landscape")
        
        # Get detailed competitive analysis using available methods
        competitive_data = {}
        
        # Get competitors by category
        competitors_by_category = self.competitive_analyzer.find_competitors_by_category(5.0)
        
        # Convert to our expected format
        all_competitors = []
        for category, df in competitors_by_category.items():
            for _, row in df.iterrows():
                all_competitors.append({
                    "name": row.get("name", "Unknown"),
                    "category": category,
                    "distance": row.get("distance", 0),
                    "is_direct_competitor": category == "direct_competitors"
                })
        
        # Sort by distance and create distance-based groups
        all_competitors.sort(key=lambda x: x["distance"])
        
        competitors_1mi = [c for c in all_competitors if c["distance"] <= 1.0]
        competitors_3mi = [c for c in all_competitors if c["distance"] <= 3.0]
        competitors_5mi = [c for c in all_competitors if c["distance"] <= 5.0]
        
        competitive_data = {
            "competitors_1_mile": competitors_1mi,
            "competitors_3_mile": competitors_3mi,
            "competitors_5_mile": competitors_5mi
        }
        
        landscape_data = {
            "competition_distribution": {},
            "business_type_analysis": {},
            "competitive_clustering": []
        }
        
        # Competition distribution by distance
        competitors_1mi = competitive_data.get("competitors_1_mile", [])
        competitors_3mi = competitive_data.get("competitors_3_mile", [])
        competitors_5mi = competitive_data.get("competitors_5_mile", [])
        
        # Calculate distribution
        landscape_data["competition_distribution"] = {
            "1_mile": {
                "direct": len([c for c in competitors_1mi if c.get("is_direct_competitor", True)]),
                "similar": len([c for c in competitors_1mi if not c.get("is_direct_competitor", True)]),
                "total": len(competitors_1mi)
            },
            "1_3_miles": {
                "direct": len([c for c in competitors_3mi if c not in competitors_1mi and c.get("is_direct_competitor", True)]),
                "similar": len([c for c in competitors_3mi if c not in competitors_1mi and not c.get("is_direct_competitor", True)]),
                "total": len([c for c in competitors_3mi if c not in competitors_1mi])
            },
            "3_5_miles": {
                "direct": len([c for c in competitors_5mi if c not in competitors_3mi and c.get("is_direct_competitor", True)]),
                "similar": len([c for c in competitors_5mi if c not in competitors_3mi and not c.get("is_direct_competitor", True)]),
                "total": len([c for c in competitors_5mi if c not in competitors_3mi])
            }
        }
        
        # Business type analysis
        all_competitors = competitors_5mi
        franchise_count = len([c for c in all_competitors if self._is_franchise(c.get("name", ""))])
        chain_count = len([c for c in all_competitors if self._is_chain(c.get("name", ""))])
        independent_count = len(all_competitors) - franchise_count - chain_count
        
        total = len(all_competitors)
        if total > 0:
            landscape_data["business_type_analysis"] = {
                "franchise": {"count": franchise_count, "percent": round(franchise_count/total * 100, 1)},
                "chain": {"count": chain_count, "percent": round(chain_count/total * 100, 1)},
                "independent": {"count": independent_count, "percent": round(independent_count/total * 100, 1)}
            }
        
        # Clustering analysis
        if len(competitors_1mi) >= 3:
            landscape_data["competitive_clustering"].append(
                f"High concentration cluster identified within 1 mile ({len(competitors_1mi)} competitors)"
            )
        
        if len([c for c in competitors_3mi if c not in competitors_1mi]) >= 5:
            landscape_data["competitive_clustering"].append(
                "Secondary cluster identified in 1-3 mile ring"
            )
            
        return landscape_data
    
    def _calculate_saturation_indicators(self, density: Dict, population: Dict,
                                       competitive: Dict, gaps: Dict) -> Dict[str, Any]:
        """Calculate comprehensive saturation indicators"""
        logger.info("Calculating saturation indicators")
        
        indicators = {
            "component_scores": {},
            "overall_saturation_score": 0
        }
        
        # 1. Business Density Score (25 points max)
        density_score = min(25, (density["saturation_score"] / 100) * 25)
        indicators["component_scores"]["density"] = round(density_score, 1)
        
        # 2. Population Coverage Score (25 points max)
        per_capita = population.get("businesses_per_1000", 0)
        benchmarks = population.get("per_capita_benchmarks", {})
        if per_capita < benchmarks.get("low", 0.5):
            pop_score = 5
        elif per_capita < benchmarks.get("adequate", 1.0):
            pop_score = 10
        elif per_capita < benchmarks.get("high", 2.0):
            pop_score = 15
        elif per_capita < benchmarks.get("saturated", 3.0):
            pop_score = 20
        else:
            pop_score = 25
        indicators["component_scores"]["population_coverage"] = pop_score
        
        # 3. Competition Level Score (25 points max)
        total_competitors = competitive["competition_distribution"]["1_mile"]["total"]
        if total_competitors < 2:
            comp_score = 5
        elif total_competitors < 5:
            comp_score = 10
        elif total_competitors < 10:
            comp_score = 15
        elif total_competitors < 15:
            comp_score = 20
        else:
            comp_score = 25
        indicators["component_scores"]["competition_level"] = comp_score
        
        # 4. Market Gap Score (25 points max) - inverse of opportunity
        gap_score = 25 - (gaps["opportunity_score"] / 100 * 25)
        indicators["component_scores"]["market_gap"] = round(gap_score, 1)
        
        # Calculate overall score
        indicators["overall_saturation_score"] = round(
            sum(indicators["component_scores"].values()), 1
        )
        
        return indicators
    
    def _assess_market_entry(self, business_type: str, saturation: Dict, gaps: Dict) -> Dict[str, Any]:
        """Assess market entry feasibility"""
        logger.info("Assessing market entry feasibility")
        
        assessment = {
            "entry_viability": "",
            "location_strategy": "",
            "differentiation_needs": "",
            "target_market_recommendation": "",
            "risk_level": "",
            "risk_concerns": [],
            "mitigation_strategies": []
        }
        
        score = saturation["overall_saturation_score"]
        
        # Entry viability assessment
        if score < 30:
            assessment["entry_viability"] = "Excellent - Market is underserved with high entry potential"
            assessment["risk_level"] = "Low"
        elif score < 50:
            assessment["entry_viability"] = "Good - Market has room for additional businesses"
            assessment["risk_level"] = "Low-Medium"
        elif score < 70:
            assessment["entry_viability"] = "Moderate - Market is competitive but viable with right approach"
            assessment["risk_level"] = "Medium"
        elif score < 85:
            assessment["entry_viability"] = "Challenging - Market is highly saturated"
            assessment["risk_level"] = "High"
        else:
            assessment["entry_viability"] = "Not Recommended - Market is oversaturated"
            assessment["risk_level"] = "Very High"
            
        # Location strategy
        if score < 50:
            assessment["location_strategy"] = "Prime locations available - focus on high-traffic areas"
        else:
            assessment["location_strategy"] = "Consider peripheral locations or underserved neighborhoods"
            
        # Differentiation needs
        if score < 30:
            assessment["differentiation_needs"] = "Standard offerings sufficient due to limited competition"
        elif score < 60:
            assessment["differentiation_needs"] = "Some differentiation recommended - quality, service, or specialization"
        else:
            assessment["differentiation_needs"] = "Strong differentiation required - unique concept, pricing, or niche focus"
            
        # Risk concerns and mitigation
        if score >= 70:
            assessment["risk_concerns"].append("High competition may limit market share")
            assessment["risk_concerns"].append("Price pressure from established competitors")
            assessment["mitigation_strategies"].append("Develop unique value proposition")
            assessment["mitigation_strategies"].append("Focus on underserved customer segments")
            
        if score >= 50:
            assessment["risk_concerns"].append("Customer acquisition may be challenging")
            assessment["mitigation_strategies"].append("Invest in targeted marketing")
            assessment["mitigation_strategies"].append("Build strong local partnerships")
            
        return assessment
    
    def _get_thresholds(self, business_type: str) -> Dict[str, float]:
        """Get saturation thresholds for business type"""
        normalized_type = business_type.lower().replace(" ", "_")
        return self.SATURATION_THRESHOLDS.get(normalized_type, self.SATURATION_THRESHOLDS["default"])
    
    def _get_per_capita_benchmarks(self, business_type: str) -> Dict[str, float]:
        """Get per capita benchmarks for business type"""
        normalized_type = business_type.lower().replace(" ", "_")
        return self.PER_CAPITA_BENCHMARKS.get(normalized_type, self.PER_CAPITA_BENCHMARKS["default"])
    
    def _is_franchise(self, name: str) -> bool:
        """Check if business is a franchise"""
        franchise_keywords = ["McDonald's", "Subway", "Starbucks", "Dunkin", "Pizza Hut",
                            "Domino's", "KFC", "Burger King", "Wendy's", "Taco Bell",
                            "Chipotle", "Panera", "Five Guys", "Jimmy John's"]
        return any(keyword.lower() in name.lower() for keyword in franchise_keywords)
    
    def _is_chain(self, name: str) -> bool:
        """Check if business is a chain"""
        chain_keywords = ["Walmart", "Target", "CVS", "Walgreens", "Home Depot",
                         "Lowe's", "Best Buy", "AutoZone", "O'Reilly", "Advance Auto"]
        return any(keyword.lower() in name.lower() for keyword in chain_keywords)
    
    def _generate_summary(self, sections: Dict[str, Any]) -> Dict[str, Any]:
        """Generate analysis summary"""
        density = sections.get("density_analysis", {})
        saturation = sections.get("saturation_indicators", {})
        entry = sections.get("market_entry_assessment", {})
        
        return {
            "saturation_level": density.get("saturation_level", "Unknown"),
            "overall_score": saturation.get("overall_saturation_score", 0),
            "entry_recommendation": entry.get("entry_viability", ""),
            "risk_level": entry.get("risk_level", ""),
            "key_findings": [
                f"Market shows {density.get('saturation_level', 'unknown')} saturation",
                f"Overall saturation score: {saturation.get('overall_saturation_score', 0)}/100",
                f"Market entry risk: {entry.get('risk_level', 'Unknown')}"
            ]
        }
    
    def generate_section_content(self, analysis_data: Dict[str, Any]) -> str:
        """Generate the formatted content for Section 2.2"""
        logger.info("Generating Section 2.2 content")
        
        # Load template
        template_path = Path("UNIVERSAL_MARKET_SATURATION_TEMPLATE.md")
        with open(template_path, 'r') as f:
            template = f.read()
            
        # Extract data from analysis
        density = analysis_data["sections"]["density_analysis"]
        population = analysis_data["sections"]["population_metrics"]
        gaps = analysis_data["sections"]["market_gaps"]
        competitive = analysis_data["sections"]["competitive_landscape"]
        saturation = analysis_data["sections"]["saturation_indicators"]
        entry = analysis_data["sections"]["market_entry_assessment"]
        
        # Create replacements dictionary
        replacements = {
            "{business_type}": analysis_data["business_type"],
            "{location}": analysis_data["location"],
            
            # Density metrics
            "{density_1_mile}": str(density["1_mile"]["count"]),
            "{density_per_sqmi_1}": str(density["1_mile"]["density_per_sq_mile"]),
            "{density_3_mile}": str(density["3_mile"]["count"]),
            "{density_per_sqmi_3}": str(density["3_mile"]["density_per_sq_mile"]),
            "{density_5_mile}": str(density["5_mile"]["count"]),
            "{density_per_sqmi_5}": str(density["5_mile"]["density_per_sq_mile"]),
            
            # Thresholds
            "{low_threshold}": str(density["thresholds"]["low"]),
            "{medium_threshold}": str(density["thresholds"]["medium"]),
            "{high_threshold}": str(density["thresholds"]["high"]),
            
            # Status
            "{saturation_level}": density["saturation_level"],
            "{saturation_score}": str(density["saturation_score"]),
            
            # Population metrics
            "{trade_area_population}": f"{population.get('trade_area_population', 0):,}",
            "{trade_area_households}": f"{population.get('trade_area_households', 0):,}",
            "{businesses_per_1000}": str(population.get("businesses_per_1000", 0)),
            "{avg_household_income}": f"{population.get('avg_household_income', 0):,}",
            
            # Drive time populations
            "{pop_5_min}": f"{population.get('drive_time_populations', {}).get('5_minutes', 0):,}",
            "{pop_10_min}": f"{population.get('drive_time_populations', {}).get('10_minutes', 0):,}",
            "{pop_15_min}": f"{population.get('drive_time_populations', {}).get('15_minutes', 0):,}",
            
            # Market gaps
            "{service_gap_analysis}": "\n".join(f"- {gap}" for gap in gaps["service_gap_analysis"]),
            "{underserved_areas}": "\n".join(f"  - {area}" for area in gaps["underserved_areas"]),
            "{market_opportunities}": "\n".join(f"  - {opp}" for opp in gaps["market_opportunities"]),
            "{opportunity_score}": str(gaps["opportunity_score"]),
            
            # Competition distribution
            "{direct_1mi}": str(competitive["competition_distribution"]["1_mile"]["direct"]),
            "{similar_1mi}": str(competitive["competition_distribution"]["1_mile"]["similar"]),
            "{total_1mi}": str(competitive["competition_distribution"]["1_mile"]["total"]),
            "{direct_3mi}": str(competitive["competition_distribution"]["1_3_miles"]["direct"]),
            "{similar_3mi}": str(competitive["competition_distribution"]["1_3_miles"]["similar"]),
            "{total_3mi}": str(competitive["competition_distribution"]["1_3_miles"]["total"]),
            "{direct_5mi}": str(competitive["competition_distribution"]["3_5_miles"]["direct"]),
            "{similar_5mi}": str(competitive["competition_distribution"]["3_5_miles"]["similar"]),
            "{total_5mi}": str(competitive["competition_distribution"]["3_5_miles"]["total"]),
            
            # Business types
            "{franchise_count}": str(competitive["business_type_analysis"].get("franchise", {}).get("count", 0)),
            "{franchise_percent}": str(competitive["business_type_analysis"].get("franchise", {}).get("percent", 0)),
            "{independent_count}": str(competitive["business_type_analysis"].get("independent", {}).get("count", 0)),
            "{independent_percent}": str(competitive["business_type_analysis"].get("independent", {}).get("percent", 0)),
            "{chain_count}": str(competitive["business_type_analysis"].get("chain", {}).get("count", 0)),
            "{chain_percent}": str(competitive["business_type_analysis"].get("chain", {}).get("percent", 0)),
            
            # Clustering
            "{clustering_analysis}": "\n".join(f"- {cluster}" for cluster in competitive["competitive_clustering"]) or "- No significant clustering patterns identified",
            
            # Saturation scores
            "{density_score}": str(saturation["component_scores"]["density"]),
            "{population_score}": str(saturation["component_scores"]["population_coverage"]),
            "{competition_score}": str(saturation["component_scores"]["competition_level"]),
            "{gap_score}": str(saturation["component_scores"]["market_gap"]),
            "{overall_saturation_score}": str(saturation["overall_saturation_score"]),
            
            # Market entry assessment
            "{market_entry_assessment}": entry["entry_viability"],
            "{entry_viability}": entry["entry_viability"],
            "{location_strategy}": entry["location_strategy"],
            "{differentiation_needs}": entry["differentiation_needs"],
            "{target_market_recommendation}": entry.get("target_market_recommendation", "Focus on quality and service differentiation"),
            "{risk_level}": entry["risk_level"],
            "{risk_concerns}": "\n".join(f"  - {concern}" for concern in entry["risk_concerns"]),
            "{mitigation_strategies}": "\n".join(f"  - {strategy}" for strategy in entry["mitigation_strategies"]),
            
            # Summary
            "{saturation_summary}": f"The market analysis reveals {density['saturation_level'].lower()} "
                                   f"with an overall saturation score of {saturation['overall_saturation_score']}/100. "
                                   f"{entry['entry_viability']}",
            
            # Visual placeholders
            "{heatmap_path}": "visualizations/market_saturation_heatmap.png",
            "{density_chart_path}": "visualizations/business_density_chart.png",
            "{gap_matrix_path}": "visualizations/market_gap_matrix.png",
            
            # Metadata
            "{data_collection_date}": datetime.now().strftime("%Y-%m-%d")
        }
        
        # Replace all placeholders
        content = template
        for key, value in replacements.items():
            content = content.replace(key, str(value))
            
        return content


def main():
    """Test the market saturation analyzer"""
    analyzer = MarketSaturationAnalyzer()
    
    # Test with Madison, WI location
    results = analyzer.analyze_market_saturation(
        business_type="restaurant",
        address="1 S Pinckney St, Madison, WI 53703",
        lat=43.0731,
        lon=-89.3831
    )
    
    # Save results
    output_path = Path("market_saturation_test_results.json")
    with open(output_path, 'w') as f:
        json.dump(results, f, indent=2)
        
    print(f"Analysis complete. Results saved to {output_path}")
    
    # Generate section content
    content = analyzer.generate_section_content(results)
    
    # Save formatted content
    content_path = Path("section_2_2_test_output.md")
    with open(content_path, 'w') as f:
        f.write(content)
        
    print(f"Section content saved to {content_path}")


if __name__ == "__main__":
    main()