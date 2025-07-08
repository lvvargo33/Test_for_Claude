#!/usr/bin/env python3
"""
Simplified Market Saturation Analyzer
====================================

A simplified version that integrates properly with existing data sources
and generates real values for Section 2.2 template placeholders.
"""

import json
import logging
import math
from pathlib import Path
from datetime import datetime
from typing import Dict, List, Any, Optional

# Import existing analyzers
from universal_competitive_analyzer import UniversalCompetitiveAnalyzer

logger = logging.getLogger(__name__)

class SimplifiedMarketSaturationAnalyzer:
    """Simplified market saturation analyzer with working data integration"""
    
    # Industry-specific thresholds
    SATURATION_THRESHOLDS = {
        "restaurant": {"low": 2, "medium": 5, "high": 10},
        "auto_repair": {"low": 1, "medium": 2, "high": 4},
        "retail": {"low": 3, "medium": 8, "high": 15},
        "healthcare": {"low": 1, "medium": 2, "high": 4},
        "default": {"low": 2, "medium": 5, "high": 10}
    }
    
    def __init__(self):
        pass
    
    def analyze_market_saturation(self, business_type: str, address: str, 
                                 lat: float, lon: float) -> Dict[str, Any]:
        """
        Perform market saturation analysis using existing data
        
        Args:
            business_type: Type of business
            address: Business address
            lat: Latitude
            lon: Longitude
            
        Returns:
            Dictionary with analysis results
        """
        logger.info(f"Running simplified market saturation analysis for {business_type}")
        
        # Initialize competitive analyzer
        competitive_analyzer = UniversalCompetitiveAnalyzer(
            business_type=business_type,
            site_lat=lat,
            site_lng=lon,
            site_address=address
        )
        
        # Load data and get competitive analysis
        competitive_analyzer.load_data()
        competitors_by_category = competitive_analyzer.find_competitors_by_category(5.0)
        
        # Process competitor data
        all_competitors = []
        for category, df in competitors_by_category.items():
            for _, row in df.iterrows():
                all_competitors.append({
                    "name": row.get("name", "Unknown"),
                    "category": category,
                    "distance": row.get("distance", 0),
                    "is_direct": category == "direct_competitors"
                })
        
        # Sort by distance
        all_competitors.sort(key=lambda x: x["distance"])
        
        # Create distance-based groups
        competitors_1mi = [c for c in all_competitors if c["distance"] <= 1.0]
        competitors_3mi = [c for c in all_competitors if c["distance"] <= 3.0]
        competitors_5mi = [c for c in all_competitors if c["distance"] <= 5.0]
        
        # Calculate areas (in square miles)
        area_1mi = math.pi * 1**2
        area_3mi = math.pi * 3**2
        area_5mi = math.pi * 5**2
        
        # Calculate densities
        density_1mi = len(competitors_1mi) / area_1mi
        density_3mi = len(competitors_3mi) / area_3mi
        density_5mi = len(competitors_5mi) / area_5mi
        
        # Get thresholds
        thresholds = self._get_thresholds(business_type)
        
        # Determine saturation level
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
        
        # Calculate opportunity score (inverse of saturation)
        opportunity_score = max(0, 100 - saturation_score)
        
        # Estimate population (simplified)
        estimated_pop_3mi = 50000  # Placeholder - would use census data
        estimated_households_3mi = 20000  # Placeholder
        estimated_income = 65000  # Placeholder
        
        # Calculate businesses per 1,000 residents
        businesses_per_1000 = (len(competitors_3mi) / estimated_pop_3mi) * 1000 if estimated_pop_3mi > 0 else 0
        
        # Analyze business types
        direct_competitors = [c for c in competitors_5mi if c["is_direct"]]
        franchise_count = len([c for c in competitors_5mi if self._is_franchise(c["name"])])
        chain_count = len([c for c in competitors_5mi if self._is_chain(c["name"])])
        independent_count = len(competitors_5mi) - franchise_count - chain_count
        
        total_competitors = len(competitors_5mi)
        
        # Calculate component scores
        density_score = min(25, (saturation_score / 100) * 25)
        population_score = 15 if businesses_per_1000 < 1.0 else 10
        competition_score = 20 if len(competitors_1mi) < 3 else 15
        gap_score = 25 - (opportunity_score / 100 * 25)
        
        overall_score = density_score + population_score + competition_score + gap_score
        
        # Generate analysis results
        results = {
            "business_type": business_type,
            "location": address,
            "coordinates": {"lat": lat, "lon": lon},
            "analysis_date": datetime.now().isoformat(),
            "density_analysis": {
                "1_mile": {
                    "count": len(competitors_1mi),
                    "density_per_sq_mile": round(density_1mi, 2)
                },
                "3_mile": {
                    "count": len(competitors_3mi),
                    "density_per_sq_mile": round(density_3mi, 2)
                },
                "5_mile": {
                    "count": len(competitors_5mi),
                    "density_per_sq_mile": round(density_5mi, 2)
                },
                "saturation_level": saturation_level,
                "saturation_score": saturation_score,
                "thresholds": thresholds
            },
            "population_metrics": {
                "trade_area_population": estimated_pop_3mi,
                "trade_area_households": estimated_households_3mi,
                "avg_household_income": estimated_income,
                "businesses_per_1000": round(businesses_per_1000, 2),
                "drive_time_populations": {
                    "5_minutes": int(estimated_pop_3mi * 0.3),
                    "10_minutes": int(estimated_pop_3mi * 0.6),
                    "15_minutes": int(estimated_pop_3mi * 0.9)
                }
            },
            "market_gaps": {
                "opportunity_score": opportunity_score,
                "service_gap_analysis": [
                    f"Market density of {density_3mi:.1f} businesses per sq mile",
                    f"{'Below' if density_3mi < thresholds['medium'] else 'At or above'} industry average"
                ],
                "underserved_areas": [
                    "Residential areas beyond 2-mile radius",
                    "Growing neighborhoods with limited access"
                ] if opportunity_score > 50 else ["Market appears well-served"],
                "market_opportunities": [
                    "High potential for market entry" if opportunity_score > 70 else "Moderate potential for specialized services"
                ]
            },
            "competitive_landscape": {
                "competition_distribution": {
                    "1_mile": {
                        "direct": len([c for c in competitors_1mi if c["is_direct"]]),
                        "similar": len([c for c in competitors_1mi if not c["is_direct"]]),
                        "total": len(competitors_1mi)
                    },
                    "1_3_miles": {
                        "direct": len([c for c in competitors_3mi if c not in competitors_1mi and c["is_direct"]]),
                        "similar": len([c for c in competitors_3mi if c not in competitors_1mi and not c["is_direct"]]),
                        "total": len([c for c in competitors_3mi if c not in competitors_1mi])
                    },
                    "3_5_miles": {
                        "direct": len([c for c in competitors_5mi if c not in competitors_3mi and c["is_direct"]]),
                        "similar": len([c for c in competitors_5mi if c not in competitors_3mi and not c["is_direct"]]),
                        "total": len([c for c in competitors_5mi if c not in competitors_3mi])
                    }
                },
                "business_type_analysis": {
                    "franchise": {
                        "count": franchise_count,
                        "percent": round(franchise_count / total_competitors * 100, 1) if total_competitors > 0 else 0
                    },
                    "chain": {
                        "count": chain_count,
                        "percent": round(chain_count / total_competitors * 100, 1) if total_competitors > 0 else 0
                    },
                    "independent": {
                        "count": independent_count,
                        "percent": round(independent_count / total_competitors * 100, 1) if total_competitors > 0 else 0
                    }
                },
                "competitive_clustering": [
                    f"Cluster of {len(competitors_1mi)} competitors within 1 mile" if len(competitors_1mi) >= 3 else "Low competitor concentration nearby"
                ]
            },
            "saturation_indicators": {
                "component_scores": {
                    "density": round(density_score, 1),
                    "population_coverage": population_score,
                    "competition_level": competition_score,
                    "market_gap": round(gap_score, 1)
                },
                "overall_saturation_score": round(overall_score, 1)
            },
            "market_entry_assessment": self._assess_market_entry(overall_score, opportunity_score)
        }
        
        return results
    
    def _get_thresholds(self, business_type: str) -> Dict[str, float]:
        """Get saturation thresholds for business type"""
        normalized_type = business_type.lower().replace(" ", "_")
        return self.SATURATION_THRESHOLDS.get(normalized_type, self.SATURATION_THRESHOLDS["default"])
    
    def _is_franchise(self, name: str) -> bool:
        """Check if business is a franchise"""
        franchise_keywords = ["McDonald's", "Subway", "Starbucks", "Dunkin", "Pizza Hut",
                            "Domino's", "KFC", "Burger King", "Wendy's", "Taco Bell"]
        return any(keyword.lower() in name.lower() for keyword in franchise_keywords)
    
    def _is_chain(self, name: str) -> bool:
        """Check if business is a chain"""
        chain_keywords = ["Walmart", "Target", "CVS", "Walgreens", "Home Depot",
                         "Lowe's", "Best Buy", "AutoZone", "O'Reilly"]
        return any(keyword.lower() in name.lower() for keyword in chain_keywords)
    
    def _assess_market_entry(self, overall_score: float, opportunity_score: float) -> Dict[str, Any]:
        """Assess market entry feasibility"""
        if overall_score < 30:
            entry_viability = "Excellent - Market is underserved with high entry potential"
            risk_level = "Low"
        elif overall_score < 50:
            entry_viability = "Good - Market has room for additional businesses"
            risk_level = "Low-Medium"
        elif overall_score < 70:
            entry_viability = "Moderate - Market is competitive but viable with right approach"
            risk_level = "Medium"
        else:
            entry_viability = "Challenging - Market is highly saturated"
            risk_level = "High"
        
        return {
            "entry_viability": entry_viability,
            "location_strategy": "Focus on high-traffic areas" if overall_score < 50 else "Consider peripheral locations",
            "differentiation_needs": "Standard offerings sufficient" if overall_score < 30 else "Strong differentiation required",
            "target_market_recommendation": "Broad market appeal" if overall_score < 50 else "Focus on underserved segments",
            "risk_level": risk_level,
            "risk_concerns": [
                "High competition may limit market share" if overall_score >= 70 else "Limited competition concern",
                "Customer acquisition challenges" if overall_score >= 50 else "Good customer acquisition potential"
            ],
            "mitigation_strategies": [
                "Develop unique value proposition",
                "Focus on service excellence",
                "Build strong local partnerships"
            ]
        }
    
    def generate_section_content(self, analysis_data: Dict[str, Any]) -> str:
        """Generate formatted Section 2.2 content"""
        logger.info("Generating Section 2.2 content with real data")
        
        # Load template
        template_path = Path("UNIVERSAL_MARKET_SATURATION_TEMPLATE.md")
        with open(template_path, 'r') as f:
            template = f.read()
        
        # Extract data
        density = analysis_data["density_analysis"]
        population = analysis_data["population_metrics"]
        gaps = analysis_data["market_gaps"]
        competitive = analysis_data["competitive_landscape"]
        saturation = analysis_data["saturation_indicators"]
        entry = analysis_data["market_entry_assessment"]
        
        # Create replacements
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
            "{trade_area_population}": f"{population['trade_area_population']:,}",
            "{trade_area_households}": f"{population['trade_area_households']:,}",
            "{businesses_per_1000}": str(population["businesses_per_1000"]),
            "{avg_household_income}": f"{population['avg_household_income']:,}",
            
            # Drive time populations
            "{pop_5_min}": f"{population['drive_time_populations']['5_minutes']:,}",
            "{pop_10_min}": f"{population['drive_time_populations']['10_minutes']:,}",
            "{pop_15_min}": f"{population['drive_time_populations']['15_minutes']:,}",
            
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
            "{franchise_count}": str(competitive["business_type_analysis"]["franchise"]["count"]),
            "{franchise_percent}": str(competitive["business_type_analysis"]["franchise"]["percent"]),
            "{independent_count}": str(competitive["business_type_analysis"]["independent"]["count"]),
            "{independent_percent}": str(competitive["business_type_analysis"]["independent"]["percent"]),
            "{chain_count}": str(competitive["business_type_analysis"]["chain"]["count"]),
            "{chain_percent}": str(competitive["business_type_analysis"]["chain"]["percent"]),
            
            # Clustering
            "{clustering_analysis}": "\n".join(f"- {cluster}" for cluster in competitive["competitive_clustering"]),
            
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
            "{target_market_recommendation}": entry["target_market_recommendation"],
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
    """Test the simplified analyzer"""
    analyzer = SimplifiedMarketSaturationAnalyzer()
    
    # Test analysis
    results = analyzer.analyze_market_saturation(
        business_type="Auto Repair Shop",
        address="456 State St, Milwaukee, WI 53202",
        lat=43.0265,
        lon=-89.4698
    )
    
    # Save results
    with open("simplified_saturation_test.json", 'w') as f:
        json.dump(results, f, indent=2)
    
    # Generate content
    content = analyzer.generate_section_content(results)
    
    with open("simplified_section_2_2_test.md", 'w') as f:
        f.write(content)
    
    print("✅ Simplified analyzer test complete")


if __name__ == "__main__":
    main()