"""
Competitive Intelligence Dashboard
=================================

Interactive dashboard for OSM competitive analysis and market intelligence.
Combines all Phase 2 functionality into a single comprehensive tool.
"""

import logging
import os
from datetime import datetime
from typing import List, Dict, Optional
from osm_competitive_analysis import OSMCompetitiveAnalysis
from market_opportunity_scanner import MarketOpportunityScanner


class CompetitiveIntelligenceDashboard:
    """Comprehensive competitive intelligence dashboard"""
    
    def __init__(self, project_id: str = "location-optimizer-1"):
        """Initialize the dashboard"""
        self.analyzer = OSMCompetitiveAnalysis(project_id)
        self.scanner = MarketOpportunityScanner(project_id)
        self.logger = logging.getLogger(self.__class__.__name__)
    
    def analyze_potential_site(self, latitude: float, longitude: float,
                             business_type: str, site_name: str = "Potential Site",
                             radius_miles: float = 3.0) -> Dict:
        """
        Comprehensive site analysis
        
        Args:
            latitude: Site latitude
            longitude: Site longitude  
            business_type: Proposed business type
            site_name: Name for the site
            radius_miles: Analysis radius
            
        Returns:
            Comprehensive analysis results
        """
        print(f"🎯 Analyzing Site: {site_name}")
        print("=" * 50)
        
        # Generate full site analysis report
        report = self.analyzer.generate_site_analysis_report(
            latitude, longitude, business_type, site_name, radius_miles
        )
        
        print(report)
        
        # Get structured data for API/integration use
        competitors = self.analyzer.find_competitors_around_site(
            latitude, longitude, radius_miles,
            self.analyzer.competitive_groups.get(business_type, [business_type])
        )
        
        density = self.analyzer.analyze_business_density(
            latitude, longitude, radius_miles, site_name
        )
        
        saturation = self.analyzer.assess_market_saturation(
            latitude, longitude, business_type, radius_miles, site_name
        )
        
        return {
            'site_name': site_name,
            'coordinates': {'lat': latitude, 'lon': longitude},
            'business_type': business_type,
            'analysis_radius': radius_miles,
            'report': report,
            'competitors': [
                {
                    'name': c.name,
                    'distance_miles': c.distance_miles,
                    'business_type': c.business_type,
                    'franchise': c.franchise_indicator,
                    'brand': c.brand
                } for c in competitors
            ],
            'density_metrics': {
                'total_businesses': density.total_businesses,
                'businesses_per_sq_mile': density.businesses_per_sq_mile,
                'franchise_percentage': density.franchise_percentage,
                'competition_score': density.competition_score
            },
            'saturation_analysis': {
                'saturation_level': saturation.saturation_level,
                'saturation_score': saturation.saturation_score,
                'opportunity_score': saturation.opportunity_score,
                'recommendation': saturation.recommended_action
            }
        }
    
    def scan_market_opportunities(self, business_type: str = None,
                                min_opportunity_score: float = 70.0,
                                max_results: int = 20) -> Dict:
        """
        Scan for market opportunities across Wisconsin
        
        Args:
            business_type: Business type to analyze (None for all)
            min_opportunity_score: Minimum opportunity threshold
            max_results: Maximum results to return
            
        Returns:
            Market opportunity analysis results
        """
        print(f"🔍 Scanning Wisconsin Market Opportunities")
        if business_type:
            print(f"Business Type: {business_type}")
        print("=" * 50)
        
        opportunities = self.scanner.scan_wisconsin_opportunities(
            business_type, min_opportunity_score
        )
        
        # Limit results
        top_opportunities = opportunities[:max_results]
        
        # Generate report
        report = self.scanner.generate_opportunity_report(
            top_opportunities, business_type
        )
        
        print(report)
        
        return {
            'business_type': business_type,
            'min_opportunity_score': min_opportunity_score,
            'total_found': len(opportunities),
            'returned_count': len(top_opportunities),
            'report': report,
            'opportunities': [
                {
                    'city': op.city,
                    'business_type': op.business_type,
                    'opportunity_score': op.opportunity_score,
                    'competition_level': op.competition_level,
                    'total_competitors': op.total_competitors,
                    'franchise_competition': op.franchise_competition,
                    'market_gap': op.market_gap_indicator,
                    'population_estimate': op.population_estimate,
                    'recommendation': op.recommendation
                } for op in top_opportunities
            ]
        }
    
    def compare_multiple_sites(self, sites: List[Dict]) -> Dict:
        """
        Compare multiple potential sites
        
        Args:
            sites: List of site dictionaries with 'name', 'lat', 'lon', 'business_type'
            
        Returns:
            Comparative analysis results
        """
        print(f"⚖️ Comparing {len(sites)} Potential Sites")
        print("=" * 50)
        
        site_analyses = []
        
        for site in sites:
            try:
                analysis = self.analyze_potential_site(
                    site['lat'], site['lon'], site['business_type'], 
                    site['name'], radius_miles=3.0
                )
                site_analyses.append(analysis)
            except Exception as e:
                self.logger.error(f"Error analyzing site {site['name']}: {e}")
                continue
        
        # Generate comparison summary
        if site_analyses:
            print(f"\n📊 SITE COMPARISON SUMMARY")
            print("=" * 40)
            
            # Sort by opportunity score
            site_analyses.sort(
                key=lambda x: x['saturation_analysis']['opportunity_score'], 
                reverse=True
            )
            
            for i, analysis in enumerate(site_analyses, 1):
                score = analysis['saturation_analysis']['opportunity_score']
                competitors = analysis['density_metrics']['total_businesses']
                rating = self._get_score_rating(score)
                
                print(f"{i}. {analysis['site_name']} - {rating}")
                print(f"   Opportunity Score: {score:.1f}/100")
                print(f"   Competitors Nearby: {competitors}")
                print(f"   Recommendation: {analysis['saturation_analysis']['recommendation']}")
                print()
        
        return {
            'comparison_date': datetime.now().isoformat(),
            'sites_analyzed': len(site_analyses),
            'analyses': site_analyses
        }
    
    def _get_score_rating(self, score: float) -> str:
        """Convert numeric score to rating string"""
        if score >= 80:
            return "🟢 EXCELLENT"
        elif score >= 60:
            return "🟡 GOOD"
        elif score >= 40:
            return "🟠 FAIR"
        else:
            return "🔴 POOR"
    
    def generate_executive_summary(self, business_type: str) -> str:
        """
        Generate executive summary for a business type
        
        Args:
            business_type: Business type to analyze
            
        Returns:
            Executive summary report
        """
        # Scan opportunities
        opportunities = self.scanner.scan_wisconsin_opportunities(
            business_type, min_opportunity_score=60.0
        )
        
        if not opportunities:
            return f"No significant market opportunities found for {business_type} in Wisconsin."
        
        # Calculate summary statistics
        avg_score = sum(op.opportunity_score for op in opportunities) / len(opportunities)
        high_opportunity_count = sum(1 for op in opportunities if op.opportunity_score >= 80)
        market_gaps = sum(1 for op in opportunities if op.market_gap_indicator)
        
        # Top markets
        top_3 = opportunities[:3]
        
        summary = []
        summary.append(f"📋 EXECUTIVE SUMMARY: {business_type.upper()}")
        summary.append("=" * 60)
        summary.append(f"Analysis Date: {datetime.now().strftime('%Y-%m-%d')}")
        
        summary.append(f"\n🎯 KEY FINDINGS:")
        summary.append(f"   • {len(opportunities)} viable market opportunities identified")
        summary.append(f"   • {high_opportunity_count} high-opportunity markets (80+ score)")
        summary.append(f"   • {market_gaps} markets with identified gaps")
        summary.append(f"   • Average opportunity score: {avg_score:.1f}/100")
        
        summary.append(f"\n🏆 TOP RECOMMENDED MARKETS:")
        for i, op in enumerate(top_3, 1):
            gap_note = " (Market Gap)" if op.market_gap_indicator else ""
            summary.append(f"   {i}. {op.city}{gap_note}")
            summary.append(f"      Score: {op.opportunity_score:.1f}/100")
            summary.append(f"      Competition: {op.competition_level}")
            summary.append(f"      Population: ~{op.population_estimate:,}")
        
        summary.append(f"\n💼 STRATEGIC RECOMMENDATIONS:")
        
        if high_opportunity_count >= 3:
            summary.append(f"   • AGGRESSIVE EXPANSION: Multiple high-opportunity markets available")
            summary.append(f"   • Prioritize markets with population >10,000 for faster ROI")
        elif len(opportunities) >= 5:
            summary.append(f"   • SELECTIVE EXPANSION: Focus on top 3-5 markets")
            summary.append(f"   • Consider pilot locations in highest-scoring cities")
        else:
            summary.append(f"   • CAUTIOUS EXPANSION: Limited opportunities identified")
            summary.append(f"   • Thorough market research recommended before entry")
        
        if market_gaps > 0:
            summary.append(f"   • {market_gaps} underserved markets offer first-mover advantage")
        
        summary.append(f"\n⚠️ RISK FACTORS:")
        low_competition_markets = sum(1 for op in opportunities if op.total_competitors <= 1)
        if low_competition_markets > len(opportunities) * 0.7:
            summary.append(f"   • Many markets have minimal existing infrastructure")
            summary.append(f"   • Market education may be required")
        
        summary.append(f"\n📈 MARKET OUTLOOK: POSITIVE")
        summary.append(f"   • Wisconsin offers {len(opportunities)} expansion opportunities")
        summary.append(f"   • Average market opportunity score indicates favorable conditions")
        
        return "\n".join(summary)
    
    def interactive_mode(self):
        """Run interactive dashboard mode"""
        print("🎯 COMPETITIVE INTELLIGENCE DASHBOARD")
        print("=" * 60)
        print("Wisconsin Business Location Intelligence System")
        print()
        
        while True:
            print("\n📋 AVAILABLE ANALYSES:")
            print("1. Analyze Specific Site")
            print("2. Scan Market Opportunities") 
            print("3. Compare Multiple Sites")
            print("4. Generate Executive Summary")
            print("5. Exit")
            
            try:
                choice = input("\nSelect analysis (1-5): ").strip()
                
                if choice == '1':
                    self._interactive_site_analysis()
                elif choice == '2':
                    self._interactive_opportunity_scan()
                elif choice == '3':
                    self._interactive_site_comparison()
                elif choice == '4':
                    self._interactive_executive_summary()
                elif choice == '5':
                    print("Thank you for using the Competitive Intelligence Dashboard!")
                    break
                else:
                    print("Invalid choice. Please select 1-5.")
                    
            except KeyboardInterrupt:
                print("\n\nGoodbye!")
                break
            except Exception as e:
                print(f"Error: {e}")
    
    def _interactive_site_analysis(self):
        """Interactive site analysis"""
        print("\n🎯 SITE ANALYSIS")
        print("-" * 30)
        
        try:
            site_name = input("Site name: ").strip()
            lat = float(input("Latitude: ").strip())
            lon = float(input("Longitude: ").strip())
            business_type = input("Business type (food_beverage, retail, automotive, etc.): ").strip()
            radius = float(input("Analysis radius (miles, default 3.0): ").strip() or "3.0")
            
            self.analyze_potential_site(lat, lon, business_type, site_name, radius)
            
        except ValueError:
            print("Invalid input. Please enter valid numbers for coordinates.")
        except Exception as e:
            print(f"Analysis error: {e}")
    
    def _interactive_opportunity_scan(self):
        """Interactive opportunity scanning"""
        print("\n🔍 MARKET OPPORTUNITY SCAN")
        print("-" * 30)
        
        business_type = input("Business type (leave blank for all): ").strip() or None
        min_score = float(input("Minimum opportunity score (default 70): ").strip() or "70")
        max_results = int(input("Maximum results (default 20): ").strip() or "20")
        
        self.scan_market_opportunities(business_type, min_score, max_results)
    
    def _interactive_site_comparison(self):
        """Interactive site comparison"""
        print("\n⚖️ SITE COMPARISON")
        print("-" * 30)
        
        sites = []
        site_count = int(input("Number of sites to compare: ").strip())
        
        for i in range(site_count):
            print(f"\nSite {i+1}:")
            name = input("  Name: ").strip()
            lat = float(input("  Latitude: ").strip())
            lon = float(input("  Longitude: ").strip())
            business_type = input("  Business type: ").strip()
            
            sites.append({
                'name': name,
                'lat': lat,
                'lon': lon,
                'business_type': business_type
            })
        
        self.compare_multiple_sites(sites)
    
    def _interactive_executive_summary(self):
        """Interactive executive summary"""
        print("\n📋 EXECUTIVE SUMMARY")
        print("-" * 30)
        
        business_type = input("Business type: ").strip()
        summary = self.generate_executive_summary(business_type)
        print(f"\n{summary}")


def main():
    """Main dashboard function"""
    logging.basicConfig(level=logging.INFO)
    
    # Set up credentials
    credentials_path = "/workspaces/Test_for_Claude/Business/wisconsin_data_collection/location-optimizer-1-449414f93a5a.json"
    os.environ['GOOGLE_APPLICATION_CREDENTIALS'] = credentials_path
    
    dashboard = CompetitiveIntelligenceDashboard()
    
    # Demo mode - show key capabilities
    print("🎯 COMPETITIVE INTELLIGENCE DASHBOARD - DEMO")
    print("=" * 60)
    
    # Demo 1: Site Analysis
    print("\n📍 DEMO 1: Site Analysis (Madison)")
    dashboard.analyze_potential_site(
        43.0731, -89.4012, 'retail', 
        "Madison Downtown Retail Location", 2.0
    )
    
    # Demo 2: Market Opportunities
    print("\n\n🔍 DEMO 2: Market Opportunities (Automotive)")
    dashboard.scan_market_opportunities('automotive', 75.0, 5)
    
    # Demo 3: Executive Summary
    print("\n\n📋 DEMO 3: Executive Summary (Food & Beverage)")
    summary = dashboard.generate_executive_summary('food_beverage')
    print(summary)
    
    # Offer interactive mode
    print(f"\n\n" + "="*60)
    choice = input("Enter interactive mode? (y/n): ").strip().lower()
    if choice == 'y':
        dashboard.interactive_mode()


if __name__ == "__main__":
    main()