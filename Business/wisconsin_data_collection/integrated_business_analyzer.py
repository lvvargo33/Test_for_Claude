"""
Integrated Business Analysis Engine
===================================

Combines wage data, employment projections, traffic patterns, and business
registration data to provide comprehensive business location and market
opportunity analysis for Wisconsin.

This analyzer integrates all collected data sources to answer key questions:
1. What are the labor costs for different business types?
2. Which industries are growing/declining?
3. Where are the best locations considering both opportunity and costs?
4. What wage levels can businesses sustain in different markets?
"""

import pandas as pd
import logging
import json
from datetime import datetime
from typing import Dict, List, Optional, Any, Tuple
from pathlib import Path


class IntegratedBusinessAnalyzer:
    """
    Comprehensive business analysis using integrated Wisconsin data
    """
    
    def __init__(self, data_directory: str = "."):
        self.data_dir = Path(data_directory)
        self.logger = logging.getLogger(__name__)
        
        # Data sources
        self.wage_data = None
        self.employment_projections = None
        self.traffic_data = None
        self.business_data = None
        
        # Analysis results
        self.market_analysis = {}
        self.cost_analysis = {}
        self.opportunity_scores = {}
        
        self.logger.info("Integrated Business Analyzer initialized")
    
    def load_data_sources(self) -> bool:
        """
        Load all available data sources
        
        Returns:
            True if data loaded successfully
        """
        try:
            # Load wage data
            wage_files = list(self.data_dir.glob("wisconsin_historical_wages_*.csv"))
            if wage_files:
                self.wage_data = pd.read_csv(wage_files[0])
                self.logger.info(f"Loaded {len(self.wage_data)} wage records")
            
            # Load employment projections
            proj_files = list(self.data_dir.glob("wisconsin_employment_projections_*.csv"))
            if proj_files:
                self.employment_projections = pd.read_csv(proj_files[0])
                self.logger.info(f"Loaded {len(self.employment_projections)} industry projections")
            
            # Load traffic data (if available)
            traffic_files = list(self.data_dir.glob("wisconsin_traffic_data_*.csv"))
            if traffic_files:
                self.traffic_data = pd.read_csv(traffic_files[0])
                self.logger.info(f"Loaded {len(self.traffic_data)} traffic records")
            
            # Load business registration data (if available) 
            business_files = list(self.data_dir.glob("*business*.csv"))
            if business_files:
                # Try to load the most comprehensive file
                for file in business_files:
                    if 'sample' not in file.name.lower():
                        try:
                            self.business_data = pd.read_csv(file)
                            self.logger.info(f"Loaded {len(self.business_data)} business records from {file.name}")
                            break
                        except:
                            continue
            
            return True
            
        except Exception as e:
            self.logger.error(f"Error loading data sources: {e}")
            return False
    
    def analyze_labor_market_by_industry(self) -> Dict[str, Any]:
        """
        Analyze labor market conditions by industry
        
        Returns:
            Industry-specific labor market analysis
        """
        if self.wage_data is None:
            self.logger.warning("No wage data available")
            return {}
        
        analysis = {}
        
        try:
            # Get latest year data
            latest_year = self.wage_data['data_year'].max()
            current_wages = self.wage_data[self.wage_data['data_year'] == latest_year]
            
            # Group by occupation (industry proxy)
            by_occupation = current_wages.groupby('occupation_name').agg({
                'annual_median_wage': 'mean',
                'total_employment': 'sum',
                'area_name': 'count'  # Number of areas with this occupation
            }).reset_index()
            
            # Calculate wage competitiveness and labor availability
            for _, row in by_occupation.iterrows():
                occ = row['occupation_name']
                
                # Get wage trend (5-year growth)
                occ_trend = self.wage_data[self.wage_data['occupation_name'] == occ].groupby('data_year')['annual_median_wage'].mean()
                if len(occ_trend) >= 2:
                    wage_growth = ((occ_trend.iloc[-1] - occ_trend.iloc[0]) / occ_trend.iloc[0]) * 100
                else:
                    wage_growth = 0
                
                # Analyze by area to find cost variations
                area_wages = current_wages[current_wages['occupation_name'] == occ].groupby('area_name')['annual_median_wage'].mean().sort_values()
                
                lowest_cost_area = area_wages.index[0] if len(area_wages) > 0 else "Unknown"
                highest_cost_area = area_wages.index[-1] if len(area_wages) > 0 else "Unknown" 
                wage_range = area_wages.iloc[-1] - area_wages.iloc[0] if len(area_wages) > 1 else 0
                
                analysis[occ] = {
                    'median_annual_wage': round(row['annual_median_wage'], 0),
                    'total_employment': int(row['total_employment']),
                    'areas_available': int(row['area_name']),
                    'wage_growth_5yr': round(wage_growth, 1),
                    'lowest_cost_area': lowest_cost_area,
                    'highest_cost_area': highest_cost_area,
                    'area_wage_range': round(wage_range, 0),
                    'labor_cost_category': self._categorize_labor_cost(row['annual_median_wage']),
                    'availability_score': self._score_labor_availability(row['total_employment'], row['area_name'])
                }
            
            self.logger.info(f"Analyzed labor market for {len(analysis)} occupations")
            
        except Exception as e:
            self.logger.error(f"Error in labor market analysis: {e}")
        
        return analysis
    
    def analyze_industry_opportunities(self) -> Dict[str, Any]:
        """
        Analyze business opportunities by industry combining growth and wages
        
        Returns:
            Industry opportunity analysis
        """
        if self.employment_projections is None:
            self.logger.warning("No employment projections available")
            return {}
        
        opportunities = {}
        
        try:
            # Process each industry projection
            for _, row in self.employment_projections.iterrows():
                industry = row['industry_title']
                
                # Base opportunity metrics
                growth_score = self._score_growth_rate(row['percent_change'])
                business_suitability = self._assess_business_entry(row)
                market_size_score = self._score_market_size(row['base_year_employment'])
                
                # Wage cost impact (if wage data available)
                wage_impact = self._assess_wage_impact(industry)
                
                # Calculate overall opportunity score
                opportunity_score = (
                    growth_score * 0.3 +
                    business_suitability * 0.25 +
                    market_size_score * 0.20 +
                    wage_impact['affordability_score'] * 0.25
                )
                
                opportunities[industry] = {
                    'growth_rate': row['percent_change'],
                    'growth_outlook': row['growth_outlook'],
                    'job_openings_annual': row['average_annual_openings'],
                    'current_employment': row['base_year_employment'],
                    'small_business_suitable': row['small_business_suitable'],
                    'franchise_potential': row['franchise_potential'],
                    'startup_friendly': row['startup_friendly'],
                    'capital_intensity': row['capital_intensity'],
                    'entry_barriers': row['entry_barriers'],
                    'wisconsin_advantage': row['wisconsin_advantage'],
                    'opportunity_score': round(opportunity_score, 2),
                    'wage_factors': wage_impact,
                    'recommendation': self._generate_recommendation(opportunity_score, row)
                }
            
            # Rank opportunities
            sorted_opportunities = dict(
                sorted(opportunities.items(), 
                      key=lambda x: x[1]['opportunity_score'], 
                      reverse=True)
            )
            
            self.logger.info(f"Analyzed opportunities for {len(opportunities)} industries")
            return sorted_opportunities
            
        except Exception as e:
            self.logger.error(f"Error in opportunity analysis: {e}")
            return {}
    
    def analyze_location_factors(self) -> Dict[str, Any]:
        """
        Analyze location-specific factors combining multiple data sources
        
        Returns:
            Location analysis by Wisconsin areas
        """
        location_analysis = {}
        
        try:
            if self.wage_data is not None:
                # Analyze wage competitiveness by area
                latest_wages = self.wage_data[self.wage_data['data_year'] == self.wage_data['data_year'].max()]
                
                for area in latest_wages['area_name'].unique():
                    area_data = latest_wages[latest_wages['area_name'] == area]
                    
                    location_analysis[area] = {
                        'median_wage_all_occupations': area_data['annual_median_wage'].median(),
                        'wage_competitiveness': self._rate_wage_competitiveness(area_data['annual_median_wage'].median()),
                        'occupations_available': len(area_data),
                        'high_wage_jobs': len(area_data[area_data['annual_median_wage'] >= 60000]),
                        'service_jobs': len(area_data[area_data['occupation_name'].str.contains('Service|Food|Retail', case=False, na=False)]),
                        'professional_jobs': len(area_data[area_data['occupation_name'].str.contains('Manager|Professional|Technical', case=False, na=False)]),
                        'labor_cost_assessment': self._assess_area_labor_costs(area_data)
                    }
                    
                    # Add traffic data if available
                    if self.traffic_data is not None:
                        traffic_info = self._get_area_traffic_data(area)
                        location_analysis[area].update(traffic_info)
            
            self.logger.info(f"Analyzed {len(location_analysis)} locations")
            
        except Exception as e:
            self.logger.error(f"Error in location analysis: {e}")
        
        return location_analysis
    
    def generate_business_recommendations(self, business_type: str = None, 
                                        target_area: str = None,
                                        max_labor_cost: float = None) -> Dict[str, Any]:
        """
        Generate specific business recommendations based on criteria
        
        Args:
            business_type: Type of business (retail, restaurant, professional, etc.)
            target_area: Preferred Wisconsin area
            max_labor_cost: Maximum acceptable median wage
            
        Returns:
            Tailored business recommendations
        """
        recommendations = {
            'industry_matches': [],
            'location_suggestions': [],
            'wage_analysis': {},
            'growth_outlook': {},
            'implementation_guidance': {}
        }
        
        try:
            # Filter industries based on business type
            if business_type and self.employment_projections is not None:
                type_filters = {
                    'retail': ['retail', 'store', 'shop'],
                    'restaurant': ['food service', 'restaurant', 'food'],
                    'professional': ['professional', 'technical', 'consulting'],
                    'healthcare': ['health', 'medical', 'care'],
                    'construction': ['construction', 'trade', 'contractor'],
                    'personal_services': ['personal', 'beauty', 'fitness']
                }
                
                keywords = type_filters.get(business_type.lower(), [business_type])
                
                matching_industries = []
                for _, row in self.employment_projections.iterrows():
                    if any(keyword in row['industry_title'].lower() for keyword in keywords):
                        matching_industries.append({
                            'industry': row['industry_title'],
                            'growth_rate': row['percent_change'],
                            'suitable_for_small_business': row['small_business_suitable'],
                            'job_openings': row['average_annual_openings']
                        })
                
                recommendations['industry_matches'] = sorted(
                    matching_industries, 
                    key=lambda x: x['growth_rate'], 
                    reverse=True
                )
            
            # Filter locations based on criteria
            if self.wage_data is not None:
                location_scores = {}
                latest_wages = self.wage_data[self.wage_data['data_year'] == self.wage_data['data_year'].max()]
                
                for area in latest_wages['area_name'].unique():
                    if target_area and target_area.lower() not in area.lower():
                        continue
                    
                    area_wages = latest_wages[latest_wages['area_name'] == area]
                    median_wage = area_wages['annual_median_wage'].median()
                    
                    if max_labor_cost and median_wage > max_labor_cost:
                        continue
                    
                    # Score location based on multiple factors
                    score = self._calculate_location_score(area_wages, business_type)
                    
                    location_scores[area] = {
                        'score': score,
                        'median_wage': median_wage,
                        'wage_range': area_wages['annual_median_wage'].std(),
                        'available_workforce': area_wages['total_employment'].sum()
                    }
                
                recommendations['location_suggestions'] = sorted(
                    location_scores.items(),
                    key=lambda x: x[1]['score'],
                    reverse=True
                )[:5]  # Top 5 locations
            
            # Wage analysis specific to request
            if business_type and self.wage_data is not None:
                relevant_occupations = self._get_relevant_occupations(business_type)
                wage_analysis = {}
                
                for occ in relevant_occupations:
                    occ_data = self.wage_data[
                        (self.wage_data['occupation_name'].str.contains(occ, case=False, na=False)) &
                        (self.wage_data['data_year'] == self.wage_data['data_year'].max())
                    ]
                    
                    if not occ_data.empty:
                        wage_analysis[occ] = {
                            'median_wage': occ_data['annual_median_wage'].median(),
                            'wage_range': f"${occ_data['annual_median_wage'].min():,.0f} - ${occ_data['annual_median_wage'].max():,.0f}",
                            'employment_level': occ_data['total_employment'].sum()
                        }
                
                recommendations['wage_analysis'] = wage_analysis
            
            # Implementation guidance
            recommendations['implementation_guidance'] = self._generate_implementation_guidance(
                business_type, recommendations
            )
            
        except Exception as e:
            self.logger.error(f"Error generating recommendations: {e}")
        
        return recommendations
    
    def create_summary_report(self) -> Dict[str, Any]:
        """
        Create comprehensive summary report
        
        Returns:
            Complete analysis summary
        """
        report = {
            'report_date': datetime.now().isoformat(),
            'data_summary': {},
            'key_findings': {},
            'market_opportunities': {},
            'cost_considerations': {},
            'location_insights': {}
        }
        
        try:
            # Data summary
            report['data_summary'] = {
                'wage_records': len(self.wage_data) if self.wage_data is not None else 0,
                'industry_projections': len(self.employment_projections) if self.employment_projections is not None else 0,
                'traffic_records': len(self.traffic_data) if self.traffic_data is not None else 0,
                'business_records': len(self.business_data) if self.business_data is not None else 0,
                'analysis_period': '2019-2032'
            }
            
            # Key findings
            if self.employment_projections is not None:
                high_growth = self.employment_projections[self.employment_projections['percent_change'] >= 10.0]
                declining = self.employment_projections[self.employment_projections['percent_change'] < 0]
                
                report['key_findings'] = {
                    'fastest_growing_industries': high_growth['industry_title'].tolist(),
                    'declining_industries': declining['industry_title'].tolist(),
                    'total_job_growth_projected': self.employment_projections['numeric_change'].sum(),
                    'annual_job_openings': self.employment_projections['average_annual_openings'].sum()
                }
            
            # Market opportunities
            opportunities = self.analyze_industry_opportunities()
            if opportunities:
                top_opportunities = list(opportunities.keys())[:5]
                report['market_opportunities'] = {
                    'top_5_industries': top_opportunities,
                    'small_business_friendly': [
                        industry for industry, data in opportunities.items() 
                        if data.get('small_business_suitable', False)
                    ][:5]
                }
            
            # Cost considerations
            labor_market = self.analyze_labor_market_by_industry()
            if labor_market:
                low_cost_occupations = [
                    occ for occ, data in labor_market.items()
                    if data['labor_cost_category'] in ['Low Cost', 'Moderate Cost']
                ]
                
                report['cost_considerations'] = {
                    'low_cost_labor_categories': low_cost_occupations[:10],
                    'wage_growth_trends': {
                        occ: data['wage_growth_5yr'] 
                        for occ, data in labor_market.items()
                    }
                }
            
            # Location insights
            location_analysis = self.analyze_location_factors()
            if location_analysis:
                sorted_by_cost = sorted(
                    location_analysis.items(),
                    key=lambda x: x[1].get('median_wage_all_occupations', float('inf'))
                )
                
                report['location_insights'] = {
                    'lowest_cost_areas': [area for area, _ in sorted_by_cost[:3]],
                    'highest_wage_areas': [area for area, _ in sorted_by_cost[-3:]],
                    'area_summaries': {
                        area: {
                            'median_wage': data.get('median_wage_all_occupations'),
                            'job_diversity': data.get('occupations_available')
                        }
                        for area, data in location_analysis.items()
                    }
                }
            
        except Exception as e:
            self.logger.error(f"Error creating summary report: {e}")
        
        return report
    
    # Helper methods
    def _categorize_labor_cost(self, annual_wage: float) -> str:
        """Categorize labor cost level"""
        if annual_wage >= 60000:
            return "High Cost"
        elif annual_wage >= 35000:
            return "Moderate Cost"
        else:
            return "Low Cost"
    
    def _score_labor_availability(self, employment: int, areas: int) -> str:
        """Score labor availability"""
        if employment >= 50000 and areas >= 8:
            return "High"
        elif employment >= 20000 and areas >= 5:
            return "Moderate"
        else:
            return "Limited"
    
    def _score_growth_rate(self, percent_change: float) -> float:
        """Score growth rate (0-100)"""
        if percent_change >= 15:
            return 90
        elif percent_change >= 10:
            return 80
        elif percent_change >= 5:
            return 60
        elif percent_change >= 0:
            return 40
        else:
            return 20
    
    def _assess_business_entry(self, row) -> float:
        """Assess business entry favorability (0-100)"""
        score = 50  # Base score
        
        if row.get('small_business_suitable', False):
            score += 20
        if row.get('startup_friendly', False):
            score += 15
        if row.get('capital_intensity') == 'Low':
            score += 10
        elif row.get('capital_intensity') == 'High':
            score -= 15
        if row.get('entry_barriers') == 'Low':
            score += 10
        elif row.get('entry_barriers') == 'High':
            score -= 15
        
        return min(100, max(0, score))
    
    def _score_market_size(self, employment: int) -> float:
        """Score market size (0-100)"""
        if employment >= 200000:
            return 90
        elif employment >= 100000:
            return 75
        elif employment >= 50000:
            return 60
        elif employment >= 20000:
            return 45
        else:
            return 30
    
    def _assess_wage_impact(self, industry: str) -> Dict:
        """Assess wage cost impact for industry"""
        impact = {
            'affordability_score': 50,
            'wage_trend': 'stable',
            'labor_availability': 'moderate'
        }
        
        if self.wage_data is not None:
            # Find relevant occupations for industry
            industry_lower = industry.lower()
            relevant_wages = self.wage_data[
                self.wage_data['occupation_name'].str.lower().str.contains(
                    '|'.join(industry_lower.split()), na=False
                )
            ]
            
            if not relevant_wages.empty:
                latest_wages = relevant_wages[relevant_wages['data_year'] == relevant_wages['data_year'].max()]
                median_wage = latest_wages['annual_median_wage'].median()
                
                # Score affordability (lower wages = higher score)
                if median_wage <= 30000:
                    impact['affordability_score'] = 80
                elif median_wage <= 45000:
                    impact['affordability_score'] = 60
                elif median_wage <= 60000:
                    impact['affordability_score'] = 40
                else:
                    impact['affordability_score'] = 20
        
        return impact
    
    def _generate_recommendation(self, score: float, row) -> str:
        """Generate text recommendation"""
        if score >= 75:
            return "Highly Recommended - Strong growth, favorable business conditions"
        elif score >= 60:
            return "Recommended - Good opportunity with manageable challenges"
        elif score >= 45:
            return "Consider Carefully - Mixed signals, detailed analysis needed"
        else:
            return "Challenging - Significant barriers or declining market"
    
    def _rate_wage_competitiveness(self, median_wage: float) -> str:
        """Rate wage competitiveness"""
        if median_wage >= 50000:
            return "High Cost"
        elif median_wage >= 38000:
            return "Competitive"
        else:
            return "Cost Advantage"
    
    def _assess_area_labor_costs(self, area_data) -> str:
        """Assess overall labor costs for area"""
        median_wage = area_data['annual_median_wage'].median()
        
        if median_wage >= 55000:
            return "High labor cost area - suitable for high-margin businesses"
        elif median_wage >= 40000:
            return "Moderate labor costs - balanced opportunity"
        else:
            return "Low labor cost area - good for cost-sensitive businesses"
    
    def _get_area_traffic_data(self, area: str) -> Dict:
        """Get traffic data for area if available"""
        traffic_info = {
            'traffic_data_available': False,
            'avg_daily_traffic': None,
            'peak_traffic_times': None
        }
        
        # Implementation would depend on traffic data structure
        return traffic_info
    
    def _calculate_location_score(self, area_wages, business_type: str) -> float:
        """Calculate location score based on multiple factors"""
        score = 50  # Base score
        
        median_wage = area_wages['annual_median_wage'].median()
        employment = area_wages['total_employment'].sum()
        
        # Lower wages = higher score for cost-sensitive businesses
        if median_wage <= 35000:
            score += 20
        elif median_wage >= 55000:
            score -= 10
        
        # Higher employment = more workforce availability
        if employment >= 50000:
            score += 15
        elif employment <= 10000:
            score -= 10
        
        return min(100, max(0, score))
    
    def _get_relevant_occupations(self, business_type: str) -> List[str]:
        """Get relevant occupations for business type"""
        occupation_mapping = {
            'retail': ['Retail', 'Sales', 'Cashier', 'Customer Service'],
            'restaurant': ['Food', 'Cook', 'Server', 'Bartender', 'Manager'],
            'professional': ['Manager', 'Professional', 'Technical', 'Administrative'],
            'healthcare': ['Nurse', 'Medical', 'Health', 'Care'],
            'construction': ['Construction', 'Carpenter', 'Electrician', 'Plumber'],
            'personal_services': ['Personal', 'Beauty', 'Fitness', 'Childcare']
        }
        
        return occupation_mapping.get(business_type.lower(), [business_type])
    
    def _generate_implementation_guidance(self, business_type: str, recommendations: Dict) -> Dict:
        """Generate implementation guidance"""
        guidance = {
            'next_steps': [],
            'key_considerations': [],
            'timeline_estimate': '3-6 months',
            'success_factors': []
        }
        
        if business_type:
            type_guidance = {
                'retail': {
                    'next_steps': ['Analyze foot traffic patterns', 'Research local competition', 'Evaluate lease terms'],
                    'key_considerations': ['Location visibility', 'Parking availability', 'Local demographics'],
                    'success_factors': ['Strong location', 'Inventory management', 'Customer service']
                },
                'restaurant': {
                    'next_steps': ['Test menu concepts', 'Analyze food costs', 'Plan kitchen layout'],
                    'key_considerations': ['Food service licenses', 'Health regulations', 'Labor scheduling'],
                    'success_factors': ['Quality food', 'Efficient operations', 'Marketing']
                }
            }
            
            specific_guidance = type_guidance.get(business_type.lower(), {})
            guidance.update(specific_guidance)
        
        return guidance


def main():
    """Run integrated business analysis"""
    logging.basicConfig(
        level=logging.INFO,
        format='%(asctime)s - %(levelname)s - %(message)s'
    )
    
    try:
        analyzer = IntegratedBusinessAnalyzer()
        
        # Load data
        if not analyzer.load_data_sources():
            print("Error loading data sources")
            return
        
        # Run analyses
        labor_market = analyzer.analyze_labor_market_by_industry()
        opportunities = analyzer.analyze_industry_opportunities()
        locations = analyzer.analyze_location_factors()
        
        # Generate recommendations for restaurant business
        restaurant_recs = analyzer.generate_business_recommendations(
            business_type='restaurant',
            max_labor_cost=35000
        )
        
        # Create summary report
        report = analyzer.create_summary_report()
        
        # Save results
        results = {
            'labor_market_analysis': labor_market,
            'industry_opportunities': opportunities,
            'location_analysis': locations,
            'restaurant_recommendations': restaurant_recs,
            'summary_report': report
        }
        
        output_file = f"wisconsin_integrated_analysis_{datetime.now().strftime('%Y%m%d')}.json"
        with open(output_file, 'w') as f:
            json.dump(results, f, indent=2, default=str)
        
        print(f"\n{'='*60}")
        print("WISCONSIN INTEGRATED BUSINESS ANALYSIS")
        print(f"{'='*60}")
        print(f"Analysis saved to: {output_file}")
        print(f"\nData Sources Loaded:")
        print(f"- Wage Records: {len(analyzer.wage_data) if analyzer.wage_data is not None else 0}")
        print(f"- Industry Projections: {len(analyzer.employment_projections) if analyzer.employment_projections is not None else 0}")
        print(f"- Traffic Records: {len(analyzer.traffic_data) if analyzer.traffic_data is not None else 0}")
        
        print(f"\nTop 5 Industry Opportunities:")
        for i, (industry, data) in enumerate(list(opportunities.items())[:5], 1):
            print(f"{i}. {industry}")
            print(f"   Growth: {data['growth_rate']:.1f}% | Score: {data['opportunity_score']}")
            print(f"   {data['recommendation']}")
        
        print(f"\nTop Location Recommendations for Restaurants:")
        for location, score_data in restaurant_recs['location_suggestions'][:3]:
            print(f"- {location}: Score {score_data['score']:.1f}")
            print(f"  Median Wage: ${score_data['median_wage']:,.0f}")
        
    except Exception as e:
        logging.error(f"Error in main analysis: {e}")


if __name__ == "__main__":
    main()