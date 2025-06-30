#!/usr/bin/env python3
"""
BLS PPI Construction Cost Integration
====================================

Integrates BLS Producer Price Index construction materials data with existing
Wisconsin business intelligence data to enable construction cost analysis for
site selection and development planning.

Integration Features:
- Combine construction cost trends with location data
- Calculate site development cost implications
- Generate construction cost benchmarks by region
- Create unified analysis views for decision making
"""

import logging
import json
import pandas as pd
from datetime import datetime, timedelta
from typing import List, Dict, Optional, Any
from pathlib import Path
import time

from google.cloud import bigquery
from bls_ppi_construction_collector import BLSPPIConstructionCollector


class PPIConstructionIntegrator:
    """Integrates BLS PPI construction cost data with Wisconsin business intelligence"""
    
    def __init__(self):
        self.logger = self._setup_logging()
        self.bigquery_client = bigquery.Client(project="location-optimizer-1")
        self.ppi_collector = BLSPPIConstructionCollector()
        
    def _setup_logging(self) -> logging.Logger:
        """Setup logging for integration processes"""
        logger = logging.getLogger('ppi_construction_integrator')
        logger.setLevel(logging.INFO)
        
        if not logger.handlers:
            handler = logging.FileHandler('location_optimizer.log')
            formatter = logging.Formatter(
                '%(asctime)s - %(name)s - %(levelname)s - %(message)s'
            )
            handler.setFormatter(formatter)
            logger.addHandler(handler)
            
        return logger
    
    def create_construction_cost_view(self) -> bool:
        """Create BigQuery view combining PPI data with location analysis"""
        
        try:
            # Ensure business_intelligence dataset exists
            dataset_id = "business_intelligence"
            dataset_ref = self.bigquery_client.dataset(dataset_id)
            
            try:
                self.bigquery_client.get_dataset(dataset_ref)
                self.logger.info(f"Dataset {dataset_id} already exists")
            except Exception:
                self.logger.info(f"Creating dataset {dataset_id}")
                dataset = bigquery.Dataset(dataset_ref)
                dataset.location = "US"
                self.bigquery_client.create_dataset(dataset)
            
            # Create comprehensive construction cost analysis view
            view_sql = """
            CREATE OR REPLACE VIEW `location-optimizer-1.business_intelligence.construction_cost_analysis` AS
            WITH ppi_latest AS (
                SELECT 
                    material_category,
                    material_subcategory,
                    series_title,
                    AVG(index_value) as avg_current_index,
                    AVG(yearly_change_pct) as avg_yearly_change,
                    COUNT(*) as data_points,
                    MAX(year) as latest_year,
                    MAX(data_extraction_date) as last_updated
                FROM `location-optimizer-1.raw_business_data.bls_ppi_construction`
                WHERE year >= EXTRACT(YEAR FROM CURRENT_DATE()) - 2
                GROUP BY material_category, material_subcategory, series_title
            ),
            
            cost_trends AS (
                SELECT 
                    material_category,
                    avg_current_index,
                    avg_yearly_change,
                    CASE 
                        WHEN avg_yearly_change >= 5.0 THEN 'Rising'
                        WHEN avg_yearly_change <= -2.0 THEN 'Falling' 
                        ELSE 'Stable'
                    END as cost_trend,
                    CASE
                        WHEN material_category IN ('Steel', 'Lumber', 'Concrete') THEN 'High Impact'
                        WHEN material_category IN ('Roofing', 'Insulation', 'Paint') THEN 'Medium Impact'
                        ELSE 'Low Impact'
                    END as construction_impact,
                    last_updated
                FROM ppi_latest
            ),
            
            wisconsin_business_context AS (
                SELECT 
                    COUNT(*) as total_wisconsin_businesses,
                    COUNT(CASE WHEN naics_code LIKE '23%' THEN 1 END) as construction_businesses
                FROM `location-optimizer-1.raw_business_data.census_economic_benchmarks`
                WHERE state_name = 'Wisconsin'
            ),
            
            industry_context AS (
                SELECT 
                    naics_code,
                    naics_title as industry_title,
                    AVG(revenue_per_establishment) as avg_revenue_per_establishment,
                    AVG(revenue_per_employee) as avg_revenue_per_employee,
                    COUNT(*) as benchmark_records
                FROM `location-optimizer-1.raw_business_data.census_economic_benchmarks`
                WHERE naics_code LIKE '23%' -- Construction industry
                GROUP BY naics_code, naics_title
            )
            
            SELECT 
                -- Construction cost data
                ct.material_category,
                ct.avg_current_index,
                ct.avg_yearly_change,
                ct.cost_trend,
                ct.construction_impact,
                
                -- Cost implications by impact level
                CASE 
                    WHEN ct.construction_impact = 'High Impact' AND ct.cost_trend = 'Rising' THEN 'High Risk'
                    WHEN ct.construction_impact = 'High Impact' AND ct.cost_trend = 'Falling' THEN 'Cost Opportunity' 
                    WHEN ct.construction_impact = 'Medium Impact' AND ct.cost_trend = 'Rising' THEN 'Monitor'
                    ELSE 'Low Risk'
                END as cost_risk_level,
                
                -- Relative cost position
                CASE
                    WHEN ct.avg_current_index > 400 THEN 'Very High Cost'
                    WHEN ct.avg_current_index > 300 THEN 'High Cost'
                    WHEN ct.avg_current_index > 200 THEN 'Moderate Cost'
                    ELSE 'Low Cost'
                END as cost_position,
                
                -- Construction industry context
                ic.avg_revenue_per_establishment as construction_revenue_per_establishment,
                ic.avg_revenue_per_employee as construction_revenue_per_employee,
                
                -- Wisconsin business context
                'Statewide' as geographic_scope,
                wbc.total_wisconsin_businesses,
                wbc.construction_businesses,
                
                -- Analysis metadata
                ct.last_updated,
                CURRENT_TIMESTAMP() as analysis_timestamp,
                
                -- Strategic recommendations
                CASE 
                    WHEN ct.cost_trend = 'Rising' AND ct.construction_impact = 'High Impact' THEN
                        'Consider accelerating construction timelines or alternative materials'
                    WHEN ct.cost_trend = 'Falling' AND ct.construction_impact = 'High Impact' THEN
                        'Favorable timing for construction projects'
                    WHEN ct.cost_trend = 'Stable' THEN
                        'Normal construction cost environment'
                    ELSE 'Monitor for cost changes'
                END as strategic_recommendation
                
            FROM cost_trends ct
            CROSS JOIN wisconsin_business_context wbc
            LEFT JOIN industry_context ic ON ic.naics_code = '236' -- Building construction
            ORDER BY ct.construction_impact DESC, ct.avg_yearly_change DESC
            """
            
            # Execute view creation
            job = self.bigquery_client.query(view_sql)
            job.result()
            
            self.logger.info("Created construction_cost_analysis view successfully")
            return True
            
        except Exception as e:
            self.logger.error(f"Error creating construction cost view: {e}")
            return False
    
    def create_county_construction_impact_view(self) -> bool:
        """Create view showing construction cost impact by Wisconsin county"""
        
        try:
            view_sql = """
            CREATE OR REPLACE VIEW `location-optimizer-1.business_intelligence.county_construction_impact` AS
            WITH county_business_data AS (
                SELECT 
                    county_name,
                    county_fips,
                    SUM(establishments_total) as total_businesses,
                    SUM(CASE WHEN naics_code LIKE '23%' THEN establishments_total ELSE 0 END) as construction_businesses,
                    AVG(revenue_per_establishment) as avg_revenue_per_establishment
                FROM `location-optimizer-1.raw_business_data.census_economic_benchmarks`
                WHERE geo_level = 'county' AND state_name = 'Wisconsin'
                GROUP BY county_name, county_fips
            ),
            
            construction_cost_summary AS (
                SELECT 
                    AVG(CASE WHEN material_category = 'Steel' THEN index_value END) as steel_cost_index,
                    AVG(CASE WHEN material_category = 'Lumber' THEN index_value END) as lumber_cost_index,
                    AVG(CASE WHEN material_category = 'Concrete' THEN index_value END) as concrete_cost_index,
                    AVG(CASE WHEN material_category = 'General Construction' THEN index_value END) as general_cost_index,
                    AVG(CASE WHEN material_category = 'Steel' THEN yearly_change_pct END) as steel_trend,
                    AVG(CASE WHEN material_category = 'Lumber' THEN yearly_change_pct END) as lumber_trend,
                    AVG(CASE WHEN material_category = 'Concrete' THEN yearly_change_pct END) as concrete_trend,
                    MAX(data_extraction_date) as cost_data_date
                FROM `location-optimizer-1.raw_business_data.bls_ppi_construction`
                WHERE year = EXTRACT(YEAR FROM CURRENT_DATE()) - 1 -- Most recent complete year
            ),
            
            county_economic_context AS (
                SELECT 
                    county_name,
                    AVG(revenue_per_establishment) as avg_revenue_per_establishment,
                    SUM(establishments_total) as total_establishments,
                    SUM(employees_total) as total_employees
                FROM `location-optimizer-1.raw_business_data.census_economic_benchmarks`
                WHERE geo_level = 'county' AND state_name = 'Wisconsin'
                GROUP BY county_name
            )
            
            SELECT 
                cbd.county_name,
                cbd.county_fips,
                cbd.total_businesses,
                cbd.construction_businesses,
                ROUND(cbd.construction_businesses * 100.0 / NULLIF(cbd.total_businesses, 0), 2) as construction_business_pct,
                -- Construction cost environment
                ccs.steel_cost_index,
                ccs.lumber_cost_index, 
                ccs.concrete_cost_index,
                ccs.general_cost_index,
                ccs.steel_trend,
                ccs.lumber_trend,
                ccs.concrete_trend,
                
                -- Economic context
                cec.avg_revenue_per_establishment,
                cec.total_establishments as census_establishments,
                cec.total_employees as census_employees,
                
                -- Construction favorability score (0-100)
                ROUND(
                    50 + -- Base score
                    (CASE WHEN ccs.steel_trend < 0 THEN 10 ELSE -5 END) + -- Steel cost trend
                    (CASE WHEN ccs.lumber_trend < 0 THEN 10 ELSE -5 END) + -- Lumber cost trend  
                    (CASE WHEN ccs.concrete_trend < 0 THEN 8 ELSE -3 END) + -- Concrete cost trend
                    (CASE WHEN cbd.construction_businesses > 50 THEN 10 ELSE 5 END) + -- Construction capacity
                    (CASE WHEN cec.avg_revenue_per_establishment > 1000000 THEN 10 ELSE 0 END) -- Economic strength
                , 0) as construction_favorability_score,
                
                -- Risk assessment
                CASE 
                    WHEN ccs.steel_trend > 10 OR ccs.lumber_trend > 10 THEN 'High Cost Risk'
                    WHEN ccs.steel_trend > 5 OR ccs.lumber_trend > 5 THEN 'Moderate Cost Risk'
                    WHEN ccs.steel_trend < -5 AND ccs.lumber_trend < -5 THEN 'Cost Opportunity'
                    ELSE 'Stable Cost Environment'
                END as cost_risk_assessment,
                
                -- Recommendations
                CASE 
                    WHEN cbd.construction_businesses < 20 THEN 'Limited construction capacity - plan for longer timelines'
                    WHEN ccs.steel_trend > 10 THEN 'Consider steel alternatives or accelerated timeline'
                    WHEN ccs.lumber_trend < -10 THEN 'Favorable timing for wood construction'
                    WHEN cbd.construction_businesses > 100 THEN 'Strong construction market - competitive pricing likely'
                    ELSE 'Normal construction environment'
                END as county_recommendation,
                
                ccs.cost_data_date,
                CURRENT_TIMESTAMP() as analysis_timestamp
                
            FROM county_business_data cbd
            CROSS JOIN construction_cost_summary ccs
            LEFT JOIN county_economic_context cec ON 
                cec.county_name = cbd.county_name
                
            ORDER BY construction_favorability_score DESC, total_businesses DESC
            """
            
            job = self.bigquery_client.query(view_sql)
            job.result()
            
            self.logger.info("Created county_construction_impact view successfully")
            return True
            
        except Exception as e:
            self.logger.error(f"Error creating county construction impact view: {e}")
            return False
    
    def generate_construction_cost_report(self) -> Dict[str, Any]:
        """Generate comprehensive construction cost analysis report"""
        
        try:
            # Query current construction cost data
            cost_query = """
            SELECT 
                material_category,
                avg_current_index,
                avg_yearly_change,
                cost_trend,
                construction_impact,
                cost_risk_level,
                strategic_recommendation
            FROM `location-optimizer-1.business_intelligence.construction_cost_analysis`
            ORDER BY construction_impact DESC, avg_yearly_change DESC
            """
            
            cost_df = self.bigquery_client.query(cost_query).to_dataframe()
            
            # Query county impact analysis
            county_query = """
            SELECT 
                county_name,
                construction_business_pct,
                construction_favorability_score,
                cost_risk_assessment,
                county_recommendation
            FROM `location-optimizer-1.business_intelligence.county_construction_impact`
            WHERE construction_favorability_score IS NOT NULL
            ORDER BY construction_favorability_score DESC
            LIMIT 10
            """
            
            county_df = self.bigquery_client.query(county_query).to_dataframe()
            
            # Generate report
            report = {
                'report_date': datetime.now().isoformat(),
                'report_type': 'Wisconsin Construction Cost Analysis',
                
                'cost_overview': {
                    'total_materials_tracked': len(cost_df),
                    'high_risk_materials': len(cost_df[cost_df['cost_risk_level'] == 'High Risk']),
                    'cost_opportunities': len(cost_df[cost_df['cost_risk_level'] == 'Cost Opportunity']),
                    'average_cost_change': cost_df['avg_yearly_change'].mean() if not cost_df.empty else 0
                },
                
                'material_analysis': cost_df.to_dict('records') if not cost_df.empty else [],
                
                'county_rankings': {
                    'top_construction_counties': county_df.head(5).to_dict('records') if not county_df.empty else [],
                    'average_favorability_score': county_df['construction_favorability_score'].mean() if not county_df.empty else 0
                },
                
                'key_insights': self._generate_key_insights(cost_df, county_df),
                
                'strategic_recommendations': self._generate_strategic_recommendations(cost_df, county_df)
            }
            
            # Save report
            report_path = f"construction_cost_report_{datetime.now().strftime('%Y%m%d_%H%M%S')}.json"
            with open(report_path, 'w') as f:
                json.dump(report, f, indent=2, default=str)
            
            self.logger.info(f"Generated construction cost report: {report_path}")
            return report
            
        except Exception as e:
            self.logger.error(f"Error generating construction cost report: {e}")
            return {}
    
    def _generate_key_insights(self, cost_df: pd.DataFrame, county_df: pd.DataFrame) -> List[str]:
        """Generate key insights from cost and county data"""
        
        insights = []
        
        if not cost_df.empty:
            # Cost trend insights
            rising_materials = cost_df[cost_df['cost_trend'] == 'Rising']['material_category'].tolist()
            falling_materials = cost_df[cost_df['cost_trend'] == 'Falling']['material_category'].tolist()
            
            if rising_materials:
                insights.append(f"Rising cost materials: {', '.join(rising_materials[:3])}")
            if falling_materials:
                insights.append(f"Falling cost materials: {', '.join(falling_materials[:3])}")
            
            # High impact analysis
            high_impact = cost_df[cost_df['construction_impact'] == 'High Impact']
            if not high_impact.empty:
                avg_change = high_impact['avg_yearly_change'].mean()
                insights.append(f"High-impact materials showing {avg_change:+.1f}% average cost change")
        
        if not county_df.empty:
            # County insights
            top_county = county_df.iloc[0]
            insights.append(f"Top construction-favorable county: {top_county['county_name']} (Score: {top_county['construction_favorability_score']:.0f})")
            
            high_risk_counties = len(county_df[county_df['cost_risk_assessment'] == 'High Cost Risk'])
            if high_risk_counties > 0:
                insights.append(f"{high_risk_counties} counties identified as high construction cost risk")
        
        return insights
    
    def _generate_strategic_recommendations(self, cost_df: pd.DataFrame, county_df: pd.DataFrame) -> List[str]:
        """Generate strategic recommendations based on analysis"""
        
        recommendations = []
        
        if not cost_df.empty:
            # Material-based recommendations
            high_risk = cost_df[cost_df['cost_risk_level'] == 'High Risk']
            if not high_risk.empty:
                recommendations.append("Consider alternative materials or accelerated timelines for high-risk materials")
            
            opportunities = cost_df[cost_df['cost_risk_level'] == 'Cost Opportunity']
            if not opportunities.empty:
                recommendations.append(f"Favorable timing for projects using: {', '.join(opportunities['material_category'].tolist())}")
        
        if not county_df.empty:
            # County-based recommendations
            top_counties = county_df.head(3)['county_name'].tolist()
            recommendations.append(f"Consider prioritizing construction in: {', '.join(top_counties)}")
            
            low_capacity = county_df[county_df['county_recommendation'].str.contains('Limited construction capacity', na=False)]
            if not low_capacity.empty:
                recommendations.append("Plan for extended timelines in counties with limited construction capacity")
        
        # General recommendations
        recommendations.append("Monitor PPI trends monthly for construction planning")
        recommendations.append("Integrate construction cost data into site selection criteria")
        
        return recommendations
    
    def run_full_integration(self) -> Dict[str, Any]:
        """Run complete PPI construction integration process"""
        
        start_time = time.time()
        
        integration_summary = {
            'integration_date': datetime.now(),
            'steps_completed': [],
            'errors': [],
            'success': False,
            'processing_time_seconds': 0
        }
        
        try:
            self.logger.info("Starting PPI construction integration")
            
            # Step 1: Create construction cost analysis view
            if self.create_construction_cost_view():
                integration_summary['steps_completed'].append('Construction cost analysis view created')
            else:
                integration_summary['errors'].append('Failed to create construction cost view')
            
            # Step 2: Create county impact view
            if self.create_county_construction_impact_view():
                integration_summary['steps_completed'].append('County construction impact view created')
            else:
                integration_summary['errors'].append('Failed to create county impact view')
            
            # Step 3: Generate comprehensive report
            report = self.generate_construction_cost_report()
            if report:
                integration_summary['steps_completed'].append('Construction cost report generated')
                integration_summary['report_summary'] = {
                    'materials_analyzed': report.get('cost_overview', {}).get('total_materials_tracked', 0),
                    'counties_analyzed': len(report.get('county_rankings', {}).get('top_construction_counties', [])),
                    'key_insights_count': len(report.get('key_insights', [])),
                    'recommendations_count': len(report.get('strategic_recommendations', []))
                }
            else:
                integration_summary['errors'].append('Failed to generate cost report')
            
            # Set success status
            integration_summary['success'] = len(integration_summary['errors']) == 0
            
        except Exception as e:
            error_msg = f"Error in PPI construction integration: {e}"
            self.logger.error(error_msg)
            integration_summary['errors'].append(error_msg)
            integration_summary['success'] = False
        
        integration_summary['processing_time_seconds'] = time.time() - start_time
        
        self.logger.info(f"PPI construction integration complete: {integration_summary}")
        return integration_summary


def main():
    """Run PPI construction integration"""
    logging.basicConfig(level=logging.INFO)
    
    print("🏗️ BLS PPI Construction Cost Integration")
    print("=" * 50)
    
    try:
        integrator = PPIConstructionIntegrator()
        
        # Run full integration
        summary = integrator.run_full_integration()
        
        print(f"\nIntegration Summary:")
        print(f"✅ Steps Completed: {len(summary['steps_completed'])}")
        for step in summary['steps_completed']:
            print(f"   • {step}")
        
        if summary['errors']:
            print(f"\n❌ Errors: {len(summary['errors'])}")
            for error in summary['errors']:
                print(f"   • {error}")
        
        print(f"\n📊 Report Summary:")
        if 'report_summary' in summary:
            rs = summary['report_summary']
            print(f"   • Materials Analyzed: {rs['materials_analyzed']}")
            print(f"   • Counties Analyzed: {rs['counties_analyzed']}")
            print(f"   • Key Insights: {rs['key_insights_count']}")
            print(f"   • Recommendations: {rs['recommendations_count']}")
        
        print(f"\n⏱️ Processing Time: {summary['processing_time_seconds']:.1f} seconds")
        print(f"🎯 Success: {summary['success']}")
        
        if summary['success']:
            print("\n🎉 PPI Construction Integration completed successfully!")
            print("Construction cost data is now integrated with Wisconsin business intelligence.")
        
    except Exception as e:
        print(f"❌ Integration failed: {e}")


if __name__ == "__main__":
    main()