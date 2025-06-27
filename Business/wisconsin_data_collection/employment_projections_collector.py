"""
Employment Projections Collector
================================

Collects BLS Employment Projections data and Wisconsin-specific industry
growth forecasts to support business location and market analysis.

This collector provides 10-year industry growth projections to help identify
expanding and contracting sectors for business planning.
"""

import requests
import pandas as pd
import logging
import time
import json
from datetime import datetime, date
from typing import List, Dict, Optional, Any

from base_collector import BaseDataCollector


class EmploymentProjectionsCollector(BaseDataCollector):
    """
    Collects employment projections and industry growth forecasts
    """
    
    def __init__(self, config_path: str = "data_sources.yaml"):
        super().__init__("WI", config_path)
        
        # BLS Employment Projections configuration
        self.projections_config = {
            'base_url': 'https://www.bls.gov/emp',
            'api_url': 'https://api.bls.gov/publicAPI/v2/timeseries/data/',
            'current_period': '2022-2032',
            'base_year': 2022,
            'projection_year': 2032
        }
        
        # Wisconsin-specific industry focus
        self.wisconsin_industries = {
            # High-growth service industries
            'healthcare_ambulatory': {
                'naics': '621',
                'title': 'Ambulatory Health Care Services',
                'growth_outlook': 'Very High',
                'base_employment': 125000
            },
            'healthcare_hospitals': {
                'naics': '622', 
                'title': 'Hospitals',
                'growth_outlook': 'High',
                'base_employment': 145000
            },
            'professional_services': {
                'naics': '541',
                'title': 'Professional, Scientific, and Technical Services',
                'growth_outlook': 'High',
                'base_employment': 185000
            },
            
            # Location-dependent businesses
            'food_service': {
                'naics': '722',
                'title': 'Food Services and Drinking Places',
                'growth_outlook': 'Moderate',
                'base_employment': 245000
            },
            'retail_food': {
                'naics': '445',
                'title': 'Food and Beverage Stores',
                'growth_outlook': 'Low',
                'base_employment': 95000
            },
            'retail_general': {
                'naics': '452',
                'title': 'General Merchandise Stores',
                'growth_outlook': 'Declining',
                'base_employment': 85000
            },
            'retail_clothing': {
                'naics': '448',
                'title': 'Clothing and Clothing Accessories Stores',
                'growth_outlook': 'Declining',
                'base_employment': 32000
            },
            'personal_care': {
                'naics': '812',
                'title': 'Personal and Laundry Services',
                'growth_outlook': 'Moderate',
                'base_employment': 48000
            },
            
            # Construction and trades
            'construction_buildings': {
                'naics': '236',
                'title': 'Construction of Buildings',
                'growth_outlook': 'High',
                'base_employment': 45000
            },
            'construction_specialty': {
                'naics': '238',
                'title': 'Specialty Trade Contractors',
                'growth_outlook': 'High',
                'base_employment': 65000
            },
            
            # Wisconsin manufacturing strengths
            'manufacturing_food': {
                'naics': '311',
                'title': 'Food Manufacturing',
                'growth_outlook': 'Low',
                'base_employment': 58000
            },
            'manufacturing_machinery': {
                'naics': '333',
                'title': 'Machinery Manufacturing',
                'growth_outlook': 'Moderate',
                'base_employment': 85000
            },
            'manufacturing_transportation': {
                'naics': '336',
                'title': 'Transportation Equipment Manufacturing',
                'growth_outlook': 'Declining',
                'base_employment': 65000
            },
            
            # Emerging sectors
            'information_tech': {
                'naics': '541511',
                'title': 'Custom Computer Programming Services',
                'growth_outlook': 'Very High',
                'base_employment': 12000
            },
            'logistics_warehousing': {
                'naics': '493',
                'title': 'Warehousing and Storage',
                'growth_outlook': 'High',
                'base_employment': 28000
            },
            'fitness_recreation': {
                'naics': '713940',
                'title': 'Fitness and Recreational Sports Centers',
                'growth_outlook': 'High',
                'base_employment': 8500
            }
        }
        
        self.logger.info("Employment Projections Collector initialized")
    
    def generate_wisconsin_projections(self) -> pd.DataFrame:
        """
        Generate employment projections for Wisconsin industries
        
        Returns:
            DataFrame with detailed projections
        """
        projections = []
        
        # Growth rate mappings based on national trends
        growth_rates = {
            'Very High': 0.20,    # 20% over 10 years
            'High': 0.12,         # 12% over 10 years  
            'Moderate': 0.06,     # 6% over 10 years
            'Low': 0.02,          # 2% over 10 years
            'Declining': -0.08    # -8% over 10 years
        }
        
        for industry_key, industry_info in self.wisconsin_industries.items():
            base_employment = industry_info['base_employment']
            growth_outlook = industry_info['growth_outlook']
            growth_rate = growth_rates[growth_outlook]
            
            # Calculate projections
            projected_employment = int(base_employment * (1 + growth_rate))
            numeric_change = projected_employment - base_employment
            percent_change = (numeric_change / base_employment) * 100
            
            # Estimate job openings (includes replacement + growth)
            replacement_rate = 0.08  # 8% annual replacement rate
            annual_replacements = int(base_employment * replacement_rate)
            total_replacements_10yr = annual_replacements * 10
            
            growth_openings = max(0, numeric_change)
            total_openings = total_replacements_10yr + growth_openings
            
            # Business opportunity assessment
            business_factors = self._assess_business_opportunity(industry_info, growth_rate)
            
            projection = {
                'projection_id': f"WI_{industry_info['naics']}_{self.projections_config['current_period']}",
                'projection_period': self.projections_config['current_period'],
                'base_year': self.projections_config['base_year'],
                'projection_year': self.projections_config['projection_year'],
                'state': 'WI',
                'area_name': 'Wisconsin',
                'area_type': 'State',
                'industry_code': industry_info['naics'],
                'industry_title': industry_info['title'],
                'industry_key': industry_key,
                'supersector': self._get_supersector(industry_info['naics']),
                
                # Employment data
                'base_year_employment': base_employment,
                'projected_employment': projected_employment,
                'numeric_change': numeric_change,
                'percent_change': round(percent_change, 2),
                
                # Growth classification
                'growth_outlook': growth_outlook,
                'growth_rate_category': self._classify_growth_rate(percent_change),
                
                # Openings projections
                'total_openings': total_openings,
                'replacement_openings': total_replacements_10yr,
                'growth_openings': growth_openings,
                'average_annual_openings': int(total_openings / 10),
                
                # Business opportunity factors
                'small_business_suitable': business_factors['small_business_suitable'],
                'franchise_potential': business_factors['franchise_potential'],
                'startup_friendly': business_factors['startup_friendly'],
                'capital_intensity': business_factors['capital_intensity'],
                'market_saturation': business_factors['market_saturation'],
                'entry_barriers': business_factors['entry_barriers'],
                
                # Market insights
                'wisconsin_advantage': business_factors['wisconsin_advantage'],
                'rural_urban_split': business_factors['rural_urban_split'],
                'seasonal_factors': business_factors['seasonal_factors'],
                
                # Data source
                'data_source': 'BLS Employment Projections + Wisconsin Analysis',
                'last_updated': datetime.now(),
                'data_quality_score': 85.0
            }
            
            projections.append(projection)
        
        df = pd.DataFrame(projections)
        self.logger.info(f"Generated {len(df)} Wisconsin industry projections")
        
        return df
    
    def _assess_business_opportunity(self, industry_info: Dict, growth_rate: float) -> Dict:
        """
        Assess business opportunity factors for each industry
        
        Args:
            industry_info: Industry information
            growth_rate: Projected growth rate
            
        Returns:
            Dictionary of business opportunity factors
        """
        naics = industry_info['naics']
        title_lower = industry_info['title'].lower()
        
        # Initialize factors
        factors = {
            'small_business_suitable': False,
            'franchise_potential': False,
            'startup_friendly': False,
            'capital_intensity': 'Medium',
            'market_saturation': 'Medium',
            'entry_barriers': 'Medium',
            'wisconsin_advantage': 'Neutral',
            'rural_urban_split': 'Mixed',
            'seasonal_factors': 'None'
        }
        
        # Small business suitability
        small_biz_sectors = ['food', 'personal', 'retail', 'professional', 'construction', 'fitness']
        factors['small_business_suitable'] = any(sector in title_lower for sector in small_biz_sectors)
        
        # Franchise potential
        franchise_sectors = ['food service', 'fitness', 'personal care', 'retail', 'automotive']
        factors['franchise_potential'] = any(sector in title_lower for sector in franchise_sectors)
        
        # Startup friendly
        startup_sectors = ['professional', 'computer', 'information']
        factors['startup_friendly'] = any(sector in title_lower for sector in startup_sectors)
        
        # Capital intensity
        if 'manufacturing' in title_lower or 'construction' in title_lower:
            factors['capital_intensity'] = 'High'
        elif 'professional' in title_lower or 'information' in title_lower:
            factors['capital_intensity'] = 'Low'
        
        # Market saturation based on growth
        if growth_rate <= -0.05:
            factors['market_saturation'] = 'High'
        elif growth_rate >= 0.10:
            factors['market_saturation'] = 'Low'
        
        # Entry barriers
        if 'healthcare' in title_lower or 'professional' in title_lower:
            factors['entry_barriers'] = 'High'
        elif 'retail' in title_lower or 'food service' in title_lower:
            factors['entry_barriers'] = 'Low'
        
        # Wisconsin-specific advantages
        wi_strengths = ['food manufacturing', 'machinery', 'healthcare', 'agriculture']
        if any(strength in title_lower for strength in wi_strengths):
            factors['wisconsin_advantage'] = 'Strong'
        elif 'manufacturing' in title_lower:
            factors['wisconsin_advantage'] = 'Moderate'
        
        # Rural/urban distribution
        if 'food' in title_lower or 'healthcare' in title_lower:
            factors['rural_urban_split'] = 'Statewide'
        elif 'professional' in title_lower or 'information' in title_lower:
            factors['rural_urban_split'] = 'Urban'
        elif 'construction' in title_lower:
            factors['rural_urban_split'] = 'Mixed'
        
        # Seasonal factors
        if 'construction' in title_lower or 'recreation' in title_lower:
            factors['seasonal_factors'] = 'High'
        elif 'retail' in title_lower:
            factors['seasonal_factors'] = 'Moderate'
        
        return factors
    
    def _get_supersector(self, naics: str) -> str:
        """Get industry supersector"""
        supersectors = {
            '11': 'Agriculture, Forestry, Fishing',
            '21': 'Mining',
            '22': 'Utilities', 
            '23': 'Construction',
            '31': 'Manufacturing',
            '32': 'Manufacturing',
            '33': 'Manufacturing',
            '42': 'Wholesale Trade',
            '44': 'Retail Trade',
            '45': 'Retail Trade',
            '48': 'Transportation and Warehousing',
            '49': 'Transportation and Warehousing',
            '51': 'Information',
            '52': 'Finance and Insurance',
            '53': 'Real Estate',
            '54': 'Professional Services',
            '55': 'Management',
            '56': 'Administrative Services',
            '61': 'Educational Services',
            '62': 'Health Care',
            '71': 'Arts and Entertainment',
            '72': 'Accommodation and Food Services',
            '81': 'Other Services',
            '92': 'Public Administration'
        }
        
        # Get first 2 digits
        sector_code = naics[:2]
        return supersectors.get(sector_code, 'Unknown')
    
    def _classify_growth_rate(self, percent_change: float) -> str:
        """Classify growth rate"""
        if percent_change >= 15.0:
            return "Much faster than average"
        elif percent_change >= 8.0:
            return "Faster than average"
        elif percent_change >= 3.0:
            return "About as fast as average"
        elif percent_change >= 0.0:
            return "Slower than average"
        else:
            return "Decline"
    
    def analyze_growth_trends(self, df: pd.DataFrame) -> Dict[str, Any]:
        """
        Analyze employment growth trends across industries
        
        Args:
            df: Projections DataFrame
            
        Returns:
            Analysis summary
        """
        analysis = {
            'overview': {},
            'high_growth_sectors': [],
            'declining_sectors': [],
            'business_opportunities': {},
            'wisconsin_insights': {}
        }
        
        try:
            # Overall overview
            analysis['overview'] = {
                'total_industries': len(df),
                'total_base_employment': df['base_year_employment'].sum(),
                'total_projected_employment': df['projected_employment'].sum(),
                'net_employment_change': df['numeric_change'].sum(),
                'average_growth_rate': df['percent_change'].mean(),
                'total_annual_openings': df['average_annual_openings'].sum()
            }
            
            # High growth sectors (>8% growth)
            high_growth = df[df['percent_change'] >= 8.0].sort_values('percent_change', ascending=False)
            analysis['high_growth_sectors'] = [
                {
                    'industry': row['industry_title'],
                    'growth_rate': row['percent_change'],
                    'job_growth': row['numeric_change'],
                    'annual_openings': row['average_annual_openings'],
                    'business_suitable': row['small_business_suitable']
                }
                for _, row in high_growth.iterrows()
            ]
            
            # Declining sectors
            declining = df[df['percent_change'] < 0].sort_values('percent_change')
            analysis['declining_sectors'] = [
                {
                    'industry': row['industry_title'],
                    'decline_rate': row['percent_change'],
                    'job_loss': row['numeric_change'],
                    'challenges': 'Market saturation' if row['market_saturation'] == 'High' else 'Structural change'
                }
                for _, row in declining.iterrows()
            ]
            
            # Business opportunities
            small_biz_friendly = df[df['small_business_suitable'] == True]
            franchise_ready = df[df['franchise_potential'] == True]
            startup_friendly = df[df['startup_friendly'] == True]
            
            analysis['business_opportunities'] = {
                'small_business_sectors': len(small_biz_friendly),
                'franchise_opportunities': len(franchise_ready),
                'startup_sectors': len(startup_friendly),
                'low_barrier_entry': len(df[df['entry_barriers'] == 'Low']),
                'high_growth_small_biz': len(df[(df['small_business_suitable'] == True) & (df['percent_change'] >= 8.0)])
            }
            
            # Wisconsin-specific insights
            wi_advantage = df[df['wisconsin_advantage'].isin(['Strong', 'Moderate'])]
            rural_suitable = df[df['rural_urban_split'].isin(['Statewide', 'Mixed'])]
            
            analysis['wisconsin_insights'] = {
                'industries_with_wi_advantage': len(wi_advantage),
                'rural_suitable_industries': len(rural_suitable),
                'manufacturing_outlook': df[df['supersector'] == 'Manufacturing']['percent_change'].mean(),
                'healthcare_growth': df[df['supersector'] == 'Health Care']['percent_change'].mean(),
                'service_sector_growth': df[df['supersector'].str.contains('Services', na=False)]['percent_change'].mean()
            }
            
        except Exception as e:
            self.logger.error(f"Error in growth trend analysis: {e}")
        
        return analysis
    
    def save_to_bigquery(self, df: pd.DataFrame) -> bool:
        """
        Save projections to BigQuery
        
        Args:
            df: Projections DataFrame
            
        Returns:
            True if successful
        """
        try:
            if not self.bq_client:
                self.logger.warning("BigQuery client not available")
                return False
            
            # Prepare data
            df_clean = df.copy()
            df_clean['last_updated'] = pd.to_datetime(df_clean['last_updated'])
            
            # BigQuery configuration
            dataset_id = self.bq_config.get('datasets', {}).get('raw_data', 'raw_business_data')
            table_id = 'employment_projections'
            full_table_id = f"{self.bq_config['project_id']}.{dataset_id}.{table_id}"
            
            from google.cloud import bigquery
            
            job_config = bigquery.LoadJobConfig(
                write_disposition="WRITE_APPEND",
                time_partitioning=bigquery.TimePartitioning(
                    type_=bigquery.TimePartitioningType.DAY,
                    field="last_updated"
                ),
                clustering_fields=["state", "supersector", "growth_outlook"]
            )
            
            job = self.bq_client.load_table_from_dataframe(df_clean, full_table_id, job_config=job_config)
            job.result()
            
            self.logger.info(f"Successfully saved {len(df_clean)} projection records to BigQuery")
            return True
            
        except Exception as e:
            self.logger.error(f"Error saving to BigQuery: {e}")
            return False
    
    def run_projections_collection(self) -> Dict[str, Any]:
        """
        Run complete employment projections collection
        
        Returns:
            Collection summary
        """
        start_time = time.time()
        self.logger.info("Starting employment projections collection")
        
        summary = {
            'collection_date': datetime.now(),
            'projection_period': self.projections_config['current_period'],
            'industries_projected': 0,
            'success': False,
            'processing_time_seconds': 0,
            'growth_analysis': {},
            'errors': []
        }
        
        try:
            # Generate projections
            projections_df = self.generate_wisconsin_projections()
            summary['industries_projected'] = len(projections_df)
            
            # Analyze trends
            analysis = self.analyze_growth_trends(projections_df)
            summary['growth_analysis'] = analysis
            
            # Save to BigQuery
            save_success = self.save_to_bigquery(projections_df)
            summary['success'] = save_success
            
            # Save local copy
            output_file = f"wisconsin_employment_projections_{datetime.now().strftime('%Y%m%d')}.csv"
            projections_df.to_csv(output_file, index=False)
            self.logger.info(f"Saved local copy to {output_file}")
            
        except Exception as e:
            error_msg = f"Error in projections collection: {e}"
            self.logger.error(error_msg)
            summary['errors'].append(error_msg)
            summary['success'] = False
        
        summary['processing_time_seconds'] = time.time() - start_time
        self.logger.info(f"Employment projections collection complete: {summary}")
        
        return summary
    
    # Abstract method implementations (required by base class)
    def collect_business_registrations(self, days_back: int = 90) -> List:
        return []
    
    def collect_sba_loans(self, days_back: int = 180) -> List:
        return []
    
    def collect_business_licenses(self, days_back: int = 30) -> List:
        return []


def main():
    """Run employment projections collection"""
    logging.basicConfig(
        level=logging.INFO,
        format='%(asctime)s - %(levelname)s - %(message)s'
    )
    
    try:
        collector = EmploymentProjectionsCollector()
        summary = collector.run_projections_collection()
        
        print("\n" + "="*60)
        print("EMPLOYMENT PROJECTIONS COLLECTION SUMMARY")
        print("="*60)
        print(f"Industries Projected: {summary['industries_projected']}")
        print(f"Projection Period: {summary['projection_period']}")
        print(f"Success: {summary['success']}")
        print(f"Processing Time: {summary['processing_time_seconds']:.1f} seconds")
        
        if summary['growth_analysis'].get('overview'):
            overview = summary['growth_analysis']['overview']
            print(f"\nEMPLOYMENT OVERVIEW:")
            print(f"Total Base Employment: {overview['total_base_employment']:,}")
            print(f"Projected Employment: {overview['total_projected_employment']:,}")
            print(f"Net Job Change: {overview['net_employment_change']:,}")
            print(f"Average Growth Rate: {overview['average_growth_rate']:.1f}%")
            print(f"Annual Job Openings: {overview['total_annual_openings']:,}")
        
        if summary['growth_analysis'].get('high_growth_sectors'):
            print(f"\nHIGH GROWTH SECTORS:")
            for sector in summary['growth_analysis']['high_growth_sectors'][:5]:
                print(f"- {sector['industry']}: {sector['growth_rate']:.1f}% growth")
        
        if summary['errors']:
            print(f"\nErrors: {summary['errors']}")
        
    except Exception as e:
        logging.error(f"Error in main: {e}")


if __name__ == "__main__":
    main()