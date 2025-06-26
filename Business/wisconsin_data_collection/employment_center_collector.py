#!/usr/bin/env python3
"""
Employment Center Collector - Analyzes where people work using Census LEHD data
Uses LODES (LEHD Origin-Destination Employment Statistics) to find employment clusters
"""

import requests
import pandas as pd
import numpy as np
import logging
from datetime import datetime, date
from typing import Dict, List, Tuple
import json
from google.cloud import bigquery
from google.oauth2 import service_account
from collections import defaultdict

# Configure logging
logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(name)s - %(levelname)s - %(message)s'
)
logger = logging.getLogger(__name__)

# Configuration
PROJECT_ID = "location-optimizer-1"
DATASET_ID = "location_optimizer"
CREDENTIALS_PATH = "location-optimizer-1-449414f93a5a.json"

# Census API configuration
CENSUS_API_KEY = "dd75feaae49ed1a1884869cf57289ceacb0962f5"
LEHD_BASE_URL = "https://lehd.ces.census.gov/data/lodes/LODES8"

class EmploymentCenterCollector:
    def __init__(self):
        """Initialize the employment center collector"""
        # BigQuery client
        credentials = service_account.Credentials.from_service_account_file(
            CREDENTIALS_PATH
        )
        self.bq_client = bigquery.Client(project=PROJECT_ID, credentials=credentials)
        
        # Wisconsin counties and their FIPS codes
        self.wisconsin_counties = {
            '55001': 'Adams', '55003': 'Ashland', '55005': 'Barron', '55007': 'Bayfield',
            '55009': 'Brown', '55011': 'Buffalo', '55013': 'Burnett', '55015': 'Calumet',
            '55017': 'Chippewa', '55019': 'Clark', '55021': 'Columbia', '55023': 'Crawford',
            '55025': 'Dane', '55027': 'Dodge', '55029': 'Door', '55031': 'Douglas',
            '55033': 'Dunn', '55035': 'Eau Claire', '55037': 'Florence', '55039': 'Fond du Lac',
            '55041': 'Forest', '55043': 'Grant', '55045': 'Green', '55047': 'Green Lake',
            '55049': 'Iowa', '55051': 'Iron', '55053': 'Jackson', '55055': 'Jefferson',
            '55057': 'Juneau', '55059': 'Kenosha', '55061': 'Kewaunee', '55063': 'La Crosse',
            '55065': 'Lafayette', '55067': 'Langlade', '55069': 'Lincoln', '55071': 'Manitowoc',
            '55073': 'Marathon', '55075': 'Marinette', '55077': 'Marquette', '55078': 'Menominee',
            '55079': 'Milwaukee', '55081': 'Monroe', '55083': 'Oconto', '55085': 'Oneida',
            '55087': 'Outagamie', '55089': 'Ozaukee', '55091': 'Pepin', '55093': 'Pierce',
            '55095': 'Polk', '55097': 'Portage', '55099': 'Price', '55101': 'Racine',
            '55103': 'Richland', '55105': 'Rock', '55107': 'Rusk', '55109': 'Saint Croix',
            '55111': 'Sauk', '55113': 'Sawyer', '55115': 'Shawano', '55117': 'Sheboygan',
            '55119': 'Taylor', '55121': 'Trempealeau', '55123': 'Vernon', '55125': 'Vilas',
            '55127': 'Walworth', '55129': 'Washburn', '55131': 'Washington', '55133': 'Waukesha',
            '55135': 'Waupaca', '55137': 'Waushara', '55139': 'Winnebago', '55141': 'Wood'
        }
        
        # NAICS sector mappings
        self.naics_sectors = {
            '11': 'Agriculture, Forestry, Fishing and Hunting',
            '21': 'Mining, Quarrying, and Oil and Gas Extraction', 
            '22': 'Utilities',
            '23': 'Construction',
            '31': 'Manufacturing',
            '42': 'Wholesale Trade',
            '44': 'Retail Trade',
            '48': 'Transportation and Warehousing',
            '51': 'Information',
            '52': 'Finance and Insurance',
            '53': 'Real Estate and Rental and Leasing',
            '54': 'Professional, Scientific, and Technical Services',
            '55': 'Management of Companies and Enterprises',
            '56': 'Administrative and Support and Waste Management',
            '61': 'Educational Services',
            '62': 'Health Care and Social Assistance',
            '71': 'Arts, Entertainment, and Recreation',
            '72': 'Accommodation and Food Services',
            '81': 'Other Services (except Public Administration)',
            '92': 'Public Administration'
        }
    
    def download_lodes_data(self, state: str = "wi", year: int = 2021) -> pd.DataFrame:
        """
        Download LODES Workplace Area Characteristics (WAC) data
        This shows jobs by block where people work
        """
        logger.info(f"Downloading LODES WAC data for {state.upper()} {year}")
        
        # WAC file shows characteristics of jobs by work location
        url = f"{LEHD_BASE_URL}/{state}/wac/{state}_wac_S000_JT00_{year}.csv.gz"
        
        try:
            df = pd.read_csv(url, compression='gzip', dtype={
                'w_geocode': 'str',  # Work location census block code
                'C000': 'int32',     # Total jobs
                'CA01': 'int32',     # Age 29 or younger
                'CA02': 'int32',     # Age 30-54  
                'CA03': 'int32',     # Age 55 or older
                'CE01': 'int32',     # $1250/month or less
                'CE02': 'int32',     # $1251-3333/month
                'CE03': 'int32',     # More than $3333/month
                'CNS01': 'int32',    # Agriculture, Forestry, Fishing
                'CNS02': 'int32',    # Mining
                'CNS03': 'int32',    # Utilities  
                'CNS04': 'int32',    # Construction
                'CNS05': 'int32',    # Manufacturing
                'CNS06': 'int32',    # Wholesale Trade
                'CNS07': 'int32',    # Retail Trade
                'CNS08': 'int32',    # Transportation and Warehousing
                'CNS09': 'int32',    # Information
                'CNS10': 'int32',    # Finance and Insurance
                'CNS11': 'int32',    # Real Estate
                'CNS12': 'int32',    # Professional Services
                'CNS13': 'int32',    # Management
                'CNS14': 'int32',    # Administrative Support
                'CNS15': 'int32',    # Educational Services
                'CNS16': 'int32',    # Health Care
                'CNS17': 'int32',    # Arts and Entertainment
                'CNS18': 'int32',    # Accommodation and Food
                'CNS19': 'int32',    # Other Services
                'CNS20': 'int32',    # Public Administration
            })
            
            logger.info(f"Downloaded {len(df):,} employment records")
            return df
            
        except Exception as e:
            logger.error(f"Error downloading LODES data: {e}")
            return pd.DataFrame()
    
    def get_block_locations(self, df: pd.DataFrame) -> pd.DataFrame:
        """
        Get geographic coordinates for census blocks from Census API
        """
        logger.info("Getting block locations from Census API...")
        
        # Extract unique county codes from block geocodes
        df['county_fips'] = df['w_geocode'].str[:5]
        unique_counties = df['county_fips'].unique()
        
        # Wisconsin counties only
        wi_counties = [c for c in unique_counties if c.startswith('55')]
        
        all_blocks = []
        
        for county_fips in wi_counties[:5]:  # Start with first 5 counties
            county_name = self.wisconsin_counties.get(county_fips, county_fips)
            logger.info(f"Processing {county_name} County ({county_fips})")
            
            try:
                # Get census blocks for this county
                url = "https://api.census.gov/data/2020/dec/pl"
                params = {
                    'get': 'NAME,GEO_ID',
                    'for': 'block:*',
                    'in': f'state:55 county:{county_fips[-3:]}',  # Last 3 digits
                    'key': CENSUS_API_KEY
                }
                
                response = requests.get(url, params=params, timeout=30)
                if response.status_code == 200:
                    data = response.json()
                    if len(data) > 1:
                        # Convert to DataFrame
                        block_df = pd.DataFrame(data[1:], columns=data[0])
                        block_df['county_fips'] = county_fips
                        all_blocks.append(block_df)
                        logger.info(f"  Found {len(block_df):,} blocks")
                else:
                    logger.warning(f"  No data for {county_name}: {response.status_code}")
                    
            except Exception as e:
                logger.error(f"  Error processing {county_name}: {e}")
                continue
        
        if all_blocks:
            blocks_df = pd.concat(all_blocks, ignore_index=True)
            logger.info(f"Total blocks processed: {len(blocks_df):,}")
            return blocks_df
        else:
            return pd.DataFrame()
    
    def identify_employment_clusters(self, df: pd.DataFrame, min_jobs: int = 100) -> List[Dict]:
        """
        Identify employment clusters from LODES data
        """
        logger.info(f"Identifying employment clusters with {min_jobs}+ jobs")
        
        # Filter to blocks with significant employment
        major_employers = df[df['C000'] >= min_jobs].copy()
        
        if major_employers.empty:
            logger.warning("No employment clusters found")
            return []
        
        logger.info(f"Found {len(major_employers)} high-employment census blocks")
        
        clusters = []
        
        for idx, row in major_employers.iterrows():
            # Basic cluster information
            cluster = {
                'center_id': f"WI_BLOCK_{row['w_geocode']}",
                'center_name': f"Employment Block {row['w_geocode'][-4:]}",
                'center_type': self._classify_employment_center(row),
                'center_location': f"POINT(-89.4 43.0)",  # Placeholder - would need geocoding
                'center_boundary': None,  # Would need to generate polygon
                'address': f"Census Block {row['w_geocode']}",
                'city': self._get_city_from_block(row['w_geocode']),
                'county': self.wisconsin_counties.get(row['w_geocode'][:5], 'Unknown'),
                'state': 'WI',
                
                # Employment data
                'total_jobs': int(row['C000']),
                'jobs_by_earnings': [
                    {'earnings_category': 'Low <$1250', 'job_count': int(row['CE01'])},
                    {'earnings_category': 'Med $1251-3333', 'job_count': int(row['CE02'])},
                    {'earnings_category': 'High >$3333', 'job_count': int(row['CE03'])}
                ],
                'jobs_by_sector': self._get_sector_breakdown(row),
                
                # Worker flow patterns (would need OD data)
                'worker_inflow_counties': [],
                
                # Time patterns (estimates based on sector mix)
                'peak_work_hours': self._estimate_work_hours(row),
                'lunch_break_pattern': 'Noon-1pm',
                'after_work_departure': '5pm',
                
                # Business opportunity metrics
                'lunch_market_size': int(row['C000'] * 0.6),  # 60% eat out
                'after_work_market_size': int(row['C000'] * 0.3),  # 30% shop after work
                'avg_worker_income': self._estimate_avg_income(row),
                
                # Growth indicators (would need historical data)
                'job_growth_1yr': 0.02,  # Placeholder 2%
                'job_growth_5yr': 0.08,  # Placeholder 8%
                'new_companies_1yr': max(1, int(row['C000'] / 500)),  # Estimate
                
                # Data tracking
                'data_year': 2021,
                'data_source': 'Census LEHD LODES',
                'last_updated': datetime.now().isoformat(),
            }
            
            clusters.append(cluster)
        
        # Sort by total jobs
        clusters.sort(key=lambda x: x['total_jobs'], reverse=True)
        
        logger.info(f"Created {len(clusters)} employment center profiles")
        return clusters[:50]  # Top 50 employment centers
    
    def _classify_employment_center(self, row: pd.Series) -> str:
        """Classify employment center based on sector composition"""
        # Check dominant sectors
        if row['CNS16'] > row['C000'] * 0.3:  # 30%+ healthcare
            return 'Hospital'
        elif row['CNS15'] > row['C000'] * 0.5:  # 50%+ education
            return 'University'
        elif row['CNS05'] > row['C000'] * 0.4:  # 40%+ manufacturing
            return 'Industrial'
        elif row['CNS07'] > row['C000'] * 0.3:  # 30%+ retail
            return 'Mall'
        elif (row['CNS10'] + row['CNS11'] + row['CNS12']) > row['C000'] * 0.4:  # Finance + RE + Professional
            return 'Office Park'
        else:
            return 'Downtown'
    
    def _get_sector_breakdown(self, row: pd.Series) -> List[Dict]:
        """Get sector breakdown for employment center"""
        sectors = []
        sector_mapping = {
            'CNS01': ('11', 'Agriculture, Forestry, Fishing'),
            'CNS04': ('23', 'Construction'),
            'CNS05': ('31', 'Manufacturing'),
            'CNS07': ('44', 'Retail Trade'),
            'CNS10': ('52', 'Finance and Insurance'),
            'CNS12': ('54', 'Professional Services'),
            'CNS15': ('61', 'Educational Services'),
            'CNS16': ('62', 'Health Care'),
            'CNS18': ('72', 'Accommodation and Food'),
        }
        
        for col, (naics, name) in sector_mapping.items():
            if col in row and row[col] > 0:
                sectors.append({
                    'naics_sector': naics,
                    'sector_name': name,
                    'job_count': int(row[col])
                })
        
        return sorted(sectors, key=lambda x: x['job_count'], reverse=True)[:10]
    
    def _get_city_from_block(self, geocode: str) -> str:
        """Extract city from census block (simplified)"""
        county_fips = geocode[:5]
        county_name = self.wisconsin_counties.get(county_fips, 'Unknown')
        
        # Major cities by county (simplified mapping)
        major_cities = {
            '55025': 'Madison',      # Dane County
            '55079': 'Milwaukee',    # Milwaukee County
            '55009': 'Green Bay',    # Brown County
            '55063': 'La Crosse',    # La Crosse County
            '55059': 'Kenosha',      # Kenosha County
            '55101': 'Racine',       # Racine County
            '55035': 'Eau Claire',   # Eau Claire County
            '55139': 'Appleton',     # Winnebago County
        }
        
        return major_cities.get(county_fips, county_name)
    
    def _estimate_work_hours(self, row: pd.Series) -> str:
        """Estimate work hours based on sector mix"""
        if row['CNS16'] > row['C000'] * 0.3:  # Healthcare
            return '6am-6pm (Shifts)'
        elif row['CNS07'] > row['C000'] * 0.3:  # Retail
            return '9am-9pm'
        elif row['CNS18'] > row['C000'] * 0.3:  # Food service
            return '7am-10pm'
        else:
            return '8am-5pm'
    
    def _estimate_avg_income(self, row: pd.Series) -> float:
        """Estimate average worker income from earnings categories"""
        total_jobs = row['C000']
        if total_jobs == 0:
            return 35000.0
        
        # Midpoint of each earnings category (monthly to annual)
        low_income = row['CE01'] * 625 * 12    # $625/month average
        med_income = row['CE02'] * 2292 * 12   # $2292/month average  
        high_income = row['CE03'] * 5000 * 12  # $5000/month average
        
        return (low_income + med_income + high_income) / total_jobs
    
    def save_to_bigquery(self, clusters: List[Dict]):
        """Save employment centers to BigQuery"""
        if not clusters:
            logger.warning("No clusters to save")
            return
            
        table_id = f"{PROJECT_ID}.{DATASET_ID}.employment_centers"
        
        errors = self.bq_client.insert_rows_json(table_id, clusters)
        
        if errors:
            logger.error(f"Error inserting to BigQuery: {errors}")
        else:
            logger.info(f"Successfully saved {len(clusters)} employment centers")
    
    def run_analysis(self, year: int = 2021):
        """Run complete employment center analysis"""
        logger.info(f"Starting employment center analysis for {year}")
        
        # Download LODES data
        lodes_df = self.download_lodes_data("wi", year)
        
        if lodes_df.empty:
            logger.error("Failed to download LODES data")
            return
        
        # Identify employment clusters
        clusters = self.identify_employment_clusters(lodes_df, min_jobs=50)
        
        # Save to BigQuery
        self.save_to_bigquery(clusters)
        
        # Print summary
        print(f"\n=== EMPLOYMENT CENTER ANALYSIS COMPLETE ===")
        print(f"Year: {year}")
        print(f"Total employment centers identified: {len(clusters)}")
        print(f"Total jobs analyzed: {lodes_df['C000'].sum():,}")
        
        if clusters:
            print(f"\nTop 5 Employment Centers:")
            for i, cluster in enumerate(clusters[:5]):
                print(f"  {i+1}. {cluster['center_name']} ({cluster['city']})")
                print(f"     Jobs: {cluster['total_jobs']:,}, Type: {cluster['center_type']}")

def main():
    """Run employment center collection"""
    collector = EmploymentCenterCollector()
    collector.run_analysis(2021)

if __name__ == "__main__":
    main()