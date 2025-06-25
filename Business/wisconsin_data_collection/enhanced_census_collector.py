#!/usr/bin/env python3
"""
Enhanced Census Data Collector with Extended Population Estimates
================================================================

Extends the census collector to fetch:
- Population estimates from 2020-2023 using newer Census APIs
- County population totals and components of change
- Birth, death, and migration data
"""

import requests
import time
import logging
from typing import List, Dict, Optional, Any
from datetime import datetime

logger = logging.getLogger(__name__)


class EnhancedPopulationEstimates:
    """Collects extended population estimates data from Census Bureau"""
    
    def __init__(self, api_key: str):
        self.api_key = api_key
        self.base_url = "https://api.census.gov/data"
        
    def collect_recent_population_estimates(self, state_fips: str = "55") -> Dict[str, List[Dict[str, Any]]]:
        """
        Collect population estimates for 2020-2023 using available APIs
        
        Returns:
            Dictionary with year as key and list of county estimates as value
        """
        estimates_by_year = {}
        
        # Strategy for recent years:
        # 2020-2022: Use vintage 2022 estimates (includes 2020 census base)
        # 2023: Use vintage 2023 estimates (most recent)
        
        vintage_configs = [
            {
                'vintage': 2023,
                'years_covered': [2020, 2021, 2022, 2023],
                'endpoint': '/2023/pep/population'
            },
            {
                'vintage': 2022,
                'years_covered': [2020, 2021, 2022],
                'endpoint': '/2022/pep/population'
            }
        ]
        
        for config in vintage_configs:
            try:
                logger.info(f"Collecting vintage {config['vintage']} population estimates...")
                estimates = self._collect_vintage_estimates(
                    config['endpoint'],
                    state_fips,
                    config['vintage']
                )
                
                # Store by year
                for year in config['years_covered']:
                    if year not in estimates_by_year:
                        estimates_by_year[year] = estimates
                        
            except Exception as e:
                logger.error(f"Failed to collect vintage {config['vintage']} estimates: {str(e)}")
        
        # Also collect components of population change
        components = self._collect_population_components(state_fips)
        
        return {
            'estimates': estimates_by_year,
            'components': components
        }
    
    def _collect_vintage_estimates(self, endpoint: str, state_fips: str, vintage: int) -> List[Dict[str, Any]]:
        """Collect population estimates for a specific vintage"""
        url = f"{self.base_url}{endpoint}"
        
        # Variables to collect based on vintage
        if vintage >= 2022:
            variables = [
                'NAME',           # County name
                'POP_2020',      # 2020 Population (Census base)
                'POP_2021',      # 2021 Population estimate
                'POP_2022',      # 2022 Population estimate
                'POPESTIMATE',   # Latest estimate
                'NPOPCHG_2021',  # Population change 2020-2021
                'NPOPCHG_2022',  # Population change 2021-2022
                'BIRTHS2022',    # Births in 2022
                'DEATHS2022',    # Deaths in 2022
                'NETMIG2022',    # Net migration 2022
                'RBIRTH2022',    # Birth rate per 1000
                'RDEATH2022'     # Death rate per 1000
            ]
            
            if vintage >= 2023:
                variables.extend([
                    'POP_2023',      # 2023 Population estimate
                    'NPOPCHG_2023',  # Population change 2022-2023
                    'BIRTHS2023',    # Births in 2023
                    'DEATHS2023',    # Deaths in 2023
                    'NETMIG2023',    # Net migration 2023
                ])
        
        params = {
            'get': ','.join(variables),
            'for': 'county:*',
            'in': f'state:{state_fips}',
            'key': self.api_key
        }
        
        try:
            response = requests.get(url, params=params, timeout=30)
            response.raise_for_status()
            data = response.json()
            
            if not data or len(data) <= 1:
                return []
            
            # Parse response
            headers = data[0]
            records = []
            
            for row in data[1:]:
                record = {}
                for i, header in enumerate(headers):
                    if i < len(row):
                        # Convert numeric values
                        if header not in ['NAME', 'state', 'county']:
                            try:
                                record[header] = int(row[i]) if row[i] not in [None, ''] else None
                            except ValueError:
                                try:
                                    record[header] = float(row[i])
                                except ValueError:
                                    record[header] = row[i]
                        else:
                            record[header] = row[i]
                
                # Add metadata
                record['vintage'] = vintage
                record['county_fips'] = f"{state_fips}{record.get('county', '')}"
                
                records.append(record)
            
            logger.info(f"Collected {len(records)} county records from vintage {vintage}")
            return records
            
        except Exception as e:
            logger.error(f"API request failed: {str(e)}")
            return []
    
    def _collect_population_components(self, state_fips: str) -> Dict[str, Any]:
        """Collect detailed components of population change"""
        
        # Use vintage 2023 components endpoint for detailed data
        url = f"{self.base_url}/2023/pep/components"
        
        variables = [
            'NAME',
            'BIRTHS',
            'DEATHS',
            'NATURALCHG',    # Natural change (births - deaths)
            'INTERNATIONALMIG',
            'DOMESTICMIG',
            'NETMIG',
            'RESIDUAL'
        ]
        
        params = {
            'get': ','.join(variables),
            'for': 'county:*',
            'in': f'state:{state_fips}',
            'PERIOD_CODE': '12',  # Annual period
            'key': self.api_key
        }
        
        try:
            response = requests.get(url, params=params, timeout=30)
            response.raise_for_status()
            data = response.json()
            
            return self._parse_components_response(data)
            
        except Exception as e:
            logger.error(f"Failed to collect population components: {str(e)}")
            return {}
    
    def _parse_components_response(self, data: List[List[str]]) -> Dict[str, Any]:
        """Parse components of change response"""
        if not data or len(data) <= 1:
            return {}
        
        headers = data[0]
        components_by_county = {}
        
        for row in data[1:]:
            county_data = {}
            for i, header in enumerate(headers):
                if i < len(row):
                    county_data[header] = row[i]
            
            county_fips = f"55{county_data.get('county', '')}"
            components_by_county[county_fips] = county_data
        
        return components_by_county
    
    def format_for_bigquery(self, estimates_data: Dict[str, Any]) -> List[Dict[str, Any]]:
        """Format collected estimates for BigQuery census_demographics table"""
        formatted_records = []
        
        estimates = estimates_data.get('estimates', {})
        components = estimates_data.get('components', {})
        
        # Create records for each county with all available years
        county_records = {}
        
        for year, year_estimates in estimates.items():
            for county_estimate in year_estimates:
                county_fips = county_estimate.get('county_fips')
                if county_fips not in county_records:
                    county_records[county_fips] = {
                        'county_fips': county_fips,
                        'state_fips': '55',
                        'geographic_level': 'county',
                        'data_extraction_date': datetime.now().isoformat()
                    }
                
                # Map fields to BigQuery schema
                record = county_records[county_fips]
                
                # Population estimates
                if 'POP_2020' in county_estimate:
                    record['population_2020'] = county_estimate.get('POP_2020')
                if 'POP_2021' in county_estimate:
                    record['population_2021'] = county_estimate.get('POP_2021')
                if 'POP_2022' in county_estimate:
                    record['population_2022'] = county_estimate.get('POP_2022')
                if 'POP_2023' in county_estimate:
                    record['population_2023'] = county_estimate.get('POP_2023')
                
                # Population changes
                if 'NPOPCHG_2021' in county_estimate:
                    record['net_population_change_2021'] = county_estimate.get('NPOPCHG_2021')
                if 'NPOPCHG_2022' in county_estimate:
                    record['net_population_change_2022'] = county_estimate.get('NPOPCHG_2022')
                if 'NPOPCHG_2023' in county_estimate:
                    record['net_population_change_2023'] = county_estimate.get('NPOPCHG_2023')
                
                # Components of change
                if 'BIRTHS2022' in county_estimate:
                    record['births_2022'] = county_estimate.get('BIRTHS2022')
                if 'DEATHS2022' in county_estimate:
                    record['deaths_2022'] = county_estimate.get('DEATHS2022')
                if 'NETMIG2022' in county_estimate:
                    record['net_migration_2022'] = county_estimate.get('NETMIG2022')
                
                # Rates
                if 'RBIRTH2022' in county_estimate:
                    record['birth_rate_2022'] = county_estimate.get('RBIRTH2022')
                if 'RDEATH2022' in county_estimate:
                    record['death_rate_2022'] = county_estimate.get('RDEATH2022')
        
        # Calculate growth rates
        for county_fips, record in county_records.items():
            # 2021-2022 growth rate
            if 'population_2021' in record and 'population_2022' in record and record['population_2021'] > 0:
                growth = ((record['population_2022'] - record['population_2021']) / record['population_2021']) * 100
                record['population_growth_rate_2022'] = round(growth, 2)
            
            # 2020-2021 growth rate
            if 'population_2020' in record and 'population_2021' in record and record['population_2020'] > 0:
                growth = ((record['population_2021'] - record['population_2020']) / record['population_2020']) * 100
                record['population_growth_rate_2021'] = round(growth, 2)
            
            # Average annual growth rate 2020-2022
            if 'population_2020' in record and 'population_2022' in record and record['population_2020'] > 0:
                years = 2
                total_growth = ((record['population_2022'] - record['population_2020']) / record['population_2020'])
                avg_annual_growth = (pow(1 + total_growth, 1/years) - 1) * 100
                record['avg_annual_growth_rate'] = round(avg_annual_growth, 2)
            
            # Natural increase
            if 'births_2022' in record and 'deaths_2022' in record:
                record['natural_increase_2022'] = record['births_2022'] - record['deaths_2022']
            
            formatted_records.append(record)
        
        return list(county_records.values())


def test_enhanced_collector():
    """Test the enhanced population estimates collector"""
    api_key = "dd75feaae49ed1a1884869cf57289ceacb0962f5"
    
    collector = EnhancedPopulationEstimates(api_key)
    
    logger.info("Testing enhanced population estimates collection...")
    
    # Collect recent estimates
    estimates_data = collector.collect_recent_population_estimates()
    
    # Format for BigQuery
    formatted_records = collector.format_for_bigquery(estimates_data)
    
    logger.info(f"Collected {len(formatted_records)} county records with population estimates")
    
    # Show sample record
    if formatted_records:
        sample = formatted_records[0]
        logger.info("Sample record:")
        for key, value in sample.items():
            if value is not None:
                logger.info(f"  {key}: {value}")
    
    return formatted_records


if __name__ == "__main__":
    logging.basicConfig(level=logging.INFO)
    test_enhanced_collector()