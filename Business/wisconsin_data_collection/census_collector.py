"""
Census Data Collector
====================

Collects demographic data from the U.S. Census Bureau API for location optimization analysis.
Implements the Census API for American Community Survey (ACS) and Population Estimates data.

Key Features:
- Multi-level geographic data (county, tract, block group)
- Comprehensive demographic variables for location analysis
- Rate limiting and error handling
- Data validation and quality scoring
"""

import requests
import time
import logging
from typing import List, Dict, Optional, Any, Tuple
from datetime import datetime
import yaml
from pathlib import Path

from models import CensusGeography, CensusDataSummary
from base_collector import BaseDataCollector


class CensusDataCollector:
    """Collector for U.S. Census Bureau demographic data"""
    
    def __init__(self, config_path: str = "data_sources.yaml"):
        self.config = self._load_config(config_path)
        self.api_key = "dd75feaae49ed1a1884869cf57289ceacb0962f5"  # From business plan
        self.base_url = "https://api.census.gov/data"
        self.logger = self._setup_logging()
        
        # Rate limiting (Census API allows reasonable use)
        self.request_delay = 0.1  # 100ms between requests
        self.max_retries = 3
        
    def _load_config(self, config_path: str) -> Dict[str, Any]:
        """Load configuration from YAML file"""
        config_file = Path(config_path)
        if config_file.exists():
            with open(config_file, 'r') as f:
                return yaml.safe_load(f)
        return {}
    
    def _setup_logging(self) -> logging.Logger:
        """Setup logging for Census data collection"""
        logger = logging.getLogger('census_collector')
        logger.setLevel(logging.INFO)
        
        if not logger.handlers:
            handler = logging.FileHandler('location_optimizer.log')
            formatter = logging.Formatter(
                '%(asctime)s - %(name)s - %(levelname)s - %(message)s'
            )
            handler.setFormatter(formatter)
            logger.addHandler(handler)
            
        return logger
    
    def collect_wisconsin_demographics(self, geographic_levels: List[str] = None, 
                                     acs_year: int = 2022, 
                                     include_population_estimates: bool = True) -> CensusDataSummary:
        """
        Collect comprehensive demographic data for Wisconsin
        
        Args:
            geographic_levels: List of levels to collect ['county', 'tract', 'block_group']
            acs_year: ACS data year (default: 2022)
            include_population_estimates: Whether to include population estimates data
            
        Returns:
            CensusDataSummary with collection results
        """
        if geographic_levels is None:
            geographic_levels = ['county', 'tract']  # Start with county and tract
            
        summary = CensusDataSummary(
            state_fips="55",  # Wisconsin
            acs_year=acs_year
        )
        
        start_time = time.time()
        collected_records = []
        
        try:
            self.logger.info(f"Starting Wisconsin Census data collection for {geographic_levels}")
            
            # Collect data for each geographic level
            for geo_level in geographic_levels:
                self.logger.info(f"Collecting {geo_level} level data...")
                
                if geo_level == 'county':
                    records = self._collect_county_data(acs_year)
                    summary.counties_collected = len(records)
                elif geo_level == 'tract':
                    records = self._collect_tract_data(acs_year)
                    summary.tracts_collected = len(records)
                elif geo_level == 'block_group':
                    records = self._collect_block_group_data(acs_year)
                    summary.block_groups_collected = len(records)
                else:
                    self.logger.warning(f"Unknown geographic level: {geo_level}")
                    continue
                    
                collected_records.extend(records)
                summary.api_requests_made += len(self._get_wisconsin_counties())
                
                # Brief pause between geographic levels
                time.sleep(1)
            
            # Collect population estimates data if requested
            if include_population_estimates and 'county' in geographic_levels:
                self.logger.info("Collecting population estimates data...")
                population_estimates = self._collect_population_estimates(acs_year)
                
                # Merge population estimates with existing records
                if population_estimates:
                    collected_records = self._merge_population_estimates(collected_records, population_estimates)
                    self.logger.info(f"Merged population estimates for {len(population_estimates)} counties")
            
            # Store data in BigQuery
            if collected_records:
                self._store_census_data(collected_records)
                
            # Calculate summary statistics
            summary.calculate_totals()
            summary.complete_records = len([r for r in collected_records if r.data_quality_score and r.data_quality_score >= 80])
            summary.partial_records = len(collected_records) - summary.complete_records
            summary.avg_data_quality_score = sum(r.data_quality_score or 0 for r in collected_records) / len(collected_records) if collected_records else 0
            
            summary.success = True
            summary.processing_time_seconds = time.time() - start_time
            summary.api_key_used = True
            
            self.logger.info(f"Census collection completed successfully. "
                           f"Collected {len(collected_records)} records in {summary.processing_time_seconds:.1f}s")
            
        except Exception as e:
            self.logger.error(f"Census data collection failed: {str(e)}")
            summary.success = False
            summary.api_errors += 1
            summary.processing_time_seconds = time.time() - start_time
            
        return summary
    
    def _collect_county_data(self, acs_year: int) -> List[CensusGeography]:
        """Collect county-level demographic data for Wisconsin"""
        records = []
        counties = self._get_wisconsin_counties()
        
        # Build variables list from config
        variables = self._build_variable_list()
        
        for county_fips in counties:
            try:
                data = self._make_census_api_request(
                    acs_year=acs_year,
                    variables=variables,
                    geography=f"county:{county_fips[2:]}",  # Remove state prefix
                    state="55"
                )
                
                if data and len(data) > 1:  # Skip header row
                    record = self._parse_census_response(data[1], variables, 'county', acs_year)
                    if record:
                        records.append(record)
                
                # Rate limiting
                time.sleep(self.request_delay)
                
            except Exception as e:
                self.logger.error(f"Failed to collect county data for {county_fips}: {str(e)}")
                continue
                
        self.logger.info(f"Collected {len(records)} county records")
        return records
    
    def _collect_tract_data(self, acs_year: int) -> List[CensusGeography]:
        """Collect tract-level demographic data for Wisconsin"""
        records = []
        counties = self._get_wisconsin_counties()
        variables = self._build_variable_list()
        
        # Collect tracts for priority counties (top 10 by population)
        priority_counties = [
            "55079",  # Milwaukee
            "55025",  # Dane (Madison)
            "55009",  # Brown (Green Bay)
            "55063",  # La Crosse
            "55133",  # Waukesha
            "55139",  # Winnebago (Appleton)
            "55101",  # Racine
            "55059",  # Kenosha
            "55035",  # Eau Claire
            "55105"   # Rock (Janesville)
        ]
        
        for county_fips in priority_counties:
            try:
                data = self._make_census_api_request(
                    acs_year=acs_year,
                    variables=variables,
                    geography=f"tract:*",
                    state="55",
                    county=county_fips[2:]  # Remove state prefix
                )
                
                if data and len(data) > 1:
                    for row in data[1:]:  # Skip header
                        record = self._parse_census_response(row, variables, 'tract', acs_year)
                        if record:
                            records.append(record)
                
                # Rate limiting - longer pause for tract data
                time.sleep(0.5)
                
            except Exception as e:
                self.logger.error(f"Failed to collect tract data for county {county_fips}: {str(e)}")
                continue
                
        self.logger.info(f"Collected {len(records)} tract records")
        return records
    
    def _collect_block_group_data(self, acs_year: int) -> List[CensusGeography]:
        """Collect block group data for major metro areas only"""
        records = []
        variables = self._build_variable_list()
        
        # Only collect block groups for Milwaukee and Madison metro areas
        metro_counties = {
            "55079": "Milwaukee",
            "55025": "Dane (Madison)"
        }
        
        for county_fips, county_name in metro_counties.items():
            try:
                self.logger.info(f"Collecting block group data for {county_name}")
                
                data = self._make_census_api_request(
                    acs_year=acs_year,
                    variables=variables,
                    geography="block group:*",
                    state="55",
                    county=county_fips[2:]
                )
                
                if data and len(data) > 1:
                    for row in data[1:]:
                        record = self._parse_census_response(row, variables, 'block_group', acs_year)
                        if record:
                            records.append(record)
                
                # Longer pause for block group data
                time.sleep(1.0)
                
            except Exception as e:
                self.logger.error(f"Failed to collect block group data for {county_name}: {str(e)}")
                continue
                
        self.logger.info(f"Collected {len(records)} block group records")
        return records
    
    def _make_census_api_request(self, acs_year: int, variables: List[str], 
                                geography: str, state: str, county: str = None) -> Optional[List[List[str]]]:
        """Make API request to Census Bureau"""
        
        # Build URL based on parameters
        url = f"{self.base_url}/{acs_year}/acs/acs5"
        
        params = {
            'get': ','.join(variables),
            'for': geography,
            'in': f'state:{state}',
            'key': self.api_key
        }
        
        if county:
            params['in'] += f' county:{county}'
        
        for attempt in range(self.max_retries):
            try:
                response = requests.get(url, params=params, timeout=30)
                response.raise_for_status()
                
                data = response.json()
                return data
                
            except requests.exceptions.RequestException as e:
                self.logger.warning(f"API request attempt {attempt + 1} failed: {str(e)}")
                if attempt < self.max_retries - 1:
                    time.sleep(2 ** attempt)  # Exponential backoff
                else:
                    self.logger.error(f"All API request attempts failed for {geography}")
                    return None
                    
        return None
    
    def _build_variable_list(self) -> List[str]:
        """Build list of Census variables to collect"""
        config_vars = self.config.get('states', {}).get('wisconsin', {}).get('demographics', {}).get('census_acs', {}).get('target_variables', {})
        
        variables = []
        
        # Add all configured variables
        for category, vars_list in config_vars.items():
            for var_info in vars_list:
                variables.append(var_info['variable'])
        
        return variables
    
    def _parse_census_response(self, row: List[str], variables: List[str], 
                              geo_level: str, acs_year: int) -> Optional[CensusGeography]:
        """Parse Census API response row into CensusGeography object"""
        
        try:
            # Geographic identifiers are at the end of the row
            if geo_level == 'county':
                state_fips = row[-2]
                county_fips = f"{state_fips}{row[-1]}"
                geo_id = f"{state_fips}{row[-1]}"
                tract_code = None
                block_group = None
            elif geo_level == 'tract':
                state_fips = row[-3]
                county_fips = f"{state_fips}{row[-2]}"
                tract_code = row[-1]
                geo_id = f"{state_fips}{row[-2]}{row[-1]}"
                block_group = None
            elif geo_level == 'block_group':
                state_fips = row[-4]
                county_fips = f"{state_fips}{row[-3]}"
                tract_code = row[-2]
                block_group = row[-1]
                geo_id = f"{state_fips}{row[-3]}{row[-2]}{row[-1]}"
            else:
                return None
            
            # Parse demographic data
            record = CensusGeography(
                geo_id=geo_id,
                state_fips=state_fips,
                county_fips=county_fips,
                tract_code=tract_code,
                block_group=block_group,
                geographic_level=geo_level,
                acs_year=acs_year
            )
            
            # Map variables to values
            var_mapping = {
                'B01003_001E': 'total_population',
                'B01002_001E': 'median_age',
                'B19013_001E': 'median_household_income',
                'B23025_005E': 'unemployment_count',
                'B23025_002E': 'labor_force',
                'B15003_022E': 'bachelor_degree_count',
                'B15003_001E': 'total_education_pop',
                'B25001_001E': 'total_housing_units',
                'B25003_002E': 'owner_occupied_units',
                'B25003_001E': 'total_occupied_units',
                'B08303_001E': 'total_commuters',
                'B08303_013E': 'commute_60_plus_min',
                'B08301_010E': 'public_transport_count',
                'B08301_001E': 'total_transport_pop'
            }
            
            # Parse each variable value
            for i, variable in enumerate(variables):
                if i < len(row) - 4:  # Account for geographic columns
                    value = row[i]
                    field_name = var_mapping.get(variable)
                    
                    if field_name and value not in [None, '', '-']:
                        try:
                            # Convert to appropriate type
                            if field_name in ['median_age']:
                                setattr(record, field_name, float(value))
                            else:
                                setattr(record, field_name, int(value))
                        except (ValueError, TypeError):
                            # Handle missing or invalid data
                            pass
            
            # Calculate derived metrics and data quality score
            record.calculate_derived_metrics()
            record.calculate_data_quality_score()
            
            return record
            
        except Exception as e:
            self.logger.error(f"Failed to parse Census response: {str(e)}")
            return None
    
    def _get_wisconsin_counties(self) -> List[str]:
        """Get list of Wisconsin county FIPS codes from config"""
        return self.config.get('states', {}).get('wisconsin', {}).get('demographics', {}).get('census_acs', {}).get('wisconsin_counties', {}).get('target_fips_codes', [])
    
    def _store_census_data(self, records: List[CensusGeography]):
        """Store Census data in BigQuery"""
        try:
            from google.cloud import bigquery
            import pandas as pd
            import os
            
            # Initialize BigQuery client
            client = bigquery.Client(project="location-optimizer-1")
            
            # Convert records to dictionaries for BigQuery
            data_dicts = []
            for record in records:
                data_dict = record.dict()
                # Convert datetime to proper timestamp string for BigQuery
                if 'data_extraction_date' in data_dict and data_dict['data_extraction_date']:
                    if hasattr(data_dict['data_extraction_date'], 'isoformat'):
                        data_dict['data_extraction_date'] = data_dict['data_extraction_date'].isoformat()
                    else:
                        data_dict['data_extraction_date'] = str(data_dict['data_extraction_date'])
                data_dicts.append(data_dict)
            
            if not data_dicts:
                self.logger.warning("No Census data to store")
                return
            
            # Convert to DataFrame
            df = pd.DataFrame(data_dicts)
            
            # Clean up data types for BigQuery compatibility
            if 'data_extraction_date' in df.columns:
                df['data_extraction_date'] = pd.to_datetime(df['data_extraction_date'])
            
            # Set table reference
            table_id = "location-optimizer-1.raw_business_data.census_demographics"
            
            # Configure load job
            job_config = bigquery.LoadJobConfig(
                write_disposition=bigquery.WriteDisposition.WRITE_APPEND,
                schema_update_options=[bigquery.SchemaUpdateOption.ALLOW_FIELD_ADDITION]
            )
            
            # Load data to BigQuery
            job = client.load_table_from_dataframe(df, table_id, job_config=job_config)
            job.result()  # Wait for job to complete
            
            self.logger.info(f"Successfully stored {len(data_dicts)} Census records to BigQuery table {table_id}")
            
        except Exception as e:
            self.logger.error(f"Failed to store Census data to BigQuery: {str(e)}")
            raise
    
    def _collect_population_estimates(self, pep_year: int = 2022) -> List[Dict[str, Any]]:
        """
        Collect population estimates data from Census Population Estimates Program
        
        Args:
            pep_year: Population estimates data year
            
        Returns:
            List of population estimates dictionaries by county FIPS
        """
        population_estimates = []
        
        try:
            # Build variables list for population estimates
            pep_variables = self._build_pep_variable_list()
            
            # Make API request for Wisconsin counties
            data = self._make_census_pep_request(
                pep_year=pep_year,
                variables=pep_variables,
                geography="county:*",
                state="55"
            )
            
            if data and len(data) > 1:
                for row in data[1:]:  # Skip header
                    estimates = self._parse_pep_response(row, pep_variables, pep_year)
                    if estimates:
                        population_estimates.append(estimates)
            
            self.logger.info(f"Collected population estimates for {len(population_estimates)} counties")
            
        except Exception as e:
            self.logger.error(f"Failed to collect population estimates: {str(e)}")
            
        return population_estimates
    
    def _build_pep_variable_list(self) -> List[str]:
        """Build list of Population Estimates Program variables to collect"""
        config_vars = self.config.get('states', {}).get('wisconsin', {}).get('demographics', {}).get('population_estimates', {}).get('target_variables', {})
        
        variables = []
        
        # Add all configured PEP variables
        for category, vars_list in config_vars.items():
            for var_info in vars_list:
                variables.append(var_info['variable'])
        
        return variables
    
    def _make_census_pep_request(self, pep_year: int, variables: List[str], 
                                geography: str, state: str) -> Optional[List[List[str]]]:
        """Make API request to Census Population Estimates Program"""
        
        # Use 2019 PEP API (most recent year with full county support)
        if pep_year >= 2020:
            pep_year = 2019
            self.logger.info(f"Using 2019 PEP data (most recent available for county-level queries)")
        
        # Build URL for PEP API
        url = f"https://api.census.gov/data/{pep_year}/pep/population"
        
        params = {
            'get': ','.join(variables),
            'for': geography,
            'in': f'state:{state}',
            'key': self.api_key
        }
        
        for attempt in range(self.max_retries):
            try:
                response = requests.get(url, params=params, timeout=30)
                response.raise_for_status()
                
                data = response.json()
                return data
                
            except requests.exceptions.RequestException as e:
                self.logger.warning(f"PEP API request attempt {attempt + 1} failed: {str(e)}")
                if attempt < self.max_retries - 1:
                    time.sleep(2 ** attempt)  # Exponential backoff
                else:
                    self.logger.error(f"All PEP API request attempts failed for {geography}")
                    return None
                    
        return None
    
    def _parse_pep_response(self, row: List[str], variables: List[str], 
                           pep_year: int) -> Optional[Dict[str, Any]]:
        """Parse Population Estimates Program API response row"""
        
        try:
            # Geographic identifiers are at the end
            state_fips = row[-2]
            county_code = row[-1]
            county_fips = f"{state_fips}{county_code}"
            
            # Parse PEP data
            estimates = {
                'county_fips': county_fips,
                'pep_year': pep_year
            }
            
            # Map variables to values (using 2019 PEP variables)
            pep_var_mapping = {
                'POP': 'population_2019',
                'DENSITY': 'population_density_2019'
            }
            
            # Parse each variable value
            for i, variable in enumerate(variables):
                if i < len(row) - 2:  # Account for geographic columns
                    value = row[i]
                    field_name = pep_var_mapping.get(variable)
                    
                    if field_name and value not in [None, '', '-']:
                        try:
                            # Convert to appropriate type
                            if field_name in ['birth_rate_2022', 'death_rate_2022']:
                                estimates[field_name] = float(value)
                            else:
                                estimates[field_name] = int(value)
                        except (ValueError, TypeError):
                            # Handle missing or invalid data
                            pass
            
            return estimates
            
        except Exception as e:
            self.logger.error(f"Failed to parse PEP response: {str(e)}")
            return None
    
    def _merge_population_estimates(self, census_records: List[CensusGeography], 
                                  population_estimates: List[Dict[str, Any]]) -> List[CensusGeography]:
        """
        Merge population estimates data with existing census records
        
        Args:
            census_records: List of CensusGeography records with ACS data
            population_estimates: List of population estimates data by county
            
        Returns:
            Updated census records with population estimates merged
        """
        # Create lookup dictionary by county FIPS
        estimates_by_county = {est['county_fips']: est for est in population_estimates}
        
        updated_records = []
        
        for record in census_records:
            if record.geographic_level == 'county' and record.county_fips in estimates_by_county:
                estimates = estimates_by_county[record.county_fips]
                
                # Update record with population estimates data
                for field, value in estimates.items():
                    if hasattr(record, field) and field != 'pep_year':  # Skip pep_year as it's not in schema
                        setattr(record, field, value)
                
                # Recalculate derived metrics to include population growth rates
                record.calculate_derived_metrics()
                
            updated_records.append(record)
        
        return updated_records
    
    def get_demographic_summary(self, location: str, radius_miles: float = 5.0) -> Dict[str, Any]:
        """
        Get demographic summary for a specific location
        
        Args:
            location: Address or lat/lng
            radius_miles: Radius for demographic analysis
            
        Returns:
            Dictionary with demographic summary
        """
        # This would integrate with geocoding and spatial analysis
        # For now, return placeholder structure
        
        return {
            'location': location,
            'radius_miles': radius_miles,
            'population': {
                'total': None,
                'density': None,
                'median_age': None
            },
            'economic': {
                'median_income': None,
                'unemployment_rate': None
            },
            'education': {
                'bachelor_degree_pct': None
            },
            'housing': {
                'total_units': None,
                'owner_occupied_pct': None
            },
            'transportation': {
                'avg_commute_time': None,
                'public_transport_pct': None
            },
            'data_quality_score': None,
            'last_updated': datetime.now().isoformat()
        }
    
