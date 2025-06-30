"""
Census Economic Census Data Collector
=====================================

Collects industry revenue benchmark data from the U.S. Census Economic Census
for Wisconsin industries. Provides comprehensive economic metrics including:
- Revenue by industry
- Employment by industry
- Payroll data
- Operating expenses
- Capital expenditures

Key Features:
- Integrates with Census API for Economic Census data
- Supports industry-level and geographic granularity
- Provides 5-year comprehensive data (Economic Census runs every 5 years)
- Benchmarking metrics for business intelligence
"""

import requests
import pandas as pd
import logging
import time
import json
from datetime import datetime
from typing import List, Dict, Optional, Any, Union
from pydantic import BaseModel, Field, validator

from base_collector import BaseDataCollector, DataCollectionError


class EconomicCensusRecord(BaseModel):
    """Model for Economic Census industry benchmark data"""
    
    # Industry identifiers
    record_id: str = Field(..., description="Unique record identifier")
    naics_code: str = Field(..., description="NAICS industry code")
    naics_title: str = Field(..., description="Industry description")
    naics_level: int = Field(..., description="NAICS hierarchy level (2-6 digit)")
    
    # Geographic identifiers
    geo_id: str = Field(..., description="Geographic identifier")
    geo_level: str = Field(..., description="Geographic level (state, county, metro)")
    state_fips: str = Field(..., description="State FIPS code")
    state_name: str = Field(default="Wisconsin", description="State name")
    county_fips: Optional[str] = Field(None, description="County FIPS code")
    county_name: Optional[str] = Field(None, description="County name")
    metro_area_code: Optional[str] = Field(None, description="Metropolitan area code")
    metro_area_name: Optional[str] = Field(None, description="Metropolitan area name")
    
    # Establishment counts
    establishments_total: Optional[int] = Field(None, description="Total number of establishments")
    establishments_1_4_employees: Optional[int] = Field(None, description="Establishments with 1-4 employees")
    establishments_5_9_employees: Optional[int] = Field(None, description="Establishments with 5-9 employees")
    establishments_10_19_employees: Optional[int] = Field(None, description="Establishments with 10-19 employees")
    establishments_20_49_employees: Optional[int] = Field(None, description="Establishments with 20-49 employees")
    establishments_50_99_employees: Optional[int] = Field(None, description="Establishments with 50-99 employees")
    establishments_100_plus_employees: Optional[int] = Field(None, description="Establishments with 100+ employees")
    
    # Revenue metrics (in thousands of dollars)
    revenue_total: Optional[float] = Field(None, description="Total revenue ($1000s)")
    revenue_per_establishment: Optional[float] = Field(None, description="Average revenue per establishment")
    revenue_per_employee: Optional[float] = Field(None, description="Revenue per employee")
    
    # Employment metrics
    employees_total: Optional[int] = Field(None, description="Total number of employees")
    employees_production_workers: Optional[int] = Field(None, description="Production workers")
    employees_avg_per_establishment: Optional[float] = Field(None, description="Average employees per establishment")
    
    # Payroll metrics (in thousands of dollars)
    payroll_annual_total: Optional[float] = Field(None, description="Annual payroll ($1000s)")
    payroll_q1: Optional[float] = Field(None, description="Q1 payroll ($1000s)")
    payroll_per_employee: Optional[float] = Field(None, description="Average payroll per employee")
    payroll_as_pct_of_revenue: Optional[float] = Field(None, description="Payroll as % of revenue")
    
    # Operating metrics
    operating_expenses_total: Optional[float] = Field(None, description="Total operating expenses ($1000s)")
    cost_of_materials: Optional[float] = Field(None, description="Cost of materials ($1000s)")
    value_added: Optional[float] = Field(None, description="Value added ($1000s)")
    value_of_shipments: Optional[float] = Field(None, description="Value of shipments ($1000s)")
    
    # Capital expenditures
    capital_expenditures_total: Optional[float] = Field(None, description="Total capital expenditures ($1000s)")
    capital_expenditures_buildings: Optional[float] = Field(None, description="Capital expenditures on buildings ($1000s)")
    capital_expenditures_equipment: Optional[float] = Field(None, description="Capital expenditures on equipment ($1000s)")
    
    # Inventory metrics
    inventory_beginning_year: Optional[float] = Field(None, description="Beginning of year inventory ($1000s)")
    inventory_end_year: Optional[float] = Field(None, description="End of year inventory ($1000s)")
    inventory_turnover: Optional[float] = Field(None, description="Inventory turnover ratio")
    
    # Productivity metrics
    value_added_per_employee: Optional[float] = Field(None, description="Value added per employee")
    output_per_hour: Optional[float] = Field(None, description="Output per labor hour")
    
    # Data metadata
    census_year: int = Field(..., description="Economic Census year")
    data_collection_date: datetime = Field(default_factory=datetime.now, description="Collection timestamp")
    data_quality_score: Optional[float] = Field(None, description="Data quality score (0-100)")
    data_suppression_flag: bool = Field(default=False, description="Data suppressed for confidentiality")
    
    @validator('naics_code')
    def validate_naics_code(cls, v):
        """Validate NAICS code format"""
        if v:
            cleaned = ''.join(filter(str.isdigit, v))
            if 2 <= len(cleaned) <= 6:
                return cleaned
        return v
    
    def calculate_derived_metrics(self):
        """Calculate derived metrics from raw data"""
        # Revenue per establishment
        if self.revenue_total and self.establishments_total and self.establishments_total > 0:
            self.revenue_per_establishment = self.revenue_total / self.establishments_total
        
        # Revenue per employee
        if self.revenue_total and self.employees_total and self.employees_total > 0:
            self.revenue_per_employee = self.revenue_total / self.employees_total
        
        # Average employees per establishment
        if self.employees_total and self.establishments_total and self.establishments_total > 0:
            self.employees_avg_per_establishment = self.employees_total / self.establishments_total
        
        # Payroll per employee
        if self.payroll_annual_total and self.employees_total and self.employees_total > 0:
            self.payroll_per_employee = self.payroll_annual_total / self.employees_total
        
        # Payroll as percentage of revenue
        if self.payroll_annual_total and self.revenue_total and self.revenue_total > 0:
            self.payroll_as_pct_of_revenue = (self.payroll_annual_total / self.revenue_total) * 100
        
        # Value added per employee
        if self.value_added and self.employees_total and self.employees_total > 0:
            self.value_added_per_employee = self.value_added / self.employees_total
        
        # Inventory turnover
        if self.cost_of_materials and self.inventory_beginning_year and self.inventory_end_year:
            avg_inventory = (self.inventory_beginning_year + self.inventory_end_year) / 2
            if avg_inventory > 0:
                self.inventory_turnover = self.cost_of_materials / avg_inventory
    
    def calculate_data_quality_score(self):
        """Calculate data quality score based on completeness"""
        score = 0.0
        
        # Core metrics (50 points)
        core_fields = [
            self.establishments_total, self.revenue_total, 
            self.employees_total, self.payroll_annual_total
        ]
        populated_core = sum(1 for field in core_fields if field is not None and field > 0)
        score += (populated_core / len(core_fields)) * 50
        
        # Detailed metrics (30 points)
        detailed_fields = [
            self.revenue_per_establishment, self.revenue_per_employee,
            self.payroll_per_employee, self.value_added,
            self.operating_expenses_total, self.capital_expenditures_total
        ]
        populated_detailed = sum(1 for field in detailed_fields if field is not None)
        score += (populated_detailed / len(detailed_fields)) * 30
        
        # Geographic detail (10 points)
        if self.county_fips or self.metro_area_code:
            score += 10
        
        # Not suppressed data (10 points)
        if not self.data_suppression_flag:
            score += 10
        
        self.data_quality_score = round(score, 1)
        return self.data_quality_score


class CensusEconomicCollector(BaseDataCollector):
    """
    Census Economic Census Data Collector
    
    Collects comprehensive industry economic data from the U.S. Census Bureau
    """
    
    def __init__(self, config_path: str = "data_sources.yaml"):
        super().__init__("WI", config_path)
        
        # Census API configuration
        self.api_key = "dd75feaae49ed1a1884869cf57289ceacb0962f5"  # From census_collector.py
        self.base_url = "https://api.census.gov/data"
        
        # Economic Census years (every 5 years ending in 2 and 7)
        # 2022 data not yet available, so we'll use 2017, 2012, and supplement with CBP
        self.census_years = [2017, 2012]  
        self.current_census_year = 2017  # Most recent available
        
        # County Business Patterns years for annual data (2013-2022)
        self.cbp_years = list(range(2013, 2023))  # 10 years of annual data
        
        # Wisconsin FIPS codes
        self.wisconsin_fips = "55"
        self.wisconsin_counties = self._get_wisconsin_counties()
        
        # Target industries for benchmarking (matching industry_benchmarks_collector)
        self.target_industries = {
            '72': 'Accommodation and Food Services',
            '722': 'Food Services and Drinking Places',
            '7225': 'Restaurants and Other Eating Places',
            '44-45': 'Retail Trade',
            '441': 'Motor Vehicle and Parts Dealers',
            '445': 'Food and Beverage Stores',
            '448': 'Clothing and Clothing Accessories Stores',
            '452': 'General Merchandise Stores',
            '54': 'Professional, Scientific, and Technical Services',
            '541': 'Professional, Scientific, and Technical Services',
            '56': 'Administrative and Support Services',
            '561': 'Administrative and Support Services',
            '62': 'Health Care and Social Assistance',
            '621': 'Ambulatory Health Care Services',
            '71': 'Arts, Entertainment, and Recreation',
            '713': 'Amusement, Gambling, and Recreation Industries',
            '81': 'Other Services (except Public Administration)',
            '811': 'Repair and Maintenance',
            '812': 'Personal and Laundry Services'
        }
        
        # API endpoints by program
        self.api_endpoints = {
            'business_patterns': '/cbp',  # County Business Patterns (annual)
            'economic_census': '/ecnbasic',  # Economic Census Basic Statistics
            'statistics_of_businesses': '/abscs',  # Annual Business Survey
            'nonemployer': '/nonemp',  # Nonemployer Statistics
        }
        
        # Rate limiting
        self.request_delay = 0.2  # 200ms between requests
        self.max_retries = 3
        
        self.logger.info("Census Economic Collector initialized")
    
    def _get_wisconsin_counties(self) -> Dict[str, str]:
        """Get Wisconsin county FIPS codes and names"""
        # Top Wisconsin counties by population/economic activity
        return {
            "55001": "Adams",
            "55009": "Brown",  # Green Bay
            "55015": "Calumet",
            "55017": "Chippewa",
            "55021": "Columbia",
            "55025": "Dane",  # Madison
            "55027": "Dodge",
            "55029": "Door",
            "55031": "Douglas",
            "55035": "Eau Claire",
            "55039": "Fond du Lac",
            "55045": "Green",
            "55055": "Jefferson",
            "55059": "Kenosha",
            "55061": "Kewaunee",
            "55063": "La Crosse",
            "55071": "Manitowoc",
            "55073": "Marathon",
            "55079": "Milwaukee",  # Milwaukee
            "55083": "Oconto",
            "55087": "Outagamie",  # Appleton
            "55089": "Ozaukee",
            "55095": "Polk",
            "55097": "Portage",
            "55101": "Racine",
            "55105": "Rock",  # Janesville
            "55109": "St. Croix",
            "55111": "Sauk",
            "55117": "Sheboygan",
            "55131": "Washington",
            "55133": "Waukesha",
            "55135": "Waupaca",
            "55137": "Waushara",
            "55139": "Winnebago",  # Oshkosh
            "55141": "Wood"
        }
    
    def collect_economic_census_data(self, 
                                   census_year: int = None,
                                   geographic_levels: List[str] = None,
                                   naics_codes: List[str] = None) -> List[EconomicCensusRecord]:
        """
        Collect Economic Census data for Wisconsin industries
        
        Args:
            census_year: Census year (2017, 2012, etc.)
            geographic_levels: List of levels ['state', 'county', 'metro']
            naics_codes: Specific NAICS codes to collect
            
        Returns:
            List of EconomicCensusRecord objects
        """
        if census_year is None:
            census_year = self.current_census_year
        
        if geographic_levels is None:
            geographic_levels = ['state', 'county']
        
        if naics_codes is None:
            naics_codes = list(self.target_industries.keys())
        
        records = []
        
        try:
            self.logger.info(f"Starting Economic Census data collection for year {census_year}")
            
            for geo_level in geographic_levels:
                self.logger.info(f"Collecting {geo_level} level data")
                
                if geo_level == 'state':
                    geo_records = self._collect_state_level_data(census_year, naics_codes)
                elif geo_level == 'county':
                    geo_records = self._collect_county_level_data(census_year, naics_codes)
                elif geo_level == 'metro':
                    geo_records = self._collect_metro_level_data(census_year, naics_codes)
                else:
                    self.logger.warning(f"Unknown geographic level: {geo_level}")
                    continue
                
                records.extend(geo_records)
                
                # Brief pause between geographic levels
                time.sleep(1.0)
            
            self.logger.info(f"Collected {len(records)} Economic Census records")
            
        except Exception as e:
            self.logger.error(f"Error collecting Economic Census data: {e}")
            raise DataCollectionError(f"Economic Census collection failed: {e}")
        
        return records
    
    def _collect_state_level_data(self, census_year: int, naics_codes: List[str]) -> List[EconomicCensusRecord]:
        """Collect state-level Economic Census data"""
        records = []
        
        # Economic Census Basic Statistics variables
        variables = [
            'NAICS2017',  # NAICS code
            'NAICS2017_LABEL',  # Industry description
            'ESTAB',  # Number of establishments
            'EMP',  # Number of employees
            'PAYANN',  # Annual payroll ($1000)
            'RCPTOT',  # Total receipts/revenue ($1000)
            'PAYQTR1',  # Q1 payroll ($1000)
        ]
        
        for naics_code in naics_codes:
            try:
                # Make API request
                data = self._make_census_api_request(
                    program='ecnbasic',
                    year=census_year,
                    variables=variables,
                    geography='state:55',  # Wisconsin
                    naics=naics_code
                )
                
                if data and len(data) > 1:
                    for row in data[1:]:  # Skip header
                        record = self._parse_economic_census_response(
                            row, variables, 'state', census_year
                        )
                        if record:
                            records.append(record)
                
                time.sleep(self.request_delay)
                
            except Exception as e:
                self.logger.error(f"Failed to collect state data for NAICS {naics_code}: {e}")
                continue
        
        return records
    
    def _collect_county_level_data(self, census_year: int, naics_codes: List[str]) -> List[EconomicCensusRecord]:
        """Collect county-level Economic Census data"""
        records = []
        
        # Focus on major counties for efficiency
        priority_counties = [
            "079",  # Milwaukee
            "025",  # Dane
            "009",  # Brown
            "133",  # Waukesha
            "139",  # Winnebago
            "101",  # Racine
            "059",  # Kenosha
            "035",  # Eau Claire
            "063",  # La Crosse
            "105"   # Rock
        ]
        
        variables = [
            'NAICS2017',
            'NAICS2017_LABEL',
            'ESTAB',
            'EMP',
            'PAYANN',
            'RCPTOT'
        ]
        
        for county in priority_counties:
            for naics_code in naics_codes[:5]:  # Limit to top 5 industries for counties
                try:
                    data = self._make_census_api_request(
                        program='ecnbasic',
                        year=census_year,
                        variables=variables,
                        geography=f'county:{county}',
                        state='55',
                        naics=naics_code
                    )
                    
                    if data and len(data) > 1:
                        for row in data[1:]:
                            record = self._parse_economic_census_response(
                                row, variables, 'county', census_year,
                                county_fips=f"55{county}"
                            )
                            if record:
                                records.append(record)
                    
                    time.sleep(self.request_delay)
                    
                except Exception as e:
                    self.logger.warning(f"Failed to collect county {county} data for NAICS {naics_code}: {e}")
                    continue
        
        return records
    
    def _collect_metro_level_data(self, census_year: int, naics_codes: List[str]) -> List[EconomicCensusRecord]:
        """Collect metropolitan area level data"""
        records = []
        
        # Wisconsin metropolitan areas
        metro_areas = {
            "33340": "Milwaukee-Waukesha-West Allis",
            "31540": "Madison",
            "20260": "Duluth (MN-WI)",
            "48580": "Warner Robins",
            "29100": "La Crosse-Onalaska",
            "20740": "Eau Claire",
            "36780": "Oshkosh-Neenah",
            "43100": "Sheboygan",
            "39540": "Racine",
            "27500": "Janesville-Beloit",
            "12660": "Appleton"
        }
        
        # Note: Metro area data may require different API endpoint
        # This is a placeholder for metro-level collection
        self.logger.info("Metro area data collection not fully implemented in this version")
        
        return records
    
    def _make_census_api_request(self, program: str, year: int, variables: List[str],
                                geography: str, state: str = None, naics: str = None,
                                county: str = None) -> Optional[List[List[str]]]:
        """Make API request to Census Bureau"""
        
        # Build URL based on program
        if program == 'ecnbasic':
            url = f"{self.base_url}/{year}/ecnbasic"
        elif program == 'cbp':
            url = f"{self.base_url}/{year}/cbp"
        else:
            url = f"{self.base_url}/{year}/{program}"
        
        # Build parameters
        params = {
            'get': ','.join(variables),
            'for': geography,
            'key': self.api_key
        }
        
        if state:
            params['in'] = f'state:{state}'
        
        if naics:
            params['NAICS2017'] = naics
        
        if county and state:
            params['in'] = f'state:{state} county:{county}'
        
        for attempt in range(self.max_retries):
            try:
                response = requests.get(url, params=params, timeout=30)
                
                if response.status_code == 204:
                    # No data available
                    return None
                
                response.raise_for_status()
                data = response.json()
                return data
                
            except requests.exceptions.RequestException as e:
                self.logger.warning(f"API request attempt {attempt + 1} failed: {e}")
                if attempt < self.max_retries - 1:
                    time.sleep(2 ** attempt)  # Exponential backoff
                else:
                    return None
        
        return None
    
    def _parse_economic_census_response(self, row: List[str], variables: List[str],
                                       geo_level: str, census_year: int,
                                       county_fips: str = None) -> Optional[EconomicCensusRecord]:
        """Parse Economic Census API response"""
        try:
            # Create variable mapping
            var_map = {var: idx for idx, var in enumerate(variables)}
            
            # Extract basic fields
            naics_code = row[var_map.get('NAICS2017', 0)]
            naics_title = row[var_map.get('NAICS2017_LABEL', 1)]
            
            # Skip if no valid NAICS code
            if not naics_code or naics_code == 'null':
                return None
            
            # Determine NAICS level
            naics_level = len(str(naics_code).replace('-', ''))
            
            # Create record
            record = EconomicCensusRecord(
                record_id=f"ec_{census_year}_{geo_level}_{naics_code}_{county_fips or '55'}",
                naics_code=naics_code,
                naics_title=naics_title,
                naics_level=naics_level,
                geo_id=county_fips or "55",
                geo_level=geo_level,
                state_fips="55",
                census_year=census_year
            )
            
            # Set county information if available
            if county_fips:
                record.county_fips = county_fips
                record.county_name = self.wisconsin_counties.get(county_fips, "Unknown")
            
            # Parse numeric fields
            numeric_fields = {
                'ESTAB': 'establishments_total',
                'EMP': 'employees_total',
                'PAYANN': 'payroll_annual_total',
                'RCPTOT': 'revenue_total',
                'PAYQTR1': 'payroll_q1'
            }
            
            for api_field, model_field in numeric_fields.items():
                if api_field in var_map:
                    value = row[var_map[api_field]]
                    if value and value not in ['null', 'D', 'S', 'X', 'N']:
                        try:
                            setattr(record, model_field, float(value))
                        except (ValueError, TypeError):
                            pass
                    elif value in ['D', 'S']:
                        # Data suppressed for confidentiality
                        record.data_suppression_flag = True
            
            # Calculate derived metrics
            record.calculate_derived_metrics()
            record.calculate_data_quality_score()
            
            return record
            
        except Exception as e:
            self.logger.error(f"Failed to parse Economic Census response: {e}")
            return None
    
    def collect_county_business_patterns(self, year: int = None) -> List[EconomicCensusRecord]:
        """
        Collect County Business Patterns data (annual updates between Economic Census years)
        
        Args:
            year: Year of data (defaults to most recent)
            
        Returns:
            List of EconomicCensusRecord objects
        """
        if year is None:
            year = 2022  # Most recent typically available
        
        records = []
        
        # CBP provides annual establishment and employment data
        variables = [
            'NAICS2017',
            'NAICS2017_LABEL',
            'ESTAB',  # Establishments
            'EMP',  # Employment
            'PAYANN',  # Annual payroll
            'EMP_N',  # Employment noise flag
            'PAYANN_N'  # Payroll noise flag
        ]
        
        try:
            # Collect state-level CBP data
            for naics_code in list(self.target_industries.keys())[:10]:  # Top 10 industries
                data = self._make_census_api_request(
                    program='cbp',
                    year=year,
                    variables=variables,
                    geography='state:55',
                    naics=naics_code
                )
                
                if data and len(data) > 1:
                    for row in data[1:]:
                        record = self._parse_cbp_response(row, variables, year)
                        if record:
                            records.append(record)
                
                time.sleep(self.request_delay)
            
        except Exception as e:
            self.logger.error(f"Error collecting County Business Patterns data: {e}")
        
        return records
    
    def _parse_cbp_response(self, row: List[str], variables: List[str], year: int) -> Optional[EconomicCensusRecord]:
        """Parse County Business Patterns response"""
        try:
            var_map = {var: idx for idx, var in enumerate(variables)}
            
            naics_code = row[var_map.get('NAICS2017', 0)]
            if not naics_code or naics_code == 'null':
                return None
            
            record = EconomicCensusRecord(
                record_id=f"cbp_{year}_state_{naics_code}",
                naics_code=naics_code,
                naics_title=row[var_map.get('NAICS2017_LABEL', 1)],
                naics_level=len(str(naics_code)),
                geo_id="55",
                geo_level="state",
                state_fips="55",
                census_year=year
            )
            
            # Parse numeric fields
            if 'ESTAB' in var_map:
                value = row[var_map['ESTAB']]
                if value and value not in ['null', 'D', 'S']:
                    record.establishments_total = int(value)
            
            if 'EMP' in var_map:
                value = row[var_map['EMP']]
                if value and value not in ['null', 'D', 'S']:
                    record.employees_total = int(value)
            
            if 'PAYANN' in var_map:
                value = row[var_map['PAYANN']]
                if value and value not in ['null', 'D', 'S']:
                    record.payroll_annual_total = float(value)
            
            record.calculate_derived_metrics()
            record.calculate_data_quality_score()
            
            return record
            
        except Exception as e:
            self.logger.error(f"Failed to parse CBP response: {e}")
            return None
    
    def save_to_bigquery(self, records: List[EconomicCensusRecord]) -> bool:
        """Save Economic Census data to BigQuery"""
        try:
            if not self.bq_client:
                self.logger.warning("BigQuery client not available")
                return False
            
            # Convert to DataFrame
            data = [record.model_dump() for record in records]
            df = pd.DataFrame(data)
            
            if df.empty:
                self.logger.warning("No Economic Census data to save")
                return True
            
            # Ensure proper data types
            df['data_collection_date'] = pd.to_datetime(df['data_collection_date'])
            df['census_year'] = df['census_year'].astype(int)
            
            # Numeric fields
            numeric_columns = [
                'establishments_total', 'revenue_total', 'employees_total',
                'payroll_annual_total', 'revenue_per_establishment',
                'revenue_per_employee', 'payroll_per_employee'
            ]
            
            for col in numeric_columns:
                if col in df.columns:
                    df[col] = pd.to_numeric(df[col], errors='coerce')
            
            # Load to BigQuery
            dataset_id = self.bq_config.get('datasets', {}).get('raw_data', 'raw_business_data')
            table_id = 'census_economic_benchmarks'
            full_table_id = f"{self.bq_config['project_id']}.{dataset_id}.{table_id}"
            
            from google.cloud import bigquery
            
            job_config = bigquery.LoadJobConfig(
                write_disposition="WRITE_APPEND",
                time_partitioning=bigquery.TimePartitioning(
                    type_=bigquery.TimePartitioningType.DAY,
                    field="data_collection_date"
                ),
                clustering_fields=["naics_code", "geo_level", "census_year"]
            )
            
            job = self.bq_client.load_table_from_dataframe(df, full_table_id, job_config=job_config)
            job.result()
            
            self.logger.info(f"Loaded {len(df)} Economic Census records to BigQuery")
            return True
            
        except Exception as e:
            self.logger.error(f"Error saving Economic Census data to BigQuery: {e}")
            return False
    
    def run_collection(self, census_year: int = None, include_cbp: bool = True) -> Dict[str, Any]:
        """
        Run Economic Census data collection
        
        Args:
            census_year: Economic Census year (2017, 2012, etc.)
            include_cbp: Also collect County Business Patterns data
            
        Returns:
            Collection summary
        """
        start_time = time.time()
        
        summary = {
            'collection_date': datetime.now(),
            'census_year': census_year or self.current_census_year,
            'economic_census_records': 0,
            'cbp_records': 0,
            'total_records': 0,
            'industries_collected': set(),
            'success': False,
            'processing_time_seconds': 0,
            'errors': []
        }
        
        try:
            all_records = []
            
            # Collect Economic Census data
            self.logger.info("Collecting Economic Census data")
            ec_records = self.collect_economic_census_data(
                census_year=census_year,
                geographic_levels=['state', 'county']
            )
            all_records.extend(ec_records)
            summary['economic_census_records'] = len(ec_records)
            
            # Track industries
            for record in ec_records:
                summary['industries_collected'].add(record.naics_code)
            
            # Collect County Business Patterns data if requested
            if include_cbp:
                self.logger.info("Collecting County Business Patterns data")
                cbp_records = self.collect_county_business_patterns()
                all_records.extend(cbp_records)
                summary['cbp_records'] = len(cbp_records)
            
            summary['total_records'] = len(all_records)
            summary['industries_collected'] = list(summary['industries_collected'])
            
            # Save to BigQuery
            if all_records:
                success = self.save_to_bigquery(all_records)
                summary['success'] = success
            else:
                self.logger.warning("No records collected")
                summary['success'] = False
            
        except Exception as e:
            error_msg = f"Error in Economic Census collection: {e}"
            self.logger.error(error_msg)
            summary['errors'].append(error_msg)
            summary['success'] = False
        
        summary['processing_time_seconds'] = time.time() - start_time
        
        self.logger.info(f"Economic Census collection complete: {summary}")
        return summary
    
    # Required abstract method implementations
    def collect_business_registrations(self, days_back: int = 90) -> List:
        """Not applicable for Economic Census collector"""
        return []
    
    def collect_sba_loans(self, days_back: int = 180) -> List:
        """Not applicable for Economic Census collector"""
        return []
    
    def collect_business_licenses(self, days_back: int = 30) -> List:
        """Not applicable for Economic Census collector"""
        return []


def main():
    """Test the Census Economic data collector"""
    logging.basicConfig(level=logging.INFO)
    
    try:
        # Initialize collector
        collector = CensusEconomicCollector()
        
        # Run collection
        summary = collector.run_collection(
            census_year=2017,  # Most recent available
            include_cbp=True   # Include annual County Business Patterns
        )
        
        print(f"\nCollection Summary:")
        print(f"- Economic Census Records: {summary['economic_census_records']}")
        print(f"- County Business Patterns Records: {summary['cbp_records']}")
        print(f"- Total Records: {summary['total_records']}")
        print(f"- Industries Collected: {len(summary['industries_collected'])}")
        print(f"- Success: {summary['success']}")
        print(f"- Processing Time: {summary['processing_time_seconds']:.1f} seconds")
        
    except Exception as e:
        logging.error(f"Error in main: {e}")


if __name__ == "__main__":
    main()