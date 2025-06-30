#!/usr/bin/env python3
"""
BLS Producer Price Index (PPI) Construction Materials Collector
===============================================================

Collects construction and materials cost data from the Bureau of Labor Statistics
Producer Price Index for construction cost analysis and benchmarking.

Key Features:
- Construction materials cost tracking (lumber, steel, concrete, etc.)
- Industry-specific construction cost indexes
- Historical cost trends for site development analysis
- Integration with existing Wisconsin business intelligence data

Coverage:
- Construction materials (lumber, steel, concrete, asphalt, etc.)
- Construction services and labor costs
- Building materials by category
- Specialty construction products
"""

import requests
import time
import logging
import json
import pandas as pd
from datetime import datetime, timedelta
from typing import List, Dict, Optional, Any, Tuple
from pydantic import BaseModel, Field, validator
import yaml
from pathlib import Path

from google.cloud import bigquery


class PPIConstructionRecord(BaseModel):
    """Model for BLS Producer Price Index construction materials data"""
    
    # Series identification
    series_id: str = Field(..., description="BLS series identifier")
    series_title: str = Field(..., description="Full series title/description")
    
    # Material/category information
    material_category: str = Field(..., description="Material category (Lumber, Steel, Concrete, etc.)")
    material_subcategory: Optional[str] = Field(None, description="Specific material subcategory")
    commodity_code: Optional[str] = Field(None, description="BLS commodity code")
    industry_code: Optional[str] = Field(None, description="NAICS industry code if applicable")
    
    # Time period
    year: int = Field(..., description="Data year")
    period: str = Field(..., description="Period code (M01-M12, Q01-Q04, etc.)")
    period_name: str = Field(..., description="Period name (January, 1st Quarter, etc.)")
    month: Optional[int] = Field(None, description="Month number (1-12)")
    quarter: Optional[int] = Field(None, description="Quarter number (1-4)")
    
    # Index values
    index_value: float = Field(..., description="Producer price index value")
    base_year: str = Field(default="2012=100", description="Base year for index")
    
    # Calculated metrics
    monthly_change_pct: Optional[float] = Field(None, description="Month-over-month percentage change")
    yearly_change_pct: Optional[float] = Field(None, description="Year-over-year percentage change")
    
    # Cost implications
    cost_trend: Optional[str] = Field(None, description="Cost trend classification (Rising, Falling, Stable)")
    volatility_level: Optional[str] = Field(None, description="Price volatility (High, Medium, Low)")
    
    # Geographic scope
    geographic_scope: str = Field(default="National", description="Geographic scope of data")
    
    # Data quality and source
    data_source: str = Field(default="BLS_PPI", description="Data source identifier")
    data_extraction_date: datetime = Field(default_factory=datetime.now, description="Data collection timestamp")
    seasonally_adjusted: bool = Field(default=False, description="Whether data is seasonally adjusted")
    
    @validator('period')
    def extract_time_components(cls, v, values):
        """Extract month/quarter from period code"""
        if v.startswith('M') and len(v) == 3:
            try:
                month = int(v[1:])
                if 1 <= month <= 12:
                    values['month'] = month
            except ValueError:
                pass
        elif v.startswith('Q') and len(v) == 3:
            try:
                quarter = int(v[1:])
                if 1 <= quarter <= 4:
                    values['quarter'] = quarter
            except ValueError:
                pass
        return v
    
    def calculate_trends(self, previous_value: Optional[float] = None, 
                        previous_year_value: Optional[float] = None):
        """Calculate trend metrics"""
        if previous_value and previous_value > 0:
            self.monthly_change_pct = ((self.index_value - previous_value) / previous_value) * 100
            
        if previous_year_value and previous_year_value > 0:
            self.yearly_change_pct = ((self.index_value - previous_year_value) / previous_year_value) * 100
            
        # Classify cost trend
        if self.yearly_change_pct:
            if self.yearly_change_pct >= 5.0:
                self.cost_trend = "Rising"
            elif self.yearly_change_pct <= -2.0:
                self.cost_trend = "Falling"
            else:
                self.cost_trend = "Stable"


class BLSPPIConstructionCollector:
    """
    BLS Producer Price Index Construction Materials Collector
    
    Collects construction materials cost data from BLS PPI for cost analysis
    """
    
    def __init__(self, config_path: str = "data_sources.yaml"):
        self.config = self._load_config(config_path)
        self.api_key = "c177d400482b4df282ff74850f23a7d9"  # Same as existing BLS collector
        self.base_url = "https://api.bls.gov/publicAPI/v2"
        self.logger = self._setup_logging()
        
        # Rate limiting (500 queries per day with API key)
        self.request_delay = 0.5
        self.max_retries = 3
        
        # Construction materials PPI series definitions
        self.construction_series = self._get_construction_materials_series()
        
        self.logger.info("BLS PPI Construction Collector initialized")
    
    def _load_config(self, config_path: str) -> Dict[str, Any]:
        """Load configuration from YAML file"""
        config_file = Path(config_path)
        if config_file.exists():
            with open(config_file, 'r') as f:
                return yaml.safe_load(f)
        return {}
    
    def _setup_logging(self) -> logging.Logger:
        """Setup logging for PPI data collection"""
        logger = logging.getLogger('bls_ppi_construction')
        logger.setLevel(logging.INFO)
        
        if not logger.handlers:
            handler = logging.FileHandler('location_optimizer.log')
            formatter = logging.Formatter(
                '%(asctime)s - %(name)s - %(levelname)s - %(message)s'
            )
            handler.setFormatter(formatter)
            logger.addHandler(handler)
            
        return logger
    
    def _get_construction_materials_series(self) -> Dict[str, Dict[str, Any]]:
        """Define construction materials PPI series to collect"""
        
        return {
            # General Construction Materials Index
            'construction_materials': {
                'series_id': 'WPUSI012011',
                'title': 'Construction Materials',
                'category': 'General Construction',
                'subcategory': 'All Materials',
                'seasonally_adjusted': False
            },
            
            # Lumber and Wood Products
            'lumber_general': {
                'series_id': 'WPU081',
                'title': 'Lumber and Wood Products',
                'category': 'Lumber',
                'subcategory': 'General',
                'seasonally_adjusted': False
            },
            'lumber_softwood': {
                'series_id': 'WPU0811',
                'title': 'Softwood Lumber',
                'category': 'Lumber',
                'subcategory': 'Softwood',
                'seasonally_adjusted': False
            },
            'plywood': {
                'series_id': 'WPU08110611',
                'title': 'Plywood',
                'category': 'Lumber',
                'subcategory': 'Plywood',
                'seasonally_adjusted': False
            },
            
            # Steel and Metal Products
            'steel_mill_products': {
                'series_id': 'WPU101',
                'title': 'Steel Mill Products',
                'category': 'Steel',
                'subcategory': 'Mill Products',
                'seasonally_adjusted': False
            },
            'structural_steel': {
                'series_id': 'PCU33231233231212',
                'title': 'Fabricated Structural Steel',
                'category': 'Steel',
                'subcategory': 'Structural',
                'seasonally_adjusted': False
            },
            'steel_pipe_tube': {
                'series_id': 'WPU1017',
                'title': 'Steel Pipe and Tube',
                'category': 'Steel',
                'subcategory': 'Pipe and Tube',
                'seasonally_adjusted': False
            },
            
            # Concrete and Cement
            'ready_mix_concrete': {
                'series_id': 'PCU3273232732',
                'title': 'Ready-Mix Concrete',
                'category': 'Concrete',
                'subcategory': 'Ready-Mix',
                'seasonally_adjusted': False
            },
            'cement': {
                'series_id': 'WPU1321',
                'title': 'Cement',
                'category': 'Concrete',
                'subcategory': 'Cement',
                'seasonally_adjusted': False
            },
            'concrete_products': {
                'series_id': 'PCU3273903273904',
                'title': 'Precast Concrete Products',
                'category': 'Concrete',
                'subcategory': 'Precast',
                'seasonally_adjusted': False
            },
            
            # Roofing and Insulation
            'asphalt_roofing': {
                'series_id': 'WPU0573',
                'title': 'Asphalt Roofing',
                'category': 'Roofing',
                'subcategory': 'Asphalt',
                'seasonally_adjusted': False
            },
            'insulation_materials': {
                'series_id': 'WPU0576',
                'title': 'Insulation Materials',
                'category': 'Insulation',
                'subcategory': 'General',
                'seasonally_adjusted': False
            },
            
            # Gypsum and Wallboard
            'gypsum_products': {
                'series_id': 'WPU1322',
                'title': 'Gypsum Products',
                'category': 'Gypsum',
                'subcategory': 'General',
                'seasonally_adjusted': False
            },
            
            # Glass and Windows
            'flat_glass': {
                'series_id': 'WPU1211',
                'title': 'Flat Glass',
                'category': 'Glass',
                'subcategory': 'Flat Glass',
                'seasonally_adjusted': False
            },
            
            # Paint and Coatings
            'architectural_coatings': {
                'series_id': 'WPU0613',
                'title': 'Architectural Coatings',
                'category': 'Paint',
                'subcategory': 'Architectural',
                'seasonally_adjusted': False
            },
            
            # HVAC and Mechanical
            'heating_equipment': {
                'series_id': 'WPU1171',
                'title': 'Heating Equipment',
                'category': 'HVAC',
                'subcategory': 'Heating',
                'seasonally_adjusted': False
            },
            'air_conditioning_equipment': {
                'series_id': 'WPU1172',
                'title': 'Air Conditioning Equipment',
                'category': 'HVAC',
                'subcategory': 'Air Conditioning',
                'seasonally_adjusted': False
            },
            
            # Electrical Materials
            'electrical_equipment': {
                'series_id': 'WPU117',
                'title': 'Electrical Equipment',
                'category': 'Electrical',
                'subcategory': 'General',
                'seasonally_adjusted': False
            },
            
            # Copper and Aluminum
            'copper_brass_mill': {
                'series_id': 'WPU102',
                'title': 'Copper and Brass Mill Shapes',
                'category': 'Metals',
                'subcategory': 'Copper',
                'seasonally_adjusted': False
            },
            'aluminum_mill': {
                'series_id': 'WPU103',
                'title': 'Aluminum Mill Shapes',
                'category': 'Metals',
                'subcategory': 'Aluminum',
                'seasonally_adjusted': False
            }
        }
    
    def collect_construction_materials_data(self, start_year: int = None, 
                                           end_year: int = None) -> List[PPIConstructionRecord]:
        """
        Collect PPI construction materials data
        
        Args:
            start_year: Starting year (defaults to 10 years ago)
            end_year: Ending year (defaults to current year)
            
        Returns:
            List of PPIConstructionRecord objects
        """
        if start_year is None:
            start_year = datetime.now().year - 10
        if end_year is None:
            end_year = datetime.now().year
            
        self.logger.info(f"Collecting PPI construction data ({start_year}-{end_year})")
        
        all_records = []
        
        # Collect data for each material category
        for material_key, series_info in self.construction_series.items():
            try:
                self.logger.info(f"Collecting {series_info['title']} data...")
                
                # Make API request for this series
                series_data = self._make_bls_api_request(
                    [series_info['series_id']], 
                    start_year, 
                    end_year
                )
                
                if series_data and series_data.get('Results', {}).get('series'):
                    # Parse the response
                    for series in series_data['Results']['series']:
                        records = self._parse_ppi_series_data(
                            series, 
                            series_info,
                            material_key
                        )
                        all_records.extend(records)
                
                # Rate limiting
                time.sleep(self.request_delay)
                
            except Exception as e:
                self.logger.error(f"Error collecting {material_key} data: {e}")
                continue
        
        # Calculate trends for all records
        self._calculate_trends_for_records(all_records)
        
        self.logger.info(f"Collected {len(all_records)} PPI construction records")
        return all_records
    
    def _make_bls_api_request(self, series_ids: List[str], start_year: int, 
                             end_year: int) -> Optional[Dict[str, Any]]:
        """Make API request to BLS"""
        
        data = {
            'seriesid': series_ids,
            'startyear': str(start_year),
            'endyear': str(end_year),
            'registrationkey': self.api_key
        }
        
        for attempt in range(self.max_retries):
            try:
                response = requests.post(
                    f"{self.base_url}/timeseries/data/",
                    json=data,
                    timeout=30
                )
                response.raise_for_status()
                
                result = response.json()
                
                if result.get('status') != 'REQUEST_SUCCEEDED':
                    self.logger.warning(f"BLS API warning: {result.get('message', 'Unknown error')}")
                
                return result
                
            except requests.exceptions.RequestException as e:
                self.logger.warning(f"BLS API request attempt {attempt + 1} failed: {e}")
                if attempt < self.max_retries - 1:
                    time.sleep(2 ** attempt)
                else:
                    return None
                    
        return None
    
    def _parse_ppi_series_data(self, series_data: Dict[str, Any], 
                              series_info: Dict[str, Any],
                              material_key: str) -> List[PPIConstructionRecord]:
        """Parse PPI series data into records"""
        
        records = []
        
        for data_point in series_data.get('data', []):
            try:
                # Skip if no value
                value = data_point.get('value')
                if not value or value == '-':
                    continue
                
                index_value = float(value)
                
                record = PPIConstructionRecord(
                    series_id=series_data.get('seriesID', ''),
                    series_title=series_info['title'],
                    material_category=series_info['category'],
                    material_subcategory=series_info['subcategory'],
                    year=int(data_point.get('year', 0)),
                    period=data_point.get('period', ''),
                    period_name=data_point.get('periodName', ''),
                    index_value=index_value,
                    seasonally_adjusted=series_info['seasonally_adjusted']
                )
                
                records.append(record)
                
            except (ValueError, TypeError) as e:
                self.logger.warning(f"Error parsing data point for {material_key}: {e}")
                continue
        
        return records
    
    def _calculate_trends_for_records(self, records: List[PPIConstructionRecord]):
        """Calculate trend metrics for all records"""
        
        # Group records by series and sort by date
        series_groups = {}
        for record in records:
            key = f"{record.series_id}"
            if key not in series_groups:
                series_groups[key] = []
            series_groups[key].append(record)
        
        # Sort each series by year and period
        for series_id, series_records in series_groups.items():
            series_records.sort(key=lambda x: (x.year, x.period))
            
            # Calculate trends
            for i, record in enumerate(series_records):
                # Monthly change (previous month)
                if i > 0:
                    prev_record = series_records[i - 1]
                    record.calculate_trends(previous_value=prev_record.index_value)
                
                # Yearly change (same period previous year)
                for j in range(i):
                    other_record = series_records[j]
                    if (other_record.year == record.year - 1 and 
                        other_record.period == record.period):
                        record.calculate_trends(
                            previous_value=record.monthly_change_pct and series_records[i-1].index_value,
                            previous_year_value=other_record.index_value
                        )
                        break
    
    def get_construction_cost_summary(self, records: List[PPIConstructionRecord]) -> Dict[str, Any]:
        """Generate construction cost summary from collected data"""
        
        summary = {
            'collection_date': datetime.now(),
            'total_records': len(records),
            'materials_tracked': set(),
            'latest_year': 0,
            'cost_trends': {},
            'high_volatility_materials': [],
            'cost_increases': {},
            'cost_decreases': {}
        }
        
        # Analyze by material category
        category_data = {}
        
        for record in records:
            summary['materials_tracked'].add(record.material_category)
            summary['latest_year'] = max(summary['latest_year'], record.year)
            
            cat = record.material_category
            if cat not in category_data:
                category_data[cat] = []
            category_data[cat].append(record)
        
        # Calculate trends by category
        for category, cat_records in category_data.items():
            # Get latest records
            latest_records = [r for r in cat_records if r.year == summary['latest_year']]
            
            if latest_records:
                # Average yearly change
                yearly_changes = [r.yearly_change_pct for r in latest_records 
                                if r.yearly_change_pct is not None]
                
                if yearly_changes:
                    avg_change = sum(yearly_changes) / len(yearly_changes)
                    summary['cost_trends'][category] = {
                        'avg_yearly_change_pct': round(avg_change, 2),
                        'trend': 'Rising' if avg_change > 2 else 'Falling' if avg_change < -2 else 'Stable',
                        'latest_index': round(latest_records[-1].index_value, 1)
                    }
                    
                    # Track significant changes
                    if avg_change >= 10:
                        summary['cost_increases'][category] = round(avg_change, 1)
                    elif avg_change <= -5:
                        summary['cost_decreases'][category] = round(avg_change, 1)
        
        # Convert sets to lists for JSON serialization
        summary['materials_tracked'] = sorted(list(summary['materials_tracked']))
        
        return summary
    
    def save_to_bigquery(self, records: List[PPIConstructionRecord]) -> bool:
        """Save PPI construction data to BigQuery"""
        try:
            client = bigquery.Client(project="location-optimizer-1")
            
            # Convert to DataFrame
            data = [record.model_dump() for record in records]
            df = pd.DataFrame(data)
            
            if df.empty:
                self.logger.warning("No PPI construction data to save")
                return True
            
            # Ensure proper data types
            df['data_extraction_date'] = pd.to_datetime(df['data_extraction_date'])
            df['year'] = df['year'].astype(int)
            df['index_value'] = pd.to_numeric(df['index_value'])
            
            # Set table reference
            table_id = "location-optimizer-1.raw_business_data.bls_ppi_construction"
            
            # Configure load job
            job_config = bigquery.LoadJobConfig(
                write_disposition=bigquery.WriteDisposition.WRITE_APPEND,
                time_partitioning=bigquery.TimePartitioning(
                    type_=bigquery.TimePartitioningType.DAY,
                    field="data_extraction_date"
                ),
                clustering_fields=["material_category", "year", "series_id"]
            )
            
            # Load data to BigQuery
            job = client.load_table_from_dataframe(df, table_id, job_config=job_config)
            job.result()  # Wait for completion
            
            self.logger.info(f"Loaded {len(df)} PPI construction records to BigQuery")
            return True
            
        except Exception as e:
            self.logger.error(f"Error saving PPI construction data to BigQuery: {e}")
            return False
    
    def run_collection(self, start_year: int = None, end_year: int = None) -> Dict[str, Any]:
        """
        Run complete PPI construction materials collection
        
        Args:
            start_year: Starting year for data collection
            end_year: Ending year for data collection
            
        Returns:
            Collection summary
        """
        start_time = time.time()
        
        summary = {
            'collection_date': datetime.now(),
            'start_year': start_year or (datetime.now().year - 10),
            'end_year': end_year or datetime.now().year,
            'records_collected': 0,
            'materials_tracked': 0,
            'series_collected': 0,
            'success': False,
            'processing_time_seconds': 0,
            'cost_summary': {},
            'errors': []
        }
        
        try:
            # Collect PPI data
            records = self.collect_construction_materials_data(start_year, end_year)
            summary['records_collected'] = len(records)
            summary['series_collected'] = len(set(r.series_id for r in records))
            
            # Generate cost summary
            cost_summary = self.get_construction_cost_summary(records)
            summary['cost_summary'] = cost_summary
            summary['materials_tracked'] = len(cost_summary['materials_tracked'])
            
            # Save to BigQuery
            if records:
                success = self.save_to_bigquery(records)
                summary['success'] = success
            else:
                summary['success'] = False
                summary['errors'].append("No records collected")
            
        except Exception as e:
            error_msg = f"Error in PPI construction collection: {e}"
            self.logger.error(error_msg)
            summary['errors'].append(error_msg)
            summary['success'] = False
        
        summary['processing_time_seconds'] = time.time() - start_time
        
        self.logger.info(f"PPI construction collection complete: {summary}")
        return summary


def main():
    """Test the BLS PPI Construction collector"""
    logging.basicConfig(level=logging.INFO)
    
    try:
        collector = BLSPPIConstructionCollector()
        
        # Run collection for recent years
        summary = collector.run_collection(start_year=2020, end_year=2024)
        
        print(f"\nPPI Construction Collection Summary:")
        print(f"- Records Collected: {summary['records_collected']:,}")
        print(f"- Materials Tracked: {summary['materials_tracked']}")
        print(f"- Series Collected: {summary['series_collected']}")
        print(f"- Success: {summary['success']}")
        print(f"- Processing Time: {summary['processing_time_seconds']:.1f} seconds")
        
        if summary['cost_summary']:
            print(f"\nCost Trends:")
            for material, trend_data in summary['cost_summary']['cost_trends'].items():
                print(f"  {material}: {trend_data['avg_yearly_change_pct']:+.1f}% ({trend_data['trend']})")
        
    except Exception as e:
        logging.error(f"Error in main: {e}")


if __name__ == "__main__":
    main()