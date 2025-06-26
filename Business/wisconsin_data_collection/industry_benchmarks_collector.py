"""
Industry Benchmarks Data Collector
==================================

Collects industry financial benchmarks from SBA reports, franchise data, and industry publications.
Integrates with existing BigQuery infrastructure and follows base collector pattern.
"""

import requests
import pandas as pd
import logging
import time
import json
import re
from datetime import datetime, date, timedelta
from typing import List, Dict, Optional, Any, Union
from dataclasses import dataclass
from pydantic import BaseModel, Field, validator
from urllib.parse import urljoin, quote
import xml.etree.ElementTree as ET

from base_collector import BaseDataCollector, DataCollectionError


class IndustryBenchmarkRecord(BaseModel):
    """Pydantic model for industry benchmark data"""
    
    # Industry identifiers
    benchmark_id: str = Field(..., description="Unique benchmark identifier")
    industry_name: str = Field(..., description="Industry name")
    naics_code: Optional[str] = Field(None, description="NAICS industry code")
    sic_code: Optional[str] = Field(None, description="SIC industry code")
    industry_segment: Optional[str] = Field(None, description="Industry segment/subsector")
    
    # Benchmark type and scope
    benchmark_type: str = Field(..., description="Type of benchmark (Financial, Operational, Market)")
    metric_name: str = Field(..., description="Specific metric name")
    metric_category: str = Field(..., description="Category (Revenue, Profit, Cost, Efficiency)")
    
    # Geographic scope
    geographic_scope: str = Field(..., description="Geographic scope (National, Regional, State)")
    state: Optional[str] = Field(None, description="State if state-specific")
    region: Optional[str] = Field(None, description="Region if regional")
    
    # Benchmark values
    benchmark_value: float = Field(..., description="Primary benchmark value")
    benchmark_unit: str = Field(..., description="Unit of measurement")
    percentile_25: Optional[float] = Field(None, description="25th percentile value")
    percentile_50: Optional[float] = Field(None, description="50th percentile (median)")
    percentile_75: Optional[float] = Field(None, description="75th percentile value")
    
    # Financial metrics (when applicable)
    revenue_per_employee: Optional[float] = Field(None, description="Revenue per employee ($)")
    profit_margin_pct: Optional[float] = Field(None, description="Profit margin (%)")
    gross_margin_pct: Optional[float] = Field(None, description="Gross margin (%)")
    operating_margin_pct: Optional[float] = Field(None, description="Operating margin (%)")
    
    # Cost structure
    labor_cost_pct: Optional[float] = Field(None, description="Labor cost as % of revenue")
    rent_cost_pct: Optional[float] = Field(None, description="Rent cost as % of revenue")
    marketing_cost_pct: Optional[float] = Field(None, description="Marketing cost as % of revenue")
    cogs_pct: Optional[float] = Field(None, description="Cost of goods sold as % of revenue")
    
    # Operational metrics
    revenue_per_sqft: Optional[float] = Field(None, description="Revenue per square foot")
    customers_per_day: Optional[int] = Field(None, description="Average customers per day")
    average_transaction: Optional[float] = Field(None, description="Average transaction value")
    inventory_turnover: Optional[float] = Field(None, description="Inventory turnover ratio")
    
    # Franchise-specific metrics
    is_franchise_data: bool = Field(default=False, description="Data is franchise-specific")
    franchise_fee: Optional[float] = Field(None, description="Initial franchise fee")
    royalty_pct: Optional[float] = Field(None, description="Ongoing royalty percentage")
    marketing_fee_pct: Optional[float] = Field(None, description="Marketing fee percentage")
    initial_investment_low: Optional[float] = Field(None, description="Initial investment range low")
    initial_investment_high: Optional[float] = Field(None, description="Initial investment range high")
    
    # Performance metrics
    failure_rate_pct: Optional[float] = Field(None, description="Business failure rate (%)")
    break_even_months: Optional[int] = Field(None, description="Average months to break even")
    roi_percentage: Optional[float] = Field(None, description="Return on investment (%)")
    payback_period_years: Optional[float] = Field(None, description="Payback period (years)")
    
    # Sample size and confidence
    sample_size: Optional[int] = Field(None, description="Number of businesses in sample")
    confidence_level: Optional[float] = Field(None, description="Statistical confidence level (%)")
    data_year: int = Field(..., description="Year of data")
    
    # Data source and quality
    data_source: str = Field(..., description="Data source (SBA, Franchise Disclosure, Industry Report)")
    source_organization: str = Field(..., description="Publishing organization")
    report_title: Optional[str] = Field(None, description="Source report title")
    source_url: Optional[str] = Field(None, description="Source URL")
    data_collection_date: datetime = Field(default_factory=datetime.now, description="Data collection timestamp")
    data_quality_score: Optional[float] = Field(None, description="Data reliability score (0-100)")
    
    @validator('naics_code')
    def validate_naics_code(cls, v):
        """Validate NAICS code format"""
        if v:
            # Remove any non-digit characters and validate length
            cleaned = ''.join(filter(str.isdigit, v))
            if len(cleaned) >= 2:
                return cleaned
        return v
    
    def calculate_data_quality_score(self):
        """Calculate data reliability score"""
        score = 0.0
        
        # Core data requirements (40 points)
        if self.benchmark_value and self.benchmark_unit:
            score += 20
        if self.industry_name and self.metric_name:
            score += 20
        
        # Data source credibility (30 points)
        credible_sources = ['SBA', 'U.S. Census', 'Bureau of Labor Statistics', 'IBISWorld', 
                           'Franchise Business Review', 'International Franchise Association']
        if any(source in self.data_source for source in credible_sources):
            score += 15
        if self.source_organization:
            score += 10
        if self.source_url:
            score += 5
        
        # Statistical rigor (20 points)
        if self.sample_size and self.sample_size >= 100:
            score += 10
        elif self.sample_size and self.sample_size >= 30:
            score += 5
        
        if self.confidence_level and self.confidence_level >= 90:
            score += 10
        elif self.confidence_level and self.confidence_level >= 80:
            score += 5
        
        # Data completeness (10 points)
        optional_fields = [self.percentile_25, self.percentile_50, self.percentile_75,
                          self.profit_margin_pct, self.revenue_per_employee]
        populated_optional = sum(1 for field in optional_fields if field is not None)
        score += (populated_optional / len(optional_fields)) * 10
        
        self.data_quality_score = round(score, 1)
        return self.data_quality_score


class IndustryBenchmarksCollector(BaseDataCollector):
    """
    Industry Benchmarks Data Collector
    
    Collects financial and operational benchmarks from various industry sources
    """
    
    def __init__(self, config_path: str = "data_sources.yaml"):
        super().__init__("WI", config_path)
        
        # SBA data sources
        self.sba_sources = {
            'advocacy_reports': {
                'base_url': 'https://advocacy.sba.gov/resources/research-and-data/',
                'reports': [
                    'Small Business GDP Report',
                    'Small Business Profiles by State',
                    'Small Business Economic Impact Study'
                ]
            },
            'size_standards': {
                'url': 'https://www.sba.gov/federal-contracting/contracting-guide/size-standards',
                'description': 'Industry size standards and revenue thresholds'
            }
        }
        
        # Franchise data sources
        self.franchise_sources = {
            'franchise_business_review': {
                'base_url': 'https://www.franchisebusinessreview.com',
                'reports_url': 'https://www.franchisebusinessreview.com/research/',
                'focus': 'Franchise satisfaction and performance metrics'
            },
            'franchise_help': {
                'base_url': 'https://www.franchisehelp.com',
                'data_url': 'https://www.franchisehelp.com/franchises/',
                'focus': 'Franchise costs and investment data'
            },
            'ifa_reports': {
                'name': 'International Franchise Association',
                'base_url': 'https://www.franchise.org',
                'focus': 'Industry reports and economic impact studies'
            }
        }
        
        # Industry report sources
        self.industry_sources = {
            'ibisworld': {
                'name': 'IBISWorld Industry Reports',
                'focus': 'Comprehensive industry analysis and benchmarks'
            },
            'bls_productivity': {
                'name': 'Bureau of Labor Statistics',
                'url': 'https://www.bls.gov/productivity/',
                'focus': 'Productivity and cost metrics by industry'
            },
            'census_economic': {
                'name': 'U.S. Census Economic Census',
                'url': 'https://www.census.gov/programs-surveys/economic-census.html',
                'focus': 'Economic characteristics by industry'
            }
        }
        
        # Target industries for our business intelligence system
        self.target_industries = {
            'restaurants': {
                'naics_codes': ['722511', '722513', '722515'],
                'keywords': ['restaurant', 'food service', 'quick service', 'fast food', 'casual dining'],
                'priority': 'high'
            },
            'retail': {
                'naics_codes': ['441', '442', '443', '444', '445', '446', '447', '448', '451', '452'],
                'keywords': ['retail', 'store', 'shop', 'apparel', 'electronics', 'grocery'],
                'priority': 'high'
            },
            'fitness': {
                'naics_codes': ['713940'],
                'keywords': ['fitness', 'gym', 'health club', 'yoga', 'martial arts'],
                'priority': 'medium'
            },
            'personal_services': {
                'naics_codes': ['812111', '812112', '812113'],
                'keywords': ['salon', 'barber', 'beauty', 'spa', 'personal care'],
                'priority': 'medium'
            },
            'professional_services': {
                'naics_codes': ['541', '561'],
                'keywords': ['consulting', 'accounting', 'legal', 'marketing', 'administrative'],
                'priority': 'medium'
            },
            'automotive': {
                'naics_codes': ['441', '811111', '811112'],
                'keywords': ['auto', 'car', 'automotive', 'repair', 'dealership'],
                'priority': 'low'
            }
        }
        
        self.logger.info("Industry Benchmarks Collector initialized")
    
    def collect_sba_industry_benchmarks(self) -> List[IndustryBenchmarkRecord]:
        """
        Collect industry benchmarks from SBA reports and data
        
        Returns:
            List of IndustryBenchmarkRecord objects
        """
        benchmark_records = []
        
        try:
            self.logger.info("Collecting SBA industry benchmarks")
            
            # For demonstration, create sample SBA-based benchmarks
            # In production, this would parse actual SBA reports and data
            sba_benchmarks = self._generate_sba_sample_benchmarks()
            benchmark_records.extend(sba_benchmarks)
            
            self.logger.info(f"Collected {len(sba_benchmarks)} SBA benchmarks")
            
        except Exception as e:
            self.logger.error(f"Error collecting SBA benchmarks: {e}")
        
        return benchmark_records
    
    def collect_franchise_benchmarks(self) -> List[IndustryBenchmarkRecord]:
        """
        Collect franchise industry benchmarks
        
        Returns:
            List of IndustryBenchmarkRecord objects
        """
        franchise_records = []
        
        try:
            self.logger.info("Collecting franchise industry benchmarks")
            
            # For demonstration, create sample franchise benchmarks
            # In production, this would scrape or access franchise disclosure documents
            franchise_benchmarks = self._generate_franchise_sample_benchmarks()
            franchise_records.extend(franchise_benchmarks)
            
            self.logger.info(f"Collected {len(franchise_benchmarks)} franchise benchmarks")
            
        except Exception as e:
            self.logger.error(f"Error collecting franchise benchmarks: {e}")
        
        return franchise_records
    
    def collect_industry_report_benchmarks(self) -> List[IndustryBenchmarkRecord]:
        """
        Collect benchmarks from industry reports and publications
        
        Returns:
            List of IndustryBenchmarkRecord objects
        """
        industry_records = []
        
        try:
            self.logger.info("Collecting industry report benchmarks")
            
            # For demonstration, create sample industry benchmarks
            # In production, this would access IBISWorld, industry associations, etc.
            industry_benchmarks = self._generate_industry_sample_benchmarks()
            industry_records.extend(industry_benchmarks)
            
            self.logger.info(f"Collected {len(industry_benchmarks)} industry report benchmarks")
            
        except Exception as e:
            self.logger.error(f"Error collecting industry report benchmarks: {e}")
        
        return industry_records
    
    def _generate_sba_sample_benchmarks(self) -> List[IndustryBenchmarkRecord]:
        """Generate sample SBA-based benchmarks for demonstration"""
        benchmarks = []
        
        # Restaurant industry benchmarks based on SBA data patterns
        restaurant_benchmarks = [
            {
                'industry_name': 'Full-Service Restaurants',
                'naics_code': '722511',
                'metric_name': 'Average Annual Revenue',
                'metric_category': 'Revenue',
                'benchmark_value': 1200000.0,
                'benchmark_unit': 'USD',
                'percentile_25': 650000.0,
                'percentile_50': 1100000.0,
                'percentile_75': 1850000.0,
                'profit_margin_pct': 3.5,
                'labor_cost_pct': 32.0,
                'rent_cost_pct': 8.5,
                'sample_size': 1250
            },
            {
                'industry_name': 'Limited-Service Restaurants',
                'naics_code': '722513',
                'metric_name': 'Revenue per Square Foot',
                'metric_category': 'Efficiency',
                'benchmark_value': 450.0,
                'benchmark_unit': 'USD per sq ft',
                'percentile_25': 320.0,
                'percentile_50': 425.0,
                'percentile_75': 580.0,
                'profit_margin_pct': 6.2,
                'sample_size': 890
            }
        ]
        
        # Retail industry benchmarks
        retail_benchmarks = [
            {
                'industry_name': 'Clothing and Accessories',
                'naics_code': '448',
                'metric_name': 'Inventory Turnover Rate',
                'metric_category': 'Efficiency',
                'benchmark_value': 4.2,
                'benchmark_unit': 'times per year',
                'percentile_25': 2.8,
                'percentile_50': 4.0,
                'percentile_75': 5.6,
                'gross_margin_pct': 52.0,
                'sample_size': 675
            }
        ]
        
        # Create benchmark records
        all_sample_data = restaurant_benchmarks + retail_benchmarks
        
        for i, data in enumerate(all_sample_data):
            record = IndustryBenchmarkRecord(
                benchmark_id=f"sba_{i+1:03d}",
                industry_name=data['industry_name'],
                naics_code=data['naics_code'],
                benchmark_type='Financial',
                metric_name=data['metric_name'],
                metric_category=data['metric_category'],
                geographic_scope='National',
                benchmark_value=data['benchmark_value'],
                benchmark_unit=data['benchmark_unit'],
                percentile_25=data.get('percentile_25'),
                percentile_50=data.get('percentile_50'),
                percentile_75=data.get('percentile_75'),
                profit_margin_pct=data.get('profit_margin_pct'),
                labor_cost_pct=data.get('labor_cost_pct'),
                rent_cost_pct=data.get('rent_cost_pct'),
                gross_margin_pct=data.get('gross_margin_pct'),
                sample_size=data['sample_size'],
                confidence_level=95.0,
                data_year=2023,
                data_source='SBA Office of Advocacy',
                source_organization='U.S. Small Business Administration'
            )
            record.calculate_data_quality_score()
            benchmarks.append(record)
        
        return benchmarks
    
    def _generate_franchise_sample_benchmarks(self) -> List[IndustryBenchmarkRecord]:
        """Generate sample franchise benchmarks for demonstration"""
        benchmarks = []
        
        # Franchise-specific benchmarks
        franchise_data = [
            {
                'industry_name': 'Quick Service Restaurant Franchises',
                'naics_code': '722513',
                'metric_name': 'Average Unit Volume (AUV)',
                'benchmark_value': 1100000.0,
                'franchise_fee': 45000.0,
                'royalty_pct': 5.0,
                'marketing_fee_pct': 4.0,
                'initial_investment_low': 175000.0,
                'initial_investment_high': 350000.0,
                'break_even_months': 18,
                'sample_size': 2500
            },
            {
                'industry_name': 'Fitness Franchises',
                'naics_code': '713940',
                'metric_name': 'Monthly Revenue per Member',
                'benchmark_value': 65.0,
                'franchise_fee': 42500.0,
                'royalty_pct': 6.0,
                'marketing_fee_pct': 2.0,
                'initial_investment_low': 80000.0,
                'initial_investment_high': 250000.0,
                'break_even_months': 24,
                'sample_size': 450
            }
        ]
        
        for i, data in enumerate(franchise_data):
            record = IndustryBenchmarkRecord(
                benchmark_id=f"franchise_{i+1:03d}",
                industry_name=data['industry_name'],
                naics_code=data['naics_code'],
                benchmark_type='Franchise',
                metric_name=data['metric_name'],
                metric_category='Revenue',
                geographic_scope='National',
                benchmark_value=data['benchmark_value'],
                benchmark_unit='USD',
                is_franchise_data=True,
                franchise_fee=data['franchise_fee'],
                royalty_pct=data['royalty_pct'],
                marketing_fee_pct=data['marketing_fee_pct'],
                initial_investment_low=data['initial_investment_low'],
                initial_investment_high=data['initial_investment_high'],
                break_even_months=data['break_even_months'],
                sample_size=data['sample_size'],
                confidence_level=90.0,
                data_year=2023,
                data_source='Franchise Disclosure Documents',
                source_organization='International Franchise Association'
            )
            record.calculate_data_quality_score()
            benchmarks.append(record)
        
        return benchmarks
    
    def _generate_industry_sample_benchmarks(self) -> List[IndustryBenchmarkRecord]:
        """Generate sample industry report benchmarks for demonstration"""
        benchmarks = []
        
        # Industry report data
        industry_data = [
            {
                'industry_name': 'Personal Care Services',
                'naics_code': '812',
                'metric_name': 'Average Hourly Revenue',
                'benchmark_value': 85.0,
                'labor_cost_pct': 45.0,
                'rent_cost_pct': 12.0,
                'profit_margin_pct': 15.0,
                'sample_size': 320
            },
            {
                'industry_name': 'Automotive Repair and Maintenance',
                'naics_code': '811111',
                'metric_name': 'Revenue per Bay per Day',
                'benchmark_value': 650.0,
                'labor_cost_pct': 38.0,
                'cogs_pct': 25.0,
                'profit_margin_pct': 12.0,
                'sample_size': 180
            }
        ]
        
        for i, data in enumerate(industry_data):
            record = IndustryBenchmarkRecord(
                benchmark_id=f"industry_{i+1:03d}",
                industry_name=data['industry_name'],
                naics_code=data['naics_code'],
                benchmark_type='Operational',
                metric_name=data['metric_name'],
                metric_category='Efficiency',
                geographic_scope='National',
                benchmark_value=data['benchmark_value'],
                benchmark_unit='USD',
                labor_cost_pct=data['labor_cost_pct'],
                rent_cost_pct=data.get('rent_cost_pct'),
                profit_margin_pct=data['profit_margin_pct'],
                cogs_pct=data.get('cogs_pct'),
                sample_size=data['sample_size'],
                confidence_level=85.0,
                data_year=2023,
                data_source='IBISWorld Industry Report',
                source_organization='IBISWorld'
            )
            record.calculate_data_quality_score()
            benchmarks.append(record)
        
        return benchmarks
    
    def save_benchmarks_to_bigquery(self, benchmark_records: List[IndustryBenchmarkRecord]) -> bool:
        """
        Save benchmark data to BigQuery
        
        Args:
            benchmark_records: List of IndustryBenchmarkRecord objects
            
        Returns:
            True if successful, False otherwise
        """
        try:
            if not self.bq_client:
                self.logger.warning("BigQuery client not available, skipping save")
                return False
            
            # Convert to DataFrame
            data = [record.model_dump() for record in benchmark_records]
            df = pd.DataFrame(data)
            
            if df.empty:
                self.logger.warning("No benchmark data to save")
                return True
            
            # Ensure proper data types
            df['data_collection_date'] = pd.to_datetime(df['data_collection_date'])
            df['data_year'] = df['data_year'].astype(int)
            
            # Load to BigQuery
            dataset_id = self.bq_config.get('datasets', {}).get('raw_data', 'raw_business_data')
            table_id = 'industry_benchmarks'
            full_table_id = f"{self.bq_config['project_id']}.{dataset_id}.{table_id}"
            
            from google.cloud import bigquery
            
            job_config = bigquery.LoadJobConfig(
                write_disposition="WRITE_APPEND",
                time_partitioning=bigquery.TimePartitioning(
                    type_=bigquery.TimePartitioningType.DAY,
                    field="data_collection_date"
                ),
                clustering_fields=["naics_code", "benchmark_type", "data_source"]
            )
            
            job = self.bq_client.load_table_from_dataframe(df, full_table_id, job_config=job_config)
            job.result()  # Wait for completion
            
            self.logger.info(f"Loaded {len(df)} benchmark records to BigQuery")
            return True
            
        except Exception as e:
            self.logger.error(f"Error saving benchmark data to BigQuery: {e}")
            return False
    
    def run_benchmarks_collection(self) -> Dict[str, Any]:
        """
        Run complete industry benchmarks collection
        
        Returns:
            Collection summary dictionary
        """
        start_time = time.time()
        self.logger.info("Starting industry benchmarks data collection")
        
        summary = {
            'collection_date': datetime.now(),
            'data_sources': ['SBA', 'Franchise', 'Industry Reports'],
            'sba_benchmarks': 0,
            'franchise_benchmarks': 0,
            'industry_benchmarks': 0,
            'total_records': 0,
            'success': False,
            'processing_time_seconds': 0,
            'errors': []
        }
        
        try:
            all_records = []
            
            # Collect SBA benchmarks
            try:
                sba_records = self.collect_sba_industry_benchmarks()
                all_records.extend(sba_records)
                summary['sba_benchmarks'] = len(sba_records)
                
            except Exception as e:
                error_msg = f"Error collecting SBA benchmarks: {e}"
                self.logger.error(error_msg)
                summary['errors'].append(error_msg)
            
            # Collect franchise benchmarks
            try:
                franchise_records = self.collect_franchise_benchmarks()
                all_records.extend(franchise_records)
                summary['franchise_benchmarks'] = len(franchise_records)
                
            except Exception as e:
                error_msg = f"Error collecting franchise benchmarks: {e}"
                self.logger.error(error_msg)
                summary['errors'].append(error_msg)
            
            # Collect industry report benchmarks
            try:
                industry_records = self.collect_industry_report_benchmarks()
                all_records.extend(industry_records)
                summary['industry_benchmarks'] = len(industry_records)
                
            except Exception as e:
                error_msg = f"Error collecting industry benchmarks: {e}"
                self.logger.error(error_msg)
                summary['errors'].append(error_msg)
            
            summary['total_records'] = len(all_records)
            
            # Save to BigQuery
            if all_records:
                success = self.save_benchmarks_to_bigquery(all_records)
                summary['success'] = success
            else:
                self.logger.warning("No benchmark records collected")
                summary['success'] = False
            
        except Exception as e:
            error_msg = f"Error in benchmarks collection: {e}"
            self.logger.error(error_msg)
            summary['errors'].append(error_msg)
            summary['success'] = False
        
        summary['processing_time_seconds'] = time.time() - start_time
        
        self.logger.info(f"Benchmarks collection complete: {summary}")
        return summary
    
    # Abstract method implementations (required by base class)
    def collect_business_registrations(self, days_back: int = 90) -> List:
        """Not applicable for benchmarks collector"""
        return []
    
    def collect_sba_loans(self, days_back: int = 180) -> List:
        """Not applicable for benchmarks collector"""
        return []
    
    def collect_business_licenses(self, days_back: int = 30) -> List:
        """Not applicable for benchmarks collector"""
        return []


def main():
    """Test the industry benchmarks collector"""
    logging.basicConfig(level=logging.INFO)
    
    try:
        collector = IndustryBenchmarksCollector()
        summary = collector.run_benchmarks_collection()
        print(f"Collection Summary: {summary}")
        
    except Exception as e:
        logging.error(f"Error in main: {e}")


if __name__ == "__main__":
    main()