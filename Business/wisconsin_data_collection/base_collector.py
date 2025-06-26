"""
Base Data Collector Architecture
===============================

Abstract base classes for state-specific data collection.
Provides standardized interface and common functionality.
"""

import abc
import logging
import yaml
import requests
import time
from datetime import datetime, timedelta
from typing import List, Dict, Optional, Any, Union
from pathlib import Path
from tenacity import retry, stop_after_attempt, wait_exponential
# Optional BigQuery support - import only if available
try:
    from google.cloud import bigquery
    import pandas as pd
    BIGQUERY_AVAILABLE = True
except ImportError:
    BIGQUERY_AVAILABLE = False
    bigquery = None
    pd = None

from models import (
    BusinessEntity, SBALoanRecord, BusinessLicense, 
    DataCollectionSummary, BusinessType, BusinessStatus, DataSource
)


class DataCollectionError(Exception):
    """Custom exception for data collection errors"""
    pass


class DataValidationError(Exception):
    """Custom exception for data validation errors"""
    pass


class BaseDataCollector(abc.ABC):
    """
    Abstract base class for state-specific data collectors
    
    Provides common functionality and standardized interface
    for collecting business data from various sources
    """
    
    def __init__(self, state_code: str, config_path: str = "data_sources.yaml"):
        """
        Initialize base data collector
        
        Args:
            state_code: Two-letter state code (e.g., 'WI', 'IL')
            config_path: Path to data sources configuration file
        """
        self.state_code = state_code.upper()
        self.config = self._load_config(config_path)
        
        # Handle state code mapping (WI -> wisconsin)
        state_mapping = {
            'WI': 'wisconsin',
            'IL': 'illinois'
        }
        config_key = state_mapping.get(state_code.upper(), state_code.lower())
        self.state_config = self.config['states'].get(config_key, {})
        
        if not self.state_config:
            raise ValueError(f"No configuration found for state: {state_code}")
        
        # Set up logging
        self.logger = logging.getLogger(f"{self.__class__.__name__}_{self.state_code}")
        self.logger.setLevel(logging.INFO)
        
        # Set up HTTP session with retry logic
        self.session = requests.Session()
        self.session.headers.update({
            'User-Agent': 'LocationOptimizer/2.0 Business Research Tool'
        })
        
        # Initialize BigQuery client (optional for testing)
        self.bq_config = self.config.get('bigquery', {})
        self.bq_client = None
        try:
            if BIGQUERY_AVAILABLE:
                self.bq_client = bigquery.Client(project=self.bq_config.get('project_id'))
        except Exception as e:
            self.logger.warning(f"BigQuery client not available: {e}")
        
        # Data collection summary
        self.collection_summary = DataCollectionSummary(state=self.state_code)
        
    def _load_config(self, config_path: str) -> Dict:
        """Load configuration from YAML file"""
        try:
            with open(config_path, 'r') as file:
                return yaml.safe_load(file)
        except FileNotFoundError:
            raise FileNotFoundError(f"Configuration file not found: {config_path}")
        except yaml.YAMLError as e:
            raise ValueError(f"Invalid YAML configuration: {e}")
    
    @retry(stop=stop_after_attempt(3), wait=wait_exponential(multiplier=1, min=4, max=10))
    def _make_request(self, url: str, **kwargs) -> requests.Response:
        """
        Make HTTP request with retry logic
        
        Args:
            url: URL to request
            **kwargs: Additional arguments for requests
            
        Returns:
            Response object
        """
        try:
            response = self.session.get(url, timeout=30, **kwargs)
            response.raise_for_status()
            return response
        except requests.exceptions.RequestException as e:
            self.logger.error(f"Request failed for {url}: {e}")
            raise DataCollectionError(f"HTTP request failed: {e}")
    
    def validate_business_entity(self, entity: BusinessEntity) -> bool:
        """
        Validate business entity data
        
        Args:
            entity: BusinessEntity to validate
            
        Returns:
            True if valid, False otherwise
        """
        required_fields = self.config.get('data_quality', {}).get('required_fields', {}).get('business_entity', [])
        
        for field in required_fields:
            if not getattr(entity, field, None):
                self.logger.warning(f"Missing required field '{field}' for business {entity.business_id}")
                return False
                
        return True
    
    def calculate_confidence_score(self, entity: BusinessEntity) -> float:
        """
        Calculate data quality confidence score
        
        Args:
            entity: BusinessEntity to score
            
        Returns:
            Confidence score (0-100)
        """
        score = 50  # Base score
        
        # Check for complete contact information
        if entity.phone and entity.email:
            score += 20
        elif entity.phone or entity.email:
            score += 10
            
        # Check for complete address
        if entity.address_full and entity.zip_code:
            score += 15
        elif entity.address_full or entity.zip_code:
            score += 7
            
        # Check for business details
        if entity.naics_code:
            score += 10
        if entity.business_description:
            score += 5
            
        return min(score, 100)
    
    def standardize_business_type(self, raw_business_type: str) -> BusinessType:
        """
        Standardize business type classification
        
        Args:
            raw_business_type: Raw business type string
            
        Returns:
            Standardized BusinessType enum
        """
        if not raw_business_type:
            return BusinessType.OTHER
            
        business_type_lower = raw_business_type.lower()
        
        # Restaurant/Food Service
        if any(keyword in business_type_lower for keyword in ['restaurant', 'food', 'cafe', 'bar', 'grill']):
            return BusinessType.RESTAURANT
            
        # Retail
        if any(keyword in business_type_lower for keyword in ['retail', 'store', 'shop', 'market']):
            return BusinessType.RETAIL
            
        # Professional Services
        if any(keyword in business_type_lower for keyword in ['professional', 'consulting', 'legal', 'accounting']):
            return BusinessType.PROFESSIONAL_SERVICES
            
        # Personal Services
        if any(keyword in business_type_lower for keyword in ['salon', 'spa', 'cleaning', 'repair']):
            return BusinessType.PERSONAL_SERVICES
            
        # Healthcare
        if any(keyword in business_type_lower for keyword in ['medical', 'dental', 'health', 'clinic']):
            return BusinessType.HEALTHCARE
            
        # Automotive
        if any(keyword in business_type_lower for keyword in ['auto', 'car', 'vehicle', 'mechanic']):
            return BusinessType.AUTOMOTIVE
            
        # Fitness
        if any(keyword in business_type_lower for keyword in ['gym', 'fitness', 'yoga', 'martial']):
            return BusinessType.FITNESS
            
        # Franchise
        if 'franchise' in business_type_lower:
            return BusinessType.FRANCHISE
            
        return BusinessType.OTHER
    
    def save_to_bigquery(self, 
                        businesses: List[BusinessEntity] = None,
                        sba_loans: List[SBALoanRecord] = None,
                        licenses: List[BusinessLicense] = None) -> bool:
        """
        Save collected data to BigQuery
        
        Args:
            businesses: List of business entities
            sba_loans: List of SBA loan records
            licenses: List of business licenses
            
        Returns:
            True if successful, False otherwise
        """
        try:
            success = True
            
            # Save businesses
            if businesses:
                success &= self._save_businesses_to_bq(businesses)
                
            # Save SBA loans
            if sba_loans:
                success &= self._save_sba_loans_to_bq(sba_loans)
                
            # Save licenses
            if licenses:
                success &= self._save_licenses_to_bq(licenses)
                
            return success
            
        except Exception as e:
            self.logger.error(f"Error saving to BigQuery: {e}")
            return False
    
    def _save_businesses_to_bq(self, businesses: List[BusinessEntity]) -> bool:
        """Save business entities to BigQuery"""
        try:
            # Convert to DataFrame
            data = [business.dict() for business in businesses]
            df = pd.DataFrame(data)
            
            # Ensure proper data types
            if 'registration_date' in df.columns:
                df['registration_date'] = pd.to_datetime(df['registration_date']).dt.date
            if 'data_extraction_date' in df.columns:
                df['data_extraction_date'] = pd.to_datetime(df['data_extraction_date'])
            
            # Load to BigQuery
            dataset_id = self.bq_config.get('datasets', {}).get('raw_data', 'raw_business_data')
            table_id = self.bq_config.get('tables', {}).get('business_entities', 'business_entities')
            full_table_id = f"{self.bq_config['project_id']}.{dataset_id}.{table_id}"
            
            job_config = bigquery.LoadJobConfig(
                write_disposition="WRITE_APPEND",
                time_partitioning=bigquery.TimePartitioning(
                    type_=bigquery.TimePartitioningType.DAY,
                    field="data_extraction_date"
                )
            )
            
            job = self.bq_client.load_table_from_dataframe(df, full_table_id, job_config=job_config)
            job.result()  # Wait for completion
            
            self.logger.info(f"Loaded {len(df)} businesses to BigQuery")
            return True
            
        except Exception as e:
            self.logger.error(f"Error saving businesses to BigQuery: {e}")
            return False
    
    def _save_sba_loans_to_bq(self, sba_loans: List[SBALoanRecord]) -> bool:
        """Save SBA loans to BigQuery"""
        try:
            # Convert to DataFrame
            data = [loan.dict() for loan in sba_loans]
            df = pd.DataFrame(data)
            
            # Ensure proper data types
            if 'approval_date' in df.columns:
                df['approval_date'] = pd.to_datetime(df['approval_date']).dt.date
            if 'data_extraction_date' in df.columns:
                df['data_extraction_date'] = pd.to_datetime(df['data_extraction_date'])
            
            # Load to BigQuery
            dataset_id = self.bq_config.get('datasets', {}).get('raw_data', 'raw_business_data')
            table_id = self.bq_config.get('tables', {}).get('sba_loans', 'sba_loan_approvals')
            full_table_id = f"{self.bq_config['project_id']}.{dataset_id}.{table_id}"
            
            job_config = bigquery.LoadJobConfig(write_disposition="WRITE_APPEND")
            job = self.bq_client.load_table_from_dataframe(df, full_table_id, job_config=job_config)
            job.result()
            
            self.logger.info(f"Loaded {len(df)} SBA loans to BigQuery")
            return True
            
        except Exception as e:
            self.logger.error(f"Error saving SBA loans to BigQuery: {e}")
            return False
    
    def _save_licenses_to_bq(self, licenses: List[BusinessLicense]) -> bool:
        """Save business licenses to BigQuery"""
        try:
            # Convert to DataFrame
            data = [license.dict() for license in licenses]
            df = pd.DataFrame(data)
            
            # Ensure proper data types
            if 'issue_date' in df.columns:
                df['issue_date'] = pd.to_datetime(df['issue_date']).dt.date
            if 'expiration_date' in df.columns:
                df['expiration_date'] = pd.to_datetime(df['expiration_date']).dt.date
            if 'data_extraction_date' in df.columns:
                df['data_extraction_date'] = pd.to_datetime(df['data_extraction_date'])
            
            # Load to BigQuery
            dataset_id = self.bq_config.get('datasets', {}).get('raw_data', 'raw_business_data')
            table_id = self.bq_config.get('tables', {}).get('business_licenses', 'business_licenses')
            full_table_id = f"{self.bq_config['project_id']}.{dataset_id}.{table_id}"
            
            job_config = bigquery.LoadJobConfig(write_disposition="WRITE_APPEND")
            job = self.bq_client.load_table_from_dataframe(df, full_table_id, job_config=job_config)
            job.result()
            
            self.logger.info(f"Loaded {len(df)} licenses to BigQuery")
            return True
            
        except Exception as e:
            self.logger.error(f"Error saving licenses to BigQuery: {e}")
            return False
    
    # Abstract methods that must be implemented by subclasses
    @abc.abstractmethod
    def collect_business_registrations(self, days_back: int = 90) -> List[BusinessEntity]:
        """
        Collect business registration data
        
        Args:
            days_back: Number of days to look back
            
        Returns:
            List of BusinessEntity objects
        """
        pass
    
    @abc.abstractmethod
    def collect_sba_loans(self, days_back: int = 180) -> List[SBALoanRecord]:
        """
        Collect SBA loan approval data
        
        Args:
            days_back: Number of days to look back
            
        Returns:
            List of SBALoanRecord objects
        """
        pass
    
    @abc.abstractmethod
    def collect_business_licenses(self, days_back: int = 30) -> List[BusinessLicense]:
        """
        Collect business license data
        
        Args:
            days_back: Number of days to look back
            
        Returns:
            List of BusinessLicense objects
        """
        pass
    
    def run_full_collection(self, days_back: int = 90) -> DataCollectionSummary:
        """
        Run complete data collection process
        
        Args:
            days_back: Number of days to look back
            
        Returns:
            DataCollectionSummary with results
        """
        start_time = time.time()
        self.logger.info(f"Starting full data collection for {self.state_code}")
        
        try:
            # Collect all data sources
            businesses = self.collect_business_registrations(days_back)
            sba_loans = self.collect_sba_loans(days_back)
            licenses = self.collect_business_licenses(days_back)
            
            # Update summary
            self.collection_summary.businesses_collected = len(businesses)
            self.collection_summary.sba_loans_collected = len(sba_loans)
            self.collection_summary.licenses_collected = len(licenses)
            self.collection_summary.calculate_totals()
            
            # Save to BigQuery
            success = self.save_to_bigquery(businesses, sba_loans, licenses)
            self.collection_summary.success = success
            
            # Calculate processing time
            self.collection_summary.processing_time_seconds = time.time() - start_time
            
            self.logger.info(f"Collection complete: {self.collection_summary.dict()}")
            
        except Exception as e:
            self.logger.error(f"Error in full collection: {e}")
            self.collection_summary.success = False
            self.collection_summary.errors_encountered += 1
            
        return self.collection_summary