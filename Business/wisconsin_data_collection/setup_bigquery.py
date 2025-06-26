"""
BigQuery Schema Setup - Optimized Version
========================================

Creates optimized BigQuery tables with proper partitioning,
clustering, and schema design for the Location Optimizer platform.
"""

import logging
from google.cloud import bigquery
from google.cloud.exceptions import NotFound
import yaml

# Configure logging
logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)


class BigQuerySetup:
    """Handles BigQuery dataset and table creation with optimization"""
    
    def __init__(self, project_id: str = "location-optimizer-1", config_path: str = "data_sources.yaml"):
        """
        Initialize BigQuery setup
        
        Args:
            project_id: GCP project ID
            config_path: Path to configuration file
        """
        self.project_id = project_id
        
        # Initialize BigQuery client with credentials
        import os
        credentials_path = os.getenv('GOOGLE_APPLICATION_CREDENTIALS', 'location-optimizer-1-449414f93a5a.json')
        
        if os.path.exists(credentials_path):
            self.client = bigquery.Client.from_service_account_json(credentials_path, project=project_id)
            logger.info(f"Using credentials from: {credentials_path}")
        else:
            self.client = bigquery.Client(project=project_id)
            logger.info("Using default credentials")
        
        # Load configuration
        with open(config_path, 'r') as file:
            self.config = yaml.safe_load(file)
        
        self.bq_config = self.config.get('bigquery', {})
    
    def create_datasets(self):
        """Create required datasets"""
        datasets = self.bq_config.get('datasets', {})
        
        for dataset_name, dataset_id in datasets.items():
            try:
                # Create dataset
                dataset_ref = self.client.dataset(dataset_id)
                dataset = bigquery.Dataset(dataset_ref)
                dataset.location = "US"  # Use US multi-region for better performance
                dataset.description = f"Location Optimizer {dataset_name.replace('_', ' ').title()}"
                
                # Check if dataset exists
                try:
                    self.client.get_dataset(dataset_ref)
                    logger.info(f"Dataset {dataset_id} already exists")
                except NotFound:
                    dataset = self.client.create_dataset(dataset)
                    logger.info(f"Created dataset {dataset_id}")
                    
            except Exception as e:
                logger.error(f"Error creating dataset {dataset_id}: {e}")
    
    def create_business_entities_table(self):
        """Create optimized business entities table"""
        dataset_id = self.bq_config.get('datasets', {}).get('raw_data', 'raw_business_data')
        table_id = self.bq_config.get('tables', {}).get('business_entities', 'business_entities')
        full_table_id = f"{self.project_id}.{dataset_id}.{table_id}"
        
        schema = [
            # Core identifiers
            bigquery.SchemaField("business_id", "STRING", mode="REQUIRED", description="Unique business identifier"),
            bigquery.SchemaField("source_id", "STRING", mode="REQUIRED", description="Source system identifier"),
            
            # Business information
            bigquery.SchemaField("business_name", "STRING", mode="REQUIRED", description="Official business name"),
            bigquery.SchemaField("owner_name", "STRING", mode="NULLABLE", description="Primary owner name"),
            bigquery.SchemaField("business_type", "STRING", mode="REQUIRED", description="Categorized business type"),
            bigquery.SchemaField("naics_code", "STRING", mode="NULLABLE", description="NAICS industry code"),
            bigquery.SchemaField("entity_type", "STRING", mode="NULLABLE", description="Legal entity type"),
            
            # Status and dates
            bigquery.SchemaField("status", "STRING", mode="REQUIRED", description="Current business status"),
            bigquery.SchemaField("registration_date", "DATE", mode="NULLABLE", description="Business registration date"),
            bigquery.SchemaField("last_updated", "DATE", mode="NULLABLE", description="Last update from source"),
            
            # Location information
            bigquery.SchemaField("address_full", "STRING", mode="NULLABLE", description="Complete address"),
            bigquery.SchemaField("city", "STRING", mode="REQUIRED", description="Business city"),
            bigquery.SchemaField("state", "STRING", mode="REQUIRED", description="Business state"),
            bigquery.SchemaField("zip_code", "STRING", mode="NULLABLE", description="ZIP code"),
            bigquery.SchemaField("county", "STRING", mode="NULLABLE", description="County name"),
            
            # Contact information
            bigquery.SchemaField("phone", "STRING", mode="NULLABLE", description="Business phone"),
            bigquery.SchemaField("email", "STRING", mode="NULLABLE", description="Business email"),
            bigquery.SchemaField("website", "STRING", mode="NULLABLE", description="Business website"),
            
            # Additional details
            bigquery.SchemaField("business_description", "STRING", mode="NULLABLE", description="Business description"),
            bigquery.SchemaField("employee_count", "INTEGER", mode="NULLABLE", description="Number of employees"),
            bigquery.SchemaField("annual_revenue", "FLOAT", mode="NULLABLE", description="Annual revenue estimate"),
            
            # Metadata
            bigquery.SchemaField("data_source", "STRING", mode="REQUIRED", description="Original data source"),
            bigquery.SchemaField("source_url", "STRING", mode="NULLABLE", description="Source URL"),
            bigquery.SchemaField("data_extraction_date", "TIMESTAMP", mode="REQUIRED", description="Data extraction timestamp"),
            bigquery.SchemaField("confidence_score", "FLOAT", mode="NULLABLE", description="Data quality confidence score"),
            
            # System fields
            bigquery.SchemaField("created_at", "TIMESTAMP", mode="NULLABLE", description="Record creation timestamp"),
            bigquery.SchemaField("updated_at", "TIMESTAMP", mode="NULLABLE", description="Record update timestamp")
        ]
        
        # Configure table with partitioning and clustering
        table = bigquery.Table(full_table_id, schema=schema)
        
        # Partition by data extraction date (daily partitions)
        table.time_partitioning = bigquery.TimePartitioning(
            type_=bigquery.TimePartitioningType.DAY,
            field="data_extraction_date"
        )
        
        # Cluster by state, business_type, and city for optimal query performance
        table.clustering_fields = ["state", "business_type", "city"]
        
        # Set table description
        table.description = "Business entities collected from various state and local sources"
        
        try:
            table = self.client.create_table(table, exists_ok=True)
            logger.info(f"Created/verified business entities table: {full_table_id}")
        except Exception as e:
            logger.error(f"Error creating business entities table: {e}")
    
    def create_sba_loans_table(self):
        """Create optimized SBA loans table"""
        dataset_id = self.bq_config.get('datasets', {}).get('raw_data', 'raw_business_data')
        table_id = self.bq_config.get('tables', {}).get('sba_loans', 'sba_loan_approvals')
        full_table_id = f"{self.project_id}.{dataset_id}.{table_id}"
        
        schema = [
            # Loan identifiers
            bigquery.SchemaField("loan_id", "STRING", mode="REQUIRED", description="Unique loan identifier"),
            bigquery.SchemaField("borrower_name", "STRING", mode="REQUIRED", description="Borrower business name"),
            
            # Loan details
            bigquery.SchemaField("loan_amount", "NUMERIC", mode="REQUIRED", description="Approved loan amount"),
            bigquery.SchemaField("approval_date", "DATE", mode="REQUIRED", description="Loan approval date"),
            bigquery.SchemaField("program_type", "STRING", mode="REQUIRED", description="SBA program type"),
            
            # Borrower information
            bigquery.SchemaField("borrower_address", "STRING", mode="NULLABLE", description="Borrower address"),
            bigquery.SchemaField("borrower_city", "STRING", mode="REQUIRED", description="Borrower city"),
            bigquery.SchemaField("borrower_state", "STRING", mode="REQUIRED", description="Borrower state"),
            bigquery.SchemaField("borrower_zip", "STRING", mode="NULLABLE", description="Borrower ZIP code"),
            
            # Business details
            bigquery.SchemaField("naics_code", "STRING", mode="NULLABLE", description="NAICS code"),
            bigquery.SchemaField("business_type", "STRING", mode="NULLABLE", description="Business type"),
            bigquery.SchemaField("jobs_supported", "INTEGER", mode="NULLABLE", description="Jobs supported"),
            
            # Franchise information
            bigquery.SchemaField("franchise_code", "STRING", mode="NULLABLE", description="Franchise code"),
            bigquery.SchemaField("franchise_name", "STRING", mode="NULLABLE", description="Franchise name"),
            
            # Lender information
            bigquery.SchemaField("lender_name", "STRING", mode="NULLABLE", description="Lender name"),
            
            # Metadata
            bigquery.SchemaField("data_source", "STRING", mode="REQUIRED", description="Data source"),
            bigquery.SchemaField("data_extraction_date", "TIMESTAMP", mode="REQUIRED", description="Extraction timestamp"),
            
            # System fields
            bigquery.SchemaField("created_at", "TIMESTAMP", mode="NULLABLE", description="Record creation timestamp"),
            bigquery.SchemaField("updated_at", "TIMESTAMP", mode="NULLABLE", description="Record update timestamp")
        ]
        
        table = bigquery.Table(full_table_id, schema=schema)
        
        # Partition by approval date
        table.time_partitioning = bigquery.TimePartitioning(
            type_=bigquery.TimePartitioningType.DAY,
            field="approval_date"
        )
        
        # Cluster by state, approval date, and program type
        table.clustering_fields = ["borrower_state", "program_type", "franchise_name"]
        
        table.description = "SBA loan approvals data for franchise opportunity analysis"
        
        try:
            table = self.client.create_table(table, exists_ok=True)
            logger.info(f"Created/verified SBA loans table: {full_table_id}")
        except Exception as e:
            logger.error(f"Error creating SBA loans table: {e}")
    
    def create_business_licenses_table(self):
        """Create business licenses table"""
        dataset_id = self.bq_config.get('datasets', {}).get('raw_data', 'raw_business_data')
        table_id = self.bq_config.get('tables', {}).get('business_licenses', 'business_licenses')
        full_table_id = f"{self.project_id}.{dataset_id}.{table_id}"
        
        schema = [
            # License identifiers
            bigquery.SchemaField("license_id", "STRING", mode="REQUIRED", description="License identifier"),
            bigquery.SchemaField("business_name", "STRING", mode="REQUIRED", description="Licensed business name"),
            
            # License details
            bigquery.SchemaField("license_type", "STRING", mode="REQUIRED", description="Type of license"),
            bigquery.SchemaField("issue_date", "DATE", mode="NULLABLE", description="License issue date"),
            bigquery.SchemaField("expiration_date", "DATE", mode="NULLABLE", description="License expiration date"),
            bigquery.SchemaField("status", "STRING", mode="REQUIRED", description="License status"),
            
            # Location information
            bigquery.SchemaField("address", "STRING", mode="NULLABLE", description="Business address"),
            bigquery.SchemaField("city", "STRING", mode="REQUIRED", description="Business city"),
            bigquery.SchemaField("state", "STRING", mode="REQUIRED", description="Business state"),
            bigquery.SchemaField("zip_code", "STRING", mode="NULLABLE", description="ZIP code"),
            
            # Metadata
            bigquery.SchemaField("issuing_authority", "STRING", mode="NULLABLE", description="License issuing authority"),
            bigquery.SchemaField("data_source", "STRING", mode="REQUIRED", description="Data source"),
            bigquery.SchemaField("data_extraction_date", "TIMESTAMP", mode="REQUIRED", description="Extraction timestamp"),
            
            # System fields
            bigquery.SchemaField("created_at", "TIMESTAMP", mode="NULLABLE", description="Record creation timestamp"),
            bigquery.SchemaField("updated_at", "TIMESTAMP", mode="NULLABLE", description="Record update timestamp")
        ]
        
        table = bigquery.Table(full_table_id, schema=schema)
        
        # Partition by issue date
        table.time_partitioning = bigquery.TimePartitioning(
            type_=bigquery.TimePartitioningType.DAY,
            field="issue_date"
        )
        
        # Cluster by state, city, and license type
        table.clustering_fields = ["state", "city", "license_type"]
        
        table.description = "Business licenses from municipal and county sources"
        
        try:
            table = self.client.create_table(table, exists_ok=True)
            logger.info(f"Created/verified business licenses table: {full_table_id}")
        except Exception as e:
            logger.error(f"Error creating business licenses table: {e}")
    
    def create_opportunity_scores_table(self):
        """Create opportunity scores table for analytics"""
        dataset_id = self.bq_config.get('datasets', {}).get('analytics', 'business_analytics')
        table_id = self.bq_config.get('tables', {}).get('opportunity_scores', 'opportunity_scores')
        full_table_id = f"{self.project_id}.{dataset_id}.{table_id}"
        
        schema = [
            bigquery.SchemaField("business_id", "STRING", mode="REQUIRED", description="Business identifier"),
            bigquery.SchemaField("market_opportunity_score", "FLOAT", mode="REQUIRED", description="Market opportunity score (0-100)"),
            bigquery.SchemaField("competition_density_score", "FLOAT", mode="REQUIRED", description="Competition density score (0-100)"),
            bigquery.SchemaField("demographic_match_score", "FLOAT", mode="REQUIRED", description="Demographic match score (0-100)"),
            bigquery.SchemaField("location_quality_score", "FLOAT", mode="REQUIRED", description="Location quality score (0-100)"),
            bigquery.SchemaField("overall_score", "FLOAT", mode="REQUIRED", description="Overall opportunity score (0-100)"),
            bigquery.SchemaField("score_date", "TIMESTAMP", mode="REQUIRED", description="Score calculation date"),
            bigquery.SchemaField("model_version", "STRING", mode="REQUIRED", description="Scoring model version")
        ]
        
        table = bigquery.Table(full_table_id, schema=schema)
        
        # Partition by score date
        table.time_partitioning = bigquery.TimePartitioning(
            type_=bigquery.TimePartitioningType.DAY,
            field="score_date"
        )
        
        # Cluster by overall score for fast filtering
        table.clustering_fields = ["overall_score"]
        
        table.description = "Business opportunity scores for lead prioritization"
        
        try:
            table = self.client.create_table(table, exists_ok=True)
            logger.info(f"Created/verified opportunity scores table: {full_table_id}")
        except Exception as e:
            logger.error(f"Error creating opportunity scores table: {e}")
    
    def create_census_demographics_table(self):
        """Create census demographics table"""
        dataset_id = self.bq_config.get('datasets', {}).get('raw_data', 'raw_business_data')
        table_id = self.bq_config.get('tables', {}).get('census_demographics', 'census_demographics')
        full_table_id = f"{self.project_id}.{dataset_id}.{table_id}"
        
        schema = [
            # Geographic identifiers
            bigquery.SchemaField("geo_id", "STRING", mode="REQUIRED", description="Full Census geographic identifier"),
            bigquery.SchemaField("state_fips", "STRING", mode="REQUIRED", description="State FIPS code"),
            bigquery.SchemaField("county_fips", "STRING", mode="REQUIRED", description="County FIPS code"),
            bigquery.SchemaField("tract_code", "STRING", mode="NULLABLE", description="Census tract code"),
            bigquery.SchemaField("block_group", "STRING", mode="NULLABLE", description="Block group code"),
            
            # Geographic metadata
            bigquery.SchemaField("geographic_level", "STRING", mode="REQUIRED", description="Geographic level"),
            bigquery.SchemaField("area_land_sqmi", "FLOAT", mode="NULLABLE", description="Land area in square miles"),
            bigquery.SchemaField("area_water_sqmi", "FLOAT", mode="NULLABLE", description="Water area in square miles"),
            
            # Population data
            bigquery.SchemaField("total_population", "INTEGER", mode="NULLABLE", description="Total population"),
            bigquery.SchemaField("median_age", "FLOAT", mode="NULLABLE", description="Median age"),
            
            # Economic data
            bigquery.SchemaField("median_household_income", "INTEGER", mode="NULLABLE", description="Median household income"),
            bigquery.SchemaField("unemployment_count", "INTEGER", mode="NULLABLE", description="Unemployed population"),
            bigquery.SchemaField("labor_force", "INTEGER", mode="NULLABLE", description="Total labor force"),
            bigquery.SchemaField("unemployment_rate", "FLOAT", mode="NULLABLE", description="Unemployment rate (%)"),
            
            # Education data
            bigquery.SchemaField("bachelor_degree_count", "INTEGER", mode="NULLABLE", description="Population with bachelor's degree"),
            bigquery.SchemaField("total_education_pop", "INTEGER", mode="NULLABLE", description="Total population 25+ for education"),
            bigquery.SchemaField("bachelor_degree_pct", "FLOAT", mode="NULLABLE", description="% with bachelor's degree"),
            
            # Housing data
            bigquery.SchemaField("total_housing_units", "INTEGER", mode="NULLABLE", description="Total housing units"),
            bigquery.SchemaField("owner_occupied_units", "INTEGER", mode="NULLABLE", description="Owner occupied units"),
            bigquery.SchemaField("total_occupied_units", "INTEGER", mode="NULLABLE", description="Total occupied units"),
            bigquery.SchemaField("owner_occupied_pct", "FLOAT", mode="NULLABLE", description="% owner occupied"),
            
            # Transportation data
            bigquery.SchemaField("total_commuters", "INTEGER", mode="NULLABLE", description="Total commuters"),
            bigquery.SchemaField("commute_60_plus_min", "INTEGER", mode="NULLABLE", description="Commute 60+ minutes"),
            bigquery.SchemaField("public_transport_count", "INTEGER", mode="NULLABLE", description="Public transportation users"),
            bigquery.SchemaField("total_transport_pop", "INTEGER", mode="NULLABLE", description="Total transportation population"),
            bigquery.SchemaField("avg_commute_time", "FLOAT", mode="NULLABLE", description="Average commute time"),
            bigquery.SchemaField("public_transport_pct", "FLOAT", mode="NULLABLE", description="% using public transport"),
            
            # Derived metrics
            bigquery.SchemaField("population_density", "FLOAT", mode="NULLABLE", description="Population per square mile"),
            bigquery.SchemaField("household_density", "FLOAT", mode="NULLABLE", description="Housing units per square mile"),
            
            # Population Estimates Program (PEP) data (2019 - most recent available via API)
            bigquery.SchemaField("population_2019", "INTEGER", mode="NULLABLE", description="2019 Population Estimate (POP)"),
            bigquery.SchemaField("population_density_2019", "FLOAT", mode="NULLABLE", description="2019 Population Density per Square Mile (DENSITY)"),
            bigquery.SchemaField("population_2022", "INTEGER", mode="NULLABLE", description="2022 Population Estimate (POP_2022)"),
            bigquery.SchemaField("population_2021", "INTEGER", mode="NULLABLE", description="2021 Population Estimate (POP_2021)"),
            bigquery.SchemaField("population_2020", "INTEGER", mode="NULLABLE", description="2020 Population Estimate (POP_2020)"),
            
            # Population change data
            bigquery.SchemaField("net_population_change_2022", "INTEGER", mode="NULLABLE", description="Net Population Change 2021-2022 (NPOPCHG_2022)"),
            bigquery.SchemaField("net_population_change_2021", "INTEGER", mode="NULLABLE", description="Net Population Change 2020-2021 (NPOPCHG_2021)"),
            
            # Components of population change
            bigquery.SchemaField("births_2022", "INTEGER", mode="NULLABLE", description="Births 2022 (BIRTHS2022)"),
            bigquery.SchemaField("deaths_2022", "INTEGER", mode="NULLABLE", description="Deaths 2022 (DEATHS2022)"),
            bigquery.SchemaField("net_migration_2022", "INTEGER", mode="NULLABLE", description="Net Migration 2022 (NETMIG2022)"),
            
            # Population rates
            bigquery.SchemaField("birth_rate_2022", "FLOAT", mode="NULLABLE", description="Birth Rate per 1000 population 2022 (RBIRTH2022)"),
            bigquery.SchemaField("death_rate_2022", "FLOAT", mode="NULLABLE", description="Death Rate per 1000 population 2022 (RDEATH2022)"),
            
            # Calculated population metrics
            bigquery.SchemaField("population_growth_rate_2022", "FLOAT", mode="NULLABLE", description="Calculated population growth rate 2021-2022 (%)"),
            bigquery.SchemaField("population_growth_rate_2021", "FLOAT", mode="NULLABLE", description="Calculated population growth rate 2020-2021 (%)"),
            bigquery.SchemaField("avg_annual_growth_rate", "FLOAT", mode="NULLABLE", description="Average annual growth rate 2020-2022 (%)"),
            bigquery.SchemaField("natural_increase_2022", "INTEGER", mode="NULLABLE", description="Natural increase (births - deaths) 2022"),
            
            # Metadata
            bigquery.SchemaField("acs_year", "INTEGER", mode="REQUIRED", description="ACS data year"),
            bigquery.SchemaField("pep_year", "INTEGER", mode="NULLABLE", description="Population Estimates Program data year"),
            bigquery.SchemaField("data_source", "STRING", mode="REQUIRED", description="Data source identifier"),
            bigquery.SchemaField("data_extraction_date", "TIMESTAMP", mode="REQUIRED", description="Data extraction timestamp"),
            bigquery.SchemaField("data_quality_score", "FLOAT", mode="NULLABLE", description="Data completeness score (0-100)"),
            
            # System fields
            bigquery.SchemaField("created_at", "TIMESTAMP", mode="NULLABLE", description="Record creation timestamp"),
            bigquery.SchemaField("updated_at", "TIMESTAMP", mode="NULLABLE", description="Record update timestamp")
        ]
        
        table = bigquery.Table(full_table_id, schema=schema)
        
        # Partition by data extraction date for efficient querying
        table.time_partitioning = bigquery.TimePartitioning(
            type_=bigquery.TimePartitioningType.DAY,
            field="data_extraction_date"
        )
        
        # Cluster by state_fips, county_fips, geographic_level for optimal Wisconsin queries
        table.clustering_fields = ["state_fips", "county_fips", "geographic_level"]
        
        table.description = "Census ACS demographic data and Population Estimates at multiple geographic levels for market analysis"
        
        try:
            table = self.client.create_table(table, exists_ok=True)
            logger.info(f"Created/verified census demographics table: {full_table_id}")
        except Exception as e:
            logger.error(f"Error creating census demographics table: {e}")
    
    def create_bls_qcew_table(self):
        """Create BLS QCEW (Quarterly Census of Employment and Wages) table"""
        dataset_id = self.bq_config.get('datasets', {}).get('raw_data', 'raw_business_data')
        table_id = 'bls_qcew_data'
        full_table_id = f"{self.project_id}.{dataset_id}.{table_id}"
        
        schema = [
            # Geographic identifiers
            bigquery.SchemaField("county_fips", "STRING", mode="REQUIRED", description="County FIPS code"),
            bigquery.SchemaField("county_name", "STRING", mode="REQUIRED", description="County name"),
            
            # Time identifiers
            bigquery.SchemaField("year", "INTEGER", mode="REQUIRED", description="Data year"),
            bigquery.SchemaField("period", "STRING", mode="REQUIRED", description="Period code (Q1, Q2, Q3, Q4)"),
            bigquery.SchemaField("period_name", "STRING", mode="NULLABLE", description="Period name"),
            bigquery.SchemaField("quarter", "INTEGER", mode="NULLABLE", description="Quarter number (1-4)"),
            
            # Data fields
            bigquery.SchemaField("value", "FLOAT", mode="NULLABLE", description="Data value"),
            bigquery.SchemaField("data_type", "STRING", mode="REQUIRED", description="Type of data (employment, establishments, total_wages)"),
            
            # Metadata
            bigquery.SchemaField("series_id", "STRING", mode="REQUIRED", description="BLS series identifier"),
            bigquery.SchemaField("data_source", "STRING", mode="REQUIRED", description="Data source (BLS_QCEW)"),
            bigquery.SchemaField("data_extraction_date", "TIMESTAMP", mode="REQUIRED", description="Data extraction timestamp"),
            
            # System fields
            bigquery.SchemaField("created_at", "TIMESTAMP", mode="NULLABLE", description="Record creation timestamp"),
            bigquery.SchemaField("updated_at", "TIMESTAMP", mode="NULLABLE", description="Record update timestamp")
        ]
        
        table = bigquery.Table(full_table_id, schema=schema)
        
        # Partition by data extraction date for efficient querying
        table.time_partitioning = bigquery.TimePartitioning(
            type_=bigquery.TimePartitioningType.DAY,
            field="data_extraction_date"
        )
        
        # Cluster by county_fips, year, data_type for optimal Wisconsin queries
        table.clustering_fields = ["county_fips", "year", "data_type"]
        
        table.description = "BLS Quarterly Census of Employment and Wages data for Wisconsin counties"
        
        try:
            table = self.client.create_table(table, exists_ok=True)
            logger.info(f"Created/verified BLS QCEW table: {full_table_id}")
        except Exception as e:
            logger.error(f"Error creating BLS QCEW table: {e}")
    
    def create_bls_laus_table(self):
        """Create BLS LAUS (Local Area Unemployment Statistics) table"""
        dataset_id = self.bq_config.get('datasets', {}).get('raw_data', 'raw_business_data')
        table_id = 'bls_laus_data'
        full_table_id = f"{self.project_id}.{dataset_id}.{table_id}"
        
        schema = [
            # Geographic identifiers
            bigquery.SchemaField("county_fips", "STRING", mode="REQUIRED", description="County FIPS code"),
            bigquery.SchemaField("county_name", "STRING", mode="REQUIRED", description="County name"),
            
            # Time identifiers
            bigquery.SchemaField("year", "INTEGER", mode="REQUIRED", description="Data year"),
            bigquery.SchemaField("period", "STRING", mode="REQUIRED", description="Period code (M01-M12)"),
            bigquery.SchemaField("period_name", "STRING", mode="NULLABLE", description="Period name"),
            bigquery.SchemaField("month", "INTEGER", mode="NULLABLE", description="Month number (1-12)"),
            
            # Data fields
            bigquery.SchemaField("value", "FLOAT", mode="NULLABLE", description="Data value"),
            bigquery.SchemaField("measure_type", "STRING", mode="REQUIRED", description="Type of measure (unemployment_rate, unemployment_level, employment_level, labor_force)"),
            
            # Metadata
            bigquery.SchemaField("series_id", "STRING", mode="REQUIRED", description="BLS series identifier"),
            bigquery.SchemaField("data_source", "STRING", mode="REQUIRED", description="Data source (BLS_LAUS)"),
            bigquery.SchemaField("data_extraction_date", "TIMESTAMP", mode="REQUIRED", description="Data extraction timestamp"),
            
            # System fields
            bigquery.SchemaField("created_at", "TIMESTAMP", mode="NULLABLE", description="Record creation timestamp"),
            bigquery.SchemaField("updated_at", "TIMESTAMP", mode="NULLABLE", description="Record update timestamp")
        ]
        
        table = bigquery.Table(full_table_id, schema=schema)
        
        # Partition by data extraction date for efficient querying
        table.time_partitioning = bigquery.TimePartitioning(
            type_=bigquery.TimePartitioningType.DAY,
            field="data_extraction_date"
        )
        
        # Cluster by county_fips, year, measure_type for optimal Wisconsin queries
        table.clustering_fields = ["county_fips", "year", "measure_type"]
        
        table.description = "BLS Local Area Unemployment Statistics data for Wisconsin counties"
        
        try:
            table = self.client.create_table(table, exists_ok=True)
            logger.info(f"Created/verified BLS LAUS table: {full_table_id}")
        except Exception as e:
            logger.error(f"Error creating BLS LAUS table: {e}")
    
    def setup_all_tables(self):
        """Create all required datasets and tables"""
        logger.info("Setting up BigQuery infrastructure...")
        
        # Create datasets
        self.create_datasets()
        
        # Create tables
        self.create_business_entities_table()
        self.create_sba_loans_table()
        self.create_business_licenses_table()
        self.create_opportunity_scores_table()
        self.create_census_demographics_table()
        
        # Create BLS tables
        self.create_bls_qcew_table()
        self.create_bls_laus_table()
        
        logger.info("BigQuery setup complete!")
    
    def create_analytics_views(self):
        """Create useful views for analytics"""
        analytics_dataset = self.bq_config.get('datasets', {}).get('analytics', 'business_analytics')
        
        views = {
            'hot_prospects': f"""
            WITH recent_businesses AS (
              SELECT 
                business_id,
                business_name,
                business_type,
                city,
                state,
                registration_date,
                confidence_score,
                'NEW_BUSINESS' as lead_source
              FROM `{self.project_id}.raw_business_data.business_entities`
              WHERE registration_date >= DATE_SUB(CURRENT_DATE(), INTERVAL 30 DAY)
                AND business_type IN ('restaurant', 'retail', 'personal_services', 'fitness')
            ),
            recent_sba_loans AS (
              SELECT
                loan_id as business_id,
                borrower_name as business_name,
                business_type,
                borrower_city as city,
                borrower_state as state,
                approval_date as registration_date,
                90.0 as confidence_score,
                'SBA_LOAN' as lead_source
              FROM `{self.project_id}.raw_business_data.sba_loan_approvals`
              WHERE approval_date >= DATE_SUB(CURRENT_DATE(), INTERVAL 60 DAY)
                AND loan_amount >= 100000
            )
            SELECT * FROM recent_businesses
            UNION ALL
            SELECT * FROM recent_sba_loans
            ORDER BY registration_date DESC, confidence_score DESC
            """,
            
            'market_summary': f"""
            SELECT 
              state,
              city,
              business_type,
              COUNT(*) as total_businesses,
              COUNT(CASE WHEN registration_date >= DATE_SUB(CURRENT_DATE(), INTERVAL 30 DAY) THEN 1 END) as new_this_month,
              AVG(confidence_score) as avg_confidence_score
            FROM `{self.project_id}.raw_business_data.business_entities`
            WHERE registration_date >= DATE_SUB(CURRENT_DATE(), INTERVAL 90 DAY)
            GROUP BY state, city, business_type
            HAVING COUNT(*) >= 3
            ORDER BY new_this_month DESC, total_businesses DESC
            """
        }
        
        for view_name, query in views.items():
            try:
                view_id = f"{self.project_id}.{analytics_dataset}.{view_name}"
                view = bigquery.Table(view_id)
                view.view_query = query
                
                view = self.client.create_table(view, exists_ok=True)
                logger.info(f"Created/updated view: {view_name}")
                
            except Exception as e:
                logger.error(f"Error creating view {view_name}: {e}")


def main():
    """Main setup function"""
    print("BigQuery Infrastructure Setup")
    print("=" * 40)
    
    setup = BigQuerySetup()
    
    # Setup all tables
    setup.setup_all_tables()
    
    # Create analytics views
    setup.create_analytics_views()
    
    print("\nSetup complete! You can now:")
    print("1. Run data collection: python wisconsin_setup.py --collect-data")
    print("2. Run analysis: python wisconsin_setup.py --run-analysis")
    print("3. Export prospects: python wisconsin_setup.py --export-prospects")


if __name__ == "__main__":
    main()