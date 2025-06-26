#!/usr/bin/env python3
"""
Phase 2: Market Dynamics - Leading Indicators & Employment Analysis
Sets up BigQuery infrastructure for permit activity, employment centers, and retail evolution
"""

import os
import json
import logging
from datetime import datetime
from google.cloud import bigquery
from google.oauth2 import service_account

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

def get_bigquery_client():
    """Initialize BigQuery client with service account credentials"""
    credentials = service_account.Credentials.from_service_account_file(
        CREDENTIALS_PATH
    )
    return bigquery.Client(project=PROJECT_ID, credentials=credentials)

def create_permit_activity_table(client):
    """Create table for building permit tracking - leading indicator of growth"""
    table_id = f"{PROJECT_ID}.{DATASET_ID}.permit_activity"
    
    schema = [
        bigquery.SchemaField("permit_id", "STRING", mode="REQUIRED"),
        bigquery.SchemaField("permit_number", "STRING"),
        bigquery.SchemaField("permit_type", "STRING", mode="REQUIRED", 
                           description="Residential, Commercial, Industrial, Mixed-Use"),
        bigquery.SchemaField("permit_subtype", "STRING", 
                           description="New Construction, Addition, Renovation, Demolition"),
        
        # Location information
        bigquery.SchemaField("address", "STRING"),
        bigquery.SchemaField("location", "GEOGRAPHY"),
        bigquery.SchemaField("city", "STRING", mode="REQUIRED"),
        bigquery.SchemaField("county", "STRING"),
        bigquery.SchemaField("state", "STRING", mode="REQUIRED"),
        bigquery.SchemaField("zip_code", "STRING"),
        
        # Permit details
        bigquery.SchemaField("project_description", "STRING"),
        bigquery.SchemaField("estimated_cost", "FLOAT", description="Project cost in USD"),
        bigquery.SchemaField("square_footage", "INTEGER"),
        bigquery.SchemaField("units_added", "INTEGER", description="Residential or commercial units"),
        
        # Timeline tracking
        bigquery.SchemaField("application_date", "DATE", mode="REQUIRED"),
        bigquery.SchemaField("issued_date", "DATE"),
        bigquery.SchemaField("expiration_date", "DATE"),
        bigquery.SchemaField("completion_date", "DATE"),
        bigquery.SchemaField("permit_status", "STRING", 
                           description="Applied, Issued, Under Construction, Completed, Expired"),
        
        # Economic impact indicators
        bigquery.SchemaField("jobs_created_construction", "INTEGER"),
        bigquery.SchemaField("jobs_created_permanent", "INTEGER"),
        bigquery.SchemaField("property_value_impact", "FLOAT", 
                           description="Estimated impact on surrounding property values"),
        
        # Business relevance scoring
        bigquery.SchemaField("market_impact_score", "FLOAT", 
                           description="0-1 score for impact on local business market"),
        bigquery.SchemaField("foot_traffic_impact", "STRING", 
                           description="Increase, Decrease, Neutral"),
        
        # Data source tracking
        bigquery.SchemaField("data_source", "STRING", mode="REQUIRED",
                           description="Municipal source of permit data"),
        bigquery.SchemaField("data_collection_date", "DATE", mode="REQUIRED"),
        bigquery.SchemaField("last_updated", "TIMESTAMP", mode="REQUIRED"),
    ]
    
    table = bigquery.Table(table_id, schema=schema)
    
    # Add partitioning on application_date
    table.time_partitioning = bigquery.TimePartitioning(
        type_=bigquery.TimePartitioningType.DAY,
        field="application_date"
    )
    
    # Add clustering
    table.clustering_fields = ["permit_type", "city", "permit_status"]
    
    # Create table
    try:
        table = client.create_table(table)
        logger.info(f"Created table {table_id}")
    except Exception as e:
        if "Already Exists" in str(e):
            logger.info(f"Table {table_id} already exists")
        else:
            raise e

def create_employment_centers_table(client):
    """Create table for employment center analysis - where people work"""
    table_id = f"{PROJECT_ID}.{DATASET_ID}.employment_centers"
    
    schema = [
        bigquery.SchemaField("center_id", "STRING", mode="REQUIRED"),
        bigquery.SchemaField("center_name", "STRING"),
        bigquery.SchemaField("center_type", "STRING", mode="REQUIRED",
                           description="Office Park, Industrial, Downtown, Hospital, University, Mall"),
        
        # Location information
        bigquery.SchemaField("center_location", "GEOGRAPHY", mode="REQUIRED"),
        bigquery.SchemaField("center_boundary", "GEOGRAPHY", 
                           description="Polygon boundary of employment center"),
        bigquery.SchemaField("address", "STRING"),
        bigquery.SchemaField("city", "STRING", mode="REQUIRED"),
        bigquery.SchemaField("county", "STRING"),
        bigquery.SchemaField("state", "STRING", mode="REQUIRED"),
        
        # Employment data (from LEHD LODES)
        bigquery.SchemaField("total_jobs", "INTEGER", mode="REQUIRED"),
        bigquery.SchemaField("jobs_by_earnings", "RECORD", mode="REPEATED",
            fields=[
                bigquery.SchemaField("earnings_category", "STRING", 
                                   description="Low <$1250, Med $1251-3333, High >$3333"),
                bigquery.SchemaField("job_count", "INTEGER"),
            ]
        ),
        
        bigquery.SchemaField("jobs_by_sector", "RECORD", mode="REPEATED",
            fields=[
                bigquery.SchemaField("naics_sector", "STRING"),
                bigquery.SchemaField("sector_name", "STRING"),
                bigquery.SchemaField("job_count", "INTEGER"),
            ]
        ),
        
        # Worker flow patterns
        bigquery.SchemaField("worker_inflow_counties", "RECORD", mode="REPEATED",
            fields=[
                bigquery.SchemaField("home_county", "STRING"),
                bigquery.SchemaField("worker_count", "INTEGER"),
                bigquery.SchemaField("avg_commute_time", "FLOAT"),
            ]
        ),
        
        # Time-based patterns (for lunch/after-work business opportunities)
        bigquery.SchemaField("peak_work_hours", "STRING", 
                           description="Typical work schedule for this center"),
        bigquery.SchemaField("lunch_break_pattern", "STRING",
                           description="Noon-1pm, Staggered 11-2, etc"),
        bigquery.SchemaField("after_work_departure", "STRING",
                           description="5pm, Staggered 4-6pm, etc"),
        
        # Business opportunity metrics
        bigquery.SchemaField("lunch_market_size", "INTEGER", 
                           description="Workers likely to eat out for lunch"),
        bigquery.SchemaField("after_work_market_size", "INTEGER",
                           description="Workers likely to shop/dine after work"),
        bigquery.SchemaField("avg_worker_income", "FLOAT",
                           description="Average annual income of workers"),
        
        # Growth indicators
        bigquery.SchemaField("job_growth_1yr", "FLOAT", description="1-year job growth rate"),
        bigquery.SchemaField("job_growth_5yr", "FLOAT", description="5-year job growth rate"),
        bigquery.SchemaField("new_companies_1yr", "INTEGER", 
                           description="New companies established in past year"),
        
        # Data tracking
        bigquery.SchemaField("data_year", "INTEGER", mode="REQUIRED"),
        bigquery.SchemaField("data_source", "STRING", mode="REQUIRED"),
        bigquery.SchemaField("last_updated", "TIMESTAMP", mode="REQUIRED"),
    ]
    
    table = bigquery.Table(table_id, schema=schema)
    
    # Add clustering
    table.clustering_fields = ["center_type", "city", "data_year"]
    
    # Create table
    try:
        table = client.create_table(table)
        logger.info(f"Created table {table_id}")
    except Exception as e:
        if "Already Exists" in str(e):
            logger.info(f"Table {table_id} already exists")
        else:
            raise e

def create_retail_evolution_table(client):
    """Create table for tracking retail center changes - anchor tenant tracking"""
    table_id = f"{PROJECT_ID}.{DATASET_ID}.retail_evolution"
    
    schema = [
        bigquery.SchemaField("evolution_id", "STRING", mode="REQUIRED"),
        bigquery.SchemaField("retail_center_id", "STRING", mode="REQUIRED"),
        bigquery.SchemaField("center_name", "STRING"),
        
        # Location information  
        bigquery.SchemaField("center_location", "GEOGRAPHY", mode="REQUIRED"),
        bigquery.SchemaField("address", "STRING"),
        bigquery.SchemaField("city", "STRING", mode="REQUIRED"),
        bigquery.SchemaField("state", "STRING", mode="REQUIRED"),
        
        # Change tracking
        bigquery.SchemaField("change_type", "STRING", mode="REQUIRED",
                           description="Anchor_Added, Anchor_Closed, Tenant_Added, Tenant_Closed, Renovation"),
        bigquery.SchemaField("change_date", "DATE", mode="REQUIRED"),
        
        # Business details
        bigquery.SchemaField("business_name", "STRING", mode="REQUIRED"),
        bigquery.SchemaField("business_type", "STRING", mode="REQUIRED"),
        bigquery.SchemaField("business_category", "STRING",
                           description="Anchor, Major_Tenant, Small_Tenant"),
        bigquery.SchemaField("square_footage", "INTEGER"),
        bigquery.SchemaField("lease_status", "STRING",
                           description="New_Lease, Renewal, Closure, Bankruptcy"),
        
        # Previous tenant (for replacements)
        bigquery.SchemaField("previous_tenant", "STRING"),
        bigquery.SchemaField("previous_business_type", "STRING"),
        bigquery.SchemaField("vacancy_duration_days", "INTEGER"),
        
        # Impact analysis
        bigquery.SchemaField("foot_traffic_impact", "STRING",
                           description="Significant_Increase, Moderate_Increase, Neutral, Decrease"),
        bigquery.SchemaField("center_vitality_before", "FLOAT",
                           description="Occupancy rate before change"),
        bigquery.SchemaField("center_vitality_after", "FLOAT", 
                           description="Occupancy rate after change"),
        
        # Market implications
        bigquery.SchemaField("competitive_impact", "STRING",
                           description="Market leader, Follower, Niche player"),
        bigquery.SchemaField("customer_draw_radius", "FLOAT",
                           description="Miles - estimated customer draw"),
        bigquery.SchemaField("synergy_businesses", "STRING", mode="REPEATED",
                           description="Business types that benefit from this change"),
        
        # Leading indicators this creates
        bigquery.SchemaField("triggers_further_development", "BOOLEAN"),
        bigquery.SchemaField("property_value_trend", "STRING",
                           description="Improving, Stable, Declining"),
        
        # Data source
        bigquery.SchemaField("data_source", "STRING", mode="REQUIRED"),
        bigquery.SchemaField("last_updated", "TIMESTAMP", mode="REQUIRED"),
    ]
    
    table = bigquery.Table(table_id, schema=schema)
    
    # Add partitioning on change_date
    table.time_partitioning = bigquery.TimePartitioning(
        type_=bigquery.TimePartitioningType.DAY,
        field="change_date"
    )
    
    # Add clustering
    table.clustering_fields = ["change_type", "city", "business_category"]
    
    # Create table
    try:
        table = client.create_table(table)
        logger.info(f"Created table {table_id}")
    except Exception as e:
        if "Already Exists" in str(e):
            logger.info(f"Table {table_id} already exists")
        else:
            raise e

def create_market_momentum_view(client):
    """Create view that combines all Phase 2 leading indicators"""
    view_id = f"{PROJECT_ID}.{DATASET_ID}.market_momentum_v1"
    
    view_query = f"""
    CREATE OR REPLACE VIEW `{view_id}` AS
    WITH permit_momentum AS (
        SELECT 
            city,
            county,
            ST_CENTROID(ST_UNION_AGG(location)) as city_center,
            
            -- Recent permit activity (leading indicator)
            COUNT(CASE WHEN application_date >= DATE_SUB(CURRENT_DATE(), INTERVAL 12 MONTHS) 
                       THEN permit_id END) as permits_1yr,
            COUNT(CASE WHEN application_date >= DATE_SUB(CURRENT_DATE(), INTERVAL 6 MONTHS) 
                       THEN permit_id END) as permits_6mo,
            
            -- Investment levels
            SUM(CASE WHEN application_date >= DATE_SUB(CURRENT_DATE(), INTERVAL 12 MONTHS) 
                    THEN estimated_cost END) as investment_1yr,
            
            -- New commercial development
            COUNT(CASE WHEN permit_type = 'Commercial' 
                           AND application_date >= DATE_SUB(CURRENT_DATE(), INTERVAL 12 MONTHS)
                       THEN permit_id END) as commercial_permits_1yr,
                       
            -- Residential growth (creates customer base)
            SUM(CASE WHEN permit_type = 'Residential' 
                          AND application_date >= DATE_SUB(CURRENT_DATE(), INTERVAL 12 MONTHS)
                     THEN COALESCE(units_added, 1) END) as new_housing_units_1yr
        FROM `{PROJECT_ID}.{DATASET_ID}.permit_activity`
        WHERE application_date >= DATE_SUB(CURRENT_DATE(), INTERVAL 24 MONTHS)
        GROUP BY city, county
    ),
    
    employment_growth AS (
        SELECT 
            city,
            SUM(total_jobs) as total_employment,
            AVG(job_growth_1yr) as avg_job_growth_1yr,
            SUM(new_companies_1yr) as new_companies_1yr,
            SUM(lunch_market_size) as lunch_customers,
            SUM(after_work_market_size) as after_work_customers,
            AVG(avg_worker_income) as avg_worker_income
        FROM `{PROJECT_ID}.{DATASET_ID}.employment_centers`
        WHERE data_year = EXTRACT(YEAR FROM CURRENT_DATE()) - 1  -- Most recent year
        GROUP BY city
    ),
    
    retail_trends AS (
        SELECT 
            city,
            -- Recent anchor additions (very positive)
            COUNT(CASE WHEN change_type = 'Anchor_Added' 
                           AND change_date >= DATE_SUB(CURRENT_DATE(), INTERVAL 12 MONTHS)
                       THEN evolution_id END) as anchors_added_1yr,
            -- Recent anchor closures (negative)
            COUNT(CASE WHEN change_type = 'Anchor_Closed' 
                           AND change_date >= DATE_SUB(CURRENT_DATE(), INTERVAL 12 MONTHS)
                       THEN evolution_id END) as anchors_closed_1yr,
            -- Net tenant changes
            COUNT(CASE WHEN change_type IN ('Tenant_Added', 'Anchor_Added')
                           AND change_date >= DATE_SUB(CURRENT_DATE(), INTERVAL 12 MONTHS)
                       THEN evolution_id END) - 
            COUNT(CASE WHEN change_type IN ('Tenant_Closed', 'Anchor_Closed')
                           AND change_date >= DATE_SUB(CURRENT_DATE(), INTERVAL 12 MONTHS)
                       THEN evolution_id END) as net_tenant_change_1yr
        FROM `{PROJECT_ID}.{DATASET_ID}.retail_evolution`
        WHERE change_date >= DATE_SUB(CURRENT_DATE(), INTERVAL 24 MONTHS)
        GROUP BY city
    )
    
    SELECT 
        p.city,
        p.county,
        p.city_center,
        
        -- Permit momentum (leading indicator strength)
        p.permits_1yr,
        p.permits_6mo,
        p.investment_1yr,
        p.commercial_permits_1yr,
        p.new_housing_units_1yr,
        
        -- Employment dynamics  
        e.total_employment,
        e.avg_job_growth_1yr,
        e.new_companies_1yr,
        e.lunch_customers,
        e.after_work_customers,
        e.avg_worker_income,
        
        -- Retail health
        r.anchors_added_1yr,
        r.anchors_closed_1yr,
        r.net_tenant_change_1yr,
        
        -- Composite momentum score (0-100)
        LEAST(100, GREATEST(0,
            (p.permits_1yr / 50) * 20 +  -- Permit activity weight
            (COALESCE(e.avg_job_growth_1yr, 0) * 100) * 30 +  -- Job growth weight  
            (GREATEST(0, r.net_tenant_change_1yr) / 10) * 25 +  -- Retail vitality weight
            (p.commercial_permits_1yr / 10) * 25  -- Commercial development weight
        )) as momentum_score,
        
        -- Market recommendation
        CASE 
            WHEN momentum_score >= 75 THEN 'High_Growth_Market'
            WHEN momentum_score >= 50 THEN 'Stable_Growth_Market'  
            WHEN momentum_score >= 25 THEN 'Slow_Growth_Market'
            ELSE 'Declining_Market'
        END as market_recommendation
        
    FROM permit_momentum p
    LEFT JOIN employment_growth e ON p.city = e.city
    LEFT JOIN retail_trends r ON p.city = r.city
    ORDER BY momentum_score DESC
    """
    
    view = bigquery.Table(view_id)
    view.view_query = view_query.strip()
    
    try:
        # Execute the CREATE VIEW query directly
        client.query(view_query).result()
        logger.info(f"Created view {view_id}")
    except Exception as e:
        if "Already Exists" in str(e):
            # Update the view
            client.query(view_query).result()
            logger.info(f"Updated view {view_id}")
        else:
            logger.warning(f"Could not create view: {e}")

def main():
    """Set up all Phase 2 tables and views"""
    logger.info("Starting Phase 2: Market Dynamics setup")
    
    # Initialize BigQuery client
    client = get_bigquery_client()
    
    # Create tables
    logger.info("Creating Phase 2 tables...")
    create_permit_activity_table(client)
    create_employment_centers_table(client)
    create_retail_evolution_table(client)
    
    # Create views
    logger.info("Creating analysis views...")
    create_market_momentum_view(client)
    
    logger.info("Phase 2 BigQuery infrastructure setup complete!")
    
    # Print summary
    print("\n=== Phase 2 Setup Complete ===")
    print(f"Dataset: {PROJECT_ID}.{DATASET_ID}")
    print("\nNew tables created:")
    print("  - permit_activity (building permits - leading indicator)")
    print("  - employment_centers (where people work - LEHD data)")
    print("  - retail_evolution (anchor tenant changes)")
    print("\nNew views created:")
    print("  - market_momentum_v1 (combines all leading indicators)")
    print("\nNext steps:")
    print("  1. Collect permit data from Wisconsin municipalities")
    print("  2. Load LEHD employment data from Census") 
    print("  3. Track retail changes from business registrations")

if __name__ == "__main__":
    main()