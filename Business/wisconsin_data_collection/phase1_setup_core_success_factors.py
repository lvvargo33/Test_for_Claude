#!/usr/bin/env python3
"""
Phase 1: Core Success Factors - Customer-Centric Site Analysis
Sets up BigQuery infrastructure for trade areas, co-tenancy patterns, and barrier-adjusted demographics
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

def create_dataset_if_not_exists(client):
    """Create the location_optimizer dataset if it doesn't exist"""
    dataset_id = f"{PROJECT_ID}.{DATASET_ID}"
    
    try:
        client.get_dataset(dataset_id)
        logger.info(f"Dataset {dataset_id} already exists")
    except Exception:
        dataset = bigquery.Dataset(dataset_id)
        dataset.location = "US"
        dataset.description = "Customer-centric site analysis data"
        
        dataset = client.create_dataset(dataset, timeout=30)
        logger.info(f"Created dataset {dataset_id}")

def create_trade_area_profiles_table(client):
    """Create table for drive-time based trade area analysis"""
    table_id = f"{PROJECT_ID}.{DATASET_ID}.trade_area_profiles"
    
    schema = [
        bigquery.SchemaField("business_id", "STRING", mode="REQUIRED"),
        bigquery.SchemaField("business_name", "STRING"),
        bigquery.SchemaField("business_type", "STRING", mode="REQUIRED"),
        bigquery.SchemaField("location", "GEOGRAPHY", mode="REQUIRED"),
        bigquery.SchemaField("address", "STRING"),
        bigquery.SchemaField("city", "STRING"),
        bigquery.SchemaField("state", "STRING"),
        
        # Multi-ring trade areas (drive-time based)
        bigquery.SchemaField("customers_0_5_min", "INTEGER", description="Population within 5-minute drive"),
        bigquery.SchemaField("customers_5_10_min", "INTEGER", description="Population within 5-10 minute drive"),
        bigquery.SchemaField("customers_10_15_min", "INTEGER", description="Population within 10-15 minute drive"),
        
        # Peak demand windows (from LEHD/LODES data)
        bigquery.SchemaField("morning_population", "INTEGER", description="6am-10am population"),
        bigquery.SchemaField("lunch_population", "INTEGER", description="11am-2pm population"),
        bigquery.SchemaField("evening_population", "INTEGER", description="5pm-9pm population"),
        bigquery.SchemaField("weekend_population", "INTEGER", description="Weekend average population"),
        
        # Accessibility scores
        bigquery.SchemaField("car_accessibility_score", "FLOAT", description="0-1 score for car access"),
        bigquery.SchemaField("walk_accessibility_score", "FLOAT", description="0-1 score for walkability"),
        bigquery.SchemaField("transit_accessibility_score", "FLOAT", description="0-1 score for transit access"),
        
        # Competitive landscape within trade area
        bigquery.SchemaField("competitors_same_type", "INTEGER", description="Count of same business type"),
        bigquery.SchemaField("market_saturation_index", "FLOAT", description="Competitors per 1000 population"),
        
        # Data tracking
        bigquery.SchemaField("data_source", "STRING", description="Source of trade area calculation"),
        bigquery.SchemaField("calculation_date", "DATE", mode="REQUIRED"),
        bigquery.SchemaField("last_updated", "TIMESTAMP", mode="REQUIRED"),
    ]
    
    table = bigquery.Table(table_id, schema=schema)
    
    # Add partitioning on calculation_date
    table.time_partitioning = bigquery.TimePartitioning(
        type_=bigquery.TimePartitioningType.DAY,
        field="calculation_date"
    )
    
    # Add clustering
    table.clustering_fields = ["business_type", "city", "business_id"]
    
    # Create table
    try:
        table = client.create_table(table)
        logger.info(f"Created table {table_id}")
    except Exception as e:
        if "Already Exists" in str(e):
            logger.info(f"Table {table_id} already exists")
        else:
            raise e

def create_cotenancy_success_patterns_table(client):
    """Create table for co-tenancy success pattern analysis"""
    table_id = f"{PROJECT_ID}.{DATASET_ID}.cotenancy_success_patterns"
    
    schema = [
        bigquery.SchemaField("anchor_business_type", "STRING", mode="REQUIRED"),
        bigquery.SchemaField("anchor_business_category", "STRING", description="Higher level category"),
        
        # Success patterns within same plaza/center
        bigquery.SchemaField("within_same_plaza", "RECORD", mode="REPEATED",
            fields=[
                bigquery.SchemaField("business_type", "STRING"),
                bigquery.SchemaField("success_rate", "FLOAT"),
                bigquery.SchemaField("avg_revenue_impact", "FLOAT"),
                bigquery.SchemaField("sample_size", "INTEGER"),
            ]
        ),
        
        # Success patterns within quarter mile
        bigquery.SchemaField("within_quarter_mile", "RECORD", mode="REPEATED",
            fields=[
                bigquery.SchemaField("business_type", "STRING"),
                bigquery.SchemaField("success_rate", "FLOAT"),
                bigquery.SchemaField("count", "INTEGER"),
            ]
        ),
        
        # Anti-patterns (negative co-tenancy)
        bigquery.SchemaField("negative_cotenancy", "STRING", mode="REPEATED", 
                           description="Business types to avoid"),
        
        # Ideal anchor tenants
        bigquery.SchemaField("ideal_anchors", "STRING", mode="REPEATED",
                           description="Best performing anchor tenants"),
        
        # Validation metrics
        bigquery.SchemaField("sample_size", "INTEGER", description="Number of locations analyzed"),
        bigquery.SchemaField("confidence_score", "FLOAT", description="Statistical confidence 0-1"),
        bigquery.SchemaField("analysis_period_years", "INTEGER"),
        
        # Geographic specificity
        bigquery.SchemaField("applicable_states", "STRING", mode="REPEATED"),
        bigquery.SchemaField("applicable_metro_areas", "STRING", mode="REPEATED"),
        
        # Metadata
        bigquery.SchemaField("analysis_date", "DATE", mode="REQUIRED"),
        bigquery.SchemaField("last_updated", "TIMESTAMP", mode="REQUIRED"),
    ]
    
    table = bigquery.Table(table_id, schema=schema)
    
    # Add clustering for efficient queries
    table.clustering_fields = ["anchor_business_type", "anchor_business_category"]
    
    # Create table
    try:
        table = client.create_table(table)
        logger.info(f"Created table {table_id}")
    except Exception as e:
        if "Already Exists" in str(e):
            logger.info(f"Table {table_id} already exists")
        else:
            raise e

def create_retail_cluster_profiles_table(client):
    """Create table for retail cluster analysis"""
    table_id = f"{PROJECT_ID}.{DATASET_ID}.retail_cluster_profiles"
    
    schema = [
        bigquery.SchemaField("cluster_id", "STRING", mode="REQUIRED"),
        bigquery.SchemaField("cluster_name", "STRING"),
        bigquery.SchemaField("cluster_center", "GEOGRAPHY", mode="REQUIRED"),
        bigquery.SchemaField("cluster_boundary", "GEOGRAPHY", description="Polygon boundary of cluster"),
        
        # Cluster composition
        bigquery.SchemaField("anchor_tenant", "STRING", description="Primary anchor (Walmart, Target, etc)"),
        bigquery.SchemaField("total_businesses", "INTEGER"),
        bigquery.SchemaField("total_sq_ft", "INTEGER", description="Estimated total retail square footage"),
        
        bigquery.SchemaField("business_mix", "RECORD", mode="REPEATED",
            fields=[
                bigquery.SchemaField("category", "STRING"),
                bigquery.SchemaField("count", "INTEGER"),
                bigquery.SchemaField("pct_of_cluster", "FLOAT"),
            ]
        ),
        
        # Cluster vitality metrics
        bigquery.SchemaField("new_businesses_last_year", "INTEGER"),
        bigquery.SchemaField("closed_businesses_last_year", "INTEGER"),
        bigquery.SchemaField("avg_years_in_business", "FLOAT"),
        bigquery.SchemaField("vacancy_rate", "FLOAT", description="Estimated vacancy percentage"),
        
        # Cluster classification
        bigquery.SchemaField("cluster_type", "STRING", 
                           description="Power center, Strip mall, Downtown, Lifestyle center, etc"),
        bigquery.SchemaField("cluster_tier", "STRING", description="A, B, C classification"),
        
        # Performance metrics
        bigquery.SchemaField("avg_business_success_rate", "FLOAT"),
        bigquery.SchemaField("foot_traffic_index", "FLOAT", description="Relative foot traffic score"),
        
        # Location details
        bigquery.SchemaField("city", "STRING"),
        bigquery.SchemaField("county", "STRING"),
        bigquery.SchemaField("state", "STRING"),
        bigquery.SchemaField("zip_codes", "STRING", mode="REPEATED"),
        
        # Metadata
        bigquery.SchemaField("profile_date", "DATE", mode="REQUIRED"),
        bigquery.SchemaField("last_updated", "TIMESTAMP", mode="REQUIRED"),
    ]
    
    table = bigquery.Table(table_id, schema=schema)
    
    # Add partitioning
    table.time_partitioning = bigquery.TimePartitioning(
        type_=bigquery.TimePartitioningType.DAY,
        field="profile_date"
    )
    
    # Add clustering
    table.clustering_fields = ["cluster_type", "state", "city"]
    
    # Create table
    try:
        table = client.create_table(table)
        logger.info(f"Created table {table_id}")
    except Exception as e:
        if "Already Exists" in str(e):
            logger.info(f"Table {table_id} already exists")
        else:
            raise e

def create_accessibility_adjusted_demographics_table(client):
    """Create table for barrier-adjusted demographic analysis"""
    table_id = f"{PROJECT_ID}.{DATASET_ID}.accessibility_adjusted_demographics"
    
    schema = [
        bigquery.SchemaField("location", "GEOGRAPHY", mode="REQUIRED"),
        bigquery.SchemaField("location_id", "STRING", mode="REQUIRED", description="Unique identifier for location"),
        bigquery.SchemaField("address", "STRING"),
        
        # Raw population numbers
        bigquery.SchemaField("total_population_1_mile", "INTEGER", description="Raw population within 1 mile"),
        bigquery.SchemaField("total_households_1_mile", "INTEGER", description="Raw households within 1 mile"),
        bigquery.SchemaField("total_businesses_1_mile", "INTEGER", description="Business count within 1 mile"),
        
        # Barrier-adjusted populations
        bigquery.SchemaField("accessible_population", "INTEGER", 
                           description="Population that can easily reach location"),
        bigquery.SchemaField("accessible_households", "INTEGER",
                           description="Households that can easily reach location"),
        bigquery.SchemaField("accessible_daytime_population", "INTEGER",
                           description="Daytime population that can easily reach location"),
        
        # Physical barriers
        bigquery.SchemaField("highway_barriers", "INTEGER", description="Major highways to cross"),
        bigquery.SchemaField("railway_barriers", "INTEGER", description="Rail lines to cross"),
        bigquery.SchemaField("water_barriers", "INTEGER", description="Rivers/lakes to cross"),
        bigquery.SchemaField("elevation_change_ft", "INTEGER", description="Significant elevation changes"),
        
        # Psychological/administrative barriers
        bigquery.SchemaField("neighborhood_boundaries_crossed", "INTEGER", 
                           description="Different neighborhood/census tract boundaries"),
        bigquery.SchemaField("school_district_changes", "INTEGER", description="School district boundaries"),
        bigquery.SchemaField("municipality_changes", "INTEGER", description="City/town boundaries crossed"),
        bigquery.SchemaField("zip_code_changes", "INTEGER", description="ZIP code boundaries crossed"),
        
        # Competitive interference
        bigquery.SchemaField("similar_businesses_intercepting", "INTEGER",
                           description="Competitors between population and site"),
        bigquery.SchemaField("major_retail_intercepting", "INTEGER",
                           description="Major retail centers intercepting traffic"),
        
        # Accessibility scores
        bigquery.SchemaField("barrier_penalty_score", "FLOAT", 
                           description="0-1 score, where 1 = no barriers"),
        bigquery.SchemaField("accessibility_index", "FLOAT",
                           description="Composite accessibility score"),
        
        # Traffic flow
        bigquery.SchemaField("nearest_major_road", "STRING"),
        bigquery.SchemaField("distance_to_major_road_ft", "INTEGER"),
        bigquery.SchemaField("traffic_count_nearest_road", "INTEGER"),
        
        # Metadata
        bigquery.SchemaField("analysis_date", "DATE", mode="REQUIRED"),
        bigquery.SchemaField("last_updated", "TIMESTAMP", mode="REQUIRED"),
    ]
    
    table = bigquery.Table(table_id, schema=schema)
    
    # Add partitioning
    table.time_partitioning = bigquery.TimePartitioning(
        type_=bigquery.TimePartitioningType.DAY,
        field="analysis_date"
    )
    
    # Create table
    try:
        table = client.create_table(table)
        logger.info(f"Created table {table_id}")
    except Exception as e:
        if "Already Exists" in str(e):
            logger.info(f"Table {table_id} already exists")
        else:
            raise e

def create_site_success_factors_view(client):
    """Create master view combining all success factors"""
    view_id = f"{PROJECT_ID}.{DATASET_ID}.site_success_factors_v1"
    
    view_query = f"""
    WITH weighted_trade_areas AS (
        SELECT 
            business_id,
            location,
            business_type,
            -- Weighted customer base calculation
            (customers_0_5_min * 0.8 + 
             customers_5_10_min * 0.15 + 
             customers_10_15_min * 0.05) AS weighted_customer_base,
            -- Peak period populations
            GREATEST(morning_population, lunch_population, evening_population) as peak_population,
            -- Accessibility
            (car_accessibility_score * 0.6 + 
             walk_accessibility_score * 0.2 + 
             transit_accessibility_score * 0.2) as composite_accessibility,
            market_saturation_index
        FROM `{PROJECT_ID}.{DATASET_ID}.trade_area_profiles`
        WHERE calculation_date >= DATE_SUB(CURRENT_DATE(), INTERVAL 90 DAY)
    ),
    
    best_cotenancy AS (
        SELECT 
            anchor_business_type,
            ARRAY_AGG(
                STRUCT(plaza.business_type, plaza.success_rate)
                ORDER BY plaza.success_rate DESC
                LIMIT 5
            ) as top_cotenants
        FROM `{PROJECT_ID}.{DATASET_ID}.cotenancy_success_patterns`,
        UNNEST(within_same_plaza) as plaza
        WHERE plaza.success_rate > 0.7
        GROUP BY anchor_business_type
    )
    
    SELECT 
        t.business_id,
        t.location,
        t.business_type,
        
        -- Trade area strength
        t.weighted_customer_base,
        t.peak_population,
        t.composite_accessibility,
        
        -- Market saturation
        t.market_saturation_index,
        
        -- Co-tenancy recommendations
        c.top_cotenants,
        
        -- Barrier adjustments
        d.accessible_population,
        d.barrier_penalty_score,
        d.accessibility_index,
        
        -- Composite scoring
        (
            (t.weighted_customer_base / 10000) * 0.4 +
            t.composite_accessibility * 0.3 +
            d.barrier_penalty_score * 0.3
        ) AS site_score,
        
        -- Data freshness
        t.calculation_date as trade_area_date,
        d.analysis_date as demographics_date
        
    FROM weighted_trade_areas t
    LEFT JOIN best_cotenancy c ON t.business_type = c.anchor_business_type
    LEFT JOIN `{PROJECT_ID}.{DATASET_ID}.accessibility_adjusted_demographics` d 
        ON ST_EQUALS(t.location, d.location)
    """
    
    view = bigquery.Table(view_id)
    view.view_query = view_query.strip()
    
    try:
        view = client.create_table(view)
        logger.info(f"Created view {view_id}")
    except Exception as e:
        if "Already Exists" in str(e):
            # Update the view
            client.query(view_query).result()
            logger.info(f"Updated view {view_id}")
        else:
            raise e

def main():
    """Set up all Phase 1 tables and views"""
    logger.info("Starting Phase 1: Core Success Factors setup")
    
    # Initialize BigQuery client
    client = get_bigquery_client()
    
    # Create dataset
    create_dataset_if_not_exists(client)
    
    # Create tables
    logger.info("Creating Phase 1 tables...")
    create_trade_area_profiles_table(client)
    create_cotenancy_success_patterns_table(client)
    create_retail_cluster_profiles_table(client)
    create_accessibility_adjusted_demographics_table(client)
    
    # Create views
    logger.info("Creating analysis views...")
    create_site_success_factors_view(client)
    
    logger.info("Phase 1 BigQuery infrastructure setup complete!")
    
    # Print summary
    print("\n=== Phase 1 Setup Complete ===")
    print(f"Dataset: {PROJECT_ID}.{DATASET_ID}")
    print("\nTables created:")
    print("  - trade_area_profiles")
    print("  - cotenancy_success_patterns")
    print("  - retail_cluster_profiles")
    print("  - accessibility_adjusted_demographics")
    print("\nViews created:")
    print("  - site_success_factors_v1")
    print("\nNext steps:")
    print("  1. Set up OpenRouteService integration")
    print("  2. Build co-tenancy pattern analyzer")
    print("  3. Implement barrier detection")

if __name__ == "__main__":
    main()