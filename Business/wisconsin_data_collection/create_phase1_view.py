#!/usr/bin/env python3
"""Create the site success factors view"""

from google.cloud import bigquery
from google.oauth2 import service_account

PROJECT_ID = "location-optimizer-1"
DATASET_ID = "location_optimizer"
CREDENTIALS_PATH = "location-optimizer-1-449414f93a5a.json"

# Initialize client
credentials = service_account.Credentials.from_service_account_file(CREDENTIALS_PATH)
client = bigquery.Client(project=PROJECT_ID, credentials=credentials)

# Create view using query instead of table.view_query
view_query = f"""
CREATE OR REPLACE VIEW `{PROJECT_ID}.{DATASET_ID}.site_success_factors_v1` AS
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
        market_saturation_index,
        calculation_date
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

# Execute the CREATE VIEW query
query_job = client.query(view_query)
query_job.result()

print("View created successfully!")
print(f"View: {PROJECT_ID}.{DATASET_ID}.site_success_factors_v1")