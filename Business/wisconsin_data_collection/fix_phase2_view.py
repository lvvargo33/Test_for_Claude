#!/usr/bin/env python3
"""Fix the Phase 2 view syntax error"""

from google.cloud import bigquery
from google.oauth2 import service_account

PROJECT_ID = "location-optimizer-1"
DATASET_ID = "location_optimizer"
CREDENTIALS_PATH = "location-optimizer-1-449414f93a5a.json"

credentials = service_account.Credentials.from_service_account_file(CREDENTIALS_PATH)
client = bigquery.Client(project=PROJECT_ID, credentials=credentials)

# Fixed SQL with correct DATE_SUB syntax
view_query = f"""
CREATE OR REPLACE VIEW `{PROJECT_ID}.{DATASET_ID}.market_momentum_v1` AS
WITH permit_momentum AS (
    SELECT 
        city,
        county,
        ST_CENTROID(ST_UNION_AGG(location)) as city_center,
        
        -- Recent permit activity (leading indicator)
        COUNT(CASE WHEN application_date >= DATE_SUB(CURRENT_DATE(), INTERVAL 12 MONTH) 
                   THEN permit_id END) as permits_1yr,
        COUNT(CASE WHEN application_date >= DATE_SUB(CURRENT_DATE(), INTERVAL 6 MONTH) 
                   THEN permit_id END) as permits_6mo,
        
        -- Investment levels
        SUM(CASE WHEN application_date >= DATE_SUB(CURRENT_DATE(), INTERVAL 12 MONTH) 
                THEN estimated_cost END) as investment_1yr,
        
        -- New commercial development
        COUNT(CASE WHEN permit_type = 'Commercial' 
                       AND application_date >= DATE_SUB(CURRENT_DATE(), INTERVAL 12 MONTH)
                   THEN permit_id END) as commercial_permits_1yr,
                   
        -- Residential growth (creates customer base)
        SUM(CASE WHEN permit_type = 'Residential' 
                      AND application_date >= DATE_SUB(CURRENT_DATE(), INTERVAL 12 MONTH)
                 THEN COALESCE(units_added, 1) END) as new_housing_units_1yr
    FROM `{PROJECT_ID}.{DATASET_ID}.permit_activity`
    WHERE application_date >= DATE_SUB(CURRENT_DATE(), INTERVAL 24 MONTH)
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
                       AND change_date >= DATE_SUB(CURRENT_DATE(), INTERVAL 12 MONTH)
                   THEN evolution_id END) as anchors_added_1yr,
        -- Recent anchor closures (negative)
        COUNT(CASE WHEN change_type = 'Anchor_Closed' 
                       AND change_date >= DATE_SUB(CURRENT_DATE(), INTERVAL 12 MONTH)
                   THEN evolution_id END) as anchors_closed_1yr,
        -- Net tenant changes
        COUNT(CASE WHEN change_type IN ('Tenant_Added', 'Anchor_Added')
                       AND change_date >= DATE_SUB(CURRENT_DATE(), INTERVAL 12 MONTH)
                   THEN evolution_id END) - 
        COUNT(CASE WHEN change_type IN ('Tenant_Closed', 'Anchor_Closed')
                       AND change_date >= DATE_SUB(CURRENT_DATE(), INTERVAL 12 MONTH)
                   THEN evolution_id END) as net_tenant_change_1yr
    FROM `{PROJECT_ID}.{DATASET_ID}.retail_evolution`
    WHERE change_date >= DATE_SUB(CURRENT_DATE(), INTERVAL 24 MONTH)
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

# Execute the CREATE VIEW query
client.query(view_query).result()
print("✅ Fixed and created market_momentum_v1 view")