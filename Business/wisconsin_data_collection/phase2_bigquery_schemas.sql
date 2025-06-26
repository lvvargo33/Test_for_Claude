-- ============================================================================
-- Phase 2 BigQuery Schema Definitions
-- ============================================================================
-- 
-- Comprehensive table schemas for Phase 2 data sources:
-- 1. Commercial Real Estate (county records + LoopNet)
-- 2. Industry Benchmarks (SBA + franchise data)  
-- 3. Enhanced Employment Data (BLS projections + OES wages)
--
-- All tables include partitioning and clustering for optimal performance
-- ============================================================================

-- ----------------------------------------------------------------------------
-- 1. COMMERCIAL REAL ESTATE TABLES
-- ----------------------------------------------------------------------------

-- Main commercial real estate properties table
CREATE TABLE IF NOT EXISTS `location-optimizer-1.raw_business_data.commercial_real_estate` (
  -- Property identifiers
  property_id STRING NOT NULL,
  parcel_id STRING,
  mls_number STRING,
  
  -- Location information
  property_address STRING NOT NULL,
  city STRING NOT NULL,
  state STRING DEFAULT 'WI',
  zip_code STRING,
  county STRING NOT NULL,
  latitude FLOAT64,
  longitude FLOAT64,
  
  -- Property characteristics
  property_type STRING NOT NULL,
  property_subtype STRING,
  building_size_sqft INT64,
  lot_size_sqft INT64,
  lot_size_acres FLOAT64,
  year_built INT64,
  number_of_units INT64,
  
  -- Pricing information
  listing_price FLOAT64,
  price_per_sqft FLOAT64,
  assessed_value FLOAT64,
  market_value FLOAT64,
  
  -- Rental information
  lease_rate_sqft_year FLOAT64,
  lease_rate_sqft_month FLOAT64,
  gross_income FLOAT64,
  cap_rate FLOAT64,
  
  -- Transaction information
  sale_date DATE,
  sale_price FLOAT64,
  listing_date DATE,
  days_on_market INT64,
  
  -- Property features
  parking_spaces INT64,
  zoning STRING,
  utilities STRING,
  
  -- Business suitability flags
  restaurant_suitable BOOL DEFAULT FALSE,
  retail_suitable BOOL DEFAULT FALSE,
  office_suitable BOOL DEFAULT FALSE,
  warehouse_suitable BOOL DEFAULT FALSE,
  
  -- Market context
  market_segment STRING,
  submarket STRING,
  walkability_score INT64,
  
  -- Data source and quality
  data_source STRING NOT NULL,
  source_url STRING,
  listing_status STRING,
  data_quality_score FLOAT64,
  data_collection_date TIMESTAMP NOT NULL
)
PARTITION BY DATE(data_collection_date)
CLUSTER BY county, property_type, data_source;

-- Real estate market trends aggregation table
CREATE TABLE IF NOT EXISTS `location-optimizer-1.raw_business_data.real_estate_market_trends` (
  trend_id STRING NOT NULL,
  area_name STRING NOT NULL,
  area_type STRING NOT NULL, -- County, City, MSA
  property_type STRING NOT NULL,
  analysis_period STRING NOT NULL, -- e.g., "2024-Q1"
  
  -- Market metrics
  median_sale_price FLOAT64,
  median_lease_rate_sqft FLOAT64,
  average_days_on_market INT64,
  inventory_count INT64,
  new_listings_count INT64,
  sold_listings_count INT64,
  
  -- Market indicators
  price_trend_pct FLOAT64, -- % change vs previous period
  inventory_trend_pct FLOAT64,
  market_temperature STRING, -- Hot, Warm, Cool, Cold
  
  -- Analysis metadata
  data_collection_date TIMESTAMP NOT NULL,
  analysis_date DATE NOT NULL
)
PARTITION BY analysis_date
CLUSTER BY area_name, property_type;

-- ----------------------------------------------------------------------------
-- 2. INDUSTRY BENCHMARKS TABLES
-- ----------------------------------------------------------------------------

-- Industry financial benchmarks table
CREATE TABLE IF NOT EXISTS `location-optimizer-1.raw_business_data.industry_benchmarks` (
  -- Benchmark identifiers
  benchmark_id STRING NOT NULL,
  industry_name STRING NOT NULL,
  naics_code STRING,
  sic_code STRING,
  industry_segment STRING,
  
  -- Benchmark type and scope
  benchmark_type STRING NOT NULL, -- Financial, Operational, Market
  metric_name STRING NOT NULL,
  metric_category STRING NOT NULL, -- Revenue, Profit, Cost, Efficiency
  
  -- Geographic scope
  geographic_scope STRING NOT NULL, -- National, Regional, State
  state STRING,
  region STRING,
  
  -- Benchmark values
  benchmark_value FLOAT64 NOT NULL,
  benchmark_unit STRING NOT NULL,
  percentile_25 FLOAT64,
  percentile_50 FLOAT64,
  percentile_75 FLOAT64,
  
  -- Financial metrics
  revenue_per_employee FLOAT64,
  profit_margin_pct FLOAT64,
  gross_margin_pct FLOAT64,
  operating_margin_pct FLOAT64,
  
  -- Cost structure
  labor_cost_pct FLOAT64,
  rent_cost_pct FLOAT64,
  marketing_cost_pct FLOAT64,
  cogs_pct FLOAT64,
  
  -- Operational metrics
  revenue_per_sqft FLOAT64,
  customers_per_day INT64,
  average_transaction FLOAT64,
  inventory_turnover FLOAT64,
  
  -- Franchise-specific metrics
  is_franchise_data BOOL DEFAULT FALSE,
  franchise_fee FLOAT64,
  royalty_pct FLOAT64,
  marketing_fee_pct FLOAT64,
  initial_investment_low FLOAT64,
  initial_investment_high FLOAT64,
  
  -- Performance metrics
  failure_rate_pct FLOAT64,
  break_even_months INT64,
  roi_percentage FLOAT64,
  payback_period_years FLOAT64,
  
  -- Sample size and confidence
  sample_size INT64,
  confidence_level FLOAT64,
  data_year INT64 NOT NULL,
  
  -- Data source and quality
  data_source STRING NOT NULL,
  source_organization STRING NOT NULL,
  report_title STRING,
  source_url STRING,
  data_quality_score FLOAT64,
  data_collection_date TIMESTAMP NOT NULL
)
PARTITION BY DATE(data_collection_date)
CLUSTER BY naics_code, benchmark_type, data_source;

-- Franchise opportunity analysis table
CREATE TABLE IF NOT EXISTS `location-optimizer-1.raw_business_data.franchise_opportunities` (
  opportunity_id STRING NOT NULL,
  franchise_brand STRING NOT NULL,
  industry_category STRING NOT NULL,
  naics_code STRING,
  
  -- Investment requirements
  franchise_fee FLOAT64,
  total_investment_low FLOAT64,
  total_investment_high FLOAT64,
  liquid_capital_required FLOAT64,
  net_worth_required FLOAT64,
  
  -- Ongoing costs
  royalty_fee_pct FLOAT64,
  marketing_fee_pct FLOAT64,
  
  -- Performance metrics
  average_unit_volume FLOAT64,
  typical_break_even_months INT64,
  franchisee_satisfaction_score FLOAT64,
  
  -- Expansion details
  territories_available INT64,
  multi_unit_development BOOL,
  area_development BOOL,
  
  -- Geographic suitability
  wisconsin_suitable BOOL DEFAULT TRUE,
  rural_suitable BOOL,
  urban_suitable BOOL,
  suburban_suitable BOOL,
  
  -- Market analysis
  market_saturation_level STRING, -- Low, Medium, High
  competition_intensity STRING, -- Low, Medium, High
  growth_potential STRING, -- Low, Medium, High
  
  -- Data source
  data_source STRING NOT NULL,
  last_updated TIMESTAMP NOT NULL
)
PARTITION BY DATE(last_updated)
CLUSTER BY industry_category, wisconsin_suitable;

-- ----------------------------------------------------------------------------
-- 3. ENHANCED EMPLOYMENT DATA TABLES
-- ----------------------------------------------------------------------------

-- Employment projections table
CREATE TABLE IF NOT EXISTS `location-optimizer-1.raw_business_data.employment_projections` (
  -- Projection identifiers
  projection_id STRING NOT NULL,
  projection_period STRING NOT NULL, -- e.g., "2022-2032"
  data_year INT64 NOT NULL,
  projection_year INT64 NOT NULL,
  
  -- Geographic scope
  state STRING DEFAULT 'WI',
  area_name STRING NOT NULL,
  area_type STRING NOT NULL, -- State, MSA, County
  
  -- Industry classification
  industry_code STRING NOT NULL,
  industry_title STRING NOT NULL,
  industry_level STRING NOT NULL,
  supersector STRING,
  
  -- Employment projections
  base_year_employment INT64 NOT NULL,
  projected_employment INT64 NOT NULL,
  numeric_change INT64 NOT NULL,
  percent_change FLOAT64 NOT NULL,
  
  -- Growth classification
  growth_rate STRING NOT NULL, -- Fast, Average, Slow, Declining
  growth_outlook STRING NOT NULL,
  
  -- Employment characteristics
  total_openings INT64,
  replacement_openings INT64,
  growth_openings INT64,
  
  -- Business relevance scoring
  small_business_suitable BOOL DEFAULT FALSE,
  franchise_potential BOOL DEFAULT FALSE,
  startup_friendly BOOL DEFAULT FALSE,
  capital_intensity STRING DEFAULT 'Medium', -- Low, Medium, High
  
  -- Data source and quality
  data_source STRING DEFAULT 'BLS Employment Projections',
  series_id STRING,
  data_quality_score FLOAT64,
  last_updated TIMESTAMP NOT NULL
)
PARTITION BY DATE(last_updated)
CLUSTER BY state, industry_code, projection_period;

-- OES wage data table
CREATE TABLE IF NOT EXISTS `location-optimizer-1.raw_business_data.oes_wages` (
  -- Record identifiers
  wage_record_id STRING NOT NULL,
  data_year INT64 NOT NULL,
  
  -- Geographic information
  area_code STRING NOT NULL,
  area_name STRING NOT NULL,
  state STRING DEFAULT 'WI',
  
  -- Occupation information
  occupation_code STRING NOT NULL,
  occupation_title STRING NOT NULL,
  occupation_group STRING NOT NULL,
  
  -- Employment data
  total_employment INT64,
  employment_per_1000 FLOAT64,
  location_quotient FLOAT64,
  
  -- Wage data
  hourly_mean_wage FLOAT64,
  annual_mean_wage FLOAT64,
  hourly_median_wage FLOAT64,
  annual_median_wage FLOAT64,
  
  -- Wage percentiles
  wage_10th_percentile FLOAT64,
  wage_25th_percentile FLOAT64,
  wage_75th_percentile FLOAT64,
  wage_90th_percentile FLOAT64,
  
  -- Market analysis  
  wage_competitiveness STRING, -- High, Competitive, Below Average
  skill_level STRING,
  education_level STRING,
  
  -- Business insights
  labor_cost_assessment STRING, -- High Cost, Moderate Cost, Low Cost
  talent_availability STRING,
  
  -- Data source
  data_source STRING DEFAULT 'BLS OES',
  last_updated TIMESTAMP NOT NULL
)
PARTITION BY DATE(last_updated)
CLUSTER BY state, area_code, occupation_group;

-- Labor market analysis table
CREATE TABLE IF NOT EXISTS `location-optimizer-1.raw_business_data.labor_market_analysis` (
  analysis_id STRING NOT NULL,
  area_name STRING NOT NULL,
  area_type STRING NOT NULL,
  analysis_date DATE NOT NULL,
  
  -- Labor supply metrics
  total_labor_force INT64,
  unemployment_rate FLOAT64,
  labor_force_participation_rate FLOAT64,
  
  -- Skills availability
  high_skill_workers_pct FLOAT64,
  medium_skill_workers_pct FLOAT64,
  entry_level_workers_pct FLOAT64,
  
  -- Education levels
  college_degree_pct FLOAT64,
  high_school_diploma_pct FLOAT64,
  professional_certification_pct FLOAT64,
  
  -- Industry concentrations
  top_employer_industries ARRAY<STRUCT<industry STRING, employment_pct FLOAT64>>,
  
  -- Wage competitiveness
  median_wage_all_occupations FLOAT64,
  wage_competitiveness_index FLOAT64, -- Compared to state average
  
  -- Business hiring indicators
  job_posting_volume INT64,
  average_time_to_fill_days INT64,
  talent_competition_index FLOAT64,
  
  -- Data source
  data_source STRING NOT NULL,
  last_updated TIMESTAMP NOT NULL
)
PARTITION BY analysis_date
CLUSTER BY area_name, area_type;

-- ----------------------------------------------------------------------------
-- 4. INTEGRATED ANALYTICS VIEWS
-- ----------------------------------------------------------------------------

-- Business opportunity scoring view
CREATE OR REPLACE VIEW `location-optimizer-1.analytics.business_opportunity_scores` AS
SELECT 
  re.county,
  re.city,
  re.property_type,
  
  -- Real estate factors
  AVG(re.lease_rate_sqft_year) as avg_lease_rate,
  COUNT(re.property_id) as available_properties,
  AVG(re.data_quality_score) as re_data_quality,
  
  -- Employment growth factors  
  AVG(ep.percent_change) as avg_employment_growth,
  AVG(CASE WHEN ep.small_business_suitable THEN 1 ELSE 0 END) as small_biz_suitability,
  
  -- Wage factors
  AVG(oes.annual_median_wage) as median_wage,
  AVG(CASE WHEN oes.labor_cost_assessment = 'Low Cost' THEN 3
           WHEN oes.labor_cost_assessment = 'Moderate Cost' THEN 2 
           ELSE 1 END) as labor_cost_score,
  
  -- Industry benchmarks
  AVG(ib.profit_margin_pct) as avg_profit_margin,
  AVG(ib.revenue_per_sqft) as avg_revenue_per_sqft,
  
  -- Composite opportunity score (0-100)
  ROUND(
    (AVG(ep.percent_change) * 2 + -- Growth weighted 2x
     AVG(ib.profit_margin_pct) * 3 + -- Profitability weighted 3x
     (100 / NULLIF(AVG(re.lease_rate_sqft_year), 0)) + -- Affordability (inverse)
     AVG(CASE WHEN ep.small_business_suitable THEN 20 ELSE 0 END) + -- Suitability bonus
     AVG(CASE WHEN oes.labor_cost_assessment = 'Low Cost' THEN 15
              WHEN oes.labor_cost_assessment = 'Moderate Cost' THEN 10 
              ELSE 0 END)) / 6, 1 -- Average and round
  ) as opportunity_score

FROM `location-optimizer-1.raw_business_data.commercial_real_estate` re
LEFT JOIN `location-optimizer-1.raw_business_data.employment_projections` ep 
  ON SUBSTR(ep.industry_code, 1, 2) = CASE 
    WHEN re.property_type = 'restaurant' THEN '72'
    WHEN re.property_type = 'retail' THEN '44' 
    WHEN re.property_type = 'office' THEN '54'
    ELSE '99' END
LEFT JOIN `location-optimizer-1.raw_business_data.oes_wages` oes
  ON oes.area_name LIKE CONCAT('%', re.city, '%')
LEFT JOIN `location-optimizer-1.raw_business_data.industry_benchmarks` ib
  ON ib.naics_code = SUBSTR(ep.industry_code, 1, 3)

WHERE re.data_collection_date >= DATE_SUB(CURRENT_DATE(), INTERVAL 90 DAY)
  AND ep.last_updated >= DATE_SUB(CURRENT_DATE(), INTERVAL 90 DAY)
  
GROUP BY re.county, re.city, re.property_type
HAVING COUNT(re.property_id) >= 5 -- Minimum data threshold
ORDER BY opportunity_score DESC;

-- ----------------------------------------------------------------------------
-- 5. DATA QUALITY AND MONITORING TABLES
-- ----------------------------------------------------------------------------

-- Data collection monitoring table
CREATE TABLE IF NOT EXISTS `location-optimizer-1.raw_business_data.collection_monitoring` (
  collection_id STRING NOT NULL,
  collection_date TIMESTAMP NOT NULL,
  data_source STRING NOT NULL,
  collector_name STRING NOT NULL,
  
  -- Collection metrics
  records_collected INT64,
  processing_time_seconds FLOAT64,
  success_rate FLOAT64,
  
  -- Data quality metrics
  avg_data_quality_score FLOAT64,
  missing_data_pct FLOAT64,
  duplicate_records INT64,
  
  -- Error tracking
  error_count INT64,
  error_messages ARRAY<STRING>,
  
  -- Performance indicators
  api_calls_made INT64,
  rate_limit_hits INT64,
  network_errors INT64,
  
  collection_status STRING NOT NULL -- SUCCESS, PARTIAL, FAILED
)
PARTITION BY DATE(collection_date)
CLUSTER BY data_source, collector_name;

-- ----------------------------------------------------------------------------
-- INDEXES AND PERFORMANCE OPTIMIZATIONS
-- ----------------------------------------------------------------------------

-- Additional clustering recommendations for query performance:
-- 
-- For location-based queries:
-- CLUSTER BY county, city, zip_code
--
-- For time-series analysis:  
-- CLUSTER BY data_year, projection_period
--
-- For industry analysis:
-- CLUSTER BY naics_code, industry_category
--
-- For business type analysis:
-- CLUSTER BY property_type, business_suitable_flags

-- ----------------------------------------------------------------------------
-- SAMPLE QUERIES FOR VALIDATION
-- ----------------------------------------------------------------------------

/*
-- Test commercial real estate data
SELECT county, property_type, COUNT(*) as property_count, 
       AVG(lease_rate_sqft_year) as avg_lease_rate
FROM `location-optimizer-1.raw_business_data.commercial_real_estate`
WHERE data_collection_date >= DATE_SUB(CURRENT_DATE(), INTERVAL 30 DAY)
GROUP BY county, property_type
ORDER BY property_count DESC;

-- Test employment projections
SELECT industry_title, growth_rate, percent_change, 
       projected_employment - base_year_employment as job_growth
FROM `location-optimizer-1.raw_business_data.employment_projections`
WHERE state = 'WI' AND small_business_suitable = TRUE
ORDER BY percent_change DESC;

-- Test wage data by area
SELECT area_name, occupation_group, 
       AVG(annual_median_wage) as median_wage,
       COUNT(*) as occupation_count
FROM `location-optimizer-1.raw_business_data.oes_wages`
WHERE state = 'WI'
GROUP BY area_name, occupation_group
ORDER BY median_wage DESC;

-- Test industry benchmarks
SELECT industry_name, metric_name, benchmark_value, benchmark_unit,
       profit_margin_pct, revenue_per_sqft
FROM `location-optimizer-1.raw_business_data.industry_benchmarks`
WHERE data_source LIKE '%SBA%' AND profit_margin_pct > 5
ORDER BY profit_margin_pct DESC;
*/