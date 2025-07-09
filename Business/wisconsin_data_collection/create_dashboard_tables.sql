-- Create BigQuery Tables for Looker Studio Dashboards
-- Universal Business Analysis Engine Visual Implementation
-- Project: location-optimizer-1

-- =====================================================
-- TABLE 1: Financial Institution Analysis Dashboard
-- =====================================================
CREATE OR REPLACE TABLE `location-optimizer-1.business_analysis.financial_institution_dashboard` AS
WITH base_data AS (
  SELECT 
    'Sample Indian Restaurant' as business_name,
    'Indian Restaurant' as business_type,
    'Milwaukee, WI' as location,
    CURRENT_DATE() as analysis_date,
    
    -- Credit Analysis Metrics
    75 as credit_risk_score,
    1.45 as debt_service_coverage_ratio,
    0.75 as loan_to_value_ratio,
    1.35 as collateral_adequacy_ratio,
    
    -- SBA Compliance
    85 as sba_eligibility_score,
    'SBA 7(a)' as recommended_sba_program,
    'Lower real estate component favors working capital flexibility' as sba_rationale,
    
    -- Loan Structure
    325000 as total_project_cost,
    292500 as optimal_loan_amount,
    32500 as equity_requirement,
    0.10 as down_payment_percentage,
    0.0575 as expected_loan_yield,
    
    -- Risk Assessment
    78.5 as loan_approval_probability,
    85 as regulatory_compliance_score,
    'B' as institutional_risk_rating
    
  UNION ALL
  
  SELECT 
    'Sample Auto Repair Shop' as business_name,
    'Auto Repair Shop' as business_type,
    'Green Bay, WI' as location,
    CURRENT_DATE() as analysis_date,
    
    -- Credit Analysis Metrics
    68 as credit_risk_score,
    1.28 as debt_service_coverage_ratio,
    0.82 as loan_to_value_ratio,
    1.22 as collateral_adequacy_ratio,
    
    -- SBA Compliance
    92 as sba_eligibility_score,
    'SBA 504' as recommended_sba_program,
    'High real estate component (62%) favors SBA 504 structure' as sba_rationale,
    
    -- Loan Structure
    450000 as total_project_cost,
    225000 as optimal_loan_amount,
    45000 as equity_requirement,
    0.10 as down_payment_percentage,
    0.0525 as expected_loan_yield,
    
    -- Risk Assessment
    82.3 as loan_approval_probability,
    88 as regulatory_compliance_score,
    'B+' as institutional_risk_rating
)
SELECT * FROM base_data;

-- =====================================================
-- TABLE 2: Investment Opportunity Analysis Dashboard
-- =====================================================
CREATE OR REPLACE TABLE `location-optimizer-1.business_analysis.investment_opportunity_dashboard` AS
WITH investment_data AS (
  SELECT 
    'Sample Indian Restaurant' as business_name,
    'Indian Restaurant' as business_type,
    'Milwaukee, WI' as location,
    CURRENT_DATE() as analysis_date,
    
    -- Market Opportunity
    2500000 as total_addressable_market,
    1250000 as serviceable_addressable_market,
    625000 as serviceable_obtainable_market,
    0.085 as market_growth_rate,
    
    -- Financial Projections
    0.185 as projected_irr,
    2.3 as multiple_of_investment,
    3.8 as payback_period_years,
    145000 as year_1_revenue,
    425000 as year_3_revenue,
    650000 as year_5_revenue,
    
    -- Investment Analysis
    325000 as total_investment_required,
    8.5 as scalability_rating,
    'Strategic Sale' as preferred_exit_strategy,
    7.2 as competitive_moat_strength,
    
    -- PE Metrics
    85000 as year_1_ebitda,
    0.21 as year_1_ebitda_margin,
    165000 as year_3_ebitda,
    0.39 as year_3_ebitda_margin,
    285000 as year_5_ebitda,
    0.44 as year_5_ebitda_margin
    
  UNION ALL
  
  SELECT 
    'Sample Auto Repair Shop' as business_name,
    'Auto Repair Shop' as business_type,
    'Green Bay, WI' as location,
    CURRENT_DATE() as analysis_date,
    
    -- Market Opportunity
    1800000 as total_addressable_market,
    950000 as serviceable_addressable_market,
    475000 as serviceable_obtainable_market,
    0.065 as market_growth_rate,
    
    -- Financial Projections
    0.155 as projected_irr,
    1.9 as multiple_of_investment,
    4.2 as payback_period_years,
    185000 as year_1_revenue,
    385000 as year_3_revenue,
    525000 as year_5_revenue,
    
    -- Investment Analysis
    450000 as total_investment_required,
    6.8 as scalability_rating,
    'Management Buyout' as preferred_exit_strategy,
    8.1 as competitive_moat_strength,
    
    -- PE Metrics
    45000 as year_1_ebitda,
    0.24 as year_1_ebitda_margin,
    125000 as year_3_ebitda,
    0.32 as year_3_ebitda_margin,
    195000 as year_5_ebitda,
    0.37 as year_5_ebitda_margin
)
SELECT * FROM investment_data;

-- =====================================================
-- TABLE 3: Competitive Analysis Dashboard
-- =====================================================
CREATE OR REPLACE TABLE `location-optimizer-1.business_analysis.competitive_analysis_dashboard` AS
WITH competitive_data AS (
  SELECT 
    'Sample Indian Restaurant' as business_name,
    'Indian Restaurant' as business_type,
    'Milwaukee, WI' as location,
    CURRENT_DATE() as analysis_date,
    
    -- Competition Metrics
    2 as direct_competitors_1mile,
    8 as direct_competitors_3mile,
    15 as direct_competitors_5mile,
    'Low' as competitive_density,
    
    -- Market Share Projections
    0.08 as market_share_year1,
    0.15 as market_share_year2,
    0.22 as market_share_year3,
    0.28 as market_share_year5,
    
    -- Competitive Advantages
    8.5 as location_advantage_score,
    7.2 as service_differentiation_score,
    6.8 as price_positioning_score,
    9.1 as first_mover_advantage_score,
    
    -- Market Positioning
    'Premium Casual' as market_positioning,
    'Authentic Indian Cuisine' as primary_differentiation,
    75 as competitive_advantage_score
    
  UNION ALL
  
  SELECT 
    'Sample Auto Repair Shop' as business_name,
    'Auto Repair Shop' as business_type,
    'Green Bay, WI' as location,
    CURRENT_DATE() as analysis_date,
    
    -- Competition Metrics
    1 as direct_competitors_1mile,
    4 as direct_competitors_3mile,
    12 as direct_competitors_5mile,
    'Moderate' as competitive_density,
    
    -- Market Share Projections
    0.12 as market_share_year1,
    0.18 as market_share_year2,
    0.25 as market_share_year3,
    0.32 as market_share_year5,
    
    -- Competitive Advantages
    9.2 as location_advantage_score,
    8.5 as service_differentiation_score,
    7.8 as price_positioning_score,
    8.8 as first_mover_advantage_score,
    
    -- Market Positioning
    'Full Service' as market_positioning,
    'European Auto Specialization' as primary_differentiation,
    82 as competitive_advantage_score
)
SELECT * FROM competitive_data;

-- =====================================================
-- TABLE 4: Economic Development Dashboard
-- =====================================================
CREATE OR REPLACE TABLE `location-optimizer-1.business_analysis.economic_development_dashboard` AS
WITH economic_data AS (
  SELECT 
    'Sample Indian Restaurant' as business_name,
    'Indian Restaurant' as business_type,
    'Milwaukee, WI' as location,
    CURRENT_DATE() as analysis_date,
    
    -- Job Creation
    12.5 as direct_jobs_created,
    8.3 as indirect_jobs_supported,
    20.8 as total_jobs_impact,
    
    -- Tax Revenue (Annual)
    35000 as property_tax_revenue,
    28000 as sales_tax_revenue,
    15000 as income_tax_revenue,
    78000 as total_annual_tax_revenue,
    
    -- Economic Impact
    1.67 as economic_multiplier_effect,
    725000 as total_economic_impact,
    0.0035 as jobs_per_dollar_invested,
    24.0 as tax_roi_percentage,
    
    -- EDC Metrics
    'High' as community_development_impact,
    'Moderate' as workforce_development_alignment,
    'Strong' as regional_competitiveness_boost
    
  UNION ALL
  
  SELECT 
    'Sample Auto Repair Shop' as business_name,
    'Auto Repair Shop' as business_type,
    'Green Bay, WI' as location,
    CURRENT_DATE() as analysis_date,
    
    -- Job Creation
    8.5 as direct_jobs_created,
    6.2 as indirect_jobs_supported,
    14.7 as total_jobs_impact,
    
    -- Tax Revenue (Annual)
    45000 as property_tax_revenue,
    22000 as sales_tax_revenue,
    18000 as income_tax_revenue,
    85000 as total_annual_tax_revenue,
    
    -- Economic Impact
    1.73 as economic_multiplier_effect,
    785000 as total_economic_impact,
    0.0032 as jobs_per_dollar_invested,
    18.9 as tax_roi_percentage,
    
    -- EDC Metrics
    'Moderate' as community_development_impact,
    'High' as workforce_development_alignment,
    'Strong' as regional_competitiveness_boost
)
SELECT * FROM economic_data;

-- =====================================================
-- TABLE 5: Master Dashboard (Combined View)
-- =====================================================
CREATE OR REPLACE VIEW `location-optimizer-1.business_analysis.master_dashboard` AS
SELECT 
  fi.business_name,
  fi.business_type,
  fi.location,
  fi.analysis_date,
  
  -- Financial Institution Metrics
  fi.credit_risk_score,
  fi.debt_service_coverage_ratio,
  fi.sba_eligibility_score,
  fi.optimal_loan_amount,
  fi.institutional_risk_rating,
  
  -- Investment Opportunity Metrics
  io.projected_irr,
  io.multiple_of_investment,
  io.total_addressable_market,
  io.scalability_rating,
  io.year_5_ebitda,
  
  -- Competitive Analysis
  ca.direct_competitors_1mile,
  ca.market_share_year5,
  ca.competitive_advantage_score,
  ca.primary_differentiation,
  
  -- Economic Development
  ed.direct_jobs_created,
  ed.total_annual_tax_revenue,
  ed.economic_multiplier_effect,
  ed.total_economic_impact

FROM `location-optimizer-1.business_analysis.financial_institution_dashboard` fi
LEFT JOIN `location-optimizer-1.business_analysis.investment_opportunity_dashboard` io
  ON fi.business_name = io.business_name
LEFT JOIN `location-optimizer-1.business_analysis.competitive_analysis_dashboard` ca
  ON fi.business_name = ca.business_name
LEFT JOIN `location-optimizer-1.business_analysis.economic_development_dashboard` ed
  ON fi.business_name = ed.business_name;

-- =====================================================
-- VERIFICATION QUERIES
-- =====================================================

-- Check data quality
SELECT 
  'Financial Institution' as table_name,
  COUNT(*) as record_count,
  COUNT(DISTINCT business_name) as unique_businesses
FROM `location-optimizer-1.business_analysis.financial_institution_dashboard`

UNION ALL

SELECT 
  'Investment Opportunity' as table_name,
  COUNT(*) as record_count,
  COUNT(DISTINCT business_name) as unique_businesses
FROM `location-optimizer-1.business_analysis.investment_opportunity_dashboard`

UNION ALL

SELECT 
  'Competitive Analysis' as table_name,
  COUNT(*) as record_count,
  COUNT(DISTINCT business_name) as unique_businesses
FROM `location-optimizer-1.business_analysis.competitive_analysis_dashboard`

UNION ALL

SELECT 
  'Economic Development' as table_name,
  COUNT(*) as record_count,
  COUNT(DISTINCT business_name) as unique_businesses
FROM `location-optimizer-1.business_analysis.economic_development_dashboard`;

-- Sample query for Looker Studio
SELECT 
  business_name,
  credit_risk_score,
  CASE 
    WHEN credit_risk_score >= 80 THEN 'Excellent'
    WHEN credit_risk_score >= 65 THEN 'Good'
    WHEN credit_risk_score >= 50 THEN 'Fair'
    ELSE 'Poor'
  END as credit_rating,
  debt_service_coverage_ratio,
  sba_eligibility_score,
  optimal_loan_amount,
  projected_irr,
  total_addressable_market,
  direct_jobs_created,
  total_annual_tax_revenue
FROM `location-optimizer-1.business_analysis.master_dashboard`
ORDER BY credit_risk_score DESC;