
        CREATE OR REPLACE VIEW `location-optimizer-1.business_intelligence.industry_benchmarks_integrated` AS
        WITH census_data AS (
            SELECT 
                naics_code,
                naics_title as industry_name,
                geo_level,
                state_fips,
                county_fips,
                establishments_total,
                employees_total,
                revenue_total,
                payroll_annual_total,
                revenue_per_establishment,
                revenue_per_employee,
                payroll_as_pct_of_revenue,
                census_year,
                data_collection_date
            FROM `location-optimizer-1.raw_business_data.census_economic_benchmarks`
            WHERE state_fips = '55'  -- Wisconsin
        ),
        benchmark_data AS (
            SELECT 
                naics_code,
                industry_name,
                benchmark_type,
                metric_name,
                benchmark_value,
                benchmark_unit,
                percentile_25,
                percentile_50,
                percentile_75,
                profit_margin_pct,
                labor_cost_pct,
                rent_cost_pct,
                initial_investment_low,
                initial_investment_high,
                data_year,
                data_source
            FROM `location-optimizer-1.raw_business_data.industry_benchmarks`
        )
        SELECT 
            COALESCE(c.naics_code, b.naics_code) as naics_code,
            COALESCE(c.industry_name, b.industry_name) as industry_name,
            c.geo_level,
            c.county_fips,
            -- Census metrics
            c.establishments_total,
            c.employees_total,
            c.revenue_total as revenue_total_thousands,
            c.revenue_per_establishment,
            c.revenue_per_employee,
            c.payroll_as_pct_of_revenue,
            -- Benchmark metrics
            b.benchmark_value as national_benchmark_value,
            b.metric_name as benchmark_metric,
            b.percentile_50 as national_median,
            b.profit_margin_pct as typical_profit_margin,
            b.labor_cost_pct as typical_labor_cost_pct,
            b.initial_investment_low,
            b.initial_investment_high,
            -- Comparative metrics
            CASE 
                WHEN c.revenue_per_establishment > b.percentile_50 THEN 'Above National Median'
                WHEN c.revenue_per_establishment < b.percentile_50 THEN 'Below National Median'
                ELSE 'At National Median'
            END as wisconsin_vs_national,
            -- Metadata
            c.census_year,
            b.data_year as benchmark_year,
            GREATEST(c.data_collection_date, b.data_collection_date) as last_updated
        FROM census_data c
        FULL OUTER JOIN benchmark_data b
            ON c.naics_code = b.naics_code
            AND b.metric_name = 'Average Annual Revenue'
        ORDER BY naics_code, geo_level, county_fips
        