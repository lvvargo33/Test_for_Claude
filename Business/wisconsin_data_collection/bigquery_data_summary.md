# BigQuery Data Inventory Summary

## Overview
The BigQuery project contains **12 datasets** with various business, demographic, and analytical data.

## Key Datasets and Tables

### 1. **raw_business_data** (Primary Dataset)
Contains the main business data tables:

- **census_demographics** (288 rows)
  - Latest data: June 25, 2025
  - Contains 52 fields including population, income, education, housing metrics
  - Includes 2019-2022 population estimates and growth rates

- **sba_loan_approvals** (2,904 rows)
  - Date range: October 2009 - August 2023 (14 years of data)
  - Largest table by size (0.49 MB)
  - Contains loan details, borrower info, and franchise data

- **business_licenses** (90 rows)
  - Date range: May 17 - June 24, 2025 (recent data only)
  - Municipal and county business licenses

- **dfi_business_registrations** (43 rows)
  - Date range: May 27 - June 24, 2025 (recent data only)
  - Wisconsin DFI business registrations

- **business_entities** (0 rows - empty)
  - Schema ready but no data loaded yet

### 2. **raw_business_licenses**
- **state_registrations** (50 rows)
  - Date range: March - May 2025

### 3. **Other Notable Tables**
- **core_geography.geographic_hierarchy** (10 rows) - Geographic reference data
- **processed_analytics.master_location_analysis** (5 rows) - Analyzed location data
- **raw_census.acs_demographics** (5 rows) - Additional census data
- **raw_traffic.dot_traffic_counts** (5 rows) - Traffic count data

## Data History Summary

### Historical Data Available:
- **SBA Loans**: 14 years (2009-2023) - Best historical coverage
- **Census Demographics**: Current snapshot with 2019-2022 population trends

### Recent Data (2025):
- **Business Licenses**: Past ~40 days
- **DFI Registrations**: Past ~30 days
- **State Registrations**: Past ~90 days

### Empty/Minimal Data:
- Several datasets have tables with no data yet
- Analytics tables have minimal processed data

## Recommendations
1. The **SBA loan data** provides the best historical view (14 years)
2. Recent business activity is well covered with licenses and registrations from 2025
3. Census data includes both current demographics and recent population trends
4. Consider loading data into the empty business_entities table
5. Most analytics tables need data processing/population