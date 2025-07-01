# Wisconsin Data Collection Cleanup Safety Analysis

## Overview
Comprehensive analysis of file system and BigQuery cleanup opportunities while ensuring no critical data sources, collectors, or infrastructure are impacted.

---

## 📁 **FILE EXPLORER CLEANUP ANALYSIS**

### **🔒 CRITICAL FILES - DO NOT DELETE**

#### **Active Data Collectors (Production Ready)**
```
✅ KEEP - Active Collectors:
- bls_cpi_collector.py                    # CPI inflation data collector
- bls_ppi_construction_collector.py       # Construction cost data collector  
- bls_collector.py                        # Employment and wage data
- census_collector.py                     # Demographic data collector
- census_economic_collector.py            # Industry benchmark collector
- google_places_collector.py              # Business location collector
- industry_benchmarks_collector.py        # Industry analysis
- base_collector.py                       # Base class for all collectors
```

#### **Integration & Analysis Scripts**
```
✅ KEEP - Integration Scripts:
- integrate_ppi_construction.py           # Construction cost integration
- integrate_census_benchmarks.py          # Census data integration
- create_bigquery_view.py                 # BigQuery view creation
```

#### **Configuration & Credentials**
```
✅ KEEP - Essential Configuration:
- data_sources.yaml                       # Data source configuration
- location-optimizer-1-96b6102d3548.json # BigQuery credentials
- config/                                 # Any configuration directories
```

#### **Documentation & Assessments**
```
✅ KEEP - Strategic Documentation:
- wisconsin_data_deliverability_assessment.md
- traffic_data_expansion_recommendations.md
- data_refresh_frequency_analysis.md
- CENSUS_ECONOMIC_COLLECTION_SUMMARY.md
- cleanup_safety_analysis.md (this file)
```

### **🧹 SAFE TO DELETE - Development/Testing Files**

#### **Debug & Development Files**
```
❌ SAFE TO DELETE - Debug Files:
- debug_space_data.py                     # Debug script
- test_bea_api_debug.py                   # API debugging
- inspect_bea_line_codes.py               # Line code inspection
- find_pce_table.py                       # Table finder script
```

#### **Test Scripts (After Validation)**
```
⚠️ REVIEW BEFORE DELETE - Test Scripts:
- test_cpi_collection.py                  # CPI collector test (keep if actively testing)
- test_ppi_construction.py               # PPI test (keep if actively testing)
- test_census_economic.py                # Census test (keep if actively testing)
- test_fixed_bea_data.py                 # BEA test (safe to delete)
```

#### **Backup/Alternative Versions**
```
❌ SAFE TO DELETE - Backup Files:
- bea_pce_collector_fixed.py             # Backup version
- fixed_bea_collector.py                 # Alternative version
- check_bigquery_data.py                 # Data validation script (one-time use)
- detailed_data_check.py                 # Data check script (one-time use)
```

#### **Log Files**
```
❌ SAFE TO DELETE - Log Files:
- census_economic_10year_collection.log  # Old collection logs
- census_economic_efficient_collection.log # Collection logs
- location_optimizer.log                 # Application logs (if archived)
```

#### **Data Files (CSV)**
```
⚠️ REVIEW BEFORE DELETE - Data Files:
- wisconsin_bea_consumer_spending.csv     # Raw data (check if needed for reference)
- wisconsin_consumer_spending_comprehensive.csv # Processed data (safe if in BigQuery)
- wisconsin_pce_data.csv                  # PCE data (safe if in BigQuery)
```

#### **Analysis/Assessment Files**
```
⚠️ REVIEW BEFORE DELETE - Analysis Files:
- data_deliverability_assessment.md      # Duplicate of wisconsin_data_deliverability_assessment.md
- infrastructure_automation_assessment.md # Analysis file (keep if referenced)
- infrastructure_scoring_rubrics.md      # Rubric documentation (keep if actively used)
```

#### **Python Cache Files**
```
❌ SAFE TO DELETE - Python Cache:
- __pycache__/                           # All Python cache directories
- *.pyc files                            # All compiled Python files
```

#### **Test Results & Sample Data**
```
❌ SAFE TO DELETE - Test Results:
- cpi_test_results.json                  # Test output files
- ppi_construction_test_results.json     # Test output files
- construction_cost_report_*.json        # Report output files (if archived)
```

---

## 🗄️ **BIGQUERY CLEANUP ANALYSIS**

### **🔒 CRITICAL DATASETS - DO NOT DELETE**

#### **Production Data Tables**
```
✅ KEEP - Production Data:
raw_business_data:
  - bls_cpi_data (2,700 records)         # Critical: Inflation analysis
  - bls_ppi_construction (2,592 records) # Critical: Construction costs
  - bls_laus_data (10,368 records)       # Critical: Employment data
  - census_demographics (288 records)    # Critical: Demographics
  - census_economic_benchmarks (256 records) # Critical: Industry benchmarks
  - consumer_spending (50 records)       # Critical: Market demand
  - osm_businesses (9,542 records)       # Critical: Business locations
  - sba_loan_approvals (2,904 records)   # Critical: Industry benchmarks
  - zoning_data (2,890 records)          # Critical: Regulatory data
  - business_licenses (90 records)       # Important: Business activity
  - dfi_business_registrations (43 records) # Important: Registration data

wisconsin_business_data:
  - google_places_raw (3,125 records)    # Critical: Competition analysis

raw_traffic:
  - traffic_counts (9,981 records)       # Critical: Traffic analysis

raw_real_estate:
  - commercial_real_estate (150 records) # Important: Cost analysis
```

#### **Business Intelligence Views**
```
✅ KEEP - BI Views:
business_intelligence:
  - construction_cost_analysis           # Critical: Cost intelligence
  - county_construction_impact           # Critical: Location analysis
  - industry_benchmarks_integrated       # Critical: Benchmark analysis
```

#### **Processed Analytics**
```
✅ KEEP - Analytics:
processed_business_data:
  - industry_benchmarks (16 records)     # Important: Analysis results
  - employment_projections (30 records)  # Important: Forecasting
  - oes_wages (32 records)              # Important: Wage analysis
```

### **🧹 SAFE TO DELETE - Empty/Test Tables**

#### **Empty Tables**
```
❌ SAFE TO DELETE - Empty Tables:
business_analytics:
  - hot_prospects (0 records)            # Empty
  - market_summary (0 records)           # Empty
  - unified_business_opportunities (0 records) # Empty

location_optimizer:
  - accessibility_adjusted_demographics (0 records) # Empty
  - market_momentum_v1 (0 records)       # Empty
  - permit_activity (0 records)          # Empty
  - retail_cluster_profiles (0 records)  # Empty
  - retail_evolution (0 records)         # Empty
  - site_success_factors_v1 (0 records)  # Empty
  - trade_area_profiles (0 records)      # Empty

raw_business_data:
  - bls_qcew_data (0 records)            # Empty
  - business_entities (0 records)        # Empty

raw_sba_data:
  - loan_approvals (0 records)           # Empty (duplicate of raw_business_data.sba_loan_approvals)
```

#### **Minimal Data Tables (Review)**
```
⚠️ REVIEW - Minimal Data:
core_geography:
  - geographic_hierarchy (10 records)    # Small but might be reference data

processed_analytics:
  - master_location_analysis (5 rows)    # Small but might be important

raw_census:
  - acs_demographics (5 records)         # Minimal data (check if superseded)

raw_traffic:
  - dot_traffic_counts (5 records)       # Minimal data (check if needed)

raw_business_licenses:
  - state_registrations (50 records)     # Small dataset (check if needed)

location_optimizer:
  - cotenancy_success_patterns (7 records)     # Small but might be analysis
  - employment_centers (50 records)            # Small but might be reference
```

#### **Empty/Unused Datasets**
```
❌ SAFE TO DELETE - Empty Datasets:
- client_reports (no tables)            # Empty dataset
- raw_business (no tables)              # Empty dataset
```

---

## 🛡️ **SAFETY VERIFICATION CHECKLIST**

### **Before Deleting Any Files:**

#### **File System Safety Checks:**
1. ✅ Verify file is not imported by any active collector
2. ✅ Check if file contains unique configuration data
3. ✅ Confirm data is replicated in BigQuery (for CSV files)
4. ✅ Ensure no documentation references critical processes
5. ✅ Backup files to archive location before deletion

#### **BigQuery Safety Checks:**
1. ✅ Confirm table is truly empty (0 records)
2. ✅ Check for any views or queries referencing the table
3. ✅ Verify no ETL processes write to the table
4. ✅ Ensure data isn't referenced in business intelligence views
5. ✅ Export schema/metadata before deletion for potential recreation

### **Recommended Verification Commands:**

#### **File Dependency Check:**
```bash
# Search for file imports across all Python files
grep -r "import filename" .
grep -r "from filename" .
grep -r "filename.py" .
```

#### **BigQuery Dependency Check:**
```sql
-- Check for views referencing table
SELECT * FROM `location-optimizer-1.INFORMATION_SCHEMA.VIEWS` 
WHERE view_definition LIKE '%table_name%';

-- Check for table references in other datasets
SELECT * FROM `location-optimizer-1.INFORMATION_SCHEMA.TABLE_OPTIONS`
WHERE option_value LIKE '%table_name%';
```

---

## 📋 **RECOMMENDED CLEANUP PHASES**

### **Phase 1: Safe Deletions (Immediate)**
1. **Python cache files** (__pycache__, *.pyc)
2. **Empty BigQuery datasets** (client_reports, raw_business)
3. **Empty BigQuery tables** (confirmed 0 records, no dependencies)
4. **Debug scripts** (debug_space_data.py, test_bea_api_debug.py)
5. **Log files** (after archiving if needed)

### **Phase 2: Verification Required (Review First)**
1. **Test scripts** (verify not needed for validation)
2. **CSV data files** (confirm data is in BigQuery)
3. **Backup collector versions** (confirm active version works)
4. **Minimal data tables** (verify not reference data)

### **Phase 3: Archive Before Delete (Cautious)**
1. **Analysis documentation** (archive before removing)
2. **Collection logs** (archive for historical reference)
3. **Alternative implementations** (archive for learning)

---

## 🎯 **EXPECTED CLEANUP BENEFITS**

### **File System:**
- **Estimated space savings:** 50-70% file reduction
- **Improved navigation:** Focus on active, critical files
- **Reduced confusion:** Clear separation of production vs. test files

### **BigQuery:**
- **Cost reduction:** Remove storage costs for empty tables
- **Improved performance:** Faster queries without empty table scans
- **Better organization:** Clean, focused dataset structure

### **Maintenance Benefits:**
- **Easier backup/restore** with fewer files
- **Clearer documentation** with focus on active systems
- **Reduced accidentally using outdated files**

---

## ⚠️ **FINAL SAFETY RECOMMENDATION**

**Before any deletions:**
1. **Create full backup** of current directory
2. **Export BigQuery schemas** for all tables being considered for deletion
3. **Test all active collectors** to ensure they still function
4. **Verify all business intelligence views** still work
5. **Document what was deleted** for potential restoration needs

This ensures you can safely clean up while maintaining all critical data collection and analysis capabilities.