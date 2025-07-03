# **SECTION 1.3: MARKET DEMAND ANALYSIS - CLIENT TEMPLATE**

## **Template Structure & Data Integration Guide**

---

## **Opening Narrative**
**Content:** Market demand methodology explanation and data source overview
**Length:** 2-3 paragraphs
**Data Integration:** Reference all 6 data sources used in analysis

---

## **1.3.1 Total Market Size & Economic Foundation**

### **Visual Components Required:**
```
📊 Chart 1: Market Size Comparison Bar Chart
   Data: wisconsin_consumer_spending_comprehensive.csv
   Shows: Local vs State vs National per capita spending

📈 Chart 2: Market Growth Timeline (5-year)
   Data: wisconsin_consumer_spending_comprehensive.csv + growth_projections_collector.py
   Shows: $5.98B → projected growth trajectory

📋 Dashboard 3: Employment Foundation Metrics
   Data: industry_health_collector.py
   Shows: 3.18M employed, +0.69% growth, economic stability indicators
```

### **Text Content Includes:**
- **Market Size Calculation**: Population × $1,012 per capita = $X.XM local market
- **Data Point**: $5.98B Wisconsin restaurant market, 5.7% growth
- **Data Point**: 3.18M Wisconsin employment base supporting demand
- **Analysis**: Market capacity assessment and penetration opportunity

### **Data Sources Referenced:**
- `wisconsin_consumer_spending_comprehensive.csv`
- `industry_health_collector.py` (Wisconsin employment data)
- Section 1.1 population data for local calculation

---

## **1.3.2 Economic Demand Drivers**

### **Visual Components Required:**
```
📊 Chart 4: Employment Growth Comparison (Multi-Industry)
   Data: industry_health_collector.py
   Shows: 9 industries with growth rates, food services highlighted

📈 Chart 5: Wage Growth Trend Lines
   Data: wisconsin_historical_wages_20250627.csv
   Shows: 5-year wage progression supporting spending power

🎯 Dashboard 6: Economic Health Scorecard
   Data: industry_health_collector.py + wisconsin_consumer_spending_comprehensive.csv
   Shows: Employment, wage, spending health indicators
```

### **Text Content Includes:**
- **Data Point**: Wisconsin +0.69% employment growth (159.2M total jobs)
- **Data Point**: Food services 14.25% CAGR (12.3K jobs → 25.3K by 2030)
- **Analysis**: Employment trends correlation with consumer spending capacity
- **Analysis**: Regional economic health vs state/national benchmarks

### **Data Sources Referenced:**
- `industry_health_collector.py` (employment data by industry)
- `wisconsin_historical_wages_20250627.csv` (wage growth trends)
- `wisconsin_consumer_spending_comprehensive.csv` (spending correlation)

---

## **1.3.3 Industry Health Assessment**

### **Visual Components Required:**
```
📊 Chart 7: Industry Growth Matrix (9x9 Grid)
   Data: industry_health_collector.py + growth_projections_collector.py
   Shows: Growth rate vs employment size, food services positioning

📈 Chart 8: Sector Performance Rankings
   Data: growth_projections_collector.py
   Shows: 1. Food Services (14.25%), 2. Leisure/Hospitality (14.19%), etc.

🔍 Chart 9: Competitive Industry Timeline
   Data: industry_health_collector.py
   Shows: 5-year performance comparison across sectors
```

### **Text Content Includes:**
- **Data Point**: Food services 14.25% CAGR (top performing sector)
- **Data Point**: Industry ranking vs 8 other major sectors
- **Analysis**: Food services outperforming total nonfarm (4.06% CAGR)
- **Analysis**: Industry resilience and investment climate assessment

### **Data Sources Referenced:**
- `industry_health_collector.py` (66 data points per industry)
- `growth_projections_collector.py` (594 employment records, 9 categories)
- Confidence ratings (High/Medium/Low) for each projection

---

## **1.3.4 Seasonal Demand Patterns**

### **Visual Components Required:**
```
📊 Chart 10: Seasonal Demand Curves (12-Month)
   Data: seasonal_demand_collector.py
   Shows: Monthly patterns for food away from home + 9 other categories

📈 Chart 11: Quarterly Index Spider Chart
   Data: seasonal_demand_collector.py
   Shows: Q1=100.0%, Q2=100.6%, Q3=100.4%, Q4=101.8% (food away from home)

🗓️ Chart 12: Business Calendar Heat Map
   Data: seasonal_demand_collector.py
   Shows: Peak (Q4), moderate (Q2/Q3), baseline (Q1) periods
```

### **Text Content Includes:**
- **Data Point**: Q4 peak season (101.8% of baseline demand)
- **Data Point**: 1.8% seasonality strength (moderate variation)
- **Data Point**: 410 monthly records across 10 spending categories
- **Analysis**: Strategic timing recommendations and operational planning

### **Data Sources Referenced:**
- `seasonal_demand_collector.py` (410 CPI records, 10 categories)
- 3-year monthly data (2022-2025) from Consumer Price Index
- Seasonal analysis with business insights generation

---

## **1.3.5 Growth Projections & Future Demand**

### **Visual Components Required:**
```
📊 Chart 13: 5-Year Growth Projection Timeline
   Data: growth_projections_collector.py
   Shows: 2025 baseline → 2030 projections with confidence intervals

📈 Chart 14: Employment Doubling Visualization
   Data: growth_projections_collector.py
   Shows: Food services 12.3K → 25.3K jobs (doubling scenario)

🎯 Chart 15: Market Expansion Scenarios
   Data: growth_projections_collector.py + market calculations
   Shows: Conservative/Moderate/Optimistic demand growth scenarios
```

### **Text Content Includes:**
- **Data Point**: Food services employment doubling by 2030 (12.3K → 25.3K)
- **Data Point**: Total employment growth 159.2M → 194.2M (5-year)
- **Data Point**: High confidence ratings based on 66+ historical data points
- **Analysis**: Market timing and expansion planning implications

### **Data Sources Referenced:**
- `growth_projections_collector.py` (594 employment records)
- Population projections (4 records 2020-2023)
- Economic projections (5 GDP estimates)
- CAGR calculations with confidence assessment

---

## **1.3.6 Digital Demand Trends (Supplemental)**

### **Visual Components Required:**
```
📊 Chart 16: Google Trends Analysis (If Available)
   Data: pytrends (Google Trends API)
   Shows: Search volume trends for relevant keywords

📈 Chart 17: Search Interest Timeline
   Data: pytrends (Google Trends API)
   Shows: Seasonal search patterns correlating with physical demand

ℹ️ Placeholder: "Digital trends data provides supplemental validation when available"
```

### **Text Content Includes:**
- **Conditional Content**: Only include if pytrends data successfully collected
- **Data Point**: Search volume trends for business category + location
- **Analysis**: Digital demand correlation with physical demand patterns
- **Note**: Supplemental validation of physical demand trends

### **Data Sources Referenced:**
- `pytrends` (Google Trends - when available)
- Graceful fallback when API unavailable
- Supplemental validation of other demand indicators

---

## **1.3.7 Market Demand Summary & Strategic Implications**

### **Visual Components Required:**
```
🎯 Dashboard 18: Market Demand Scorecard
   Data: All previous sections integrated
   Shows: Overall market strength score with component breakdown

📊 Chart 19: Strategic Opportunity Matrix
   Data: All data sources combined
   Shows: Risk vs Opportunity assessment with data backing

📈 Chart 20: Integrated Demand Forecast
   Data: Multiple data sources combined
   Shows: Conservative/Moderate/Optimistic scenarios with confidence bands
```

### **Text Content Includes:**
- **Market Strength Summary**: 5 key strength indicators with checkmarks
- **Strategic Recommendations**: 5 numbered recommendations with data backing
- **Risk Assessment**: Identified risks with mitigation strategies
- **Overall Market Score**: X.X/10 based on comprehensive analysis

### **Data Sources Referenced:**
- **Integration of all 6 data sources**
- Cross-validation between multiple federal data sources
- Confidence levels and data quality assessments

---

## **Data Sources & Methodology Section**

### **Visual Components Required:**
```
📋 Table 21: Data Source Summary
   Shows: Source, Records, Date Range, Confidence Level

📊 Chart 22: Data Quality Assessment
   Shows: Completeness and reliability by source

🔍 Methodology Flowchart
   Shows: Data collection → Analysis → Validation → Conclusions
```

### **Complete Data Inventory:**
```
✅ wisconsin_consumer_spending_comprehensive.csv - Market size baseline
✅ wisconsin_bea_consumer_spending.csv - Economic indicators  
✅ wisconsin_historical_wages_20250627.csv - Wage growth trends
✅ wisconsin_employment_projections_20250627.csv - Industry projections
✅ industry_health_collector.py - 65 employment records, growth analysis
✅ seasonal_demand_collector.py - 410 CPI records, 10 categories, seasonal patterns
✅ growth_projections_collector.py - 594 employment records, 5-year forecasts
✅ pytrends (supplemental) - Digital demand validation when available
```

---

## **Template Delivery Specifications**

### **Total Visual Components:** 22 charts/dashboards/tables
### **Total Data Sources:** 8 (6 primary + 2 supplemental)
### **Total Data Points:** 1,000+ records across all sources
### **Analysis Depth:** 6 comprehensive sub-sections
### **Confidence Levels:** High confidence on employment/growth data
### **Scalability:** Works for any US location using federal data sources

This template provides a complete framework for delivering professional-grade market demand analysis with specific data integration points, required visualizations, and content specifications for each section.