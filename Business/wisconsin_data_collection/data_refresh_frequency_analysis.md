# Wisconsin Business Intelligence Data Refresh Frequency Analysis

## Overview
Analysis of data refresh cycles across all sources to understand data currency and update requirements for maintaining competitive business intelligence capabilities.

---

## 🤖 **AUTOMATED BIGQUERY DATA SOURCES**

### **📊 Demographic & Economic Data**

**Census ACS Demographics (288 records)**
- **Refresh Frequency:** Annual (October release)
- **Data Lag:** 1 year (2024 data released October 2025)
- **Update Schedule:** October annually via Census API
- **Business Impact:** Stable - demographics change slowly

**BLS Consumer Price Index (2,700 records)**
- **Refresh Frequency:** Monthly (mid-month release)
- **Data Lag:** 2-4 weeks
- **Update Schedule:** Monthly automated collection recommended
- **Business Impact:** High - inflation affects all cost projections

**BLS Employment Data - LAUS (10,368 records)**
- **Refresh Frequency:** Monthly (state data)
- **Data Lag:** 4-6 weeks
- **Update Schedule:** Monthly automated collection
- **Business Impact:** High - employment trends critical for market analysis

**Census Economic Benchmarks (256 records)**
- **Refresh Frequency:** Every 5 years (Economic Census) + Annual (County Business Patterns)
- **Data Lag:** 2-3 years for Economic Census, 1 year for CBP
- **Update Schedule:** Annual CBP updates, Economic Census every 5 years
- **Business Impact:** Medium - industry benchmarks change gradually

**BLS Construction Costs - PPI (2,592 records)**
- **Refresh Frequency:** Monthly
- **Data Lag:** 2-3 weeks
- **Update Schedule:** Monthly automated collection recommended
- **Business Impact:** High - construction costs volatile, affects project planning

**Consumer Spending Data (50 records)**
- **Refresh Frequency:** Quarterly (BEA release)
- **Data Lag:** 3-4 months
- **Update Schedule:** Quarterly automated collection
- **Business Impact:** Medium - spending patterns for market demand analysis

### **🏢 Business & Competition Data**

**Google Places Businesses (3,125 records)**
- **Refresh Frequency:** Real-time (API updates continuously)
- **Data Lag:** Near real-time
- **Update Schedule:** Monthly collection recommended (new businesses, closures, rating changes)
- **Business Impact:** High - competitive landscape changes frequently

**OSM Business Data (9,542 records)**
- **Refresh Frequency:** Continuous (crowd-sourced updates)
- **Data Lag:** Varies (community-driven)
- **Update Schedule:** Quarterly collection recommended
- **Business Impact:** Medium - supplementary business location data

**SBA Loan Data (2,904 records)**
- **Refresh Frequency:** Monthly
- **Data Lag:** 1-2 months
- **Update Schedule:** Quarterly collection recommended
- **Business Impact:** Medium - industry benchmark data

**Business Licenses (90 records)**
- **Refresh Frequency:** Weekly/Monthly (varies by municipality)
- **Data Lag:** 1-4 weeks
- **Update Schedule:** Monthly collection from key counties
- **Business Impact:** Medium - new business activity indicators

### **🚗 Transportation & Infrastructure**

**Traffic Count Data (9,981 records)**
- **Refresh Frequency:** Annual (WisDOT AADT updates)
- **Data Lag:** 1-2 years
- **Update Schedule:** Annual collection from WisDOT
- **Business Impact:** Medium - traffic patterns change slowly

**Zoning Data (2,890 records)**
- **Refresh Frequency:** Quarterly/Annual (varies by municipality)
- **Data Lag:** 1-6 months
- **Update Schedule:** Annual collection recommended
- **Business Impact:** Medium - zoning changes affect development opportunities

**Commercial Real Estate (150 records)**
- **Refresh Frequency:** Monthly (listing updates)
- **Data Lag:** Real-time to 1 month
- **Update Schedule:** Quarterly collection recommended
- **Business Impact:** High - real estate market changes frequently

---

## 📞 **MANUAL/AD HOC DATA SOURCES**

### **🏠 Real Estate Data (Broker Calls)**
- **Refresh Frequency:** On-demand per project
- **Data Currency:** Real-time market conditions
- **Update Process:** Per-project basis, fresh for each client
- **Business Impact:** Critical - provides current market pricing and availability
- **Recommendation:** Maintain broker relationships for immediate access

### **📈 SBA Industry Benchmarks (Ad Hoc)**
- **Refresh Frequency:** On-demand per project
- **Data Currency:** Most recent SBA publications (quarterly/annual)
- **Update Process:** Project-specific research
- **Business Impact:** High - industry-specific benchmarking
- **Recommendation:** Subscribe to SBA data feeds for latest benchmark releases

### **🛰️ Google Earth Site Analysis**
- **Refresh Frequency:** On-demand per site
- **Data Currency:** Satellite imagery updated every 1-3 years, Street View more frequent
- **Update Process:** Real-time analysis for each site assessment
- **Business Impact:** High - current site conditions and characteristics
- **Recommendation:** Always use for current project analysis

### **📱 PyTrends Google Trends Analysis**
- **Refresh Frequency:** On-demand per project
- **Data Currency:** Real-time to 7 days
- **Update Process:** Project-specific trend analysis
- **Business Impact:** High - current market demand and consumer interest
- **Recommendation:** Analyze trends 30-90 days before project delivery

### **⭐ Google Places Ratings & Reviews**
- **Refresh Frequency:** Real-time during project analysis
- **Data Currency:** Current ratings and recent reviews
- **Update Process:** Live data during competitive analysis
- **Business Impact:** High - current competitive performance
- **Recommendation:** Check during each competitive analysis

### **🔧 Infrastructure Rubric Assessment**
- **Refresh Frequency:** Per-site assessment
- **Data Currency:** Current conditions at time of assessment
- **Update Process:** Site-specific evaluation
- **Business Impact:** Critical - current infrastructure capabilities
- **Recommendation:** Assess for each site, update if conditions change

### **🚦 Google Maps Traffic Analysis**
- **Refresh Frequency:** Real-time during analysis
- **Data Currency:** Current traffic conditions and patterns
- **Update Process:** Live analysis during site evaluation
- **Business Impact:** High - current accessibility and traffic patterns
- **Recommendation:** Analyze during multiple time periods (peak/off-peak)

---

## 📅 **RECOMMENDED UPDATE SCHEDULE**

### **🔄 High-Frequency Updates (Monthly)**
- **BLS CPI Data** - Inflation critical for cost projections
- **BLS Employment Data** - Employment trends affect market demand
- **BLS Construction Costs** - Volatile construction pricing
- **Google Places Businesses** - Competitive landscape changes

### **📊 Medium-Frequency Updates (Quarterly)**
- **Consumer Spending Data** - Spending pattern analysis
- **SBA Loan Data** - Industry benchmark updates
- **OSM Business Data** - Supplementary business locations
- **Commercial Real Estate** - Market availability and pricing

### **📈 Low-Frequency Updates (Annual)**
- **Census Demographics** - Slow-changing population data
- **Traffic Count Data** - Gradual traffic pattern changes
- **Zoning Data** - Infrequent regulatory changes
- **Census Economic Benchmarks** - CBP annual updates

### **⚡ Real-Time/Project-Based (As Needed)**
- **Real Estate Broker Calls** - Current market conditions
- **SBA Benchmarks Research** - Project-specific industry data
- **Google Earth Analysis** - Site-specific conditions
- **PyTrends Analysis** - Market demand research
- **Google Places Reviews** - Competitive performance
- **Infrastructure Assessment** - Site-specific capabilities
- **Google Maps Traffic** - Current accessibility analysis

---

## 💡 **DATA CURRENCY STRATEGY**

### **Automated Collection Priority:**
1. **Monthly:** Economic indicators (CPI, employment, construction costs)
2. **Quarterly:** Business data (spending, loans, real estate)
3. **Annual:** Demographic and regulatory data

### **Manual Process Excellence:**
- **Real-time accuracy** for site-specific analysis
- **Current market conditions** through broker networks
- **Live competitive intelligence** via Google services
- **Project-specific research** ensures relevance

### **Competitive Advantage:**
- **Freshest economic data** through monthly automated updates
- **Current site intelligence** through real-time manual analysis
- **Comprehensive coverage** combining historical trends with current conditions

### **Client Value Proposition:**
- **Historical context** from comprehensive databases
- **Current market reality** from real-time manual analysis
- **Forward-looking insights** from trend analysis and economic indicators

---

## 🎯 **IMPLEMENTATION RECOMMENDATIONS**

### **Phase 1: Automated Update Framework (30 days)**
1. Set up monthly automated collection for economic indicators
2. Implement quarterly business data updates
3. Schedule annual demographic and regulatory updates

### **Phase 2: Manual Process Optimization (60 days)**
1. Establish broker relationship protocols for real-time market data
2. Create standardized infrastructure assessment procedures
3. Develop traffic analysis protocols using Google Maps

### **Phase 3: Integration & Quality Control (90 days)**
1. Integrate automated and manual data sources
2. Implement data quality monitoring
3. Create client-facing data currency indicators

This refresh strategy ensures your business intelligence platform maintains competitive advantage through optimal data currency across all sources.