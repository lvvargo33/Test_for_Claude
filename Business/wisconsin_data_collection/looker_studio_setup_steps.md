# Looker Studio Setup Steps
## Universal Business Analysis Engine Dashboards

**🎯 Goal:** Create interactive dashboards for all institutional audiences using your BigQuery data

---

## **STEP 1: Set Up BigQuery Tables (5 minutes)**

1. **Open BigQuery Console:**
   - Go to [console.cloud.google.com/bigquery](https://console.cloud.google.com/bigquery)
   - Select project: `location-optimizer-1`

2. **Create Dataset:**
   ```sql
   CREATE SCHEMA IF NOT EXISTS `location-optimizer-1.business_analysis`;
   ```

3. **Run the Dashboard Tables Script:**
   - Open the file `create_dashboard_tables.sql`
   - Copy and paste the entire script into BigQuery
   - Click "Run" to create all tables

4. **Verify Tables Created:**
   ```sql
   SELECT table_name, row_count 
   FROM `location-optimizer-1.business_analysis.INFORMATION_SCHEMA.TABLES`
   WHERE table_type = 'BASE TABLE';
   ```

---

## **STEP 2: Connect Looker Studio to BigQuery (3 minutes)**

1. **Open Looker Studio:**
   - Go to [lookerstudio.google.com](https://lookerstudio.google.com)
   - Click "Create" → "Data source"

2. **Select BigQuery Connector:**
   - Choose "BigQuery" from the list
   - Authorize with your Google account

3. **Configure Connection:**
   - Project: `location-optimizer-1`
   - Dataset: `business_analysis`
   - Table: `master_dashboard` (this is your main view)

4. **Test Connection:**
   - Click "Connect" in the top right
   - You should see all the fields from your master_dashboard view

---

## **STEP 3: Create Your First Dashboard (15 minutes)**

### **Dashboard 1: Financial Institution Analysis**

1. **Create New Report:**
   - Click "Create Report" 
   - Title: "Universal Business Analysis - Financial Institution Dashboard"

2. **Add Executive Summary Cards:**
   - **Card 1:** Credit Risk Score (Scorecard)
     - Metric: `credit_risk_score`
     - Style: Large number with color coding
   
   - **Card 2:** Debt Service Coverage Ratio (Scorecard)
     - Metric: `debt_service_coverage_ratio`
     - Style: Number with target line at 1.15
   
   - **Card 3:** SBA Eligibility Score (Scorecard)
     - Metric: `sba_eligibility_score`
     - Style: Percentage with green/yellow/red zones

3. **Add Credit Risk Visualization:**
   - **Chart Type:** Gauge Chart
   - **Metric:** `credit_risk_score`
   - **Dimension:** `business_name`
   - **Style:** Set ranges: 0-50 (Red), 51-75 (Yellow), 76-100 (Green)

4. **Add Loan Structure Chart:**
   - **Chart Type:** Stacked Bar Chart
   - **Dimension:** `business_name`
   - **Metrics:** `optimal_loan_amount`, `equity_requirement`
   - **Style:** Horizontal bars with different colors

5. **Add Business Filter:**
   - **Control Type:** Drop-down list
   - **Dimension:** `business_name`
   - **Position:** Top of dashboard

### **Dashboard 2: Investment Opportunity Analysis**

1. **Create New Page:**
   - Click "Add a page" at the bottom
   - Title: "Investment Opportunity Analysis"

2. **Add ROI Metrics:**
   - **Card 1:** Projected IRR (Scorecard)
     - Metric: `projected_irr`
     - Format: Percentage
   
   - **Card 2:** Multiple of Investment (Scorecard)
     - Metric: `multiple_of_investment`
     - Format: Number with 1 decimal
   
   - **Card 3:** Total Addressable Market (Scorecard)
     - Metric: `total_addressable_market`
     - Format: Currency

3. **Add Market Opportunity Chart:**
   - **Chart Type:** Bubble Chart
   - **X-axis:** `total_addressable_market`
   - **Y-axis:** `projected_irr`
   - **Bubble Size:** `scalability_rating`
   - **Color:** `business_type`

4. **Add Financial Projections:**
   - **Chart Type:** Line Chart
   - **Dimension:** Year (you'll need to create calculated fields)
   - **Metrics:** Revenue projections over 5 years
   - **Style:** Multiple lines for different scenarios

---

## **STEP 4: Advanced Visualizations (20 minutes)**

### **Geographic Competition Map**

1. **Add Map Chart:**
   - **Chart Type:** Google Map
   - **Dimension:** `location` (you may need to geocode this)
   - **Metric:** `competitive_advantage_score`
   - **Style:** Circle markers with size based on score

### **Economic Impact Dashboard**

1. **Create New Page:** "Economic Development Impact"

2. **Add Job Creation Chart:**
   - **Chart Type:** Waterfall Chart
   - **Metrics:** `direct_jobs_created`, `indirect_jobs_supported`
   - **Style:** Show cumulative impact

3. **Add Tax Revenue Breakdown:**
   - **Chart Type:** Pie Chart
   - **Dimension:** Tax Type (you'll need calculated fields)
   - **Metric:** `total_annual_tax_revenue`

### **Competitive Analysis**

1. **Create New Page:** "Market Competition"

2. **Add Competitor Density Chart:**
   - **Chart Type:** Bar Chart
   - **Dimension:** `business_name`
   - **Metrics:** `direct_competitors_1mile`, `direct_competitors_3mile`, `direct_competitors_5mile`
   - **Style:** Grouped bars

3. **Add Market Share Projection:**
   - **Chart Type:** Line Chart
   - **X-axis:** Years 1-5
   - **Y-axis:** `market_share_year5`
   - **Lines:** Different businesses for comparison

---

## **STEP 5: Dashboard Styling & Branding (10 minutes)**

### **Apply Consistent Theme:**

1. **Color Scheme:**
   - Primary: #1f4788 (Professional Blue)
   - Secondary: #2e7d32 (Success Green)
   - Accent: #f57c00 (Warning Orange)
   - Alert: #d32f2f (Error Red)

2. **Typography:**
   - Headers: Arial, 18px, Bold
   - Body: Arial, 12px, Regular
   - Metrics: Arial, 24px, Bold

3. **Layout:**
   - Add company logo if available
   - Consistent spacing between charts
   - Clear section headers

---

## **STEP 6: Add Interactivity (5 minutes)**

### **Interactive Filters:**

1. **Business Type Filter:**
   - Control: Drop-down
   - Dimension: `business_type`
   - Position: Top navigation

2. **Location Filter:**
   - Control: Drop-down
   - Dimension: `location`
   - Position: Top navigation

3. **Date Range Filter:**
   - Control: Date range picker
   - Dimension: `analysis_date`
   - Position: Top navigation

### **Cross-Chart Filtering:**
- Enable "Cross-filtering" in chart settings
- Click on any chart element to filter other charts

---

## **STEP 7: Create Audience-Specific Views (10 minutes)**

### **Method 1: Create Separate Reports**
1. **SBA Broker Report:** Focus on credit risk, loan structure, compliance
2. **PE Firm Report:** Focus on ROI, market size, scalability
3. **EDC Report:** Focus on jobs, tax revenue, economic impact

### **Method 2: Use Page-Level Filters**
1. Create an "Audience" calculated field
2. Add audience-specific pages
3. Use filters to show relevant metrics

---

## **STEP 8: Share & Distribute (5 minutes)**

### **Sharing Options:**

1. **View Access:**
   - Click "Share" → "Manage access"
   - Add email addresses for each audience
   - Set permissions: "Viewer" for clients, "Editor" for team

2. **Embed Options:**
   - Get embed code for client portals
   - Set up automatic email reports
   - Create PDF snapshots for presentations

3. **Mobile Optimization:**
   - Preview on mobile devices
   - Adjust layouts for smaller screens
   - Test touch interactions

---

## **STEP 9: Test with Real Data (Ongoing)**

### **Data Validation:**
1. Run the Universal Business Analysis Engine
2. Export results to BigQuery tables
3. Refresh Looker Studio dashboards
4. Verify all visualizations update correctly

### **User Testing:**
1. Share with representatives from each audience
2. Collect feedback on usability
3. Iterate on design and functionality
4. Document best practices

---

## **NEXT STEPS & AUTOMATION**

### **Phase 2 Enhancements:**
1. **Real-time Data Connection:** Connect directly to your analysis engine output
2. **Automated Reporting:** Schedule email reports for different audiences
3. **Advanced Analytics:** Add predictive modeling and trend analysis
4. **API Integration:** Connect to additional data sources

### **Maintenance:**
1. **Weekly Data Refresh:** Ensure BigQuery tables are updated
2. **Monthly Review:** Check dashboard performance and user feedback
3. **Quarterly Updates:** Add new features and visualizations
4. **Annual Redesign:** Major updates based on user needs

**🎉 Result:** Professional, interactive dashboards that serve all your institutional audiences with the same underlying data but tailored visualizations for each stakeholder group!

**Questions?**
- Need help with specific chart types?
- Want to customize for particular audiences?
- Ready to connect real business analysis data?

Let me know what you'd like to tackle first!