# Financial Institution Dashboard - Step by Step Guide
## 🏦 SBA Brokers & Banks Dashboard Implementation

**✅ COMPLETED:** BigQuery table created with sample data
**🎯 NOW:** Create the Looker Studio dashboard

---

## **YOUR BIGQUERY DATA IS READY!**

**Project:** `location-optimizer-1`
**Dataset:** `business_analysis` 
**Table:** `financial_institution_dashboard`

**Data Preview:**
- ✅ **4 sample businesses** with complete financial analysis
- ✅ **Credit risk scores** ranging from 68-82 (out of 100)
- ✅ **SBA eligibility scores** ranging from 79-92 (out of 100)
- ✅ **Loan recommendations** (SBA 7(a) vs SBA 504)
- ✅ **Risk ratings** from A- to B+

---

## **STEP 1: Create Your Looker Studio Dashboard (5 minutes)**

### **1.1 Open Looker Studio**
1. Go to [lookerstudio.google.com](https://lookerstudio.google.com)
2. Sign in with your Google account
3. Click **"Create"** → **"Report"**

### **1.2 Connect to BigQuery**
1. In the data source panel, click **"Create Data Source"**
2. Select **"BigQuery"** from the connector list
3. **Authorize** Looker Studio to access your BigQuery data
4. Configure the connection:
   - **Project:** `location-optimizer-1`
   - **Dataset:** `business_analysis`
   - **Table:** `financial_institution_dashboard`
5. Click **"Connect"** in the top right

### **1.3 Configure Fields**
You should see these fields automatically detected:
- `business_name` (Text)
- `business_type` (Text)
- `location` (Text)
- `credit_risk_score` (Number)
- `debt_service_coverage_ratio` (Number)
- `sba_eligibility_score` (Number)
- `recommended_sba_program` (Text)
- `optimal_loan_amount` (Number)
- `institutional_risk_rating` (Text)

Click **"Add to Report"** to proceed.

---

## **STEP 2: Build Key Visualizations (15 minutes)**

### **2.1 Executive Summary Cards**

**Add Credit Risk Score Card:**
1. Click **"Add a chart"** → **"Scorecard"**
2. Place it in the top-left corner (resize to ~300px wide x 150px tall)
3. **Data Setup:**
   - **Metric:** `credit_risk_score`
   - **Dimension:** `business_name`
4. **Style Configuration:**
   - Click **"Style"** tab in the properties panel
   - **Chart header:**
     - Title: "Credit Risk Score"
     - Subtitle: "Overall creditworthiness assessment"
     - Title font size: 16px
     - Title color: #1f4788 (Professional Blue)
   - **Metric:**
     - Font size: 48px
     - Font weight: Bold
     - Color: #1f4788
   - **Number format:**
     - Type: Number
     - Decimal places: 0
     - Suffix: "/100"
   - **Conditional formatting:**
     - Click "Add rule"
     - Condition: Greater than or equal to 80
     - Color: #2e7d32 (Green)
     - Add another rule: Less than 65
     - Color: #d32f2f (Red)

**Add DSCR Card:**
1. Add another **Scorecard** next to the first one (same size)
2. **Data Setup:**
   - **Metric:** `debt_service_coverage_ratio`
   - **Dimension:** `business_name`
3. **Style Configuration:**
   - **Chart header:**
     - Title: "Debt Service Coverage Ratio"
     - Subtitle: "Target: ≥1.15"
     - Title font size: 16px
     - Title color: #1f4788
   - **Metric:**
     - Font size: 48px
     - Font weight: Bold
   - **Number format:**
     - Type: Number
     - Decimal places: 2
     - Prefix: ""
     - Suffix: "x"
   - **Conditional formatting:**
     - Condition: Greater than or equal to 1.15
     - Color: #2e7d32 (Green)
     - Add rule: Less than 1.15
     - Color: #f57c00 (Orange)

**Add SBA Eligibility Card:**
1. Add third **Scorecard** (same size)
2. **Data Setup:**
   - **Metric:** `sba_eligibility_score`
   - **Dimension:** `business_name`
3. **Style Configuration:**
   - **Chart header:**
     - Title: "SBA Eligibility Score"
     - Subtitle: "Program qualification assessment"
     - Title font size: 16px
     - Title color: #1f4788
   - **Metric:**
     - Font size: 48px
     - Font weight: Bold
     - Color: #2e7d32
   - **Number format:**
     - Type: Number
     - Decimal places: 0
     - Suffix: "/100"
   - **Conditional formatting:**
     - Condition: Greater than or equal to 85
     - Color: #2e7d32 (Green)
     - Add rule: Less than 70
     - Color: #f57c00 (Orange)

**Add Loan Amount Card:**
1. Add fourth **Scorecard** to complete the top row
2. **Data Setup:**
   - **Metric:** `optimal_loan_amount`
   - **Dimension:** `business_name`
3. **Style Configuration:**
   - **Chart header:**
     - Title: "Optimal Loan Amount"
     - Subtitle: "Recommended financing"
     - Title font size: 16px
     - Title color: #1f4788
   - **Metric:**
     - Font size: 42px
     - Font weight: Bold
     - Color: #1f4788
   - **Number format:**
     - Type: Currency
     - Currency: USD ($)
     - Decimal places: 0
     - Compact numbers: Yes (shows $292.5K instead of $292,500)

### **2.2 Credit Risk Gauge Chart**

1. Click **"Add a chart"** → **"Gauge chart"**
2. Place it prominently on the dashboard (resize to ~400px wide x 300px tall)
3. **Data Setup:**
   - **Metric:** `credit_risk_score`
   - **Dimension:** `business_name`
4. **Style Configuration:**
   - **Chart header:**
     - Title: "Credit Risk Assessment"
     - Subtitle: "Real-time creditworthiness gauge"
     - Title font size: 18px
     - Title color: #1f4788
     - Show title: Yes
   - **Gauge settings:**
     - **Min value:** 0
     - **Max value:** 100
     - **Gauge type:** Arc (half circle)
     - **Show value:** Yes
   - **Number format:**
     - Type: Number
     - Decimal places: 0
     - Suffix: "/100"
   - **Color ranges:**
     - Range 1: 0-50 → Color: #d32f2f (Red) → Label: "High Risk"
     - Range 2: 51-75 → Color: #f57c00 (Orange) → Label: "Moderate Risk"
     - Range 3: 76-100 → Color: #2e7d32 (Green) → Label: "Low Risk"
   - **Value label:**
     - Font size: 24px
     - Font weight: Bold
     - Position: Center

### **2.3 SBA Program Recommendation Chart**

1. Add **"Pie chart"**
2. Position it next to the gauge chart (resize to ~350px wide x 300px tall)
3. **Data Setup:**
   - **Dimension:** `recommended_sba_program`
   - **Metric:** Count (this will count how many businesses use each program)
4. **Style Configuration:**
   - **Chart header:**
     - Title: "SBA Program Recommendations"
     - Subtitle: "Optimal financing program by business type"
     - Title font size: 18px
     - Title color: #1f4788
     - Show title: Yes
   - **Pie chart settings:**
     - **Show data labels:** Yes
     - **Label position:** Outside
     - **Show percentage:** Yes
     - **Show value:** Yes
   - **Color by:**
     - SBA 7(a): #1f4788 (Professional Blue)
     - SBA 504: #2e7d32 (Success Green)
   - **Legend:**
     - Position: Bottom
     - Font size: 12px
     - Show legend: Yes
   - **Data labels:**
     - Font size: 14px
     - Font weight: Bold
     - Show percentage: Yes

### **2.4 Loan Amount Comparison**

1. Add **"Bar chart"**
2. Position it in the bottom section (resize to ~800px wide x 250px tall)
3. **Data Setup:**
   - **Dimension:** `business_name`
   - **Metric:** `optimal_loan_amount`
4. **Style Configuration:**
   - **Chart header:**
     - Title: "Optimal Loan Amounts by Business"
     - Subtitle: "Recommended SBA financing amounts"
     - Title font size: 18px
     - Title color: #1f4788
     - Show title: Yes
   - **Bar chart settings:**
     - **Orientation:** Horizontal
     - **Bar color:** #1f4788 (Professional Blue)
     - **Bar spacing:** Medium
     - **Show data labels:** Yes
   - **Axes:**
     - **X-axis (values):**
       - Show axis: Yes
       - Title: "Loan Amount ($)"
       - Format: Currency
       - Decimal places: 0
       - Compact numbers: Yes
     - **Y-axis (dimensions):**
       - Show axis: Yes
       - Title: "Business"
       - Font size: 12px
   - **Data labels:**
     - Position: End of bar
     - Font size: 12px
     - Font weight: Bold
     - Format: Currency ($)
     - Decimal places: 0
     - Compact numbers: Yes
   - **Gridlines:**
     - Show major gridlines: Yes
     - Color: Light gray (#f0f0f0)

### **2.5 Risk Rating Overview Table**

1. Add **"Table"**
2. Position it in the bottom section (resize to ~800px wide x 200px tall)
3. **Data Setup:**
   - **Dimensions:** `business_name`, `institutional_risk_rating`, `recommended_sba_program`
   - **Metrics:** `credit_risk_score`, `sba_eligibility_score`, `optimal_loan_amount`
4. **Style Configuration:**
   - **Table header:**
     - Title: "Comprehensive Risk Assessment Summary"
     - Subtitle: "Complete loan analysis by business"
     - Title font size: 18px
     - Title color: #1f4788
     - Show title: Yes
   - **Table settings:**
     - **Show header:** Yes
     - **Header background:** #f8f9fa (Light gray)
     - **Header text color:** #1f4788
     - **Header font weight:** Bold
     - **Row height:** 40px
     - **Alternate row colors:** Yes
   - **Column formatting:**
     - **Business Name:**
       - Header: "Business"
       - Width: 200px
       - Font weight: Bold
     - **Risk Rating:**
       - Header: "Risk Rating"
       - Width: 100px
       - Text align: Center
     - **SBA Program:**
       - Header: "SBA Program"
       - Width: 120px
       - Text align: Center
     - **Credit Score:**
       - Header: "Credit Score"
       - Width: 100px
       - Format: Number
       - Suffix: "/100"
       - Text align: Center
     - **SBA Eligibility:**
       - Header: "SBA Score"
       - Width: 100px
       - Format: Number
       - Suffix: "/100"
       - Text align: Center
     - **Loan Amount:**
       - Header: "Loan Amount"
       - Width: 120px
       - Format: Currency ($)
       - Decimal places: 0
       - Text align: Right
   - **Conditional formatting:**
     - **Risk Rating column:**
       - Condition: Contains "A"
       - Background color: #e8f5e8 (Light green)
       - Text color: #2e7d32 (Green)
     - **Risk Rating column:**
       - Condition: Contains "B"
       - Background color: #fff3e0 (Light orange)
       - Text color: #f57c00 (Orange)
     - **Credit Score column:**
       - Condition: Greater than or equal to 80
       - Background color: #e8f5e8 (Light green)
       - Text color: #2e7d32 (Green)
     - **Credit Score column:**
       - Condition: Less than 65
       - Background color: #ffebee (Light red)
       - Text color: #d32f2f (Red)

---

## **STEP 3: Add Interactivity (5 minutes)**

### **3.1 Business Filter**
1. Click **"Add a control"** → **"Drop-down list"**
2. Place at the top of the dashboard
3. **Configure:**
   - **Control field:** `business_name`
   - **Default value:** "All"
4. **Style:**
   - Label: "Select Business"
   - Width: 200px

### **3.2 Business Type Filter**
1. Add another **"Drop-down list"**
2. **Configure:**
   - **Control field:** `business_type`
   - **Default value:** "All"
3. **Style:**
   - Label: "Business Type"
   - Width: 200px

### **3.3 Cross-Chart Filtering**
1. Select each chart
2. In the **"Setup"** tab, enable **"Cross-filtering"**
3. This allows clicking on one chart to filter others

---

## **STEP 4: Professional Styling (10 minutes)**

### **4.1 Color Scheme**
Apply consistent colors across all charts:
- **Primary:** #1f4788 (Professional Blue)
- **Secondary:** #2e7d32 (Success Green)
- **Accent:** #f57c00 (Warning Orange)
- **Alert:** #d32f2f (Error Red)

### **4.2 Typography**
- **Header:** Arial, 24px, Bold
- **Subheader:** Arial, 18px, Bold
- **Body:** Arial, 14px, Regular
- **Metrics:** Arial, 36px, Bold

### **4.3 Layout**
1. **Header Section:**
   - Title: "Financial Institution Analysis Dashboard"
   - Subtitle: "Universal Business Analysis Engine"
   - Date: Current date

2. **Summary Cards Row:**
   - Credit Score | DSCR | SBA Eligibility

3. **Main Visualizations:**
   - Credit Risk Gauge (left)
   - SBA Program Pie Chart (right)

4. **Detail Section:**
   - Loan Amount Bar Chart
   - Risk Rating Table

### **4.4 Mobile Optimization**
1. Click **"View"** → **"Mobile layout"**
2. Adjust chart sizes for mobile viewing
3. Stack charts vertically for better mobile experience

---

## **STEP 5: Test & Validate (5 minutes)**

### **5.1 Data Verification**
1. **Check totals:** Ensure all 4 businesses appear
2. **Verify filters:** Test business and type filters
3. **Validate formatting:** Currency, percentages, decimals

### **5.2 User Experience**
1. **Click interactions:** Test cross-chart filtering
2. **Mobile view:** Check responsive design
3. **Load time:** Ensure <3 second load time

### **5.3 Business Logic**
1. **Credit scores:** Should range 68-82
2. **DSCR values:** Should show 1.28-1.62
3. **SBA programs:** Should show both 7(a) and 504
4. **Risk ratings:** Should show A-, B, B+

---

## **STEP 6: Share & Distribute (5 minutes)**

### **6.1 Set Permissions**
1. Click **"Share"** → **"Manage access"**
2. **Add SBA brokers:** Give "Viewer" access
3. **Add bank analysts:** Give "Viewer" access
4. **Team members:** Give "Editor" access

### **6.2 Create Shareable Link**
1. Click **"Share"** → **"Get link"**
2. Set to **"Anyone with link can view"**
3. Copy the link for client presentations

### **6.3 Export Options**
1. **PDF export:** For client presentations
2. **Email scheduling:** For automated reports
3. **Embed code:** For client portals

---

## **STEP 7: Connect Real Data (Next Phase)**

### **7.1 Update Data Source**
When you run your Universal Business Analysis Engine:
1. Export results to the same BigQuery table structure
2. Looker Studio will automatically update
3. All visualizations will refresh with new data

### **7.2 Automation**
1. **Schedule data refresh:** Set up automated exports
2. **Email reports:** Schedule weekly/monthly reports
3. **Real-time updates:** Configure live data connection

---

## **🎯 SUCCESS CRITERIA**

**Your dashboard is ready when:**
- ✅ Shows credit risk scores for all businesses
- ✅ Displays SBA program recommendations
- ✅ Provides interactive filtering
- ✅ Loads in under 3 seconds
- ✅ Works on mobile devices
- ✅ Can be shared with stakeholders

**Expected Impact:**
- **SBA brokers:** Instantly see loan eligibility and structure
- **Bank analysts:** Quick credit risk assessment
- **Decision makers:** Clear go/no-go indicators
- **Clients:** Professional presentation of analysis

---

## **NEXT STEPS**

1. **Test the dashboard:** Follow steps 1-6 above
2. **Gather feedback:** Share with an SBA broker or bank analyst
3. **Iterate:** Adjust based on user feedback
4. **Scale:** Add more businesses from your analysis engine
5. **Expand:** Create additional dashboards for PE firms, EDCs

**Questions or Issues?**
- Can't connect to BigQuery? Check your Google account permissions
- Charts not displaying? Verify field types and configurations
- Performance issues? Optimize chart complexity and data filters
- Need customization? Adjust colors, fonts, and layouts

**🚀 You now have a professional financial institution dashboard that showcases the power of your Universal Business Analysis Engine!**