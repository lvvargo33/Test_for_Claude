# Manual Data Integration Guide
## Section 1.5: Site Evaluation & Location Intelligence

This guide explains how to integrate manual site data into the client report.

---

## **PROCESS OVERVIEW**

### **Step 1: Complete Site Assessment**
1. Visit 5264 Anton Dr and surrounding area
2. Fill out `SITE_DATA_ENTRY_TEMPLATE.md` completely
3. Take photos for visual documentation
4. Verify all entries for accuracy

### **Step 2: Data Validation**
Before integration, ensure all required fields are completed:

**Critical Required Fields:**
- Property address and square footage
- Lease rates and terms
- Parking count and accessibility
- Traffic visibility assessment
- Zoning compliance status
- Utility availability
- Competition count within 1 mile

### **Step 3: Integration Process**

#### **A. Traffic Data Integration**
**Automated Data (No manual entry needed):**
- Fish Hatchery Road traffic counts
- McKee Road intersection volumes
- Verona Road (US 18/151) traffic

**Manual Entry Required:**
Replace `[MANUAL ENTRY]` placeholders with data from template:
- Anton Drive traffic observations
- Peak hour traffic patterns
- Directional traffic split
- Speed limit impacts

#### **B. Property Data Integration**
Replace placeholders in CLIENT_REPORT with template data:

```
FROM TEMPLATE: **Building Square Footage:** [NUMBER] sq ft
TO REPORT: **Total Building Area:** 3,500 square feet

FROM TEMPLATE: **Asking Rent:** $[NUMBER] per sq ft annually  
TO REPORT: **Cost per Square Foot:** $18.50 annually

FROM TEMPLATE: **Overall Condition:** [Excellent/Good/Fair/Poor]
TO REPORT: **Overall Condition:** Good condition
```

#### **C. Visual Assessment Integration**
Replace visibility placeholders:

```
FROM TEMPLATE: **Street Visibility:** [Excellent/Good/Fair/Poor]
TO REPORT: **Street Visibility Rating:** Excellent

FROM TEMPLATE: **Monument Sign Rights:** [Yes/No/Shared]
TO REPORT: **Monument Sign Rights:** Shared with shopping center
```

### **Step 4: Calculations**

#### **Automatic Calculations to Perform:**
1. **Parking Ratio:** Total spaces ÷ (Building sq ft ÷ 1,000)
2. **Total Occupancy Cost:** Base rent + CAM + taxes + insurance
3. **Cost per sq ft:** Total occupancy cost ÷ building square footage

#### **Score Calculations:**
**Site Evaluation Score Components (1-10 scale):**
- **Traffic Volume:** 8+ (>15,000 AADT), 6-8 (10-15K), 4-6 (5-10K), <4 (<5K)
- **Visibility:** 8+ (Excellent), 6-8 (Good), 4-6 (Fair), <4 (Poor)
- **Accessibility:** 8+ (Easy access), 6-8 (Good), 4-6 (Moderate), <4 (Difficult)
- **Parking:** 8+ (Adequate/excess), 6-8 (Sufficient), 4-6 (Tight), <4 (Inadequate)
- **Competition:** 8+ (Low competition), 6-8 (Moderate), 4-6 (High), <4 (Saturated)
- **Cost:** 8+ (Below market), 6-8 (Market rate), 4-6 (Above market), <4 (Expensive)

**Final Score:** Average of all components

### **Step 5: Narrative Generation**

#### **Auto-Generate Key Strengths:**
Based on high scores (7+), populate:
```
✅ Excellent visibility from Fish Hatchery Road
✅ Adequate parking with 45 spaces (3.2 ratio)
✅ Zoning permits restaurant use by right
✅ [Additional strengths from high-scoring areas]
```

#### **Auto-Generate Challenges:**
Based on low scores (<6), populate:
```
⚠️ Limited natural gas service requires connection
⚠️ High competition density (3 Asian restaurants within 1 mile)
⚠️ Premium rent rate ($22/sq ft above market average)
⚠️ [Additional challenges from low-scoring areas]
```

### **Step 6: Strategic Recommendations**
Generate based on data analysis:

**High traffic + Good visibility + Zoning compliance = **
"Proceed with lease negotiations - location fundamentals strong"

**Infrastructure gaps + High rent = **
"Negotiate tenant improvement allowance for utility upgrades"

**High competition + Market gaps = **
"Focus on underserved price point/cuisine combination"

---

## **QUALITY CONTROL CHECKLIST**

### **Before Publishing:**
- [ ] All [MANUAL ENTRY] placeholders replaced
- [ ] Calculations completed correctly
- [ ] Site score calculated and justified
- [ ] Key findings match manual observations
- [ ] Visual components specified match available data
- [ ] Risk factors clearly identified
- [ ] Recommendations actionable and specific

### **Data Verification:**
- [ ] Property details verified with landlord/agent
- [ ] Zoning confirmed with city planning office
- [ ] Competition count verified through field observation
- [ ] Traffic patterns observed during peak hours
- [ ] Parking adequacy tested during busy periods

---

## **EXAMPLE INTEGRATION**

### **Template Entry:**
```
Property Address: 5264 Anton Dr, Fitchburg, WI 53711
Property Type: Strip Mall - End Unit
Building Square Footage: 3,200 sq ft
Asking Rent: $18.50 per sq ft annually
Base Rent: $4,933 per month
Overall Condition: Good
Street Visibility: Excellent
Total Parking Spaces: 48
```

### **Integrated Report Section:**
```
**Building & Property Details:**
- Total Building Area: 3,200 square feet
- Property Type: Strip mall end unit
- Overall Condition: Good condition
- Restaurant Readiness: Minor renovations needed

**Lease/Purchase Economics:**
- Base Rent: $4,933 per month
- Cost per Square Foot: $18.50 annually
- Total Occupancy Cost: $5,847 monthly (estimated)

**Site Visibility Analysis:**
- Street Visibility Rating: Excellent
- High visibility from Fish Hatchery Road main traffic flow

**Parking Assessment:**
- Total Parking Spaces: 48 spaces
- Parking Ratio: 15.0 spaces per 1,000 sq ft
- Peak Hour Availability: Adequate during field observation
```

---

## **TEMPLATE COMPLETION PRIORITY**

### **Phase 1 - Essential Data (Complete First)**
1. Property specifications and lease terms
2. Traffic visibility and signage assessment
3. Parking and accessibility evaluation
4. Basic zoning compliance check

### **Phase 2 - Enhanced Analysis**
5. Detailed competition mapping
6. Infrastructure capacity assessment
7. Development timeline planning
8. Risk factor evaluation

### **Phase 3 - Strategic Insights**
9. Market positioning analysis
10. Neighborhood character assessment
11. Long-term development considerations

---

**COMPLETION TARGET:** 2-3 hours for comprehensive site assessment + 1 hour for data integration = Professional-grade location analysis ready for client delivery.