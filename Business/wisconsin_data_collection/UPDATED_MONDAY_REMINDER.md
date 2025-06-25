# 📅 COMPREHENSIVE DATA REFRESH GUIDE
## Wisconsin Business Intelligence Platform

### 🎯 QUICK REFERENCE - WHAT TO RUN WHEN:

| **Frequency** | **Command** | **Time Required** | **What It Does** |
|---------------|-------------|-------------------|------------------|
| **Every Monday** | `python comprehensive_data_refresh.py` | 2-3 minutes | Fresh DFI business registrations |
| **Monthly** | `python comprehensive_data_refresh.py --monthly` | 5-8 minutes | DFI + data health checks |
| **Quarterly** | `python comprehensive_data_refresh.py --quarterly` | 10-15 minutes | + Current year census data |
| **Annually** | `python comprehensive_data_refresh.py --annual` | 20-30 minutes | + Historical census (2013-2023) |

---

## 📋 WEEKLY ROUTINE (Every Monday 7:00 AM)

### 1. Open Terminal/Command Line

### 2. Navigate to project directory:
```bash
cd /workspaces/Test_for_Claude/Business/wisconsin_data_collection
```

### 3. Set credentials:
```bash
export GOOGLE_APPLICATION_CREDENTIALS="location-optimizer-1-449414f93a5a.json"
```

### 4. Run weekly refresh:
```bash
python comprehensive_data_refresh.py
```

**Expected output:**
- "🔄 DFI Business Registrations - Weekly Refresh"
- Shows new restaurants, salons, fitness centers, etc.
- "✅ Weekly collection completed successfully!"

---

## 🗓️ MONTHLY ROUTINE (First Monday of Month)

### Run monthly refresh instead:
```bash
python comprehensive_data_refresh.py --monthly
```

**What this adds:**
- DFI registrations (same as weekly)
- SBA loans status check
- Business licenses status check
- Data health monitoring

---

## 📊 QUARTERLY ROUTINE (January, April, July, October)

### Run quarterly refresh:
```bash
python comprehensive_data_refresh.py --quarterly
```

**What this adds:**
- Everything from monthly
- Current year census data update
- Population estimate checks

---

## 📈 ANNUAL ROUTINE (January)

### Run annual refresh:
```bash
python comprehensive_data_refresh.py --annual
```

**What this adds:**
- Everything from quarterly
- Full historical census refresh (2013-2023)
- Complete population estimates update
- ⚠️ **Takes 20-30 minutes** - plan accordingly

---

## 🔧 INDIVIDUAL DATA SOURCE COMMANDS

### Just check data status (no updates):
```bash
python comprehensive_data_refresh.py --check-status
```

### Refresh specific data sources:
```bash
# DFI registrations only
python comprehensive_data_refresh.py --dfi-only

# Current year census only
python comprehensive_data_refresh.py --census-only

# Historical census data (2013-2023)
python comprehensive_data_refresh.py --historical-census

# Population estimates only
python comprehensive_data_refresh.py --population-estimates

# Force refresh everything
python comprehensive_data_refresh.py --all
```

---

## 📱 PHONE REMINDERS TO SET:

### Weekly (Every Monday 7:00 AM):
**Title:** "Wisconsin Business Data - Weekly"  
**Command:** `python comprehensive_data_refresh.py`  
**Time:** 2-3 minutes

### Monthly (First Monday 7:00 AM):
**Title:** "Wisconsin Business Data - Monthly"  
**Command:** `python comprehensive_data_refresh.py --monthly`  
**Time:** 5-8 minutes

### Quarterly (First Monday of Jan/Apr/Jul/Oct 7:00 AM):
**Title:** "Wisconsin Business Data - Quarterly"  
**Command:** `python comprehensive_data_refresh.py --quarterly`  
**Time:** 10-15 minutes

### Annual (First Monday of January 7:00 AM):
**Title:** "Wisconsin Business Data - Annual"  
**Command:** `python comprehensive_data_refresh.py --annual`  
**Time:** 20-30 minutes

---

## 📊 DATA SOURCES INCLUDED:

### 🔄 **Auto-Updated:**
- **DFI Business Registrations** (Weekly)
- **Current Year Census Data** (Quarterly)
- **Historical Census Data** (Annual: 2013-2023)
- **Population Estimates** (Annual)

### 🔍 **Monitored/Checked:**
- **SBA Loan Data** (Monthly check - manual update needed if outdated)
- **Business Licenses** (Monthly check - varies by municipality)

---

## ❗ TROUBLESHOOTING:

### If error occurs:
1. Check you're in the right directory
2. Verify credentials file exists
3. Check internet connection
4. Try running again
5. Use `--check-status` to diagnose issues

### If data seems outdated:
```bash
python comprehensive_data_refresh.py --check-status
```

### For fresh start:
```bash
python comprehensive_data_refresh.py --all
```

---

## 📈 SUCCESS METRICS:

### Weekly:
- 5-15 new Wisconsin businesses
- Fresh prospect list for consulting outreach

### Monthly:
- Data health monitoring
- SBA/license data status awareness

### Quarterly:
- Updated demographic insights
- Current year population trends

### Annual:
- Complete historical perspective (11 years)
- Full population growth analysis
- Comprehensive market intelligence

---

## 🎯 BUSINESS IMPACT:

**Early Data = Better Outreach = Higher Conversion**

- Get business registrations within 7 days
- Reach prospects before competitors
- Use demographic data for targeting
- Track market trends over time