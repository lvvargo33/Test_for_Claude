# 📅 MONDAY MORNING ROUTINE
## Wisconsin Business Data Collection

### ⏰ WHEN: Every Monday at 7:00 AM

### 🎯 PURPOSE: 
Get fresh Wisconsin business registrations for consulting prospects

---

## 📋 EXACT STEPS TO FOLLOW:

### 1. Open Terminal/Command Line

### 2. Copy and paste these commands ONE BY ONE:

```bash
cd /workspaces/Test_for_Claude/Business/wisconsin_data_collection
```

```bash
export GOOGLE_APPLICATION_CREDENTIALS="location-optimizer-1-449414f93a5a.json"
```

```bash
python3 weekly_dfi_collection.py
```

### 3. Watch the results:
- Should say "Weekly DFI Collection - Monday Morning Update"
- Will show businesses found (restaurants, salons, fitness, etc.)
- Should end with "✅ Weekly collection completed successfully!"

### 4. Expected time: **2-3 minutes**

---

## 📊 WHAT THIS DOES:
- Collects Wisconsin businesses registered in the last 7 days
- Adds new restaurants, salons, fitness centers, auto shops, retail stores
- Updates your BigQuery database automatically
- Prevents duplicates
- Creates fresh prospect list for your consulting business

---

## 🔍 HOW TO CHECK RESULTS:
After running, you can verify new data was added by running:

```bash
python3 check_dfi_data_detailed.py
```

---

## 📱 SET PHONE REMINDER:
**Title:** "Wisconsin Business Data Collection"  
**Time:** Every Monday 7:00 AM  
**Note:** "Run 3 commands - takes 2 minutes"  

---

## ❗ IF SOMETHING GOES WRONG:
1. Check error message
2. Make sure you're in the right directory
3. Verify internet connection
4. Try running the commands again

---

## 📈 SUCCESS METRICS:
- **Week 1:** Expect 5-15 new businesses
- **Growing database** of fresh Wisconsin prospects
- **Earlier outreach** = better conversion rates