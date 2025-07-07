# Universal Business Analysis Engine - Workflow Guardian Prompt

## 🔒 COPY AND PASTE THIS PROMPT TO ENSURE WORKFLOW COMPLIANCE

---

**CRITICAL WORKFLOW INSTRUCTION:**

I am working on the Universal Business Analysis Engine for Wisconsin business feasibility studies. This system orchestrates complete client reports through universal templates and handles manual data entry pause points.

**MANDATORY WORKFLOW RULES - YOU MUST ENFORCE THESE:**

1. **🎯 EVERYTHING MUST GO THROUGH THE UNIVERSAL ENGINE**
   - File: `UNIVERSAL_BUSINESS_ANALYSIS_ENGINE.py`
   - Never create standalone analysis files that bypass this engine
   - All client work must use the universal template system

2. **📝 WHEN ADDING NEW SECTIONS OR FEATURES:**
   - ALWAYS update `sections_config` in `UNIVERSAL_BUSINESS_ANALYSIS_ENGINE.py` first
   - Create universal templates following pattern: `UNIVERSAL_[SECTION]_TEMPLATE.md`
   - Update `PROJECT_REGISTRY.md` to reflect new capabilities
   - Mark new sections as `"implemented": False` until fully tested

3. **⏸️ MANUAL DATA ENTRY POINTS:**
   - Identify any manual data requirements upfront
   - Configure them in the engine's pause system
   - Never create manual processes outside the engine framework

4. **🔄 FILE UPDATE HIERARCHY:**
   ```
   PRIMARY: UNIVERSAL_BUSINESS_ANALYSIS_ENGINE.py (always update first)
   SECONDARY: PROJECT_REGISTRY.md (track what's implemented)
   TERTIARY: MOCK_ANALYSIS_PROGRESS.md (development progress)
   TEMPLATES: UNIVERSAL_*_TEMPLATE.md files (section-specific)
   ```

5. **✅ BEFORE ANY CHANGES:**
   - Ask: "Does this integrate with the Universal Engine?"
   - Test: Run `python UNIVERSAL_BUSINESS_ANALYSIS_ENGINE.py --help` after changes
   - Verify: All referenced templates exist and work

6. **🚨 REJECT THESE APPROACHES:**
   - Creating one-off client reports without universal templates
   - Building standalone analysis scripts
   - Manual processes that don't integrate with pause system
   - Bypassing the engine for "quick fixes"

**CURRENT SYSTEM STATUS:**
- ✅ 6 sections implemented (1.1, 1.2, 1.3, 1.4, 1.5, 2.1)
- ⏸️ 1 manual data pause point (Section 1.5 - Site Evaluation)
- 🔄 6 future sections planned (2.2-6.2) with 4+ additional manual pause points

**CORE GOAL:** One command generates complete professional client deliverable:
```bash
python UNIVERSAL_BUSINESS_ANALYSIS_ENGINE.py --business="Any Business" --address="Any Address"
```

**IF I ASK YOU TO CREATE ANYTHING THAT DOESN'T FOLLOW THIS WORKFLOW:**
- Remind me of these rules
- Suggest how to integrate it properly with the Universal Engine
- Refuse to create bypasses or standalone solutions
- Guide me back to the universal template system

**ENFORCE THIS WORKFLOW AT ALL TIMES - NO EXCEPTIONS!**

---

## Quick Reference Commands:

```bash
# Test engine functionality
python UNIVERSAL_BUSINESS_ANALYSIS_ENGINE.py --help

# Start new analysis
python UNIVERSAL_BUSINESS_ANALYSIS_ENGINE.py --business="Restaurant" --address="123 Main St, City, WI"

# Continue after manual data
python UNIVERSAL_BUSINESS_ANALYSIS_ENGINE.py --continue-project=project_name

# List all projects
python UNIVERSAL_BUSINESS_ANALYSIS_ENGINE.py --list-projects
```

---

**⚠️ REMINDER: If it's not in the Universal Business Analysis Engine, it doesn't exist in our workflow!**