#!/usr/bin/env python3
"""
Implementation Plan Generator for Section 6.2
=============================================

Creates detailed, actionable implementation roadmaps for business launches.
Converts analysis findings and recommendations into time-phased execution plans
with specific tasks, timelines, budgets, and success metrics.
"""

import json
import os
from datetime import datetime, timedelta
from typing import Dict, List, Any, Optional, Tuple
import logging

logger = logging.getLogger(__name__)


class ImplementationPlanResult:
    """Container for detailed implementation plan"""
    
    def __init__(self, business_type: str, location: str):
        self.business_type = business_type
        self.location = location
        
        # Timeline
        self.total_weeks = 27
        self.total_months = 7
        
        # Phase 1: Foundation & Planning
        self.entity_formation_timeline = "5-7 business days"
        self.entity_formation_cost = 1500
        self.financial_setup_cost = 500
        self.monthly_insurance_cost = 1200
        self.site_securing_timeline = "10-14 days"
        self.site_securing_costs = 15000
        self.professional_services_budget = 25000
        
        # Phase 2: Permits & Design
        self.permit_application_schedule = ""
        self.permit_approval_timeline = ""
        self.design_approval_date = ""
        self.permit_submission_date = ""
        self.expected_approval_date = ""
        
        # Phase 3: Construction
        self.construction_phase1_details = ""
        self.construction_phase2_details = ""
        self.construction_phase3_details = ""
        self.staffing_budget = 0
        self.marketing_launch_budget = 0
        
        # Phase 4: Soft Launch
        self.service_time_target = ""
        self.quality_standards = ""
        self.satisfaction_target = 90
        
        # Phase 5: Grand Opening
        self.grand_opening_date = ""
        self.grand_opening_marketing_plan = ""
        self.community_engagement_activities = ""
        self.promotional_strategy_details = ""
        
        # Financial Implementation
        self.phase1_capital = 0
        self.phase2_capital = 0
        self.phase3_capital = 0
        self.phase4_capital = 0
        self.working_capital = 0
        self.total_capital_deployment = 0
        
        # Revenue Projections
        self.month1_revenue = 0
        self.month1_capacity = 40
        self.month2_revenue = 0
        self.month2_capacity = 55
        self.month3_revenue = 0
        self.month3_capacity = 70
        self.month6_revenue = 0
        self.month6_capacity = 85
        
        # Risk Management
        self.contingency_percentage = 15
        self.line_of_credit = 50000
        self.operational_contingency_plans = ""
        self.market_contingency_plans = ""
        
        # Performance Monitoring
        self.quarterly_review_framework = ""
        
        # Technology
        self.technology_investment = 0
        
        # Vendors
        self.primary_supplier_list = ""
        self.backup_supplier_list = ""
        self.payment_terms = "Net 30"
        self.delivery_schedules = ""
        self.quality_agreements = ""
        self.contract_terms = "1-year minimum"
        
        # Marketing
        self.launch_marketing_budget = 0
        self.monthly_marketing_budget = 0
        self.marketing_channel_allocation = ""
        self.target_cac = 0
        self.target_ltv = 0
        self.target_roi_ratio = 3
        
        # Staffing
        self.gm_hire_date = ""
        self.am_hire_date = ""
        self.dept_heads_hire_date = ""
        self.staff_hiring_schedule = ""
        self.training_budget = 0
        
        # Success Metrics
        self.day30_revenue_target = 60
        self.day30_satisfaction = 85
        self.day90_revenue_target = 80
        self.day90_efficiency = 85
        self.day90_customers = 500
        self.year1_revenue_goal = 15
        self.year1_profit_margin = 12
        self.year1_market_share = 5
        self.year1_retention = 75
        
        # Critical Path
        self.critical_actions = []
        self.key_dependencies = ""
        self.decision_points = []
        
        # Resources
        self.professional_contacts = ""
        self.government_contacts = ""
        self.industry_resources = ""
        
        # Final Recommendation
        self.final_implementation_priorities = ""
        self.total_implementation_capital = 0
        self.roi_timeline = ""
        self.success_probability = 0
        
        # Funding Structure Recommendations
        self.funding_structure_recommendation = ""
        self.recommended_loan_type = ""
        self.loan_type_rationale = ""
        self.total_funding_needed = 0
        self.equity_requirement = 0
        self.debt_financing = 0
        self.sba_loan_amount = 0
        self.conventional_loan_amount = 0
        self.working_capital_line = 0
        self.equity_percentage = 0
        self.debt_percentage = 0
        self.loan_terms = {}
        self.drawdown_schedule = []
        self.funding_timeline = ""
        self.institutional_recommendations = {}
        self.collateral_requirements = ""
        self.personal_guarantee_requirements = ""
        self.debt_service_coverage_ratio = 0
        self.loan_to_value_ratio = 0


class ImplementationPlanGenerator:
    """Generates detailed implementation plans based on all analyses"""
    
    def __init__(self):
        self.phase_durations = {
            "foundation": 4,      # weeks
            "permits": 8,         # weeks
            "construction": 12,   # weeks
            "soft_launch": 2,     # weeks
            "stabilization": 12   # weeks post-launch
        }
        
    def generate_implementation_plan(self, business_type: str, location: str,
                                   integrated_data: Dict[str, Any],
                                   recommendations_data: Dict[str, Any]) -> ImplementationPlanResult:
        """Generate comprehensive implementation plan"""
        
        logger.info(f"Generating implementation plan for {business_type} in {location}")
        
        plan = ImplementationPlanResult(business_type, location)
        
        # Calculate timeline based on complexity
        self._calculate_implementation_timeline(plan, integrated_data)
        
        # Phase 1: Foundation & Planning
        self._generate_foundation_phase(plan, business_type, integrated_data)
        
        # Phase 2: Permits & Design
        self._generate_permits_phase(plan, integrated_data)
        
        # Phase 3: Construction & Preparation
        self._generate_construction_phase(plan, business_type, integrated_data)
        
        # Phase 4: Soft Launch
        self._generate_soft_launch_phase(plan, business_type)
        
        # Phase 5: Grand Opening
        self._generate_grand_opening_phase(plan, business_type, location)
        
        # Financial Implementation Schedule
        self._generate_financial_schedule(plan, integrated_data, recommendations_data)
        
        # Risk Management Implementation
        self._generate_risk_management_plan(plan, integrated_data)
        
        # Technology Implementation
        self._generate_technology_plan(plan, business_type)
        
        # Vendor & Supply Chain
        self._generate_vendor_plan(plan, business_type)
        
        # Marketing Implementation
        self._generate_marketing_plan(plan, business_type, integrated_data)
        
        # Staffing Implementation
        self._generate_staffing_plan(plan, business_type, integrated_data)
        
        # Success Metrics
        self._generate_success_metrics(plan, integrated_data)
        
        # Critical Path
        self._identify_critical_path(plan, business_type, integrated_data)
        
        # Resources
        self._compile_resources(plan, location)
        
        # Final Recommendations
        self._generate_final_priorities(plan, integrated_data, recommendations_data)
        
        # NEW: Funding Structure Recommendations
        self._generate_funding_structure_recommendations(plan, integrated_data, recommendations_data)
        
        return plan
    
    def _calculate_implementation_timeline(self, plan: ImplementationPlanResult,
                                         integrated_data: Dict[str, Any]):
        """Calculate total implementation timeline based on complexity"""
        
        # Base timeline
        total_weeks = sum(self.phase_durations.values())
        
        # Adjust for permit complexity
        if "permits" in integrated_data:
            permits = integrated_data["permits"]
            if permits.get("total_timeline_weeks", 12) > 12:
                total_weeks += 4  # Add buffer for complex permits
        
        # Adjust for construction complexity
        if "infrastructure" in integrated_data:
            infra = integrated_data["infrastructure"]
            if infra.get("infrastructure_investment_required", 0) > 50000:
                total_weeks += 4  # Major infrastructure work
        
        plan.total_weeks = total_weeks
        plan.total_months = round(total_weeks / 4.33)
    
    def _generate_foundation_phase(self, plan: ImplementationPlanResult,
                                  business_type: str,
                                  integrated_data: Dict[str, Any]):
        """Generate foundation and planning phase details"""
        
        # Entity formation costs vary by business type
        if "restaurant" in business_type.lower():
            plan.entity_formation_cost = 2000
            plan.monthly_insurance_cost = 1500
        elif "retail" in business_type.lower():
            plan.entity_formation_cost = 1500
            plan.monthly_insurance_cost = 1000
        else:
            plan.entity_formation_cost = 1500
            plan.monthly_insurance_cost = 1200
        
        # Site securing costs from real estate data
        if "cost" in integrated_data:
            cost_data = integrated_data["cost"]
            # Typically first month + last month + security deposit
            monthly_rent = cost_data.get("realistic_monthly_operating", 50000) * 0.15  # Assume 15% for rent
            plan.site_securing_costs = int(monthly_rent * 3)
        
        # Professional services budget
        plan.professional_services_budget = 25000 if plan.total_weeks > 30 else 15000
    
    def _generate_permits_phase(self, plan: ImplementationPlanResult,
                               integrated_data: Dict[str, Any]):
        """Generate permits and design phase details"""
        
        # From permits analysis
        if "permits" in integrated_data:
            permits = integrated_data["permits"]
            timeline_weeks = permits.get("total_timeline_weeks", 12)
            
            plan.permit_application_schedule = (
                "Week 5: Submit zoning verification and business license applications\n"
                "Week 6: Submit building permit with architectural plans\n"
                "Week 7: Submit health department applications (if applicable)\n"
                "Week 8: Submit state-specific industry licenses"
            )
            
            plan.permit_approval_timeline = (
                f"Total permit processing time: {timeline_weeks} weeks\n"
                "Expedited processing available for additional fees\n"
                "Regular follow-ups with permit offices recommended"
            )
        else:
            plan.permit_application_schedule = "Standard 8-12 week permit timeline"
            plan.permit_approval_timeline = "Allow 12 weeks for full permit approval"
        
        # Set milestone dates
        start_date = datetime.now() + timedelta(weeks=4)
        plan.design_approval_date = (start_date + timedelta(weeks=3)).strftime("%B %d, %Y")
        plan.permit_submission_date = (start_date + timedelta(weeks=4)).strftime("%B %d, %Y")
        plan.expected_approval_date = (start_date + timedelta(weeks=12)).strftime("%B %d, %Y")
    
    def _generate_construction_phase(self, plan: ImplementationPlanResult,
                                   business_type: str,
                                   integrated_data: Dict[str, Any]):
        """Generate construction and preparation phase details"""
        
        # Construction phases based on business type
        if "restaurant" in business_type.lower():
            plan.construction_phase1_details = (
                "- Kitchen infrastructure and plumbing\n"
                "- Electrical upgrades for equipment\n"
                "- HVAC system installation\n"
                "- Fire suppression system"
            )
            plan.construction_phase2_details = (
                "- Kitchen equipment installation\n"
                "- Dining room build-out\n"
                "- Bar construction (if applicable)\n"
                "- Restroom upgrades"
            )
            plan.construction_phase3_details = (
                "- POS system installation\n"
                "- Furniture and fixtures\n"
                "- Exterior signage\n"
                "- Final finishes and décor"
            )
        else:
            plan.construction_phase1_details = (
                "- Structural modifications\n"
                "- Electrical and plumbing updates\n"
                "- HVAC optimization\n"
                "- Security system pre-wire"
            )
            plan.construction_phase2_details = (
                "- Interior build-out\n"
                "- Display fixtures installation\n"
                "- Lighting design implementation\n"
                "- Technology infrastructure"
            )
            plan.construction_phase3_details = (
                "- Equipment installation\n"
                "- Inventory systems setup\n"
                "- Signage and branding\n"
                "- Final touches"
            )
        
        # Staffing budget from labor analysis
        if "cost" in integrated_data:
            cost_data = integrated_data["cost"]
            monthly_labor = cost_data.get("realistic_monthly_operating", 50000) * 0.35
            plan.staffing_budget = int(monthly_labor * 2)  # 2 months training/ramp-up
        else:
            plan.staffing_budget = 35000
        
        # Marketing launch budget
        plan.marketing_launch_budget = 15000
    
    def _generate_soft_launch_phase(self, plan: ImplementationPlanResult,
                                   business_type: str):
        """Generate soft launch phase details"""
        
        if "restaurant" in business_type.lower():
            plan.service_time_target = "Under 15 minutes for quick service, under 45 minutes for full service"
            plan.quality_standards = "100% order accuracy, consistent food quality, proper temperatures"
        elif "retail" in business_type.lower():
            plan.service_time_target = "Checkout under 5 minutes, customer assistance within 2 minutes"
            plan.quality_standards = "Product knowledge demonstration, inventory accuracy, display standards"
        else:
            plan.service_time_target = "Service delivery within promised timeframes"
            plan.quality_standards = "Quality assurance checklist completion, customer satisfaction"
        
        plan.satisfaction_target = 90
    
    def _generate_grand_opening_phase(self, plan: ImplementationPlanResult,
                                     business_type: str, location: str):
        """Generate grand opening phase details"""
        
        # Set grand opening date
        opening_date = datetime.now() + timedelta(weeks=plan.total_weeks)
        plan.grand_opening_date = opening_date.strftime("%B %d, %Y")
        
        # Marketing plan
        plan.grand_opening_marketing_plan = (
            "- Social media countdown campaign (2 weeks prior)\n"
            "- Local media press releases and interviews\n"
            "- Grand opening event with special promotions\n"
            "- Influencer partnerships and reviews\n"
            "- Email marketing to pre-launch list\n"
            "- Paid digital advertising blitz"
        )
        
        # Community engagement
        plan.community_engagement_activities = (
            "- Chamber of Commerce ribbon cutting\n"
            "- Local charity partnership announcement\n"
            "- Neighborhood association introduction\n"
            "- Free samples/demonstrations\n"
            "- Local vendor showcase"
        )
        
        # Promotional strategy
        if "restaurant" in business_type.lower():
            plan.promotional_strategy_details = (
                "- 20% off grand opening week\n"
                "- Free appetizer with entrée purchase\n"
                "- Loyalty program double points\n"
                "- Social media check-in specials"
            )
        else:
            plan.promotional_strategy_details = (
                "- Grand opening discount (15-25% off)\n"
                "- Gift with purchase promotions\n"
                "- Raffle for grand prize\n"
                "- Early bird specials"
            )
    
    def _generate_financial_schedule(self, plan: ImplementationPlanResult,
                                   integrated_data: Dict[str, Any],
                                   recommendations_data: Dict[str, Any]):
        """Generate financial implementation schedule"""
        
        # Get capital requirements from recommendations
        if recommendations_data:
            plan.total_implementation_capital = recommendations_data.get("total_capital_needed", 350000)
        elif "cost" in integrated_data:
            cost_data = integrated_data["cost"]
            startup = cost_data.get("total_startup_costs", 250000)
            working_capital = cost_data.get("realistic_monthly_operating", 50000) * 3
            plan.total_implementation_capital = int(startup + working_capital)
        
        # Phase capital allocation
        total = plan.total_implementation_capital
        plan.phase1_capital = int(total * 0.10)  # 10% for foundation
        plan.phase2_capital = int(total * 0.15)  # 15% for permits/design
        plan.phase3_capital = int(total * 0.50)  # 50% for construction
        plan.phase4_capital = int(total * 0.05)  # 5% for soft launch
        plan.working_capital = int(total * 0.20)  # 20% working capital
        
        plan.total_capital_deployment = (plan.phase1_capital + plan.phase2_capital +
                                        plan.phase3_capital + plan.phase4_capital +
                                        plan.working_capital)
        
        # Revenue projections
        if "revenue" in integrated_data:
            revenue_data = integrated_data["revenue"]
            annual = revenue_data.get("realistic_annual", 800000)
            monthly_full = annual / 12
            
            plan.month1_revenue = int(monthly_full * 0.40)
            plan.month2_revenue = int(monthly_full * 0.55)
            plan.month3_revenue = int(monthly_full * 0.70)
            plan.month6_revenue = int(monthly_full * 0.85)
        
        # Expense management strategy
        plan.expense_management_strategy = (
            "Implement zero-based budgeting with weekly variance analysis. "
            "Maintain strict cost controls during ramp-up period. "
            "Focus on variable cost optimization and fixed cost leverage."
        )
    
    def _generate_risk_management_plan(self, plan: ImplementationPlanResult,
                                     integrated_data: Dict[str, Any]):
        """Generate risk management implementation details"""
        
        plan.contingency_percentage = 15
        plan.line_of_credit = 50000
        
        # Operational contingencies
        plan.operational_contingency_plans = (
            "- Cross-training program for all positions\n"
            "- Vendor backup relationships established\n"
            "- Equipment service contracts in place\n"
            "- Emergency staffing agency on retainer\n"
            "- Detailed crisis management protocols"
        )
        
        # Market contingencies
        if "risk" in integrated_data:
            risk_data = integrated_data["risk"]
            if risk_data.get("market_risk_score", 50) > 60:
                plan.market_contingency_plans = (
                    "- Flexible pricing strategy ready\n"
                    "- Alternative revenue streams identified\n"
                    "- Pivot plans for service/product mix\n"
                    "- Enhanced marketing reserves\n"
                    "- Partnership opportunities mapped"
                )
            else:
                plan.market_contingency_plans = (
                    "- Standard competitive response plans\n"
                    "- Customer retention focus\n"
                    "- Market monitoring system\n"
                    "- Promotional reserves allocated"
                )
    
    def _generate_technology_plan(self, plan: ImplementationPlanResult,
                                 business_type: str):
        """Generate technology implementation plan"""
        
        # Technology investment varies by business type
        if "restaurant" in business_type.lower():
            plan.technology_investment = 25000
        elif "retail" in business_type.lower():
            plan.technology_investment = 20000
        else:
            plan.technology_investment = 15000
    
    def _generate_vendor_plan(self, plan: ImplementationPlanResult,
                             business_type: str):
        """Generate vendor and supply chain plan"""
        
        if "restaurant" in business_type.lower():
            plan.primary_supplier_list = (
                "- Food distributor: 2-3 deliveries/week\n"
                "- Beverage suppliers: Weekly delivery\n"
                "- Paper/disposables: Bi-weekly\n"
                "- Cleaning supplies: Monthly\n"
                "- Equipment maintenance: Quarterly"
            )
            plan.backup_supplier_list = (
                "- Secondary food distributor identified\n"
                "- Local supplier relationships\n"
                "- Emergency supply contacts\n"
                "- Equipment rental options"
            )
        else:
            plan.primary_supplier_list = (
                "- Primary inventory supplier(s)\n"
                "- Packaging/shipping supplies\n"
                "- Office supplies vendor\n"
                "- Technology vendors\n"
                "- Service providers"
            )
            plan.backup_supplier_list = (
                "- Alternative suppliers vetted\n"
                "- Drop-ship arrangements\n"
                "- Emergency inventory sources\n"
                "- Temporary staffing options"
            )
        
        plan.delivery_schedules = "Optimized for cash flow and storage capacity"
        plan.quality_agreements = "Quality standards and return policies documented"
    
    def _generate_marketing_plan(self, plan: ImplementationPlanResult,
                                business_type: str,
                                integrated_data: Dict[str, Any]):
        """Generate marketing implementation plan"""
        
        # Set marketing budgets
        if "revenue" in integrated_data:
            revenue_data = integrated_data["revenue"]
            monthly_revenue = revenue_data.get("realistic_annual", 800000) / 12
            plan.monthly_marketing_budget = int(monthly_revenue * 0.05)  # 5% of revenue
        else:
            plan.monthly_marketing_budget = 3500
        
        plan.launch_marketing_budget = plan.monthly_marketing_budget * 3
        
        # Channel allocation
        plan.marketing_channel_allocation = (
            "- Digital marketing: 50% (Google Ads, Facebook, Instagram)\n"
            "- Traditional media: 20% (print, radio, direct mail)\n"
            "- Content/SEO: 15% (website, blog, reviews)\n"
            "- Events/Partnerships: 10% (community engagement)\n"
            "- Reserve: 5% (opportunistic marketing)"
        )
        
        # ROI targets
        plan.target_cac = 50 if "restaurant" in business_type.lower() else 35
        plan.target_ltv = plan.target_cac * 10  # 10:1 LTV:CAC ratio
        plan.target_roi_ratio = 3
    
    def _generate_staffing_plan(self, plan: ImplementationPlanResult,
                               business_type: str,
                               integrated_data: Dict[str, Any]):
        """Generate staffing implementation plan"""
        
        # Hiring timeline
        start_date = datetime.now()
        plan.gm_hire_date = (start_date + timedelta(weeks=16)).strftime("Week %U")
        plan.am_hire_date = (start_date + timedelta(weeks=18)).strftime("Week %U")
        plan.dept_heads_hire_date = (start_date + timedelta(weeks=19)).strftime("Week %U")
        
        # Staff hiring schedule
        if "restaurant" in business_type.lower():
            plan.staff_hiring_schedule = (
                "- Kitchen Manager: Week 19\n"
                "- Head Chef: Week 20\n"
                "- Line Cooks: Week 21-22\n"
                "- Servers: Week 22-23\n"
                "- Host/Cashiers: Week 23\n"
                "- Support Staff: Week 23-24"
            )
        else:
            plan.staff_hiring_schedule = (
                "- Department Leads: Week 19-20\n"
                "- Full-time Staff: Week 21-22\n"
                "- Part-time Staff: Week 22-23\n"
                "- Seasonal/Backup: Week 24"
            )
        
        # Training budget
        if "cost" in integrated_data:
            cost_data = integrated_data["cost"]
            monthly_labor = cost_data.get("realistic_monthly_operating", 50000) * 0.35
            plan.training_budget = int(monthly_labor * 0.5)  # Half month of labor for training
        else:
            plan.training_budget = 15000
    
    def _generate_success_metrics(self, plan: ImplementationPlanResult,
                                integrated_data: Dict[str, Any]):
        """Generate success metrics and milestones"""
        
        # 30-day metrics
        plan.day30_revenue_target = 60
        plan.day30_satisfaction = 85
        
        # 90-day metrics
        plan.day90_revenue_target = 80
        plan.day90_efficiency = 85
        plan.day90_customers = 500
        
        # Adjust customer target based on business type
        if "restaurant" in plan.business_type.lower():
            plan.day90_customers = 1000
        elif "retail" in plan.business_type.lower():
            plan.day90_customers = 750
        
        # 1-year goals
        plan.year1_revenue_goal = 15  # % growth
        plan.year1_profit_margin = 12
        plan.year1_market_share = 5
        plan.year1_retention = 75
    
    def _identify_critical_path(self, plan: ImplementationPlanResult,
                              business_type: str,
                              integrated_data: Dict[str, Any]):
        """Identify critical path items and dependencies"""
        
        # Critical actions
        plan.critical_actions = [
            "Secure full funding commitment before lease signing",
            "Submit permit applications within first 30 days",
            "Lock in construction contractor by Week 8",
            "Complete hiring of management team by Week 20",
            "Achieve operational readiness certification before soft launch"
        ]
        
        # Key dependencies
        dependencies = []
        if "permits" in integrated_data:
            permits = integrated_data["permits"]
            if permits.get("total_timeline_weeks", 12) > 12:
                dependencies.append("Extended permit timeline may delay construction start")
        
        dependencies.extend([
            "Construction cannot begin until permits approved",
            "Staff training requires completed build-out",
            "Marketing launch tied to confirmed opening date",
            "Inventory ordering dependent on staff readiness"
        ])
        
        plan.key_dependencies = "\n".join([f"- {dep}" for dep in dependencies])
        
        # Decision points
        plan.decision_points = [
            ("Funding Go/No-Go", (datetime.now() + timedelta(weeks=2)).strftime("%B %d, %Y")),
            ("Construction Bid Acceptance", (datetime.now() + timedelta(weeks=8)).strftime("%B %d, %Y")),
            ("Soft Launch Readiness", (datetime.now() + timedelta(weeks=24)).strftime("%B %d, %Y"))
        ]
    
    def _compile_resources(self, plan: ImplementationPlanResult, location: str):
        """Compile resource contacts and information"""
        
        # Professional contacts
        plan.professional_contacts = (
            "- Small Business Development Center: (608) 263-7794\n"
            "- SCORE Wisconsin Mentors: score.org/wisconsin\n"
            "- Wisconsin Economic Development Corporation: wedc.org\n"
            "- Local Chamber of Commerce: [Contact based on location]\n"
            "- Industry Associations: [Specific to business type]"
        )
        
        # Government contacts
        county = self._extract_county(location)
        plan.government_contacts = (
            f"- Wisconsin DFI Business Services: (608) 261-7577\n"
            f"- {county} County Clerk: [Local contact]\n"
            f"- Municipal Building Inspector: [Local contact]\n"
            f"- State Licensing Division: dsps.wi.gov\n"
            f"- Wisconsin Department of Revenue: (608) 266-2776"
        )
        
        # Industry resources
        plan.industry_resources = (
            "- Industry trade publications\n"
            "- Professional training programs\n"
            "- Equipment suppliers and consultants\n"
            "- Marketing and branding specialists\n"
            "- Financial planning resources"
        )
    
    def _generate_final_priorities(self, plan: ImplementationPlanResult,
                                 integrated_data: Dict[str, Any],
                                 recommendations_data: Dict[str, Any]):
        """Generate final implementation priorities"""
        
        priorities = []
        
        # Check risk levels
        if "risk" in integrated_data:
            risk_data = integrated_data["risk"]
            if risk_data.get("financial_risk_score", 50) > 60:
                priorities.append("1. Secure 120% of required capital before proceeding")
            if risk_data.get("market_risk_score", 50) > 60:
                priorities.append("2. Validate market demand through pre-launch campaigns")
        
        # Standard priorities
        priorities.extend([
            "3. Maintain strict adherence to critical path timeline",
            "4. Build strong vendor and professional relationships early",
            "5. Focus on staff training and operational excellence",
            "6. Execute aggressive but targeted marketing strategy"
        ])
        
        plan.final_implementation_priorities = "\n".join(priorities[:5])
        
        # ROI timeline
        if recommendations_data:
            breakeven = recommendations_data.get("breakeven_timeline", 18)
            plan.roi_timeline = f"{breakeven} months to breakeven, 35% ROI by Year 3"
        else:
            plan.roi_timeline = "18-24 months to breakeven, 30-40% ROI by Year 3"
        
        # Success probability
        if recommendations_data:
            confidence = recommendations_data.get("confidence_level", 70)
            plan.success_probability = int(confidence * 0.9)  # Slightly conservative
        else:
            plan.success_probability = 75
    
    def _extract_county(self, location: str) -> str:
        """Extract county from location string"""
        location_lower = location.lower()
        if "milwaukee" in location_lower:
            return "Milwaukee"
        elif "madison" in location_lower:
            return "Dane"
        elif "green bay" in location_lower:
            return "Brown"
        else:
            return "Local"
    
    def _generate_funding_structure_recommendations(self, plan: ImplementationPlanResult,
                                                  integrated_data: Dict[str, Any],
                                                  recommendations_data: Dict[str, Any]):
        """Generate comprehensive funding structure recommendations for institutional audiences"""
        
        logger.info("Generating funding structure recommendations")
        
        # Calculate total funding needed
        plan.total_funding_needed = plan.total_implementation_capital
        
        # Determine optimal debt/equity ratio
        debt_equity_analysis = self._analyze_optimal_debt_equity_ratio(
            plan, integrated_data, recommendations_data
        )
        
        # SBA loan type decision tree
        loan_type_recommendation = self._evaluate_sba_loan_options(
            plan, integrated_data, recommendations_data
        )
        
        # Calculate specific funding amounts
        self._calculate_funding_amounts(plan, debt_equity_analysis, loan_type_recommendation)
        
        # Generate drawdown schedule
        self._generate_drawdown_schedule(plan)
        
        # Create institutional recommendations
        self._generate_institutional_recommendations(plan, integrated_data, recommendations_data)
        
        # Generate funding timeline
        self._generate_funding_timeline(plan)
        
        # Calculate debt service metrics
        self._calculate_debt_service_metrics(plan, integrated_data)
    
    def _analyze_optimal_debt_equity_ratio(self, plan: ImplementationPlanResult,
                                         integrated_data: Dict[str, Any],
                                         recommendations_data: Dict[str, Any]) -> Dict[str, Any]:
        """Analyze optimal debt-to-equity ratio based on business characteristics"""
        
        # Industry-specific debt ratios
        industry_debt_ratios = {
            'restaurant': 0.70,      # 70% debt, 30% equity
            'retail': 0.65,          # 65% debt, 35% equity
            'manufacturing': 0.60,   # 60% debt, 40% equity
            'professional_services': 0.50,  # 50% debt, 50% equity
            'healthcare': 0.55,      # 55% debt, 45% equity
            'default': 0.65          # 65% debt, 35% equity
        }
        
        # Determine business category
        business_category = 'default'
        business_lower = plan.business_type.lower()
        for category in industry_debt_ratios:
            if category in business_lower:
                business_category = category
                break
        
        base_debt_ratio = industry_debt_ratios[business_category]
        
        # Adjust based on risk analysis
        if "risk" in integrated_data:
            risk_data = integrated_data["risk"]
            composite_risk = risk_data.get("composite_risk_score", 50)
            
            # Higher risk = lower debt ratio
            if composite_risk > 70:
                base_debt_ratio -= 0.10  # Reduce debt by 10%
            elif composite_risk > 60:
                base_debt_ratio -= 0.05  # Reduce debt by 5%
            elif composite_risk < 40:
                base_debt_ratio += 0.05  # Increase debt by 5%
        
        # Adjust based on cash flow projections
        if "revenue" in integrated_data and "cost" in integrated_data:
            revenue_data = integrated_data["revenue"]
            cost_data = integrated_data["cost"]
            
            annual_revenue = revenue_data.get("realistic_annual", 800000)
            annual_costs = cost_data.get("realistic_monthly_operating", 50000) * 12
            
            # Debt service coverage ratio target: 1.25
            net_income = annual_revenue - annual_costs
            if net_income > 0:
                debt_service_capacity = net_income * 0.8  # 80% available for debt service
                max_debt_payment = debt_service_capacity / 1.25  # DSCR of 1.25
                
                # Adjust debt ratio based on capacity
                estimated_debt_service = plan.total_funding_needed * base_debt_ratio * 0.12  # 12% interest
                if estimated_debt_service > max_debt_payment:
                    adjustment = max_debt_payment / estimated_debt_service
                    base_debt_ratio *= adjustment
        
        # Cap debt ratio at reasonable limits
        final_debt_ratio = max(0.40, min(0.80, base_debt_ratio))
        
        return {
            'optimal_debt_ratio': final_debt_ratio,
            'optimal_equity_ratio': 1 - final_debt_ratio,
            'business_category': business_category,
            'risk_adjustment': base_debt_ratio - industry_debt_ratios[business_category],
            'rationale': f"Optimal for {business_category} businesses with current risk profile"
        }
    
    def _evaluate_sba_loan_options(self, plan: ImplementationPlanResult,
                                 integrated_data: Dict[str, Any],
                                 recommendations_data: Dict[str, Any]) -> Dict[str, Any]:
        """Evaluate SBA 7(a) vs 504 loan options using decision tree logic"""
        
        total_funding = plan.total_funding_needed
        
        # SBA 504 decision factors
        sba_504_factors = {
            'real_estate_percentage': 0.0,
            'equipment_percentage': 0.0,
            'job_creation': 0,
            'meets_504_requirements': False
        }
        
        # Calculate real estate and equipment percentages
        construction_capital = plan.phase3_capital
        equipment_investment = plan.technology_investment + (construction_capital * 0.3)
        
        # Estimate real estate component (lease improvements, fixtures)
        real_estate_component = construction_capital * 0.6  # 60% of construction for real estate
        
        sba_504_factors['real_estate_percentage'] = real_estate_component / total_funding
        sba_504_factors['equipment_percentage'] = equipment_investment / total_funding
        
        # Job creation estimate
        if "cost" in integrated_data:
            cost_data = integrated_data["cost"]
            monthly_labor = cost_data.get("realistic_monthly_operating", 50000) * 0.35
            avg_wage = 35000  # Annual average wage
            estimated_jobs = (monthly_labor * 12) / avg_wage
            sba_504_factors['job_creation'] = max(1, round(estimated_jobs))
        else:
            sba_504_factors['job_creation'] = 3  # Default estimate
        
        # SBA 504 requirements check
        fixed_asset_requirement = (sba_504_factors['real_estate_percentage'] + 
                                 sba_504_factors['equipment_percentage']) >= 0.51
        
        sba_504_factors['meets_504_requirements'] = (
            fixed_asset_requirement and
            total_funding >= 150000 and  # Minimum project size
            sba_504_factors['job_creation'] >= 1
        )
        
        # Decision tree logic
        if sba_504_factors['meets_504_requirements']:
            # SBA 504 preferred for real estate/equipment heavy projects
            if sba_504_factors['real_estate_percentage'] > 0.40:
                recommended_loan = "SBA 504"
                rationale = (
                    f"SBA 504 recommended: {sba_504_factors['real_estate_percentage']:.1%} "
                    f"real estate component, {sba_504_factors['job_creation']} jobs created. "
                    f"Provides lower down payment (10%) and below-market rates."
                )
            else:
                recommended_loan = "SBA 7(a)"
                rationale = (
                    f"SBA 7(a) recommended: Lower real estate component "
                    f"({sba_504_factors['real_estate_percentage']:.1%}) favors working capital "
                    f"and equipment financing flexibility."
                )
        else:
            # SBA 7(a) or conventional for smaller projects
            if total_funding <= 350000:
                recommended_loan = "SBA 7(a)"
                rationale = (
                    f"SBA 7(a) recommended: Project size (${total_funding:,}) "
                    f"suits SBA 7(a) flexibility and faster processing."
                )
            else:
                recommended_loan = "Conventional + SBA 7(a)"
                rationale = (
                    f"Combination recommended: Large project size (${total_funding:,}) "
                    f"may require conventional financing supplement to SBA limits."
                )
        
        return {
            'recommended_loan_type': recommended_loan,
            'rationale': rationale,
            'sba_504_factors': sba_504_factors,
            'loan_limits': {
                'sba_7a_limit': 5000000,
                'sba_504_limit': 5500000,
                'conventional_supplement': max(0, total_funding - 5000000)
            }
        }
    
    def _calculate_funding_amounts(self, plan: ImplementationPlanResult,
                                 debt_equity_analysis: Dict[str, Any],
                                 loan_type_recommendation: Dict[str, Any]):
        """Calculate specific funding amounts for each source"""
        
        total_funding = plan.total_funding_needed
        debt_ratio = debt_equity_analysis['optimal_debt_ratio']
        
        # Calculate equity and debt amounts
        plan.equity_requirement = round(total_funding * (1 - debt_ratio))
        plan.debt_financing = round(total_funding * debt_ratio)
        
        # Calculate percentages
        plan.equity_percentage = round((1 - debt_ratio) * 100, 1)
        plan.debt_percentage = round(debt_ratio * 100, 1)
        
        # Allocate debt between sources
        loan_type = loan_type_recommendation['recommended_loan_type']
        
        if loan_type == "SBA 504":
            # SBA 504 structure: 50% SBA, 40% bank, 10% equity
            plan.sba_loan_amount = round(total_funding * 0.50)
            plan.conventional_loan_amount = round(total_funding * 0.40)
            plan.equity_requirement = round(total_funding * 0.10)
            plan.working_capital_line = round(total_funding * 0.10)  # Separate line of credit
            
        elif loan_type == "SBA 7(a)":
            # SBA 7(a) structure: Up to 90% SBA, 10-35% equity
            sba_percentage = min(0.90, debt_ratio)
            plan.sba_loan_amount = round(total_funding * sba_percentage)
            plan.conventional_loan_amount = 0
            plan.equity_requirement = round(total_funding * (1 - sba_percentage))
            plan.working_capital_line = round(total_funding * 0.15)  # Line of credit
            
        elif loan_type == "Conventional + SBA 7(a)":
            # Mixed structure for larger projects
            sba_limit = 5000000
            sba_portion = min(sba_limit, total_funding * 0.70)
            conventional_portion = plan.debt_financing - sba_portion
            
            plan.sba_loan_amount = round(sba_portion)
            plan.conventional_loan_amount = round(conventional_portion)
            plan.working_capital_line = round(total_funding * 0.10)
            
        else:
            # Conventional financing
            plan.conventional_loan_amount = plan.debt_financing
            plan.sba_loan_amount = 0
            plan.working_capital_line = round(total_funding * 0.15)
        
        # Set loan terms
        plan.loan_terms = self._generate_loan_terms(loan_type, plan)
        
        # Set recommendations
        plan.recommended_loan_type = loan_type
        plan.loan_type_rationale = loan_type_recommendation['rationale']
        
        # Generate funding structure summary
        plan.funding_structure_recommendation = self._generate_funding_structure_summary(plan)
    
    def _generate_loan_terms(self, loan_type: str, plan: ImplementationPlanResult) -> Dict[str, Any]:
        """Generate specific loan terms based on loan type"""
        
        if loan_type == "SBA 504":
            return {
                'sba_portion': {
                    'amount': plan.sba_loan_amount,
                    'interest_rate': '5.5% - 6.5% (below market)',
                    'term': '20 years (real estate), 10 years (equipment)',
                    'payment_type': 'Fixed rate, fully amortizing',
                    'down_payment': '10% of project cost'
                },
                'bank_portion': {
                    'amount': plan.conventional_loan_amount,
                    'interest_rate': 'Prime + 1-2%',
                    'term': '10 years',
                    'payment_type': 'Fixed or variable',
                    'guarantee': 'Personal guarantee required'
                },
                'working_capital_line': {
                    'amount': plan.working_capital_line,
                    'interest_rate': 'Prime + 1-3%',
                    'term': 'Revolving line of credit',
                    'payment_type': 'Interest only on outstanding balance'
                }
            }
        
        elif loan_type == "SBA 7(a)":
            return {
                'sba_loan': {
                    'amount': plan.sba_loan_amount,
                    'interest_rate': 'Prime + 2.75-4.75%',
                    'term': '25 years (real estate), 10 years (equipment), 7 years (working capital)',
                    'payment_type': 'Fixed or variable rate',
                    'down_payment': '10% minimum',
                    'guarantee': 'Personal guarantee required'
                },
                'working_capital_line': {
                    'amount': plan.working_capital_line,
                    'interest_rate': 'Prime + 2-4%',
                    'term': 'Revolving line of credit',
                    'payment_type': 'Interest only on outstanding balance'
                }
            }
        
        else:  # Conventional
            return {
                'conventional_loan': {
                    'amount': plan.conventional_loan_amount,
                    'interest_rate': 'Prime + 2-5%',
                    'term': '5-20 years depending on collateral',
                    'payment_type': 'Fixed or variable rate',
                    'down_payment': '20-30% of project cost',
                    'guarantee': 'Personal guarantee and collateral required'
                },
                'working_capital_line': {
                    'amount': plan.working_capital_line,
                    'interest_rate': 'Prime + 2-4%',
                    'term': 'Revolving line of credit',
                    'payment_type': 'Interest only on outstanding balance'
                }
            }
    
    def _generate_funding_structure_summary(self, plan: ImplementationPlanResult) -> str:
        """Generate executive summary of funding structure"""
        
        summary = f"""
**RECOMMENDED FUNDING STRUCTURE**

**Total Project Cost:** ${plan.total_funding_needed:,}
**Recommended Loan Type:** {plan.recommended_loan_type}

**Funding Breakdown:**
• Owner Equity: ${plan.equity_requirement:,} ({plan.equity_percentage}%)
• SBA Financing: ${plan.sba_loan_amount:,} ({plan.sba_loan_amount/plan.total_funding_needed*100:.1f}%)
• Conventional Financing: ${plan.conventional_loan_amount:,} ({plan.conventional_loan_amount/plan.total_funding_needed*100:.1f}%)
• Working Capital Line: ${plan.working_capital_line:,}

**Key Advantages:**
{plan.loan_type_rationale}

**Next Steps:**
1. Secure equity commitment from owner
2. Begin SBA pre-qualification process
3. Prepare comprehensive loan application package
4. Identify preferred SBA lender relationships
"""
        
        return summary.strip()
    
    def _generate_drawdown_schedule(self, plan: ImplementationPlanResult):
        """Generate detailed drawdown schedule aligned with implementation phases"""
        
        # Calculate drawdown amounts for each phase
        phase_funding = {
            'Phase 1 - Foundation': {
                'amount': plan.phase1_capital,
                'timing': 'Week 1-4',
                'equity_portion': round(plan.phase1_capital * 0.50),
                'debt_portion': round(plan.phase1_capital * 0.50),
                'purpose': 'Entity formation, professional services, site securing'
            },
            'Phase 2 - Permits & Design': {
                'amount': plan.phase2_capital,
                'timing': 'Week 5-12',
                'equity_portion': round(plan.phase2_capital * 0.30),
                'debt_portion': round(plan.phase2_capital * 0.70),
                'purpose': 'Permits, architectural plans, engineering'
            },
            'Phase 3 - Construction': {
                'amount': plan.phase3_capital,
                'timing': 'Week 13-24',
                'equity_portion': round(plan.phase3_capital * 0.20),
                'debt_portion': round(plan.phase3_capital * 0.80),
                'purpose': 'Construction, equipment, fixtures, build-out'
            },
            'Phase 4 - Launch Preparation': {
                'amount': plan.phase4_capital,
                'timing': 'Week 25-26',
                'equity_portion': round(plan.phase4_capital * 0.40),
                'debt_portion': round(plan.phase4_capital * 0.60),
                'purpose': 'Staffing, training, marketing, inventory'
            },
            'Working Capital Reserve': {
                'amount': plan.working_capital,
                'timing': 'Available upon opening',
                'equity_portion': round(plan.working_capital * 0.30),
                'debt_portion': round(plan.working_capital * 0.70),
                'purpose': 'Operating expenses, cash flow management'
            }
        }
        
        plan.drawdown_schedule = phase_funding
    
    def _generate_institutional_recommendations(self, plan: ImplementationPlanResult,
                                              integrated_data: Dict[str, Any],
                                              recommendations_data: Dict[str, Any]):
        """Generate specific recommendations for different institutional audiences"""
        
        # SBA Lenders
        sba_recommendations = {
            'qualification_requirements': [
                'Business must be for-profit',
                'Owner must have invested equity',
                'Business must be independently owned',
                'Meet SBA size standards for industry'
            ],
            'documentation_needed': [
                'Business plan with 3-year projections',
                'Personal financial statements',
                'Tax returns (3 years)',
                'Environmental assessment',
                'Resumes of key personnel'
            ],
            'approval_timeline': '45-90 days',
            'key_advantages': [
                'Lower down payment requirements',
                'Longer repayment terms',
                'Competitive interest rates',
                'Government backing reduces lender risk'
            ]
        }
        
        # Conventional Lenders
        conventional_recommendations = {
            'qualification_requirements': [
                'Strong credit score (680+)',
                'Debt-to-income ratio <40%',
                'Collateral coverage 125%+',
                'Industry experience preferred'
            ],
            'documentation_needed': [
                'Detailed business plan',
                'Financial projections',
                'Collateral appraisals',
                'Personal guarantees',
                'UCC filings'
            ],
            'approval_timeline': '30-60 days',
            'key_advantages': [
                'Faster approval process',
                'More flexible terms',
                'Relationship banking benefits',
                'Potential for future growth financing'
            ]
        }
        
        # Equity Investors
        equity_recommendations = {
            'target_investors': [
                'Angel investors',
                'Local business partners',
                'Family and friends',
                'Industry veterans'
            ],
            'investment_terms': [
                f'{plan.equity_percentage}% equity stake',
                'Potential for future returns',
                'Active vs. passive involvement',
                'Exit strategy planning'
            ],
            'due_diligence_requirements': [
                'Market validation',
                'Management team assessment',
                'Financial model review',
                'Legal structure optimization'
            ]
        }
        
        plan.institutional_recommendations = {
            'sba_lenders': sba_recommendations,
            'conventional_lenders': conventional_recommendations,
            'equity_investors': equity_recommendations
        }
    
    def _generate_funding_timeline(self, plan: ImplementationPlanResult):
        """Generate detailed funding timeline"""
        
        timeline = f"""
**FUNDING TIMELINE**

**Pre-Application Phase (Weeks 1-4):**
• Week 1: Secure equity commitment documentation
• Week 2: Complete business plan and financial projections
• Week 3: Identify and approach preferred lenders
• Week 4: Submit preliminary loan applications

**Application Phase (Weeks 5-8):**
• Week 5: Submit complete SBA application package
• Week 6: Complete lender due diligence process
• Week 7: Provide additional documentation as requested
• Week 8: Loan committee review and preliminary approval

**Approval Phase (Weeks 9-12):**
• Week 9: Final loan approval and commitment letter
• Week 10: Complete loan documentation and legal review
• Week 11: Finalize loan terms and conditions
• Week 12: Close on financing and begin drawdowns

**Implementation Phase (Weeks 13+):**
• Ongoing: Execute drawdown schedule per implementation phases
• Monthly: Submit draw requests with supporting documentation
• Quarterly: Provide progress reports to lenders
"""
        
        plan.funding_timeline = timeline.strip()
    
    def _calculate_debt_service_metrics(self, plan: ImplementationPlanResult,
                                      integrated_data: Dict[str, Any]):
        """Calculate key debt service metrics for lender evaluation"""
        
        # Get revenue and cost projections
        if "revenue" in integrated_data and "cost" in integrated_data:
            revenue_data = integrated_data["revenue"]
            cost_data = integrated_data["cost"]
            
            annual_revenue = revenue_data.get("realistic_annual", 800000)
            annual_costs = cost_data.get("realistic_monthly_operating", 50000) * 12
            annual_net_income = annual_revenue - annual_costs
            
            # Calculate debt service
            total_debt = plan.sba_loan_amount + plan.conventional_loan_amount
            
            if total_debt > 0:
                # Estimate annual debt service (principal + interest)
                # Using average 7% interest rate and 15-year term
                interest_rate = 0.07
                term_years = 15
                monthly_payment = total_debt * (interest_rate/12) / (1 - (1 + interest_rate/12)**(-term_years*12))
                annual_debt_service = monthly_payment * 12
                
                # Calculate DSCR
                plan.debt_service_coverage_ratio = annual_net_income / annual_debt_service if annual_debt_service > 0 else 0
                
                # Calculate LTV ratio
                # Estimate collateral value as 80% of project cost
                collateral_value = plan.total_funding_needed * 0.8
                plan.loan_to_value_ratio = total_debt / collateral_value if collateral_value > 0 else 0
            
        # Set collateral and guarantee requirements
        if plan.recommended_loan_type == "SBA 504":
            plan.collateral_requirements = "Real estate and equipment financed serve as collateral"
            plan.personal_guarantee_requirements = "Personal guarantee required for SBA portion"
        elif plan.recommended_loan_type == "SBA 7(a)":
            plan.collateral_requirements = "All business assets and available personal assets"
            plan.personal_guarantee_requirements = "Personal guarantee required for owners with >20% stake"
        else:
            plan.collateral_requirements = "All business assets plus additional collateral as required"
            plan.personal_guarantee_requirements = "Personal guarantee and possibly spousal guarantee required"
    
    def populate_template(self, template_content: str, plan: ImplementationPlanResult) -> str:
        """Populate the implementation plan template with generated content"""
        
        # Basic replacements
        content = template_content.replace("{business_type}", plan.business_type)
        content = content.replace("{location}", plan.location)
        content = content.replace("{total_weeks}", str(plan.total_weeks))
        content = content.replace("{total_months}", str(plan.total_months))
        
        # Phase 1 replacements
        content = content.replace("{entity_formation_timeline}", plan.entity_formation_timeline)
        content = content.replace("{entity_formation_cost}", f"{plan.entity_formation_cost:,}")
        content = content.replace("{financial_setup_cost}", f"{plan.financial_setup_cost:,}")
        content = content.replace("{monthly_insurance_cost}", f"{plan.monthly_insurance_cost:,}")
        content = content.replace("{site_securing_timeline}", plan.site_securing_timeline)
        content = content.replace("{site_securing_costs}", f"{plan.site_securing_costs:,}")
        content = content.replace("{professional_services_budget}", f"{plan.professional_services_budget:,}")
        
        # Phase 2 replacements
        content = content.replace("{permit_application_schedule}", plan.permit_application_schedule)
        content = content.replace("{permit_approval_timeline}", plan.permit_approval_timeline)
        content = content.replace("{design_approval_date}", plan.design_approval_date)
        content = content.replace("{permit_submission_date}", plan.permit_submission_date)
        content = content.replace("{expected_approval_date}", plan.expected_approval_date)
        
        # Phase 3 replacements
        content = content.replace("{construction_phase1_details}", plan.construction_phase1_details)
        content = content.replace("{construction_phase2_details}", plan.construction_phase2_details)
        content = content.replace("{construction_phase3_details}", plan.construction_phase3_details)
        content = content.replace("{staffing_budget}", f"{plan.staffing_budget:,}")
        content = content.replace("{marketing_launch_budget}", f"{plan.marketing_launch_budget:,}")
        
        # Phase 4 replacements
        content = content.replace("{service_time_target}", plan.service_time_target)
        content = content.replace("{quality_standards}", plan.quality_standards)
        content = content.replace("{satisfaction_target}", str(plan.satisfaction_target))
        
        # Phase 5 replacements
        content = content.replace("{grand_opening_date}", plan.grand_opening_date)
        content = content.replace("{grand_opening_marketing_plan}", plan.grand_opening_marketing_plan)
        content = content.replace("{community_engagement_activities}", plan.community_engagement_activities)
        content = content.replace("{promotional_strategy_details}", plan.promotional_strategy_details)
        
        # Financial replacements
        content = content.replace("{phase1_capital}", f"{plan.phase1_capital:,}")
        content = content.replace("{phase2_capital}", f"{plan.phase2_capital:,}")
        content = content.replace("{phase3_capital}", f"{plan.phase3_capital:,}")
        content = content.replace("{phase4_capital}", f"{plan.phase4_capital:,}")
        content = content.replace("{working_capital}", f"{plan.working_capital:,}")
        content = content.replace("{total_capital_deployment}", f"{plan.total_capital_deployment:,}")
        
        # Revenue projections
        content = content.replace("{month1_revenue}", f"{plan.month1_revenue:,}")
        content = content.replace("{month1_capacity}", str(plan.month1_capacity))
        content = content.replace("{month2_revenue}", f"{plan.month2_revenue:,}")
        content = content.replace("{month2_capacity}", str(plan.month2_capacity))
        content = content.replace("{month3_revenue}", f"{plan.month3_revenue:,}")
        content = content.replace("{month3_capacity}", str(plan.month3_capacity))
        content = content.replace("{month6_revenue}", f"{plan.month6_revenue:,}")
        content = content.replace("{month6_capacity}", str(plan.month6_capacity))
        content = content.replace("{expense_management_strategy}", plan.expense_management_strategy)
        
        # Risk management
        content = content.replace("{contingency_percentage}", str(plan.contingency_percentage))
        content = content.replace("{line_of_credit}", f"{plan.line_of_credit:,}")
        content = content.replace("{operational_contingency_plans}", plan.operational_contingency_plans)
        content = content.replace("{market_contingency_plans}", plan.market_contingency_plans)
        
        # Performance monitoring
        content = content.replace("{quarterly_review_framework}", 
                                "- Complete financial analysis vs. projections\n"
                                "- Market position and competitive assessment\n"
                                "- Operational efficiency metrics review\n"
                                "- Strategic initiatives progress\n"
                                "- Capital requirements reassessment")
        
        # Technology
        content = content.replace("{technology_investment}", f"{plan.technology_investment:,}")
        
        # Vendors
        content = content.replace("{primary_supplier_list}", plan.primary_supplier_list)
        content = content.replace("{backup_supplier_list}", plan.backup_supplier_list)
        content = content.replace("{payment_terms}", plan.payment_terms)
        content = content.replace("{delivery_schedules}", plan.delivery_schedules)
        content = content.replace("{quality_agreements}", plan.quality_agreements)
        content = content.replace("{contract_terms}", plan.contract_terms)
        
        # Marketing
        content = content.replace("{launch_marketing_budget}", f"{plan.launch_marketing_budget:,}")
        content = content.replace("{monthly_marketing_budget}", f"{plan.monthly_marketing_budget:,}")
        content = content.replace("{marketing_channel_allocation}", plan.marketing_channel_allocation)
        content = content.replace("{target_cac}", str(plan.target_cac))
        content = content.replace("{target_ltv}", str(plan.target_ltv))
        content = content.replace("{target_roi_ratio}", str(plan.target_roi_ratio))
        
        # Staffing
        content = content.replace("{gm_hire_date}", plan.gm_hire_date)
        content = content.replace("{am_hire_date}", plan.am_hire_date)
        content = content.replace("{dept_heads_hire_date}", plan.dept_heads_hire_date)
        content = content.replace("{staff_hiring_schedule}", plan.staff_hiring_schedule)
        content = content.replace("{training_budget}", f"{plan.training_budget:,}")
        
        # Success metrics
        content = content.replace("{day30_revenue_target}", str(plan.day30_revenue_target))
        content = content.replace("{day30_satisfaction}", str(plan.day30_satisfaction))
        content = content.replace("{day90_revenue_target}", str(plan.day90_revenue_target))
        content = content.replace("{day90_efficiency}", str(plan.day90_efficiency))
        content = content.replace("{day90_customers}", str(plan.day90_customers))
        content = content.replace("{year1_revenue_goal}", str(plan.year1_revenue_goal))
        content = content.replace("{year1_profit_margin}", str(plan.year1_profit_margin))
        content = content.replace("{year1_market_share}", str(plan.year1_market_share))
        content = content.replace("{year1_retention}", str(plan.year1_retention))
        
        # Critical path
        critical_actions_list = "\n".join([f"{i+1}. {action}" for i, action in enumerate(plan.critical_actions)])
        content = content.replace("{critical_action_1}", plan.critical_actions[0] if plan.critical_actions else "")
        content = content.replace("{critical_action_2}", plan.critical_actions[1] if len(plan.critical_actions) > 1 else "")
        content = content.replace("{critical_action_3}", plan.critical_actions[2] if len(plan.critical_actions) > 2 else "")
        content = content.replace("{critical_action_4}", plan.critical_actions[3] if len(plan.critical_actions) > 3 else "")
        content = content.replace("{critical_action_5}", plan.critical_actions[4] if len(plan.critical_actions) > 4 else "")
        
        content = content.replace("{key_dependencies_list}", plan.key_dependencies)
        
        # Decision points
        for i, (point, date) in enumerate(plan.decision_points[:3], 1):
            content = content.replace(f"{{decision_point_{i}}}", point)
            content = content.replace(f"{{decision_date_{i}}}", date)
        
        # Resources
        content = content.replace("{professional_contacts_list}", plan.professional_contacts)
        content = content.replace("{government_contacts_list}", plan.government_contacts)
        content = content.replace("{industry_resources_list}", plan.industry_resources)
        
        # Final recommendations
        content = content.replace("{final_implementation_priorities}", plan.final_implementation_priorities)
        content = content.replace("{total_implementation_capital}", f"{plan.total_implementation_capital:,}")
        content = content.replace("{roi_timeline}", plan.roi_timeline)
        content = content.replace("{success_probability}", str(plan.success_probability))
        
        # Dates
        content = content.replace("{plan_generation_date}", datetime.now().strftime("%B %d, %Y"))
        content = content.replace("{plan_validity_date}", 
                                (datetime.now() + timedelta(days=60)).strftime("%B %d, %Y"))
        content = content.replace("{next_review_date}", 
                                (datetime.now() + timedelta(days=14)).strftime("%B %d, %Y"))
        
        # NEW: Funding Structure Replacements
        content = content.replace("{funding_structure_recommendation}", plan.funding_structure_recommendation)
        content = content.replace("{recommended_loan_type}", plan.recommended_loan_type)
        content = content.replace("{loan_type_rationale}", plan.loan_type_rationale)
        content = content.replace("{total_funding_needed}", f"{plan.total_funding_needed:,}")
        content = content.replace("{equity_requirement}", f"{plan.equity_requirement:,}")
        content = content.replace("{debt_financing}", f"{plan.debt_financing:,}")
        content = content.replace("{sba_loan_amount}", f"{plan.sba_loan_amount:,}")
        content = content.replace("{conventional_loan_amount}", f"{plan.conventional_loan_amount:,}")
        content = content.replace("{working_capital_line}", f"{plan.working_capital_line:,}")
        content = content.replace("{equity_percentage}", f"{plan.equity_percentage}%")
        content = content.replace("{debt_percentage}", f"{plan.debt_percentage}%")
        content = content.replace("{funding_timeline}", plan.funding_timeline)
        content = content.replace("{collateral_requirements}", plan.collateral_requirements)
        content = content.replace("{personal_guarantee_requirements}", plan.personal_guarantee_requirements)
        content = content.replace("{debt_service_coverage_ratio}", f"{plan.debt_service_coverage_ratio:.2f}")
        content = content.replace("{loan_to_value_ratio}", f"{plan.loan_to_value_ratio:.2f}")
        
        # Drawdown schedule replacements
        if plan.drawdown_schedule:
            for phase, details in plan.drawdown_schedule.items():
                phase_key = phase.lower().replace(' ', '_').replace('-', '_')
                content = content.replace(f"{{{phase_key}_amount}}", f"{details['amount']:,}")
                content = content.replace(f"{{{phase_key}_timing}}", details['timing'])
                content = content.replace(f"{{{phase_key}_equity_portion}}", f"{details['equity_portion']:,}")
                content = content.replace(f"{{{phase_key}_debt_portion}}", f"{details['debt_portion']:,}")
                content = content.replace(f"{{{phase_key}_purpose}}", details['purpose'])
        
        # Institutional recommendations
        if plan.institutional_recommendations:
            sba_recs = plan.institutional_recommendations.get('sba_lenders', {})
            conv_recs = plan.institutional_recommendations.get('conventional_lenders', {})
            equity_recs = plan.institutional_recommendations.get('equity_investors', {})
            
            content = content.replace("{sba_approval_timeline}", sba_recs.get('approval_timeline', '45-90 days'))
            content = content.replace("{conventional_approval_timeline}", conv_recs.get('approval_timeline', '30-60 days'))
            
            # Format qualification requirements
            sba_quals = '\n'.join([f"• {req}" for req in sba_recs.get('qualification_requirements', [])])
            conv_quals = '\n'.join([f"• {req}" for req in conv_recs.get('qualification_requirements', [])])
            
            content = content.replace("{sba_qualification_requirements}", sba_quals)
            content = content.replace("{conventional_qualification_requirements}", conv_quals)
            
            # Format documentation needed
            sba_docs = '\n'.join([f"• {doc}" for doc in sba_recs.get('documentation_needed', [])])
            conv_docs = '\n'.join([f"• {doc}" for doc in conv_recs.get('documentation_needed', [])])
            
            content = content.replace("{sba_documentation_needed}", sba_docs)
            content = content.replace("{conventional_documentation_needed}", conv_docs)
            
            # Format advantages
            sba_advs = '\n'.join([f"• {adv}" for adv in sba_recs.get('key_advantages', [])])
            conv_advs = '\n'.join([f"• {adv}" for adv in conv_recs.get('key_advantages', [])])
            
            content = content.replace("{sba_key_advantages}", sba_advs)
            content = content.replace("{conventional_key_advantages}", conv_advs)
        
        # Loan terms
        if plan.loan_terms:
            for loan_type, terms in plan.loan_terms.items():
                loan_key = loan_type.replace('_', '')
                if isinstance(terms, dict):
                    for term_key, term_value in terms.items():
                        content = content.replace(f"{{{loan_key}_{term_key}}}", str(term_value))
        
        return content