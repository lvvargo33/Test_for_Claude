#!/usr/bin/env python3
"""
Recommendations Generator for Section 6.1
=========================================

Synthesizes all previous analysis sections to generate comprehensive final recommendations.
Integrates findings from demographics, economics, competition, site analysis, financials,
risk assessment, and regulatory compliance into actionable business guidance.
"""

import json
import os
from datetime import datetime, timedelta
from typing import Dict, List, Any, Optional, Tuple
import logging

logger = logging.getLogger(__name__)


class RecommendationResult:
    """Container for final business recommendations"""
    
    def __init__(self, business_type: str, location: str):
        self.business_type = business_type
        self.location = location
        
        # Overall Assessment
        self.overall_viability_rating = "Moderate"  # High/Moderate/Low/Not Recommended
        self.primary_recommendation = ""
        self.confidence_level = 0
        
        # Key Success Factors
        self.strategic_advantages = []
        self.market_demand_summary = ""
        self.location_advantages_summary = ""
        self.economic_environment_summary = ""
        self.competitive_position_summary = ""
        self.unique_value_propositions = []
        
        # Risk Factors
        self.top_risks = []  # List of (name, score, impact, mitigation)
        self.contingency_planning = ""
        
        # Financial Recommendations
        self.total_initial_investment = 0
        self.working_capital_reserve = 0
        self.contingency_fund = 0
        self.target_monthly_revenue = 0
        self.breakeven_timeline = 0
        self.target_net_margin = 0
        self.roi_projection = 0
        self.funding_strategy = ""
        
        # Operational Recommendations
        self.immediate_action_items = []
        self.prelaunch_requirements = []
        self.launch_strategy = ""
        self.growth_phase_plan = ""
        
        # Market Positioning
        self.target_market_focus = ""
        self.competitive_differentiation = ""
        self.marketing_priorities = {}
        
        # Location-Specific
        self.site_optimization = ""
        self.infrastructure_priorities = ""
        self.community_integration = ""
        
        # Regulatory
        self.critical_permits_timeline = ""
        self.compliance_priorities = []
        self.professional_services = []
        
        # Performance Monitoring
        self.key_performance_indicators = {}
        
        # Decision Framework
        self.proceed_criteria = []
        self.reconsider_criteria = []
        self.avoid_criteria = []
        self.alternative_recommendations = ""
        
        # Executive Summary
        self.executive_summary_narrative = ""
        self.final_recommendation_statement = ""
        self.decision_deadline = ""
        self.optimal_launch_window = ""
        self.expected_profitability_timeline = ""


class RecommendationsGenerator:
    """Generates comprehensive final recommendations based on all analysis sections"""
    
    def __init__(self):
        self.viability_thresholds = {
            "high": 80,
            "moderate": 60,
            "low": 40
        }
        
    def generate_recommendations(self, business_type: str, location: str, 
                               integrated_data: Dict[str, Any]) -> RecommendationResult:
        """Generate final recommendations based on all previous analyses"""
        
        logger.info(f"Generating final recommendations for {business_type} in {location}")
        
        recommendation = RecommendationResult(business_type, location)
        
        # Calculate overall viability score
        viability_score = self._calculate_overall_viability(integrated_data)
        recommendation.confidence_level = viability_score
        
        # Set overall rating and primary recommendation
        recommendation.overall_viability_rating = self._get_viability_rating(viability_score)
        recommendation.primary_recommendation = self._generate_primary_recommendation(
            viability_score, business_type, integrated_data
        )
        
        # Generate success factors
        self._analyze_success_factors(recommendation, integrated_data)
        
        # Identify and prioritize risks
        self._analyze_risk_factors(recommendation, integrated_data)
        
        # Generate financial recommendations
        self._generate_financial_recommendations(recommendation, integrated_data)
        
        # Create operational roadmap
        self._generate_operational_recommendations(recommendation, integrated_data)
        
        # Develop market positioning strategy
        self._generate_market_positioning(recommendation, integrated_data)
        
        # Location-specific recommendations
        self._generate_location_recommendations(recommendation, integrated_data)
        
        # Regulatory compliance roadmap
        self._generate_regulatory_roadmap(recommendation, integrated_data)
        
        # Performance monitoring framework
        self._generate_kpi_framework(recommendation, business_type)
        
        # Decision framework
        self._generate_decision_framework(recommendation, viability_score, integrated_data)
        
        # Executive summary
        self._generate_executive_summary(recommendation, viability_score, integrated_data)
        
        return recommendation
    
    def _calculate_overall_viability(self, integrated_data: Dict[str, Any]) -> float:
        """Calculate comprehensive viability score from all analyses"""
        
        scores = []
        weights = []
        
        # Market demand score (weight: 20%)
        if "market" in integrated_data:
            market_data = integrated_data["market"]
            market_score = 100 - market_data.get("saturation_score", 50)
            scores.append(market_score)
            weights.append(0.20)
        
        # Revenue potential score (weight: 20%)
        if "revenue" in integrated_data:
            revenue_data = integrated_data["revenue"]
            # Score based on confidence level
            revenue_score = float(revenue_data.get("confidence_level", "70").replace("%", ""))
            scores.append(revenue_score)
            weights.append(0.20)
        
        # Cost efficiency score (weight: 15%)
        if "cost" in integrated_data:
            cost_data = integrated_data["cost"]
            # Lower operating cost ratio is better
            op_ratio = cost_data.get("operating_cost_ratio", 0.85)
            cost_score = max(0, (1 - op_ratio) * 100)
            scores.append(cost_score)
            weights.append(0.15)
        
        # Risk assessment score (weight: 20%)
        if "risk" in integrated_data:
            risk_data = integrated_data["risk"]
            # Invert risk score (lower risk = higher viability)
            risk_score = 100 - risk_data.get("composite_risk_score", 50)
            scores.append(risk_score)
            weights.append(0.20)
        
        # Location score (weight: 15%)
        location_scores = []
        if "traffic" in integrated_data:
            traffic_data = integrated_data["traffic"]
            traffic_score = traffic_data.get("overall_score", 70)
            location_scores.append(traffic_score)
        
        if "site" in integrated_data:
            site_data = integrated_data["site"]
            site_score = site_data.get("overall_suitability_score", 70)
            location_scores.append(site_score)
        
        if "infrastructure" in integrated_data:
            infra_data = integrated_data["infrastructure"]
            infra_score = infra_data.get("overall_infrastructure_score", 70)
            location_scores.append(infra_score)
        
        if location_scores:
            avg_location_score = sum(location_scores) / len(location_scores)
            scores.append(avg_location_score)
            weights.append(0.15)
        
        # Competitive position score (weight: 10%)
        if "competitive" in integrated_data:
            comp_data = integrated_data["competitive"]
            # Assume moderate competitive position if not specified
            comp_score = 65
            scores.append(comp_score)
            weights.append(0.10)
        
        # Calculate weighted average
        if scores and weights:
            total_weight = sum(weights)
            weighted_score = sum(s * w for s, w in zip(scores, weights)) / total_weight
            return round(weighted_score, 1)
        
        return 65.0  # Default moderate score
    
    def _get_viability_rating(self, score: float) -> str:
        """Convert viability score to rating"""
        if score >= self.viability_thresholds["high"]:
            return "High"
        elif score >= self.viability_thresholds["moderate"]:
            return "Moderate"
        elif score >= self.viability_thresholds["low"]:
            return "Low"
        else:
            return "Not Recommended"
    
    def _generate_primary_recommendation(self, viability_score: float, 
                                       business_type: str, 
                                       integrated_data: Dict[str, Any]) -> str:
        """Generate primary recommendation statement"""
        
        rating = self._get_viability_rating(viability_score)
        
        if rating == "High":
            return f"PROCEED with establishing the {business_type}. Market conditions, financial projections, and location factors strongly support this venture with appropriate risk mitigation strategies in place."
        elif rating == "Moderate":
            return f"PROCEED WITH CAUTION. The {business_type} shows reasonable potential but requires careful attention to identified risk factors and market challenges. Success depends on strong execution and differentiation."
        elif rating == "Low":
            return f"RECONSIDER the current approach. While not impossible, the {business_type} faces significant challenges that require substantial revision to the business model or location strategy."
        else:
            return f"NOT RECOMMENDED at this time. Current market conditions, financial projections, or location factors present excessive risk for the {business_type} venture."
    
    def _analyze_success_factors(self, recommendation: RecommendationResult, 
                               integrated_data: Dict[str, Any]):
        """Identify and prioritize key success factors"""
        
        # Strategic advantages
        advantages = []
        
        # From market analysis
        if "market" in integrated_data:
            market = integrated_data["market"]
            if market.get("growth_rate", 0) > 5:
                advantages.append("Strong market growth trajectory")
            if market.get("unmet_demand", False):
                advantages.append("Identified unmet market demand")
        
        # From revenue projections
        if "revenue" in integrated_data:
            revenue = integrated_data["revenue"]
            if revenue.get("confidence_level", "0%").replace("%", "") > "80":
                advantages.append("High revenue potential with strong confidence")
        
        # From location analysis
        if "traffic" in integrated_data:
            traffic = integrated_data["traffic"]
            if traffic.get("traffic_volume_score", 0) > 80:
                advantages.append("Excellent traffic volume and visibility")
        
        recommendation.strategic_advantages = advantages[:5]  # Top 5
        
        # Market demand summary
        recommendation.market_demand_summary = self._summarize_market_demand(integrated_data)
        
        # Location advantages
        recommendation.location_advantages_summary = self._summarize_location_advantages(integrated_data)
        
        # Economic environment
        recommendation.economic_environment_summary = self._summarize_economic_environment(integrated_data)
        
        # Competitive position
        recommendation.competitive_position_summary = self._summarize_competitive_position(integrated_data)
        
        # Unique value propositions
        recommendation.unique_value_propositions = self._identify_unique_value_props(
            recommendation.business_type, integrated_data
        )
    
    def _analyze_risk_factors(self, recommendation: RecommendationResult, 
                            integrated_data: Dict[str, Any]):
        """Identify and prioritize top risk factors with mitigation strategies"""
        
        risks = []
        
        # From risk assessment
        if "risk" in integrated_data:
            risk_data = integrated_data["risk"]
            
            # Market risks
            if risk_data.get("market_risk_score", 0) > 60:
                risks.append((
                    "Market Competition Risk",
                    risk_data.get("market_risk_score", 65),
                    "High competitive density may impact market share and pricing power",
                    "Implement strong differentiation strategy and customer loyalty programs"
                ))
            
            # Financial risks
            if risk_data.get("financial_risk_score", 0) > 60:
                risks.append((
                    "Financial Performance Risk",
                    risk_data.get("financial_risk_score", 65),
                    "Initial capital requirements and cash flow management challenges",
                    "Secure 20% contingency funding and implement strict cost controls"
                ))
            
            # Operational risks
            if risk_data.get("operational_risk_score", 0) > 60:
                risks.append((
                    "Operational Execution Risk",
                    risk_data.get("operational_risk_score", 65),
                    "Complexity in operations and potential staffing challenges",
                    "Develop detailed SOPs and invest in comprehensive staff training"
                ))
        
        # Sort by score and take top 3
        risks.sort(key=lambda x: x[1], reverse=True)
        recommendation.top_risks = risks[:3]
        
        # Contingency planning
        recommendation.contingency_planning = self._generate_contingency_plan(risks)
    
    def _generate_financial_recommendations(self, recommendation: RecommendationResult, 
                                          integrated_data: Dict[str, Any]):
        """Generate comprehensive financial recommendations"""
        
        # From cost analysis
        if "cost" in integrated_data:
            cost_data = integrated_data["cost"]
            recommendation.total_initial_investment = cost_data.get("total_startup_costs", 250000)
            
            # Calculate working capital (3 months operating costs)
            monthly_operating = cost_data.get("realistic_monthly_operating", 50000)
            recommendation.working_capital_reserve = monthly_operating * 3
            
            # 15% contingency
            recommendation.contingency_fund = int(recommendation.total_initial_investment * 0.15)
            
            # Total capital needed
            total = (recommendation.total_initial_investment + 
                    recommendation.working_capital_reserve + 
                    recommendation.contingency_fund)
            recommendation.total_capital_needed = total
        
        # From revenue projections
        if "revenue" in integrated_data:
            revenue_data = integrated_data["revenue"]
            annual_revenue = revenue_data.get("realistic_annual", 800000)
            recommendation.target_monthly_revenue = int(annual_revenue / 12)
            
            # Breakeven timeline
            if recommendation.total_initial_investment > 0:
                monthly_profit = recommendation.target_monthly_revenue * 0.15  # Assume 15% margin
                recommendation.breakeven_timeline = int(
                    recommendation.total_initial_investment / monthly_profit
                )
            
            # Target margins
            recommendation.target_net_margin = 15
            recommendation.roi_projection = 35  # 3-year projection
        
        # Funding strategy
        recommendation.funding_strategy = self._generate_funding_strategy(
            recommendation.total_capital_needed,
            recommendation.business_type
        )
    
    def _generate_operational_recommendations(self, recommendation: RecommendationResult, 
                                            integrated_data: Dict[str, Any]):
        """Create detailed operational roadmap"""
        
        # Immediate action items (0-30 days)
        immediate_items = [
            "Secure initial funding commitments and establish business banking relationships",
            "Finalize site lease negotiations and obtain necessary insurance policies",
            "File for business registration and federal EIN",
            "Engage key professional services (attorney, accountant, architect)",
            "Conduct detailed competitive site visits and customer surveys"
        ]
        recommendation.immediate_action_items = immediate_items
        
        # Pre-launch requirements (1-3 months)
        prelaunch_items = [
            "Complete all permit applications and regulatory approvals",
            "Finalize construction/renovation plans and contractor selection",
            "Develop comprehensive staff training program and begin recruitment",
            "Establish vendor relationships and supply chain agreements",
            "Design and implement marketing materials and digital presence"
        ]
        recommendation.prelaunch_requirements = prelaunch_items
        
        # Launch strategy
        recommendation.launch_strategy = (
            "Implement soft opening for 2 weeks to refine operations, followed by "
            "grand opening event with promotional campaign. Focus on building "
            "initial customer base through introductory offers and community engagement."
        )
        
        # Growth phase planning
        recommendation.growth_phase_plan = (
            "After establishing stable operations (6 months), focus on customer "
            "retention programs, operational efficiency improvements, and exploring "
            "expansion opportunities. Target 10-15% monthly revenue growth through "
            "strategic marketing and service enhancements."
        )
    
    def _generate_market_positioning(self, recommendation: RecommendationResult, 
                                   integrated_data: Dict[str, Any]):
        """Develop market positioning and marketing strategy"""
        
        # Target market focus
        recommendation.target_market_focus = self._identify_target_market(
            recommendation.business_type, integrated_data
        )
        
        # Competitive differentiation
        recommendation.competitive_differentiation = self._develop_differentiation_strategy(
            recommendation.business_type, integrated_data
        )
        
        # Marketing priorities
        monthly_revenue = recommendation.target_monthly_revenue
        marketing_budget = int(monthly_revenue * 0.05)  # 5% of revenue
        
        recommendation.marketing_priorities = {
            "primary_channel": "Digital marketing and social media presence",
            "secondary_channels": "Local partnerships, community events, referral programs",
            "budget": marketing_budget,
            "key_messages": "Quality, convenience, local expertise, competitive pricing"
        }
    
    def _generate_location_recommendations(self, recommendation: RecommendationResult, 
                                         integrated_data: Dict[str, Any]):
        """Generate location-specific optimization strategies"""
        
        # Site optimization
        site_factors = []
        if "site" in integrated_data:
            site_data = integrated_data["site"]
            if site_data.get("visibility_score", 0) < 70:
                site_factors.append("Enhance signage and exterior lighting")
            if site_data.get("accessibility_score", 0) < 70:
                site_factors.append("Improve parking and entrance accessibility")
        
        recommendation.site_optimization = (
            "Focus on maximizing site visibility and accessibility. " + 
            " ".join(site_factors) if site_factors else 
            "Maintain high visibility and ensure easy customer access."
        )
        
        # Infrastructure priorities
        infra_items = []
        if "infrastructure" in integrated_data:
            infra_data = integrated_data["infrastructure"]
            if infra_data.get("technology_infrastructure_score", 0) < 70:
                infra_items.append("Upgrade internet and technology infrastructure")
            if infra_data.get("utilities_infrastructure_score", 0) < 70:
                infra_items.append("Ensure reliable utility services and backup systems")
        
        recommendation.infrastructure_priorities = (
            "Address infrastructure gaps: " + ", ".join(infra_items) if infra_items else
            "Leverage strong existing infrastructure with minor enhancements."
        )
        
        # Community integration
        recommendation.community_integration = (
            "Build strong relationships with local business associations, "
            "participate in community events, and develop partnerships with "
            "complementary businesses to establish local presence and trust."
        )
    
    def _generate_regulatory_roadmap(self, recommendation: RecommendationResult, 
                                   integrated_data: Dict[str, Any]):
        """Create regulatory compliance roadmap"""
        
        # Critical permits timeline
        if "permits" in integrated_data:
            permits_data = integrated_data["permits"]
            timeline_weeks = permits_data.get("total_timeline_weeks", 12)
            recommendation.critical_permits_timeline = f"{timeline_weeks} weeks total for all permits and approvals"
        else:
            recommendation.critical_permits_timeline = "12-16 weeks estimated for typical permit processing"
        
        # Compliance priorities
        recommendation.compliance_priorities = [
            "Business license and zoning compliance verification",
            "Health department permits (if applicable)",
            "Building permits and construction approvals",
            "State-specific industry licenses",
            "Federal and state tax registrations"
        ]
        
        # Professional services
        recommendation.professional_services = [
            "Business attorney for entity formation and contracts",
            "CPA for tax planning and financial systems",
            "Architect/engineer for construction planning",
            "Insurance broker for comprehensive coverage",
            "Marketing consultant for brand development"
        ]
    
    def _generate_kpi_framework(self, recommendation: RecommendationResult, 
                              business_type: str):
        """Establish key performance indicators for monitoring"""
        
        # Industry-specific KPIs
        kpis = {
            "revenue_growth_target": 10,  # % monthly
            "max_cac": 50,  # Customer acquisition cost
            "min_retention_rate": 80,  # %
            "target_operating_margin": 15,  # %
            "min_cash_flow_coverage": 1.25  # ratio
        }
        
        # Adjust based on business type
        if "restaurant" in business_type.lower():
            kpis["min_retention_rate"] = 70
            kpis["target_operating_margin"] = 10
        elif "retail" in business_type.lower():
            kpis["max_cac"] = 30
            kpis["revenue_growth_target"] = 8
        
        recommendation.key_performance_indicators = kpis
    
    def _generate_decision_framework(self, recommendation: RecommendationResult, 
                                   viability_score: float, 
                                   integrated_data: Dict[str, Any]):
        """Create clear go/no-go decision criteria"""
        
        # Proceed criteria
        recommendation.proceed_criteria = [
            "Secure minimum 120% of required capital (including contingency)",
            "Confirm all critical permits can be obtained within timeline",
            "Validate market demand through pre-launch customer surveys",
            "Establish key vendor and supplier relationships",
            "Assemble core management team with relevant experience"
        ]
        
        # Reconsider criteria
        recommendation.reconsider_criteria = [
            "Unable to secure full funding within 60 days",
            "Significant permit or zoning complications arise",
            "Market conditions deteriorate significantly",
            "Key assumptions about costs increase by >20%",
            "Unable to secure qualified management team"
        ]
        
        # Avoid criteria
        recommendation.avoid_criteria = [
            "Total investment exceeds available capital by >30%",
            "Zoning changes or variances are definitively denied",
            "Major competitor announces expansion in immediate area",
            "Economic indicators show significant downturn",
            "Site becomes unavailable or lease terms change substantially"
        ]
        
        # Alternative recommendations
        recommendation.alternative_recommendations = self._generate_alternatives(
            recommendation.business_type, viability_score, integrated_data
        )
    
    def _generate_executive_summary(self, recommendation: RecommendationResult, 
                                  viability_score: float, 
                                  integrated_data: Dict[str, Any]):
        """Create comprehensive executive summary"""
        
        # Narrative summary
        rating = recommendation.overall_viability_rating
        recommendation.executive_summary_narrative = (
            f"This comprehensive feasibility analysis for establishing a {recommendation.business_type} "
            f"in {recommendation.location} indicates {rating.lower()} viability with a confidence "
            f"level of {viability_score}%. The venture shows {self._get_summary_strength(viability_score)} "
            f"potential based on market conditions, financial projections, and location factors. "
            f"Success will depend on {self._get_critical_success_factors(integrated_data)}."
        )
        
        # Final recommendation statement
        if viability_score >= 70:
            recommendation.final_recommendation_statement = (
                f"We recommend proceeding with the {recommendation.business_type} venture, "
                f"following the detailed operational roadmap and risk mitigation strategies outlined. "
                f"With proper execution and adequate capital, this business has strong potential for success."
            )
        elif viability_score >= 50:
            recommendation.final_recommendation_statement = (
                f"We cautiously recommend proceeding with the {recommendation.business_type} venture, "
                f"but only after addressing the identified risk factors and ensuring all critical "
                f"success factors are in place. Close monitoring and adaptive management will be essential."
            )
        else:
            recommendation.final_recommendation_statement = (
                f"We recommend against proceeding with the {recommendation.business_type} venture "
                f"under current conditions. Consider the alternative approaches suggested or "
                f"revisit this opportunity when market conditions improve."
            )
        
        # Timeline recommendations
        current_date = datetime.now()
        recommendation.decision_deadline = (current_date + timedelta(days=30)).strftime("%B %d, %Y")
        recommendation.optimal_launch_window = self._determine_launch_window(
            recommendation.business_type, current_date
        )
        recommendation.expected_profitability_timeline = f"{recommendation.breakeven_timeline} months"
    
    def populate_template(self, template_content: str, recommendation: RecommendationResult,
                         integrated_data: Dict[str, Any]) -> str:
        """Populate the recommendations template with analysis results"""
        
        # Basic replacements
        content = template_content.replace("{business_type}", recommendation.business_type)
        content = content.replace("{location}", recommendation.location)
        content = content.replace("{overall_viability_rating}", recommendation.overall_viability_rating)
        content = content.replace("{primary_recommendation}", recommendation.primary_recommendation)
        content = content.replace("{confidence_level}", str(recommendation.confidence_level))
        
        # Success factors
        strategic_advantages = "\n".join([f"- {adv}" for adv in recommendation.strategic_advantages])
        content = content.replace("{strategic_advantages}", strategic_advantages)
        content = content.replace("{market_demand_summary}", recommendation.market_demand_summary)
        content = content.replace("{location_advantages_summary}", recommendation.location_advantages_summary)
        content = content.replace("{economic_environment_summary}", recommendation.economic_environment_summary)
        content = content.replace("{competitive_position_summary}", recommendation.competitive_position_summary)
        
        uvps = "\n".join([f"- {uvp}" for uvp in recommendation.unique_value_propositions])
        content = content.replace("{unique_value_propositions}", uvps)
        
        # Risk factors
        if recommendation.top_risks:
            for i, (name, score, impact, mitigation) in enumerate(recommendation.top_risks[:3], 1):
                content = content.replace(f"{{risk_{i}_name}}", name)
                content = content.replace(f"{{risk_{i}_score}}", str(int(score)))
                content = content.replace(f"{{risk_{i}_impact}}", impact)
                content = content.replace(f"{{risk_{i}_mitigation}}", mitigation)
        
        content = content.replace("{contingency_planning_recommendations}", recommendation.contingency_planning)
        
        # Financial recommendations
        content = content.replace("{total_initial_investment}", f"{recommendation.total_initial_investment:,}")
        content = content.replace("{working_capital_reserve}", f"{recommendation.working_capital_reserve:,}")
        content = content.replace("{contingency_fund}", f"{recommendation.contingency_fund:,}")
        content = content.replace("{total_capital_needed}", f"{recommendation.total_capital_needed:,}")
        content = content.replace("{target_monthly_revenue}", f"{recommendation.target_monthly_revenue:,}")
        content = content.replace("{breakeven_timeline}", str(recommendation.breakeven_timeline))
        content = content.replace("{target_net_margin}", str(recommendation.target_net_margin))
        content = content.replace("{roi_projection}", str(recommendation.roi_projection))
        content = content.replace("{funding_strategy_recommendations}", recommendation.funding_strategy)
        
        # Operational recommendations
        immediate_items = "\n".join([f"- {item}" for item in recommendation.immediate_action_items])
        content = content.replace("{immediate_action_items}", immediate_items)
        
        prelaunch_items = "\n".join([f"- {item}" for item in recommendation.prelaunch_requirements])
        content = content.replace("{prelaunch_requirements}", prelaunch_items)
        
        content = content.replace("{launch_strategy_recommendations}", recommendation.launch_strategy)
        content = content.replace("{growth_phase_recommendations}", recommendation.growth_phase_plan)
        
        # Market positioning
        content = content.replace("{target_market_recommendations}", recommendation.target_market_focus)
        content = content.replace("{competitive_differentiation_strategy}", recommendation.competitive_differentiation)
        
        mp = recommendation.marketing_priorities
        content = content.replace("{primary_marketing_channel}", mp.get("primary_channel", ""))
        content = content.replace("{secondary_marketing_channels}", mp.get("secondary_channels", ""))
        content = content.replace("{marketing_budget}", f"{mp.get('budget', 0):,}")
        content = content.replace("{key_marketing_messages}", mp.get("key_messages", ""))
        
        # Location-specific
        content = content.replace("{site_optimization_recommendations}", recommendation.site_optimization)
        content = content.replace("{infrastructure_priority_recommendations}", recommendation.infrastructure_priorities)
        content = content.replace("{community_integration_strategy}", recommendation.community_integration)
        
        # Regulatory
        content = content.replace("{critical_permits_timeline}", recommendation.critical_permits_timeline)
        
        compliance_list = "\n".join([f"- {item}" for item in recommendation.compliance_priorities])
        content = content.replace("{compliance_priority_list}", compliance_list)
        
        services_list = "\n".join([f"- {service}" for service in recommendation.professional_services])
        content = content.replace("{professional_services_recommendations}", services_list)
        
        # KPIs
        kpis = recommendation.key_performance_indicators
        content = content.replace("{revenue_growth_target}", str(kpis.get("revenue_growth_target", 10)))
        content = content.replace("{max_cac}", str(kpis.get("max_cac", 50)))
        content = content.replace("{min_retention_rate}", str(kpis.get("min_retention_rate", 80)))
        content = content.replace("{target_operating_margin}", str(kpis.get("target_operating_margin", 15)))
        content = content.replace("{min_cash_flow_coverage}", str(kpis.get("min_cash_flow_coverage", 1.25)))
        
        # Decision framework
        proceed_list = "\n".join([f"- {criteria}" for criteria in recommendation.proceed_criteria])
        content = content.replace("{proceed_criteria}", proceed_list)
        
        reconsider_list = "\n".join([f"- {criteria}" for criteria in recommendation.reconsider_criteria])
        content = content.replace("{reconsider_criteria}", reconsider_list)
        
        avoid_list = "\n".join([f"- {criteria}" for criteria in recommendation.avoid_criteria])
        content = content.replace("{avoid_criteria}", avoid_list)
        
        content = content.replace("{alternative_recommendations}", recommendation.alternative_recommendations)
        
        # Executive summary
        content = content.replace("{executive_summary_narrative}", recommendation.executive_summary_narrative)
        content = content.replace("{final_recommendation_statement}", recommendation.final_recommendation_statement)
        content = content.replace("{decision_deadline}", recommendation.decision_deadline)
        content = content.replace("{optimal_launch_window}", recommendation.optimal_launch_window)
        content = content.replace("{expected_profitability_timeline}", recommendation.expected_profitability_timeline)
        
        # Data sources
        data_sources = self._list_data_sources(integrated_data)
        for i, source in enumerate(data_sources[:5], 1):
            content = content.replace(f"{{data_source_{i}}}", source)
        
        # Dates
        content = content.replace("{analysis_date}", datetime.now().strftime("%B %d, %Y"))
        validity_date = (datetime.now() + timedelta(days=90)).strftime("%B %d, %Y")
        content = content.replace("{recommendation_validity_date}", validity_date)
        
        return content
    
    # Helper methods
    def _summarize_market_demand(self, integrated_data: Dict[str, Any]) -> str:
        """Generate market demand summary"""
        if "market" in integrated_data:
            market = integrated_data["market"]
            growth = market.get("growth_rate", 3)
            saturation = market.get("saturation_score", 50)
            return f"Market shows {growth}% annual growth with {100-saturation}% unmet demand potential"
        return "Moderate market demand with opportunities for differentiated offerings"
    
    def _summarize_location_advantages(self, integrated_data: Dict[str, Any]) -> str:
        """Generate location advantages summary"""
        advantages = []
        
        if "traffic" in integrated_data:
            traffic = integrated_data["traffic"]
            if traffic.get("traffic_volume_score", 0) > 70:
                advantages.append("high traffic volume")
        
        if "site" in integrated_data:
            site = integrated_data["site"]
            if site.get("visibility_score", 0) > 70:
                advantages.append("excellent visibility")
        
        if advantages:
            return f"Location offers {', '.join(advantages)} with strong customer accessibility"
        return "Location provides adequate visibility and accessibility for target market"
    
    def _summarize_economic_environment(self, integrated_data: Dict[str, Any]) -> str:
        """Generate economic environment summary"""
        return "Stable economic conditions with positive growth indicators support business establishment"
    
    def _summarize_competitive_position(self, integrated_data: Dict[str, Any]) -> str:
        """Generate competitive position summary"""
        if "market" in integrated_data:
            saturation = integrated_data["market"].get("saturation_score", 50)
            if saturation < 40:
                return "Low competitive density provides strong market entry opportunity"
            elif saturation < 60:
                return "Moderate competition allows for differentiated positioning"
            else:
                return "Competitive market requires strong differentiation and execution"
        return "Market competition level supports entry with proper positioning"
    
    def _identify_unique_value_props(self, business_type: str, 
                                   integrated_data: Dict[str, Any]) -> List[str]:
        """Identify unique value propositions"""
        uvps = []
        
        # Generic UVPs based on analysis
        if "traffic" in integrated_data:
            traffic = integrated_data["traffic"]
            if traffic.get("accessibility_score", 0) > 80:
                uvps.append("Superior location accessibility and convenience")
        
        # Business type specific
        if "restaurant" in business_type.lower():
            uvps.append("Unique menu offerings tailored to local preferences")
            uvps.append("Fast service and quality consistency")
        elif "retail" in business_type.lower():
            uvps.append("Curated product selection not available elsewhere locally")
            uvps.append("Personalized customer service and expertise")
        
        # Add generic UVPs
        uvps.extend([
            "Strong community engagement and local partnerships",
            "Technology-enabled customer experience",
            "Competitive pricing with superior value"
        ])
        
        return uvps[:5]  # Return top 5
    
    def _generate_contingency_plan(self, risks: List[Tuple]) -> str:
        """Generate contingency planning recommendations"""
        if not risks:
            return "Maintain 15% capital reserve and monitor KPIs monthly for early warning signs"
        
        top_risk = risks[0][0] if risks else "operational challenges"
        return (
            f"Develop specific contingency plans for {top_risk}. "
            f"Maintain 20% capital reserve, establish credit facilities, "
            f"and create detailed response protocols for identified risk scenarios. "
            f"Review and update plans quarterly."
        )
    
    def _generate_funding_strategy(self, total_capital: int, business_type: str) -> str:
        """Generate funding strategy recommendations"""
        if total_capital < 100000:
            return (
                "Consider combination of personal investment (40%), "
                "small business loans (40%), and friends/family investment (20%). "
                "SBA microloans may be appropriate for this capital level."
            )
        elif total_capital < 500000:
            return (
                "Recommend SBA 7(a) loan for 60-70% of capital needs, "
                "complemented by personal investment (20-30%) and potential "
                "equipment financing. Consider ROBS if retirement funds available."
            )
        else:
            return (
                "Pursue SBA 504 loan for real estate/equipment (40%), "
                "bank term loan for working capital (30%), personal investment (20%), "
                "and potential investor partnerships (10%). Consider phased funding approach."
            )
    
    def _identify_target_market(self, business_type: str, 
                              integrated_data: Dict[str, Any]) -> str:
        """Identify primary target market"""
        # This would be enhanced with demographic data
        if "restaurant" in business_type.lower():
            return "Primary: Families and professionals within 3-mile radius. Secondary: Commuters and nearby business employees"
        elif "retail" in business_type.lower():
            return "Primary: Local residents aged 25-54 with disposable income. Secondary: Destination shoppers seeking specialty items"
        else:
            return "Primary: Local businesses and residents within 5-mile radius. Secondary: Regional customers seeking specialized services"
    
    def _develop_differentiation_strategy(self, business_type: str, 
                                        integrated_data: Dict[str, Any]) -> str:
        """Develop competitive differentiation strategy"""
        strategies = []
        
        # Based on competition level
        if "market" in integrated_data:
            saturation = integrated_data["market"].get("saturation_score", 50)
            if saturation > 60:
                strategies.append("Focus on superior customer service and experience")
                strategies.append("Implement loyalty programs and personalization")
        
        # Generic strategies
        strategies.extend([
            "Leverage technology for operational efficiency and customer convenience",
            "Build strong local brand identity and community connections",
            "Offer unique products/services not available from competitors"
        ])
        
        return ". ".join(strategies[:3])
    
    def _generate_alternatives(self, business_type: str, viability_score: float,
                             integrated_data: Dict[str, Any]) -> str:
        """Generate alternative recommendations"""
        if viability_score < 40:
            return (
                f"Consider: 1) Different location with better demographics, "
                f"2) Modified business concept with lower investment requirements, "
                f"3) Franchise opportunity with proven model, "
                f"4) Phased approach starting with minimal viable concept"
            )
        else:
            return (
                f"If primary plan faces obstacles, consider: "
                f"1) Alternative sites within same market area, "
                f"2) Partnership opportunities to share risk and resources, "
                f"3) Scaled-down initial concept with growth potential"
            )
    
    def _get_summary_strength(self, score: float) -> str:
        """Convert score to strength description"""
        if score >= 80:
            return "very strong"
        elif score >= 70:
            return "strong"
        elif score >= 60:
            return "moderate"
        elif score >= 50:
            return "adequate"
        else:
            return "limited"
    
    def _get_critical_success_factors(self, integrated_data: Dict[str, Any]) -> str:
        """Identify critical success factors from analysis"""
        factors = []
        
        if "risk" in integrated_data:
            risk_score = integrated_data["risk"].get("composite_risk_score", 50)
            if risk_score > 60:
                factors.append("strong risk management")
        
        if "market" in integrated_data:
            saturation = integrated_data["market"].get("saturation_score", 50)
            if saturation > 60:
                factors.append("effective differentiation")
        
        factors.extend(["operational excellence", "financial discipline"])
        
        return ", ".join(factors[:3])
    
    def _determine_launch_window(self, business_type: str, current_date: datetime) -> str:
        """Determine optimal launch window based on business type and seasonality"""
        month = current_date.month
        
        if "restaurant" in business_type.lower():
            if month in [11, 12, 1, 2]:  # Winter
                return "March - May (avoiding slow winter months)"
            else:
                return "Within 4-6 months (avoiding holiday season launch)"
        elif "retail" in business_type.lower():
            if month in [7, 8, 9]:  # Late summer/fall
                return "October - November (capture holiday shopping)"
            else:
                return "August - September (back-to-school season)"
        else:
            return "Within 3-5 months based on permit timeline"
    
    def _list_data_sources(self, integrated_data: Dict[str, Any]) -> List[str]:
        """List all data sources used in analysis"""
        sources = [
            "Wisconsin demographic and economic data",
            "Industry benchmarks and financial models",
            "Local market competition analysis",
            "Traffic patterns and accessibility studies",
            "Regulatory requirements and compliance data"
        ]
        
        if "market" in integrated_data:
            sources.append("Market saturation and demand analysis")
        if "risk" in integrated_data:
            sources.append("Comprehensive risk assessment modeling")
        
        return sources