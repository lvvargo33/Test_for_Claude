#!/usr/bin/env python3
"""
Risk Assessment Analyzer
========================

Comprehensive risk assessment system that integrates market, financial, operational, 
and strategic risk factors from all previous analysis sections. Uses quantitative 
risk modeling, Monte Carlo simulation, and Wisconsin-specific risk considerations.

Features:
- Multi-dimensional risk scoring (0-100 scale)
- Data integration from all previous sections
- Industry-specific risk benchmarks
- Monte Carlo simulation for sensitivity analysis
- Visual risk dashboards and heatmaps
- Risk mitigation strategy recommendations
"""

import json
import logging
import math
import numpy as np
import matplotlib.pyplot as plt
import matplotlib.patches as mpatches
import seaborn as sns
from datetime import datetime, timedelta
from typing import Dict, List, Any, Optional, Tuple
from dataclasses import dataclass

# Import existing analyzers for data integration
try:
    from integrated_business_analyzer import IntegratedBusinessAnalyzer
    from revenue_projections_analyzer import RevenueProjectionsAnalyzer
    from cost_analysis_analyzer import CostAnalysisAnalyzer
    from business_habitat_analyzer import BusinessHabitatAnalyzer
    from simplified_market_saturation_analyzer import SimplifiedMarketSaturationAnalyzer
    from universal_competitive_analyzer import UniversalCompetitiveAnalyzer
except ImportError as e:
    logging.warning(f"Some dependencies not available: {e}")

logger = logging.getLogger(__name__)

@dataclass
class RiskAssessment:
    """Data structure for comprehensive risk assessment results"""
    business_type: str
    location: str
    composite_risk_score: float
    market_risk_score: float
    financial_risk_score: float
    operational_risk_score: float
    strategic_risk_score: float
    business_risk_level: str
    risk_scenarios: Dict[str, Dict[str, float]]
    key_risk_factors: List[str]
    risk_mitigation_strategies: List[str]
    monte_carlo_results: Dict[str, float]
    risk_correlations: Dict[str, float]

class RiskAssessmentAnalyzer:
    """Comprehensive risk assessment for Section 4.3"""
    
    def __init__(self):
        """Initialize the risk assessment analyzer"""
        
        # Industry risk benchmarks by business type
        self.industry_risk_benchmarks = {
            'restaurant': {
                'failure_rate_5yr': 60.0,  # 5-year failure rate %
                'industry_volatility': 75.0,  # Volatility index 0-100
                'regulatory_burden': 65.0,  # Regulatory complexity 0-100
                'competition_intensity': 80.0,  # Competition intensity 0-100
                'seasonality_risk': 45.0,  # Seasonal variance risk 0-100
                'labor_dependency': 85.0,  # Labor dependency risk 0-100
                'capital_intensity': 70.0,  # Capital requirements risk 0-100
                'economic_sensitivity': 75.0  # Economic cycle sensitivity 0-100
            },
            'hair_salon': {
                'failure_rate_5yr': 45.0,
                'industry_volatility': 55.0,
                'regulatory_burden': 50.0,
                'competition_intensity': 70.0,
                'seasonality_risk': 30.0,
                'labor_dependency': 90.0,
                'capital_intensity': 45.0,
                'economic_sensitivity': 60.0
            },
            'auto_repair': {
                'failure_rate_5yr': 40.0,
                'industry_volatility': 45.0,
                'regulatory_burden': 55.0,
                'competition_intensity': 60.0,
                'seasonality_risk': 25.0,
                'labor_dependency': 80.0,
                'capital_intensity': 65.0,
                'economic_sensitivity': 45.0
            },
            'retail_clothing': {
                'failure_rate_5yr': 65.0,
                'industry_volatility': 80.0,
                'regulatory_burden': 40.0,
                'competition_intensity': 85.0,
                'seasonality_risk': 70.0,
                'labor_dependency': 60.0,
                'capital_intensity': 55.0,
                'economic_sensitivity': 80.0
            },
            'fitness_center': {
                'failure_rate_5yr': 55.0,
                'industry_volatility': 65.0,
                'regulatory_burden': 60.0,
                'competition_intensity': 75.0,
                'seasonality_risk': 40.0,
                'labor_dependency': 70.0,
                'capital_intensity': 75.0,
                'economic_sensitivity': 70.0
            },
            'coffee_shop': {
                'failure_rate_5yr': 50.0,
                'industry_volatility': 60.0,
                'regulatory_burden': 55.0,
                'competition_intensity': 85.0,
                'seasonality_risk': 35.0,
                'labor_dependency': 75.0,
                'capital_intensity': 60.0,
                'economic_sensitivity': 65.0
            },
            'grocery_store': {
                'failure_rate_5yr': 35.0,
                'industry_volatility': 40.0,
                'regulatory_burden': 70.0,
                'competition_intensity': 70.0,
                'seasonality_risk': 20.0,
                'labor_dependency': 65.0,
                'capital_intensity': 80.0,
                'economic_sensitivity': 35.0
            },
            'pharmacy': {
                'failure_rate_5yr': 25.0,
                'industry_volatility': 30.0,
                'regulatory_burden': 90.0,
                'competition_intensity': 50.0,
                'seasonality_risk': 15.0,
                'labor_dependency': 70.0,
                'capital_intensity': 85.0,
                'economic_sensitivity': 25.0
            }
        }
        
        # Wisconsin-specific risk factors
        self.wisconsin_risk_factors = {
            'seasonal_weather_risk': 65.0,  # Winter weather impact
            'agricultural_economy_correlation': 35.0,  # Agricultural dependency
            'tourism_dependency': 45.0,  # Tourism seasonal impact
            'manufacturing_concentration': 55.0,  # Manufacturing economy risk
            'population_stability': 75.0,  # Population retention
            'tax_burden_risk': 60.0,  # State/local tax burden
            'regulatory_environment': 50.0,  # Business-friendly rating
            'infrastructure_quality': 80.0  # Infrastructure support
        }
        
        # Risk correlation matrix
        self.risk_correlations = {
            'market_financial': 0.75,
            'market_operational': 0.45,
            'market_strategic': 0.60,
            'financial_operational': 0.55,
            'financial_strategic': 0.65,
            'operational_strategic': 0.50
        }

    def analyze_comprehensive_risk(self, business_type: str, location: str, 
                                 integrated_data: Dict = None) -> RiskAssessment:
        """Perform comprehensive risk assessment integrating all previous sections"""
        
        logger.info(f"Starting comprehensive risk assessment for {business_type} at {location}")
        
        # Initialize default data if not provided
        if integrated_data is None:
            integrated_data = self._generate_fallback_data(business_type, location)
        
        # Analyze each risk dimension
        market_risk = self._analyze_market_risk(business_type, integrated_data)
        financial_risk = self._analyze_financial_risk(business_type, integrated_data)
        operational_risk = self._analyze_operational_risk(business_type, integrated_data)
        strategic_risk = self._analyze_strategic_risk(business_type, integrated_data)
        
        # Calculate composite risk score
        composite_risk = self._calculate_composite_risk_score(
            market_risk, financial_risk, operational_risk, strategic_risk
        )
        
        # Determine risk level classification
        risk_level = self._classify_risk_level(composite_risk)
        
        # Run Monte Carlo simulation
        monte_carlo_results = self._run_monte_carlo_simulation(
            business_type, integrated_data
        )
        
        # Generate risk scenarios
        risk_scenarios = self._generate_risk_scenarios(
            market_risk, financial_risk, operational_risk, strategic_risk
        )
        
        # Identify key risk factors
        key_risk_factors = self._identify_key_risk_factors(
            business_type, market_risk, financial_risk, operational_risk, strategic_risk
        )
        
        # Generate risk mitigation strategies
        mitigation_strategies = self._generate_mitigation_strategies(
            business_type, key_risk_factors, risk_level
        )
        
        return RiskAssessment(
            business_type=business_type,
            location=location,
            composite_risk_score=composite_risk,
            market_risk_score=market_risk,
            financial_risk_score=financial_risk,
            operational_risk_score=operational_risk,
            strategic_risk_score=strategic_risk,
            business_risk_level=risk_level,
            risk_scenarios=risk_scenarios,
            key_risk_factors=key_risk_factors,
            risk_mitigation_strategies=mitigation_strategies,
            monte_carlo_results=monte_carlo_results,
            risk_correlations=self.risk_correlations
        )

    def _analyze_market_risk(self, business_type: str, data: Dict) -> float:
        """Analyze market-related risks"""
        
        # Get industry benchmarks
        industry_bench = self.industry_risk_benchmarks.get(business_type, 
                                                          self.industry_risk_benchmarks['restaurant'])
        
        # Competition and market saturation risk (30% weight)
        competition_density = data.get('competition_density', 5)
        market_saturation = data.get('market_saturation_level', 65)
        competition_risk = min(100, (competition_density * 10) + (market_saturation * 0.5))
        
        # Economic sensitivity risk (25% weight)
        economic_growth_rate = data.get('economic_growth_rate', 2.5)
        unemployment_rate = data.get('unemployment_rate', 4.5)
        economic_risk = industry_bench['economic_sensitivity'] * (1 + (unemployment_rate - 4.0) / 10)
        
        # Demand volatility risk (25% weight)
        seasonality_risk = industry_bench['seasonality_risk']
        consumer_confidence = data.get('consumer_confidence', 85)
        demand_volatility = seasonality_risk * (1 + (85 - consumer_confidence) / 100)
        
        # Market growth potential risk (20% weight)
        population_growth = data.get('population_growth_rate', 1.2)
        market_maturity = data.get('market_maturity_level', 70)
        growth_risk = max(20, market_maturity - (population_growth * 10))
        
        # Calculate weighted market risk
        market_risk = (
            competition_risk * 0.30 +
            economic_risk * 0.25 +
            demand_volatility * 0.25 +
            growth_risk * 0.20
        )
        
        return min(100, max(0, market_risk))

    def _analyze_financial_risk(self, business_type: str, data: Dict) -> float:
        """Analyze financial risks"""
        
        # Get industry benchmarks
        industry_bench = self.industry_risk_benchmarks.get(business_type, 
                                                          self.industry_risk_benchmarks['restaurant'])
        
        # Cash flow and liquidity risk (35% weight)
        breakeven_months = data.get('breakeven_months', 18)
        startup_costs = data.get('total_startup_costs', 200000)
        monthly_operating = data.get('realistic_monthly_operating', 25000)
        
        # Calculate cash flow risk based on burn rate and breakeven
        cash_flow_risk = min(100, (breakeven_months - 12) * 8 + (startup_costs / 10000))
        
        # Cost structure risk (30% weight)
        fixed_variable_ratio = data.get('fixed_variable_cost_ratio', 0.6)
        operating_leverage = min(100, fixed_variable_ratio * 100)
        
        # Revenue predictability risk (20% weight)
        revenue_variance = data.get('revenue_variance', 25)
        customer_concentration = data.get('customer_concentration_risk', 40)
        revenue_risk = (revenue_variance + customer_concentration) / 2
        
        # Capital requirements risk (15% weight)
        capital_intensity = industry_bench['capital_intensity']
        working_capital_ratio = data.get('working_capital_ratio', 0.15)
        capital_risk = capital_intensity * (1 + working_capital_ratio)
        
        # Calculate weighted financial risk
        financial_risk = (
            cash_flow_risk * 0.35 +
            operating_leverage * 0.30 +
            revenue_risk * 0.20 +
            capital_risk * 0.15
        )
        
        return min(100, max(0, financial_risk))

    def _analyze_operational_risk(self, business_type: str, data: Dict) -> float:
        """Analyze operational risks"""
        
        # Get industry benchmarks
        industry_bench = self.industry_risk_benchmarks.get(business_type, 
                                                          self.industry_risk_benchmarks['restaurant'])
        
        # Location and site risk (30% weight)
        traffic_volume = data.get('traffic_volume_score', 75)
        visibility_score = data.get('visibility_score', 80)
        parking_score = data.get('parking_score', 70)
        location_risk = 100 - ((traffic_volume + visibility_score + parking_score) / 3)
        
        # Labor and staffing risk (35% weight)
        local_unemployment = data.get('unemployment_rate', 4.5)
        labor_availability = max(0, 100 - (local_unemployment * 10))
        labor_dependency = industry_bench['labor_dependency']
        turnover_risk = data.get('expected_turnover_rate', 35)
        staffing_risk = (labor_dependency + turnover_risk + (100 - labor_availability)) / 3
        
        # Regulatory compliance risk (20% weight)
        regulatory_burden = industry_bench['regulatory_burden']
        permit_complexity = data.get('permit_complexity_score', 60)
        compliance_risk = (regulatory_burden + permit_complexity) / 2
        
        # Supply chain risk (15% weight)
        supplier_concentration = data.get('supplier_concentration_risk', 45)
        supply_chain_complexity = data.get('supply_chain_complexity', 40)
        supply_risk = (supplier_concentration + supply_chain_complexity) / 2
        
        # Calculate weighted operational risk
        operational_risk = (
            location_risk * 0.30 +
            staffing_risk * 0.35 +
            compliance_risk * 0.20 +
            supply_risk * 0.15
        )
        
        return min(100, max(0, operational_risk))

    def _analyze_strategic_risk(self, business_type: str, data: Dict) -> float:
        """Analyze strategic risks"""
        
        # Get industry benchmarks
        industry_bench = self.industry_risk_benchmarks.get(business_type, 
                                                          self.industry_risk_benchmarks['restaurant'])
        
        # Competitive position risk (40% weight)
        competition_intensity = industry_bench['competition_intensity']
        market_share_vulnerability = data.get('market_share_vulnerability', 60)
        differentiation_strength = data.get('differentiation_strength', 40)
        competitive_risk = (competition_intensity + market_share_vulnerability + 
                          (100 - differentiation_strength)) / 3
        
        # Growth sustainability risk (25% weight)
        market_growth_potential = data.get('market_growth_potential', 3.0)
        scalability_constraints = data.get('scalability_constraints', 50)
        growth_risk = max(20, scalability_constraints - (market_growth_potential * 10))
        
        # Technology and innovation risk (20% weight)
        digital_transformation_need = data.get('digital_transformation_need', 60)
        technology_adoption_rate = data.get('technology_adoption_rate', 70)
        innovation_risk = (digital_transformation_need + (100 - technology_adoption_rate)) / 2
        
        # External environment risk (15% weight)
        economic_policy_risk = data.get('economic_policy_risk', 45)
        industry_disruption_risk = data.get('industry_disruption_risk', 40)
        external_risk = (economic_policy_risk + industry_disruption_risk) / 2
        
        # Calculate weighted strategic risk
        strategic_risk = (
            competitive_risk * 0.40 +
            growth_risk * 0.25 +
            innovation_risk * 0.20 +
            external_risk * 0.15
        )
        
        return min(100, max(0, strategic_risk))

    def _calculate_composite_risk_score(self, market_risk: float, financial_risk: float,
                                      operational_risk: float, strategic_risk: float) -> float:
        """Calculate overall composite risk score with correlations"""
        
        # Base weighted average (equal weighting with slight emphasis on financial)
        base_composite = (
            market_risk * 0.25 +
            financial_risk * 0.30 +
            operational_risk * 0.25 +
            strategic_risk * 0.20
        )
        
        # Apply correlation adjustments
        correlation_adjustment = 0
        if market_risk > 70 and financial_risk > 70:
            correlation_adjustment += 5  # High correlation penalty
        if operational_risk > 70 and strategic_risk > 70:
            correlation_adjustment += 3
        
        composite_risk = min(100, base_composite + correlation_adjustment)
        return composite_risk

    def _classify_risk_level(self, composite_risk: float) -> str:
        """Classify business risk level based on composite score"""
        
        if composite_risk >= 80:
            return "Very High Risk"
        elif composite_risk >= 65:
            return "High Risk"
        elif composite_risk >= 50:
            return "Moderate Risk"
        elif composite_risk >= 35:
            return "Low-Moderate Risk"
        else:
            return "Low Risk"

    def _run_monte_carlo_simulation(self, business_type: str, data: Dict) -> Dict[str, float]:
        """Run Monte Carlo simulation for risk sensitivity analysis"""
        
        # Simulation parameters
        num_simulations = 1000
        
        # Base financial parameters
        monthly_revenue_base = data.get('realistic_monthly_revenue', 80000)
        monthly_costs_base = data.get('realistic_monthly_operating', 65000)
        
        # Variance assumptions
        revenue_std = monthly_revenue_base * 0.20  # 20% standard deviation
        cost_std = monthly_costs_base * 0.10  # 10% standard deviation
        
        # Run simulations
        monthly_profits = []
        breakeven_months = []
        
        for _ in range(num_simulations):
            # Generate random revenue and cost scenarios
            monthly_revenue = np.random.normal(monthly_revenue_base, revenue_std)
            monthly_costs = np.random.normal(monthly_costs_base, cost_std)
            
            monthly_profit = monthly_revenue - monthly_costs
            monthly_profits.append(monthly_profit)
            
            # Calculate months to break-even for this scenario
            if monthly_profit > 0:
                startup_costs = data.get('total_startup_costs', 200000)
                months_to_breakeven = startup_costs / monthly_profit
                breakeven_months.append(min(60, months_to_breakeven))  # Cap at 60 months
            else:
                breakeven_months.append(60)  # Never breaks even
        
        # Calculate statistics
        profitability_probability = (np.array(monthly_profits) > 0).mean() * 100
        breakeven_probability = (np.array(breakeven_months) <= 18).mean() * 100
        
        # Value at Risk (95% confidence)
        value_at_risk = np.percentile(monthly_profits, 5)
        expected_shortfall = np.mean([p for p in monthly_profits if p <= value_at_risk])
        
        return {
            'profitability_probability': profitability_probability,
            'breakeven_probability': breakeven_probability,
            'value_at_risk': abs(value_at_risk) if value_at_risk < 0 else 0,
            'expected_shortfall': abs(expected_shortfall) if expected_shortfall < 0 else 0,
            'avg_monthly_profit': np.mean(monthly_profits),
            'profit_std_deviation': np.std(monthly_profits)
        }

    def _generate_risk_scenarios(self, market_risk: float, financial_risk: float,
                               operational_risk: float, strategic_risk: float) -> Dict[str, Dict[str, float]]:
        """Generate risk scenarios for different conditions"""
        
        scenarios = {
            'high_risk': {
                'market_risk': min(100, market_risk * 1.3),
                'financial_risk': min(100, financial_risk * 1.25),
                'operational_risk': min(100, operational_risk * 1.2),
                'strategic_risk': min(100, strategic_risk * 1.25),
                'probability': 15.0
            },
            'moderate_risk': {
                'market_risk': market_risk,
                'financial_risk': financial_risk,
                'operational_risk': operational_risk,
                'strategic_risk': strategic_risk,
                'probability': 70.0
            },
            'low_risk': {
                'market_risk': max(10, market_risk * 0.7),
                'financial_risk': max(10, financial_risk * 0.75),
                'operational_risk': max(10, operational_risk * 0.8),
                'strategic_risk': max(10, strategic_risk * 0.75),
                'probability': 15.0
            }
        }
        
        # Calculate composite scores for each scenario
        for scenario_name, scenario_data in scenarios.items():
            composite = (
                scenario_data['market_risk'] * 0.25 +
                scenario_data['financial_risk'] * 0.30 +
                scenario_data['operational_risk'] * 0.25 +
                scenario_data['strategic_risk'] * 0.20
            )
            scenario_data['composite_risk'] = composite
        
        return scenarios

    def _identify_key_risk_factors(self, business_type: str, market_risk: float, 
                                 financial_risk: float, operational_risk: float, 
                                 strategic_risk: float) -> List[str]:
        """Identify the most critical risk factors"""
        
        risk_factors = []
        
        # High-risk thresholds
        high_risk_threshold = 70
        
        if market_risk >= high_risk_threshold:
            risk_factors.extend([
                "High market competition and saturation levels",
                "Economic sensitivity and demand volatility",
                "Limited market growth potential"
            ])
        
        if financial_risk >= high_risk_threshold:
            risk_factors.extend([
                "Extended break-even timeline and cash flow pressure",
                "High fixed cost structure and operating leverage",
                "Significant capital requirements and funding risk"
            ])
        
        if operational_risk >= high_risk_threshold:
            risk_factors.extend([
                "Location accessibility and visibility challenges",
                "Labor availability and retention difficulties",
                "Complex regulatory compliance requirements"
            ])
        
        if strategic_risk >= high_risk_threshold:
            risk_factors.extend([
                "Intense competitive pressure and market position vulnerability",
                "Limited growth scalability and sustainability",
                "Technology adoption and innovation requirements"
            ])
        
        # Industry-specific risks
        industry_bench = self.industry_risk_benchmarks.get(business_type, 
                                                          self.industry_risk_benchmarks['restaurant'])
        
        if industry_bench['failure_rate_5yr'] >= 50:
            risk_factors.append(f"High industry failure rate ({industry_bench['failure_rate_5yr']}% in 5 years)")
        
        if industry_bench['seasonality_risk'] >= 60:
            risk_factors.append("Significant seasonal revenue fluctuations")
        
        # Wisconsin-specific risks
        if self.wisconsin_risk_factors['seasonal_weather_risk'] >= 60:
            risk_factors.append("Winter weather impact on business operations")
        
        return risk_factors[:8]  # Return top 8 most critical factors

    def _generate_mitigation_strategies(self, business_type: str, key_risk_factors: List[str], 
                                      risk_level: str) -> List[str]:
        """Generate specific risk mitigation strategies"""
        
        strategies = []
        
        # Market risk mitigation
        if any("competition" in factor.lower() or "market" in factor.lower() for factor in key_risk_factors):
            strategies.extend([
                "Develop strong value proposition and customer differentiation strategy",
                "Implement comprehensive market research and competitor monitoring system",
                "Build customer loyalty programs and retention strategies"
            ])
        
        # Financial risk mitigation
        if any("cash flow" in factor.lower() or "break-even" in factor.lower() or "capital" in factor.lower() for factor in key_risk_factors):
            strategies.extend([
                "Establish 6-month operating expense emergency fund",
                "Implement robust cash flow forecasting and management systems",
                "Negotiate favorable payment terms with suppliers and flexible lease agreements"
            ])
        
        # Operational risk mitigation
        if any("location" in factor.lower() or "labor" in factor.lower() or "regulatory" in factor.lower() for factor in key_risk_factors):
            strategies.extend([
                "Develop comprehensive staff training and retention programs",
                "Establish backup suppliers and operational contingency plans",
                "Implement compliance monitoring and legal review processes"
            ])
        
        # Strategic risk mitigation
        if any("competitive" in factor.lower() or "growth" in factor.lower() or "technology" in factor.lower() for factor in key_risk_factors):
            strategies.extend([
                "Invest in technology adoption and digital transformation initiatives",
                "Build strategic partnerships and customer acquisition channels",
                "Develop scalable business processes and growth infrastructure"
            ])
        
        # Wisconsin-specific mitigation
        if any("seasonal" in factor.lower() or "weather" in factor.lower() for factor in key_risk_factors):
            strategies.extend([
                "Develop seasonal revenue diversification strategies",
                "Implement weather-resilient operational procedures"
            ])
        
        # Risk level specific strategies
        if risk_level in ["Very High Risk", "High Risk"]:
            strategies.extend([
                "Consider phased business launch to minimize initial exposure",
                "Secure additional funding cushion beyond minimum requirements",
                "Implement frequent risk monitoring and early warning systems"
            ])
        
        return strategies[:10]  # Return top 10 most relevant strategies

    def _generate_fallback_data(self, business_type: str, location: str) -> Dict:
        """Generate reasonable fallback data when integration data is unavailable"""
        
        # Get industry benchmarks
        industry_bench = self.industry_risk_benchmarks.get(business_type, 
                                                          self.industry_risk_benchmarks['restaurant'])
        
        # Base financial estimates
        if business_type == 'restaurant':
            revenue_base = 75000
            cost_base = 60000
            startup_base = 250000
        elif business_type == 'hair_salon':
            revenue_base = 35000
            cost_base = 25000
            startup_base = 120000
        elif business_type == 'auto_repair':
            revenue_base = 45000
            cost_base = 32000
            startup_base = 180000
        else:
            revenue_base = 50000
            cost_base = 37000
            startup_base = 175000
        
        return {
            # Market data
            'competition_density': 4,
            'market_saturation_level': 65,
            'economic_growth_rate': 2.3,
            'unemployment_rate': 4.2,
            'consumer_confidence': 82,
            'population_growth_rate': 1.1,
            'market_maturity_level': 70,
            
            # Financial data
            'realistic_monthly_revenue': revenue_base,
            'realistic_monthly_operating': cost_base,
            'total_startup_costs': startup_base,
            'breakeven_months': industry_bench.get('break_even_months', 18),
            'fixed_variable_cost_ratio': 0.65,
            'revenue_variance': 22,
            'working_capital_ratio': 0.15,
            
            # Operational data
            'traffic_volume_score': 78,
            'visibility_score': 75,
            'parking_score': 70,
            'expected_turnover_rate': 30,
            'permit_complexity_score': 55,
            'supplier_concentration_risk': 40,
            'supply_chain_complexity': 45,
            
            # Strategic data
            'market_share_vulnerability': 60,
            'differentiation_strength': 45,
            'market_growth_potential': 2.8,
            'scalability_constraints': 50,
            'digital_transformation_need': 65,
            'technology_adoption_rate': 70,
            'economic_policy_risk': 40,
            'industry_disruption_risk': 35,
            'customer_concentration_risk': 35
        }

    def generate_risk_charts(self, analysis: RiskAssessment, output_dir: str) -> Dict[str, str]:
        """Generate all risk analysis charts and return file paths"""
        
        logger.info("Generating risk assessment charts")
        
        # Create charts directory
        import os
        os.makedirs(output_dir, exist_ok=True)
        
        chart_paths = {}
        
        # Set style for all charts
        plt.style.use('default')
        sns.set_palette("husl")
        
        try:
            # 1. Risk Dashboard (Radar Chart)
            chart_paths['risk_dashboard'] = self._create_risk_dashboard(analysis, output_dir)
            
            # 2. Risk Heat Map
            chart_paths['risk_heatmap'] = self._create_risk_heatmap(analysis, output_dir)
            
            # 3. Risk Scenario Comparison
            chart_paths['risk_scenarios'] = self._create_risk_scenarios_chart(analysis, output_dir)
            
            # 4. Risk Correlation Matrix
            chart_paths['risk_correlation'] = self._create_risk_correlation_chart(analysis, output_dir)
            
            # 5. Monte Carlo Results
            chart_paths['monte_carlo'] = self._create_monte_carlo_chart(analysis, output_dir)
            
            logger.info(f"Generated {len(chart_paths)} risk assessment charts")
            return chart_paths
            
        except Exception as e:
            logger.error(f"Error generating risk charts: {e}")
            return {}

    def _create_risk_dashboard(self, analysis: RiskAssessment, output_dir: str) -> str:
        """Create radar chart showing all risk dimensions"""
        
        fig, ax = plt.subplots(figsize=(10, 10), subplot_kw=dict(projection='polar'))
        
        # Risk categories and scores
        categories = ['Market Risk', 'Financial Risk', 'Operational Risk', 'Strategic Risk']
        scores = [
            analysis.market_risk_score,
            analysis.financial_risk_score,
            analysis.operational_risk_score,
            analysis.strategic_risk_score
        ]
        
        # Add the first value at the end to complete the circle
        categories_circle = categories + [categories[0]]
        scores_circle = scores + [scores[0]]
        
        # Calculate angles
        angles = np.linspace(0, 2 * np.pi, len(categories), endpoint=False).tolist()
        angles_circle = angles + [angles[0]]
        
        # Plot the risk scores
        ax.plot(angles_circle, scores_circle, 'o-', linewidth=2, label='Current Risk')
        ax.fill(angles_circle, scores_circle, alpha=0.25)
        
        # Add industry benchmark (if available)
        benchmark_scores = [60, 55, 50, 58]  # Example industry averages
        benchmark_circle = benchmark_scores + [benchmark_scores[0]]
        ax.plot(angles_circle, benchmark_circle, 's--', linewidth=2, label='Industry Average', alpha=0.7)
        
        # Customize the chart
        ax.set_xticks(angles)
        ax.set_xticklabels(categories)
        ax.set_ylim(0, 100)
        ax.set_yticks([20, 40, 60, 80, 100])
        ax.set_yticklabels(['20', '40', '60', '80', '100'])
        ax.grid(True)
        
        # Add risk level zones
        ax.fill_between(angles_circle, 0, 35, alpha=0.1, color='green', label='Low Risk')
        ax.fill_between(angles_circle, 35, 65, alpha=0.1, color='yellow', label='Moderate Risk')
        ax.fill_between(angles_circle, 65, 100, alpha=0.1, color='red', label='High Risk')
        
        plt.title(f'Risk Assessment Dashboard\n{analysis.business_type} - Overall Risk: {analysis.composite_risk_score:.1f}/100', 
                 size=16, fontweight='bold', pad=20)
        plt.legend(loc='upper right', bbox_to_anchor=(1.3, 1.0))
        
        # Save chart
        chart_path = f"{output_dir}/risk_dashboard.png"
        plt.savefig(chart_path, dpi=300, bbox_inches='tight')
        plt.close()
        
        return chart_path

    def _create_risk_heatmap(self, analysis: RiskAssessment, output_dir: str) -> str:
        """Create risk heat map showing risk intensity"""
        
        fig, ax = plt.subplots(figsize=(12, 8))
        
        # Create risk matrix data
        risk_categories = ['Market', 'Financial', 'Operational', 'Strategic']
        risk_factors = [
            'Competition', 'Economic Sensitivity', 'Demand Volatility', 'Growth Potential',
            'Cash Flow', 'Cost Structure', 'Revenue Risk', 'Capital Requirements',
            'Location', 'Labor/Staffing', 'Compliance', 'Supply Chain',
            'Competitive Position', 'Growth Sustainability', 'Technology', 'External Environment'
        ]
        
        # Create risk intensity matrix (example data)
        risk_matrix = np.array([
            [analysis.market_risk_score * 0.8, analysis.market_risk_score * 1.1, 
             analysis.market_risk_score * 0.9, analysis.market_risk_score * 0.7],
            [analysis.financial_risk_score * 1.2, analysis.financial_risk_score * 0.9,
             analysis.financial_risk_score * 1.0, analysis.financial_risk_score * 0.8],
            [analysis.operational_risk_score * 0.9, analysis.operational_risk_score * 1.1,
             analysis.operational_risk_score * 0.8, analysis.operational_risk_score * 1.0],
            [analysis.strategic_risk_score * 1.0, analysis.strategic_risk_score * 0.9,
             analysis.strategic_risk_score * 1.2, analysis.strategic_risk_score * 0.8]
        ]).reshape(4, 4)
        
        # Clip values to 0-100 range
        risk_matrix = np.clip(risk_matrix, 0, 100)
        
        # Create heatmap
        im = ax.imshow(risk_matrix, cmap='RdYlGn_r', aspect='auto', vmin=0, vmax=100)
        
        # Set ticks and labels
        ax.set_xticks(np.arange(4))
        ax.set_yticks(np.arange(4))
        ax.set_xticklabels(risk_categories)
        ax.set_yticklabels(risk_categories)
        
        # Add text annotations
        for i in range(4):
            for j in range(4):
                text = ax.text(j, i, f'{risk_matrix[i, j]:.0f}',
                             ha="center", va="center", color="black", fontweight='bold')
        
        # Add colorbar
        cbar = plt.colorbar(im, ax=ax)
        cbar.set_label('Risk Score (0-100)', rotation=270, labelpad=20)
        
        plt.title(f'Risk Heat Map - {analysis.business_type}\nRisk Level: {analysis.business_risk_level}', 
                 fontsize=16, fontweight='bold')
        plt.xlabel('Risk Dimensions')
        plt.ylabel('Risk Categories')
        
        # Save chart
        chart_path = f"{output_dir}/risk_heatmap.png"
        plt.savefig(chart_path, dpi=300, bbox_inches='tight')
        plt.close()
        
        return chart_path

    def _create_risk_scenarios_chart(self, analysis: RiskAssessment, output_dir: str) -> str:
        """Create comparison chart of risk scenarios"""
        
        fig, ax = plt.subplots(figsize=(12, 8))
        
        scenarios = ['Low Risk\nScenario', 'Moderate Risk\nScenario', 'High Risk\nScenario']
        
        # Extract scenario data
        low_risk = analysis.risk_scenarios['low_risk']
        moderate_risk = analysis.risk_scenarios['moderate_risk']
        high_risk = analysis.risk_scenarios['high_risk']
        
        market_scores = [low_risk['market_risk'], moderate_risk['market_risk'], high_risk['market_risk']]
        financial_scores = [low_risk['financial_risk'], moderate_risk['financial_risk'], high_risk['financial_risk']]
        operational_scores = [low_risk['operational_risk'], moderate_risk['operational_risk'], high_risk['operational_risk']]
        strategic_scores = [low_risk['strategic_risk'], moderate_risk['strategic_risk'], high_risk['strategic_risk']]
        
        x = np.arange(len(scenarios))
        width = 0.2
        
        # Create grouped bar chart
        bars1 = ax.bar(x - 1.5*width, market_scores, width, label='Market Risk', color='#ff7f0e')
        bars2 = ax.bar(x - 0.5*width, financial_scores, width, label='Financial Risk', color='#d62728')
        bars3 = ax.bar(x + 0.5*width, operational_scores, width, label='Operational Risk', color='#2ca02c')
        bars4 = ax.bar(x + 1.5*width, strategic_scores, width, label='Strategic Risk', color='#1f77b4')
        
        # Add value labels on bars
        def autolabel(bars):
            for bar in bars:
                height = bar.get_height()
                ax.annotate(f'{height:.0f}',
                           xy=(bar.get_x() + bar.get_width() / 2, height),
                           xytext=(0, 3),
                           textcoords="offset points",
                           ha='center', va='bottom', fontsize=9)
        
        autolabel(bars1)
        autolabel(bars2)
        autolabel(bars3)
        autolabel(bars4)
        
        # Customize chart
        ax.set_ylabel('Risk Score (0-100)')
        ax.set_title(f'Risk Scenario Analysis - {analysis.business_type}')
        ax.set_xticks(x)
        ax.set_xticklabels(scenarios)
        ax.legend()
        ax.set_ylim(0, 100)
        
        # Add probability annotations
        probs = [low_risk['probability'], moderate_risk['probability'], high_risk['probability']]
        for i, prob in enumerate(probs):
            ax.text(i, 95, f'{prob:.0f}% probability', ha='center', va='top', 
                   fontweight='bold', bbox=dict(boxstyle='round', facecolor='white', alpha=0.8))
        
        plt.grid(axis='y', alpha=0.3)
        
        # Save chart
        chart_path = f"{output_dir}/risk_scenarios.png"
        plt.savefig(chart_path, dpi=300, bbox_inches='tight')
        plt.close()
        
        return chart_path

    def _create_risk_correlation_chart(self, analysis: RiskAssessment, output_dir: str) -> str:
        """Create risk correlation matrix chart"""
        
        fig, ax = plt.subplots(figsize=(10, 8))
        
        # Create correlation matrix
        risk_types = ['Market', 'Financial', 'Operational', 'Strategic']
        correlation_matrix = np.array([
            [1.0, analysis.risk_correlations['market_financial'], 
             analysis.risk_correlations['market_operational'], analysis.risk_correlations['market_strategic']],
            [analysis.risk_correlations['market_financial'], 1.0,
             analysis.risk_correlations['financial_operational'], analysis.risk_correlations['financial_strategic']],
            [analysis.risk_correlations['market_operational'], analysis.risk_correlations['financial_operational'],
             1.0, analysis.risk_correlations['operational_strategic']],
            [analysis.risk_correlations['market_strategic'], analysis.risk_correlations['financial_strategic'],
             analysis.risk_correlations['operational_strategic'], 1.0]
        ])
        
        # Create heatmap
        im = ax.imshow(correlation_matrix, cmap='RdBu', aspect='auto', vmin=-1, vmax=1)
        
        # Set ticks and labels
        ax.set_xticks(np.arange(len(risk_types)))
        ax.set_yticks(np.arange(len(risk_types)))
        ax.set_xticklabels(risk_types)
        ax.set_yticklabels(risk_types)
        
        # Add correlation values
        for i in range(len(risk_types)):
            for j in range(len(risk_types)):
                text = ax.text(j, i, f'{correlation_matrix[i, j]:.2f}',
                             ha="center", va="center", color="black", fontweight='bold')
        
        # Add colorbar
        cbar = plt.colorbar(im, ax=ax)
        cbar.set_label('Correlation Coefficient', rotation=270, labelpad=20)
        
        plt.title('Risk Correlation Matrix\nHow Different Risk Types Relate to Each Other', 
                 fontsize=14, fontweight='bold')
        
        # Save chart
        chart_path = f"{output_dir}/risk_correlation.png"
        plt.savefig(chart_path, dpi=300, bbox_inches='tight')
        plt.close()
        
        return chart_path

    def _create_monte_carlo_chart(self, analysis: RiskAssessment, output_dir: str) -> str:
        """Create Monte Carlo simulation results chart"""
        
        fig, ((ax1, ax2), (ax3, ax4)) = plt.subplots(2, 2, figsize=(15, 12))
        
        # Extract Monte Carlo results
        mc_results = analysis.monte_carlo_results
        
        # 1. Profitability Probability (Gauge Chart)
        prob_profit = mc_results['profitability_probability']
        colors = ['red', 'yellow', 'green']
        sizes = [max(0, 60-prob_profit), max(0, min(30, prob_profit-60)), max(0, prob_profit-60)]
        labels = ['High Risk\n(<60%)', 'Moderate Risk\n(60-90%)', 'Low Risk\n(>90%)']
        
        wedges, texts, autotexts = ax1.pie(sizes, labels=labels, colors=colors, autopct='%1.1f%%', startangle=90)
        ax1.set_title(f'Profitability Probability: {prob_profit:.1f}%')
        
        # 2. Break-even Probability
        prob_breakeven = mc_results['breakeven_probability']
        ax2.bar(['Break-even\nWithin 18 Months', 'Extended\nBreak-even Period'], 
               [prob_breakeven, 100-prob_breakeven], 
               color=['green' if prob_breakeven > 70 else 'orange', 'red'])
        ax2.set_ylabel('Probability (%)')
        ax2.set_title(f'Break-even Timeline Probability')
        ax2.set_ylim(0, 100)
        
        # Add percentage labels
        for i, v in enumerate([prob_breakeven, 100-prob_breakeven]):
            ax2.text(i, v + 1, f'{v:.1f}%', ha='center', va='bottom', fontweight='bold')
        
        # 3. Value at Risk
        var = mc_results['value_at_risk']
        expected_shortfall = mc_results['expected_shortfall']
        
        risk_metrics = ['Value at Risk\n(95% confidence)', 'Expected Shortfall\n(worst 5% scenarios)']
        risk_values = [var, expected_shortfall]
        
        bars = ax3.bar(risk_metrics, risk_values, color=['orange', 'red'])
        ax3.set_ylabel('Monthly Loss ($)')
        ax3.set_title('Downside Risk Metrics')
        
        # Format y-axis as currency
        ax3.yaxis.set_major_formatter(plt.FuncFormatter(lambda x, p: f'${x:,.0f}'))
        
        # Add value labels
        for bar, value in zip(bars, risk_values):
            ax3.text(bar.get_x() + bar.get_width()/2, bar.get_height() + max(risk_values)*0.02,
                    f'${value:,.0f}', ha='center', va='bottom', fontweight='bold')
        
        # 4. Profit Distribution (Histogram simulation)
        np.random.seed(42)  # For reproducible results
        avg_profit = mc_results['avg_monthly_profit']
        profit_std = mc_results['profit_std_deviation']
        
        # Generate sample distribution
        profit_samples = np.random.normal(avg_profit, profit_std, 1000)
        
        ax4.hist(profit_samples, bins=30, alpha=0.7, color='blue', edgecolor='black')
        ax4.axvline(avg_profit, color='red', linestyle='--', linewidth=2, label=f'Average: ${avg_profit:,.0f}')
        ax4.axvline(0, color='orange', linestyle='--', linewidth=2, label='Break-even')
        ax4.set_xlabel('Monthly Profit ($)')
        ax4.set_ylabel('Frequency')
        ax4.set_title('Simulated Monthly Profit Distribution')
        ax4.legend()
        
        # Format x-axis as currency
        ax4.xaxis.set_major_formatter(plt.FuncFormatter(lambda x, p: f'${x:,.0f}'))
        
        plt.suptitle(f'Monte Carlo Risk Analysis - {analysis.business_type}', fontsize=16, fontweight='bold')
        plt.tight_layout()
        
        # Save chart
        chart_path = f"{output_dir}/monte_carlo_results.png"
        plt.savefig(chart_path, dpi=300, bbox_inches='tight')
        plt.close()
        
        return chart_path

if __name__ == "__main__":
    # Test the analyzer
    analyzer = RiskAssessmentAnalyzer()
    
    # Test with sample data
    test_data = {
        'competition_density': 6,
        'market_saturation_level': 70,
        'realistic_monthly_revenue': 65000,
        'realistic_monthly_operating': 52000,
        'total_startup_costs': 220000
    }
    
    analysis = analyzer.analyze_comprehensive_risk("restaurant", "Milwaukee, WI", test_data)
    
    print(f"Composite Risk Score: {analysis.composite_risk_score:.1f}")
    print(f"Risk Level: {analysis.business_risk_level}")
    print(f"Key Risk Factors: {len(analysis.key_risk_factors)}")
    print(f"Mitigation Strategies: {len(analysis.risk_mitigation_strategies)}")