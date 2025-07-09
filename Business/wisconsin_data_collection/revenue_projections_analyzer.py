#!/usr/bin/env python3
"""
Revenue Projections Analyzer
============================

Comprehensive revenue forecasting system that integrates demographic data, competitive analysis,
traffic patterns, site characteristics, and industry benchmarks to generate realistic revenue projections.

Features:
- Multiple revenue projection models (Market Penetration, Gravity, Habitat Suitability, etc.)
- Industry benchmark integration
- Competitive adjustment calculations
- Scenario-based projections (conservative, realistic, optimistic)
- Seasonal adjustments and growth projections
"""

import json
import logging
import math
import numpy as np
from datetime import datetime, timedelta
from typing import Dict, List, Any, Optional, Tuple
from dataclasses import dataclass

# Import existing analyzers for data integration
try:
    from trade_area_analyzer import TradeAreaAnalyzer
    from universal_competitive_analyzer import UniversalCompetitiveAnalyzer
    from business_habitat_analyzer import BusinessHabitatAnalyzer
    from integrated_business_analyzer import IntegratedBusinessAnalyzer
    from consumer_spending_collector import ConsumerSpendingCollector
    from industry_benchmarks_collector import IndustryBenchmarksCollector
except ImportError as e:
    logging.warning(f"Some dependencies not available: {e}")

logger = logging.getLogger(__name__)

@dataclass
class RevenueProjection:
    """Data structure for revenue projection results"""
    business_type: str
    location: str
    conservative_annual: float
    realistic_annual: float
    optimistic_annual: float
    recommended_planning: float
    confidence_level: float
    key_assumptions: List[str]
    revenue_drivers: List[str]
    risk_factors: List[str]
    model_validation: Dict[str, float]
    seasonal_adjustments: Dict[str, float]
    
    # SBA-Compliant Financial Enhancements
    monthly_cash_flow_projections: List[Dict[str, float]] = None  # 36 months for SBA
    debt_service_coverage_ratio: float = 0.0  # DSCR calculation
    working_capital_requirements: Dict[str, float] = None  # By quarter
    cash_flow_timing: Dict[str, Any] = None  # Seasonal cash flow patterns
    revenue_concentration_risk: float = 0.0  # Customer concentration percentage
    sba_compliance_metrics: Dict[str, Any] = None  # SBA-specific calculations

class RevenueProjectionsAnalyzer:
    """Comprehensive revenue projections analysis for Section 4.1"""
    
    def __init__(self):
        """Initialize the revenue projections analyzer"""
        # Industry benchmarks by business type (annual revenue averages)
        self.industry_benchmarks = {
            'restaurant': {
                'avg_annual_revenue': 1200000,
                'revenue_per_sqft': 400,
                'avg_transaction': 25,
                'daily_transactions': 120,
                'visit_frequency': 24,
                'seasonal_peak_months': ['11', '12', '05', '06', '07'],
                'seasonal_boost': 15,
                'break_even_months': 18
            },
            'hair_salon': {
                'avg_annual_revenue': 350000,
                'revenue_per_sqft': 200,
                'avg_transaction': 45,
                'daily_transactions': 25,
                'visit_frequency': 8,
                'seasonal_peak_months': ['04', '05', '09', '10', '11', '12'],
                'seasonal_boost': 20,
                'break_even_months': 12
            },
            'auto_repair': {
                'avg_annual_revenue': 800000,
                'revenue_per_sqft': 150,
                'avg_transaction': 280,
                'daily_transactions': 15,
                'visit_frequency': 2,
                'seasonal_peak_months': ['03', '04', '09', '10'],
                'seasonal_boost': 25,
                'break_even_months': 15
            },
            'retail_clothing': {
                'avg_annual_revenue': 600000,
                'revenue_per_sqft': 300,
                'avg_transaction': 65,
                'daily_transactions': 45,
                'visit_frequency': 12,
                'seasonal_peak_months': ['11', '12', '08', '04'],
                'seasonal_boost': 35,
                'break_even_months': 24
            },
            'gym': {
                'avg_annual_revenue': 450000,
                'revenue_per_sqft': 80,
                'avg_transaction': 55,
                'daily_transactions': 35,
                'visit_frequency': 120,
                'seasonal_peak_months': ['01', '02', '03', '09'],
                'seasonal_boost': 30,
                'break_even_months': 18
            },
            'coffee_shop': {
                'avg_annual_revenue': 500000,
                'revenue_per_sqft': 600,
                'avg_transaction': 8,
                'daily_transactions': 200,
                'visit_frequency': 80,
                'seasonal_peak_months': ['09', '10', '11', '12', '01', '02'],
                'seasonal_boost': 12,
                'break_even_months': 14
            },
            'gas_station': {
                'avg_annual_revenue': 2500000,
                'revenue_per_sqft': 1200,
                'avg_transaction': 35,
                'daily_transactions': 300,
                'visit_frequency': 52,
                'seasonal_peak_months': ['05', '06', '07', '08'],
                'seasonal_boost': 18,
                'break_even_months': 20
            },
            'hardware_store': {
                'avg_annual_revenue': 750000,
                'revenue_per_sqft': 250,
                'avg_transaction': 45,
                'daily_transactions': 50,
                'visit_frequency': 8,
                'seasonal_peak_months': ['04', '05', '06', '07', '08'],
                'seasonal_boost': 25,
                'break_even_months': 16
            }
        }
        
        # Initialize external analyzers with error handling
        try:
            self.trade_area_analyzer = TradeAreaAnalyzer()
            self.competitive_analyzer = UniversalCompetitiveAnalyzer()
            self.habitat_analyzer = BusinessHabitatAnalyzer()
        except Exception as e:
            logger.warning(f"Some analyzers not available: {e}")
            self.trade_area_analyzer = None
            self.competitive_analyzer = None
            self.habitat_analyzer = None
    
    def analyze_revenue_projections(self, business_type: str, address: str, 
                                  lat: float, lon: float) -> RevenueProjection:
        """
        Perform comprehensive revenue projections analysis
        
        Args:
            business_type: Type of business
            address: Business address
            lat: Latitude
            lon: Longitude
            
        Returns:
            RevenueProjection object with complete analysis
        """
        logger.info(f"Starting revenue projections for {business_type} at {address}")
        
        try:
            # 1. Collect demographic and trade area data
            demographic_data = self._analyze_demographics(lat, lon)
            
            # 2. Analyze competitive environment
            competitive_data = self._analyze_competition(business_type, lat, lon)
            
            # 3. Get habitat suitability data
            habitat_data = self._analyze_habitat_factors(business_type, lat, lon)
            
            # 4. Calculate traffic-based revenue potential
            traffic_data = self._analyze_traffic_patterns(lat, lon)
            
            # 5. Apply industry benchmarks
            industry_data = self._get_industry_benchmarks(business_type)
            
            # 6. Run multiple revenue projection models
            revenue_models = self._calculate_revenue_models(
                business_type, demographic_data, competitive_data, 
                habitat_data, traffic_data, industry_data
            )
            
            # 7. Generate scenario-based projections
            scenarios = self._generate_revenue_scenarios(revenue_models, industry_data)
            
            # 8. Calculate seasonal adjustments
            seasonal_adjustments = self._calculate_seasonal_adjustments(business_type, scenarios)
            
            # 9. Validate models and calculate confidence
            validation = self._validate_revenue_models(revenue_models)
            
            # 10. Generate SBA-compliant financial metrics
            sba_metrics = self._generate_sba_compliant_metrics(scenarios, industry_data, seasonal_adjustments)
            
            # 11. Create final projection object
            projection = RevenueProjection(
                business_type=business_type,
                location=address,
                conservative_annual=scenarios['conservative'],
                realistic_annual=scenarios['realistic'],
                optimistic_annual=scenarios['optimistic'],
                recommended_planning=scenarios['recommended'],
                confidence_level=validation['confidence_level'],
                key_assumptions=self._generate_key_assumptions(demographic_data, competitive_data),
                revenue_drivers=self._identify_revenue_drivers(demographic_data, competitive_data, habitat_data),
                risk_factors=self._identify_risk_factors(competitive_data, habitat_data),
                model_validation=validation['model_results'],
                seasonal_adjustments=seasonal_adjustments,
                monthly_cash_flow_projections=sba_metrics['monthly_projections'],
                debt_service_coverage_ratio=sba_metrics['dscr'],
                working_capital_requirements=sba_metrics['working_capital'],
                cash_flow_timing=sba_metrics['cash_flow_timing'],
                revenue_concentration_risk=sba_metrics['concentration_risk'],
                sba_compliance_metrics=sba_metrics['compliance_metrics']
            )
            
            return projection
            
        except Exception as e:
            logger.error(f"Revenue projections analysis failed: {str(e)}")
            return self._get_fallback_projection(business_type, address)
    
    def _analyze_demographics(self, lat: float, lon: float) -> Dict[str, Any]:
        """Analyze demographic data for revenue calculations"""
        logger.info("Analyzing demographic data")
        
        try:
            if self.trade_area_analyzer:
                # Use real trade area analysis
                trade_data = self.trade_area_analyzer.analyze_trade_area(lat, lon)
                return {
                    'primary_population': trade_data.get('primary_population', 15000),
                    'secondary_population': trade_data.get('secondary_population', 45000),
                    'extended_population': trade_data.get('extended_population', 80000),
                    'median_income': trade_data.get('median_income', 65000),
                    'total_households': trade_data.get('total_households', 35000),
                    'population_density': trade_data.get('population_density', 2500)
                }
            else:
                # Fallback demographic estimates
                return self._get_fallback_demographics()
        except Exception as e:
            logger.warning(f"Demographic analysis failed: {e}")
            return self._get_fallback_demographics()
    
    def _analyze_competition(self, business_type: str, lat: float, lon: float) -> Dict[str, Any]:
        """Analyze competitive environment for revenue impact"""
        logger.info("Analyzing competitive environment")
        
        try:
            if self.competitive_analyzer:
                # Use real competitive analysis
                comp_data = self.competitive_analyzer.analyze_competition(business_type, lat, lon)
                return {
                    'direct_competitors': comp_data.get('direct_competitor_count', 3),
                    'competition_density': comp_data.get('competition_density', 1.2),
                    'market_saturation': comp_data.get('market_saturation_level', 'Moderate'),
                    'competitive_advantage': comp_data.get('competitive_advantage_score', 70),
                    'average_competitor_rating': comp_data.get('avg_competitor_rating', 4.0)
                }
            else:
                # Fallback competitive estimates
                return self._get_fallback_competition()
        except Exception as e:
            logger.warning(f"Competitive analysis failed: {e}")
            return self._get_fallback_competition()
    
    def _analyze_habitat_factors(self, business_type: str, lat: float, lon: float) -> Dict[str, Any]:
        """Analyze habitat suitability factors for revenue adjustments"""
        logger.info("Analyzing habitat suitability factors")
        
        try:
            if self.habitat_analyzer:
                # Use real habitat analysis
                habitat_result = self.habitat_analyzer.analyze_habitat_suitability(
                    business_type, lat, lon, 'urban', False
                )
                return {
                    'success_probability': habitat_result.success_probability,
                    'confidence_level': habitat_result.confidence_level,
                    'environmental_score': 0.8,  # Derived from habitat factors
                    'location_quality': 75
                }
            else:
                # Fallback habitat estimates
                return self._get_fallback_habitat()
        except Exception as e:
            logger.warning(f"Habitat analysis failed: {e}")
            return self._get_fallback_habitat()
    
    def _analyze_traffic_patterns(self, lat: float, lon: float) -> Dict[str, Any]:
        """Analyze traffic patterns for revenue calculations"""
        logger.info("Analyzing traffic patterns")
        
        # Simplified traffic analysis (could be enhanced with real traffic data)
        return {
            'daily_traffic_volume': 15000,
            'peak_hour_traffic': 1500,
            'pedestrian_traffic': 500,
            'accessibility_score': 75,
            'visibility_score': 80,
            'parking_availability': 85
        }
    
    def _get_industry_benchmarks(self, business_type: str) -> Dict[str, Any]:
        """Get industry-specific benchmarks"""
        normalized_type = business_type.lower().replace(' ', '_')
        return self.industry_benchmarks.get(normalized_type, self.industry_benchmarks['restaurant'])
    
    def _calculate_revenue_models(self, business_type: str, demographic_data: Dict, 
                                competitive_data: Dict, habitat_data: Dict, 
                                traffic_data: Dict, industry_data: Dict) -> Dict[str, float]:
        """Calculate revenue using multiple models"""
        logger.info("Calculating revenue using multiple models")
        
        models = {}
        
        # 1. Market Penetration Model
        models['market_penetration'] = self._market_penetration_model(
            demographic_data, competitive_data, industry_data
        )
        
        # 2. Gravity Model
        models['gravity_model'] = self._gravity_model(
            demographic_data, traffic_data, industry_data
        )
        
        # 3. Habitat Suitability Model
        models['habitat_model'] = self._habitat_suitability_model(
            habitat_data, industry_data
        )
        
        # 4. Consumer Spending Model
        models['consumer_spending'] = self._consumer_spending_model(
            demographic_data, competitive_data, business_type
        )
        
        # 5. Traffic-Based Model
        models['traffic_based'] = self._traffic_based_model(
            traffic_data, industry_data
        )
        
        return models
    
    def _market_penetration_model(self, demographic_data: Dict, competitive_data: Dict, 
                                industry_data: Dict) -> float:
        """Calculate revenue using market penetration model"""
        
        # Base calculation: Population × Capture Rate × Average Transaction × Visit Frequency
        total_population = (
            demographic_data['primary_population'] * 0.6 +
            demographic_data['secondary_population'] * 0.3 +
            demographic_data['extended_population'] * 0.1
        )
        
        # Adjust capture rate based on competition
        base_capture_rate = 0.05  # 5% base capture rate
        competition_adjustment = max(0.3, 1 - (competitive_data['direct_competitors'] * 0.15))
        capture_rate = base_capture_rate * competition_adjustment
        
        # Income adjustment
        income_adjustment = min(1.5, demographic_data['median_income'] / 60000)
        
        annual_revenue = (
            total_population * 
            capture_rate * 
            industry_data['avg_transaction'] * 
            industry_data['visit_frequency'] * 
            income_adjustment
        )
        
        return annual_revenue
    
    def _gravity_model(self, demographic_data: Dict, traffic_data: Dict, 
                      industry_data: Dict) -> float:
        """Calculate revenue using gravity model"""
        
        # Distance-weighted population analysis
        accessibility_factor = traffic_data['accessibility_score'] / 100
        visibility_factor = traffic_data['visibility_score'] / 100
        
        effective_population = (
            demographic_data['primary_population'] * accessibility_factor * visibility_factor
        )
        
        # Per capita spending estimate
        per_capita_annual = industry_data['avg_transaction'] * industry_data['visit_frequency']
        market_share = 0.08  # 8% market share assumption
        
        annual_revenue = effective_population * per_capita_annual * market_share
        
        return annual_revenue
    
    def _habitat_suitability_model(self, habitat_data: Dict, industry_data: Dict) -> float:
        """Calculate revenue using habitat suitability model"""
        
        # Base industry revenue adjusted by success probability
        base_revenue = industry_data['avg_annual_revenue']
        success_multiplier = habitat_data['success_probability']
        environmental_multiplier = habitat_data['environmental_score']
        
        annual_revenue = base_revenue * success_multiplier * environmental_multiplier
        
        return annual_revenue
    
    def _consumer_spending_model(self, demographic_data: Dict, competitive_data: Dict, 
                               business_type: str) -> float:
        """Calculate revenue using consumer spending allocation model"""
        
        # Estimate category spending based on business type
        spending_categories = {
            'restaurant': 3500,  # Annual per household food service spending
            'hair_salon': 800,   # Annual per household personal care
            'auto_repair': 1200, # Annual per household vehicle maintenance
            'retail_clothing': 1800,  # Annual per household clothing
            'gym': 600,         # Annual per household fitness
            'coffee_shop': 1200, # Annual per household beverages
            'gas_station': 4000, # Annual per household fuel
            'hardware_store': 800 # Annual per household home improvement
        }
        
        category_spending = spending_categories.get(business_type, 1500)
        total_households = demographic_data['total_households']
        
        # Market share based on competition
        competitors = competitive_data['direct_competitors']
        market_share = 1 / (competitors + 1) if competitors > 0 else 0.25
        
        annual_revenue = total_households * category_spending * market_share
        
        return annual_revenue
    
    def _traffic_based_model(self, traffic_data: Dict, industry_data: Dict) -> float:
        """Calculate revenue using traffic-based model"""
        
        # Convert traffic to customer visits
        daily_traffic = traffic_data['daily_traffic_volume']
        conversion_rate = 0.002  # 0.2% of traffic converts to customers
        
        daily_customers = daily_traffic * conversion_rate
        annual_customers = daily_customers * 365
        
        annual_revenue = annual_customers * industry_data['avg_transaction']
        
        return annual_revenue
    
    def _generate_revenue_scenarios(self, revenue_models: Dict[str, float], 
                                  industry_data: Dict) -> Dict[str, float]:
        """Generate conservative, realistic, and optimistic scenarios"""
        
        # Calculate weighted average of all models
        model_values = list(revenue_models.values())
        realistic_revenue = np.mean(model_values)
        
        # Generate scenarios based on percentiles
        conservative_revenue = realistic_revenue * 0.75  # 25th percentile
        optimistic_revenue = realistic_revenue * 1.35   # 75th percentile
        
        # Recommended planning revenue (slightly conservative)
        recommended_revenue = realistic_revenue * 0.85
        
        return {
            'conservative': conservative_revenue,
            'realistic': realistic_revenue,
            'optimistic': optimistic_revenue,
            'recommended': recommended_revenue
        }
    
    def _calculate_seasonal_adjustments(self, business_type: str, 
                                      scenarios: Dict[str, float]) -> Dict[str, float]:
        """Calculate seasonal revenue adjustments"""
        
        industry_data = self._get_industry_benchmarks(business_type)
        peak_months = industry_data.get('seasonal_peak_months', [])
        seasonal_boost = industry_data.get('seasonal_boost', 15)
        
        monthly_base = scenarios['realistic'] / 12
        
        adjustments = {}
        for month in range(1, 13):
            month_str = f"{month:02d}"
            if month_str in peak_months:
                adjustments[month_str] = monthly_base * (1 + seasonal_boost / 100)
            else:
                adjustments[month_str] = monthly_base * (1 - seasonal_boost / 200)
        
        return adjustments
    
    def _validate_revenue_models(self, revenue_models: Dict[str, float]) -> Dict[str, Any]:
        """Validate revenue models and calculate confidence"""
        
        model_values = list(revenue_models.values())
        mean_revenue = np.mean(model_values)
        std_deviation = np.std(model_values)
        
        # Calculate coefficient of variation for confidence assessment
        cv = std_deviation / mean_revenue if mean_revenue > 0 else 1
        
        # Confidence level based on model convergence
        confidence_level = max(60, min(95, 95 - (cv * 100)))
        
        return {
            'model_results': revenue_models,
            'confidence_level': confidence_level,
            'standard_deviation': std_deviation,
            'convergence_factor': 1 - cv
        }
    
    def _generate_key_assumptions(self, demographic_data: Dict, competitive_data: Dict) -> List[str]:
        """Generate list of key assumptions used in analysis"""
        return [
            f"Trade area population: {demographic_data['primary_population']:,} (primary)",
            f"Median household income: ${demographic_data['median_income']:,}",
            f"Direct competitors: {competitive_data['direct_competitors']}",
            f"Market capture rate: 3-8% of target population",
            f"Analysis based on current market conditions"
        ]
    
    def _identify_revenue_drivers(self, demographic_data: Dict, competitive_data: Dict, 
                                habitat_data: Dict) -> List[str]:
        """Identify key revenue drivers"""
        drivers = []
        
        if demographic_data['median_income'] > 70000:
            drivers.append("High-income demographic base")
        
        if competitive_data['direct_competitors'] <= 2:
            drivers.append("Limited direct competition")
        
        if habitat_data['success_probability'] > 0.8:
            drivers.append("High habitat suitability score")
        
        if competitive_data['competitive_advantage'] > 75:
            drivers.append("Strong competitive positioning")
        
        return drivers if drivers else ["Standard market conditions"]
    
    def _identify_risk_factors(self, competitive_data: Dict, habitat_data: Dict) -> List[str]:
        """Identify revenue risk factors"""
        risks = []
        
        if competitive_data['direct_competitors'] >= 5:
            risks.append("High competitive density")
        
        if competitive_data['market_saturation'] == 'High':
            risks.append("Market saturation concerns")
        
        if habitat_data['success_probability'] < 0.7:
            risks.append("Below-average habitat suitability")
        
        if competitive_data['competitive_advantage'] < 60:
            risks.append("Limited competitive advantages")
        
        return risks if risks else ["No significant risk factors identified"]
    
    def _get_fallback_demographics(self) -> Dict[str, Any]:
        """Fallback demographic data when analysis fails"""
        return {
            'primary_population': 15000,
            'secondary_population': 45000,
            'extended_population': 80000,
            'median_income': 65000,
            'total_households': 35000,
            'population_density': 2500
        }
    
    def _get_fallback_competition(self) -> Dict[str, Any]:
        """Fallback competitive data when analysis fails"""
        return {
            'direct_competitors': 3,
            'competition_density': 1.2,
            'market_saturation': 'Moderate',
            'competitive_advantage': 70,
            'average_competitor_rating': 4.0
        }
    
    def _get_fallback_habitat(self) -> Dict[str, Any]:
        """Fallback habitat data when analysis fails"""
        return {
            'success_probability': 0.8,
            'confidence_level': 'Medium',
            'environmental_score': 0.75,
            'location_quality': 70
        }
    
    def _get_fallback_projection(self, business_type: str, address: str) -> RevenueProjection:
        """Generate fallback projection when analysis fails"""
        industry_data = self._get_industry_benchmarks(business_type)
        base_revenue = industry_data['avg_annual_revenue']
        
        return RevenueProjection(
            business_type=business_type,
            location=address,
            conservative_annual=base_revenue * 0.7,
            realistic_annual=base_revenue,
            optimistic_annual=base_revenue * 1.3,
            recommended_planning=base_revenue * 0.8,
            confidence_level=60,
            key_assumptions=["Fallback analysis - limited data available"],
            revenue_drivers=["Industry average performance expected"],
            risk_factors=["Analysis limitations due to data availability"],
            model_validation={"fallback_model": base_revenue},
            seasonal_adjustments={}
        )
    
    def populate_template(self, template_content: str, projection: RevenueProjection) -> str:
        """Populate the revenue projections template with analysis results"""
        
        # Get industry data for additional context
        industry_data = self._get_industry_benchmarks(projection.business_type)
        
        # Create replacements dictionary
        replacements = {
            # Basic info
            "{business_type}": projection.business_type.title(),
            "{location}": projection.location,
            
            # Revenue scenarios
            "{conservative_annual_revenue}": f"{projection.conservative_annual:,.0f}",
            "{realistic_annual_revenue}": f"{projection.realistic_annual:,.0f}",
            "{optimistic_annual_revenue}": f"{projection.optimistic_annual:,.0f}",
            "{recommended_planning_revenue}": f"{projection.recommended_planning:,.0f}",
            
            # Monthly breakdowns
            "{conservative_monthly_revenue}": f"{projection.conservative_annual/12:,.0f}",
            "{realistic_monthly_revenue}": f"{projection.realistic_annual/12:,.0f}",
            "{optimistic_monthly_revenue}": f"{projection.optimistic_annual/12:,.0f}",
            
            # Daily breakdowns
            "{conservative_daily_revenue}": f"{projection.conservative_annual/365:,.0f}",
            "{realistic_daily_revenue}": f"{projection.realistic_annual/365:,.0f}",
            "{optimistic_daily_revenue}": f"{projection.optimistic_annual/365:,.0f}",
            
            # Confidence and validation
            "{revenue_confidence_level}": f"{projection.confidence_level:.0f}",
            "{confidence_interval_low}": f"{projection.realistic_annual * 0.85:,.0f}",
            "{confidence_interval_high}": f"{projection.realistic_annual * 1.15:,.0f}",
            
            # Industry benchmarks
            "{industry_avg_revenue}": f"{industry_data['avg_annual_revenue']:,}",
            "{avg_transaction_value}": f"{industry_data['avg_transaction']:.0f}",
            "{avg_daily_transactions}": f"{industry_data['daily_transactions']:,}",
            "{visit_frequency}": f"{industry_data['visit_frequency']:.0f}",
            
            # Analysis components
            "{key_revenue_drivers}": "\\n".join([f"- {driver}" for driver in projection.revenue_drivers]),
            "{revenue_risk_factors}": "\\n".join([f"- {factor}" for factor in projection.risk_factors]),
            "{conservative_assumptions}": "Lower market penetration, conservative growth",
            "{realistic_assumptions}": "Market-based projections with competitive adjustments",
            "{optimistic_assumptions}": "Strong market capture, favorable conditions",
            
            # Probabilities
            "{conservative_probability}": "75",
            "{realistic_probability}": "60",
            "{optimistic_probability}": "25",
            
            # Placeholder values for detailed analysis
            "{total_trade_area_population}": "140,000",
            "{median_household_income}": "65,000",
            "{competitor_count}": "3",
            "{market_saturation_level}": "Moderate",
            "{daily_traffic_volume}": "15,000",
            "{accessibility_score}": "75",
            "{visibility_score}": "80",
            "{site_quality_score}": "75",
            "{habitat_success_probability}": f"{projection.confidence_level:.0f}",
            
            # Additional placeholders
            "{data_collection_date}": datetime.now().strftime("%Y-%m-%d"),
            "{validation_methods}": "Multi-model convergence analysis",
            "{revenue_viability_assessment}": self._assess_viability(projection),
            
            # SBA-Compliant Financial Analysis
            "{debt_service_coverage_ratio}": f"{projection.debt_service_coverage_ratio:.2f}",
            "{recommended_loan_amount}": f"{min(projection.realistic_annual * 0.4, 300000):,.0f}",
            "{estimated_monthly_payment}": f"{self._calculate_loan_payment(min(projection.realistic_annual * 0.4, 300000), 0.07, 10):,.0f}",
            "{annual_debt_service}": f"{self._calculate_loan_payment(min(projection.realistic_annual * 0.4, 300000), 0.07, 10) * 12:,.0f}",
            "{net_operating_income}": f"{projection.realistic_annual * 0.20:,.0f}",
            "{sba_loan_type_recommendation}": projection.sba_compliance_metrics.get('recommended_loan_type', 'SBA 7(a) Standard') if projection.sba_compliance_metrics else 'SBA 7(a) Standard',
            
            # Working Capital
            "{q1_working_capital}": f"{projection.working_capital_requirements.get('q1', 0):,.0f}" if projection.working_capital_requirements else "0",
            "{q2_working_capital}": f"{projection.working_capital_requirements.get('q2', 0):,.0f}" if projection.working_capital_requirements else "0",
            "{q3_working_capital}": f"{projection.working_capital_requirements.get('q3', 0):,.0f}" if projection.working_capital_requirements else "0",
            "{q4_working_capital}": f"{projection.working_capital_requirements.get('q4', 0):,.0f}" if projection.working_capital_requirements else "0",
            "{annual_average_working_capital}": f"{projection.working_capital_requirements.get('annual_average', 0):,.0f}" if projection.working_capital_requirements else "0",
            
            # Cash Flow Timing
            "{peak_revenue_months}": ", ".join(projection.cash_flow_timing.get('peak_months', [])) if projection.cash_flow_timing else "Nov, Dec",
            "{low_revenue_months}": ", ".join(projection.cash_flow_timing.get('low_months', [])) if projection.cash_flow_timing else "Jan, Feb",
            "{cash_conversion_cycle}": f"{projection.cash_flow_timing.get('cash_conversion_cycle', 30)}" if projection.cash_flow_timing else "30",
            "{accounts_receivable_days}": f"{projection.cash_flow_timing.get('accounts_receivable_days', 15)}" if projection.cash_flow_timing else "15",
            "{inventory_turns}": f"{projection.cash_flow_timing.get('inventory_turns', 12)}" if projection.cash_flow_timing else "12",
            
            # Risk Assessment
            "{customer_concentration_risk}": f"{projection.revenue_concentration_risk * 100:.0f}",
            "{revenue_volatility}": "15",  # Default assumption
            "{seasonal_cash_flow_risk}": "Moderate",
            
            # SBA Compliance
            "{jobs_created}": f"{projection.sba_compliance_metrics.get('jobs_created', 0)}" if projection.sba_compliance_metrics else "0",
            "{owner_equity_required}": f"{projection.sba_compliance_metrics.get('owner_equity_injection', 0):,.0f}" if projection.sba_compliance_metrics else "0",
            "{collateral_coverage}": f"{projection.sba_compliance_metrics.get('collateral_coverage', 0):,.0f}" if projection.sba_compliance_metrics else "0",
            "{loan_to_value_ratio}": f"{projection.sba_compliance_metrics.get('loan_to_value_ratio', 80):.0f}" if projection.sba_compliance_metrics else "80",
            "{size_standard_compliance}": "Yes" if projection.sba_compliance_metrics and projection.sba_compliance_metrics.get('sba_size_standard_compliance') else "Yes",
            
            # Institutional Analysis
            "{recommended_sba_program}": projection.sba_compliance_metrics.get('recommended_loan_type', 'SBA 7(a) Standard') if projection.sba_compliance_metrics else 'SBA 7(a) Standard',
            "{loan_amount_min}": f"{min(projection.realistic_annual * 0.3, 250000):,.0f}",
            "{loan_amount_max}": f"{min(projection.realistic_annual * 0.5, 400000):,.0f}",
            "{dscr_qualification_status}": "Qualified" if projection.debt_service_coverage_ratio >= 1.25 else "Review Required",
            "{equity_injection_required}": f"{projection.sba_compliance_metrics.get('owner_equity_injection', 0):,.0f}" if projection.sba_compliance_metrics else "0",
            "{credit_risk_rating}": "B+" if projection.confidence_level >= 70 else "B",
            "{collateral_adequacy_assessment}": "Adequate" if projection.debt_service_coverage_ratio >= 1.25 else "Supplemental Required",
            "{cash_flow_stability_rating}": "Stable" if projection.confidence_level >= 70 else "Moderate",
            "{recommended_loan_structure}": "Term loan with seasonal line of credit",
            "{job_creation_impact}": f"{projection.sba_compliance_metrics.get('jobs_created', 0)} direct jobs" if projection.sba_compliance_metrics else "0 direct jobs",
            "{annual_tax_revenue}": f"{projection.realistic_annual * 0.08:,.0f}",  # Estimated 8% of revenue
            "{economic_multiplier_effect}": "1.4x local economic impact",
            
            # Monthly projections table
            "{sba_monthly_projections_table}": self._generate_monthly_projections_table(projection),
        }
        
        # Apply all replacements
        populated_content = template_content
        for placeholder, value in replacements.items():
            populated_content = populated_content.replace(placeholder, str(value))
        
        return populated_content
    
    def _assess_viability(self, projection: RevenueProjection) -> str:
        """Assess overall revenue viability"""
        if projection.confidence_level >= 80 and projection.realistic_annual >= 400000:
            return "**High Viability** - Strong revenue potential with high confidence"
        elif projection.confidence_level >= 70 and projection.realistic_annual >= 250000:
            return "**Moderate Viability** - Reasonable revenue potential with acceptable confidence"
        elif projection.confidence_level >= 60:
            return "**Low-Moderate Viability** - Limited revenue potential, careful planning required"
        else:
            return "**Low Viability** - Significant revenue challenges, consider alternative strategies"
    
    def _generate_sba_compliant_metrics(self, scenarios: Dict[str, float], 
                                      industry_data: Dict[str, Any], 
                                      seasonal_adjustments: Dict[str, float]) -> Dict[str, Any]:
        """Generate SBA-compliant financial metrics for institutional analysis"""
        
        realistic_annual = scenarios['realistic']
        realistic_monthly = realistic_annual / 12
        
        # 1. Generate 36-month cash flow projections (SBA standard)
        monthly_projections = []
        for month in range(36):
            year = month // 12 + 1
            month_of_year = (month % 12) + 1
            
            # Apply seasonal adjustments
            seasonal_factor = seasonal_adjustments.get(str(month_of_year).zfill(2), 1.0)
            
            # Apply ramp-up factor for first year
            if year == 1:
                ramp_factor = min(1.0, (month + 1) / 6)  # 6-month ramp to full capacity
            else:
                ramp_factor = 1.0 + (0.05 * (year - 1))  # 5% annual growth
            
            monthly_revenue = realistic_monthly * seasonal_factor * ramp_factor
            
            monthly_projections.append({
                'month': month + 1,
                'year': year,
                'month_name': self._get_month_name(month_of_year),
                'gross_revenue': round(monthly_revenue, 2),
                'seasonal_factor': seasonal_factor,
                'ramp_factor': ramp_factor
            })
        
        # 2. Calculate Debt Service Coverage Ratio (DSCR)
        # Assume typical loan terms: $300K at 7% for 10 years
        loan_amount = min(realistic_annual * 0.4, 300000)  # Typical SBA loan sizing
        interest_rate = 0.07
        loan_term_years = 10
        monthly_payment = self._calculate_loan_payment(loan_amount, interest_rate, loan_term_years)
        
        # DSCR = Net Operating Income / Debt Service
        # Assume 20% net margin for DSCR calculation
        annual_net_income = realistic_annual * 0.20
        annual_debt_service = monthly_payment * 12
        dscr = annual_net_income / annual_debt_service if annual_debt_service > 0 else 0
        
        # 3. Working capital requirements by quarter
        working_capital = {
            'q1': realistic_monthly * 1.5,  # 1.5 months revenue
            'q2': realistic_monthly * 1.2,  # Lower after ramp-up
            'q3': realistic_monthly * 1.0,  # Steady state
            'q4': realistic_monthly * 1.3,  # Holiday inventory build
            'annual_average': realistic_monthly * 1.25
        }
        
        # 4. Cash flow timing patterns
        cash_flow_timing = {
            'peak_months': [str(m) for m, factor in seasonal_adjustments.items() if factor > 1.1],
            'low_months': [str(m) for m, factor in seasonal_adjustments.items() if factor < 0.9],
            'cash_conversion_cycle': 30,  # Days
            'accounts_receivable_days': 15,
            'inventory_turns': 12,
            'payment_terms': 'Net 30'
        }
        
        # 5. Revenue concentration risk
        # Assume typical customer distribution for business type
        business_type_lower = scenarios.get('business_type', 'retail').lower()
        if 'restaurant' in business_type_lower:
            concentration_risk = 0.15  # 15% from top customer
        elif 'retail' in business_type_lower:
            concentration_risk = 0.25  # 25% from top customer
        else:
            concentration_risk = 0.20  # 20% default
        
        # 6. SBA compliance metrics
        compliance_metrics = {
            'sba_size_standard_compliance': True,  # Assume compliance
            'jobs_created': max(2, int(realistic_annual / 100000)),  # 1 job per $100K revenue
            'owner_equity_injection': loan_amount * 0.10,  # 10% equity requirement
            'collateral_coverage': loan_amount * 0.80,  # 80% collateral coverage
            'cash_injection_timing': 'Before loan closing',
            'use_of_funds': {
                'working_capital': 0.30,
                'equipment': 0.40,
                'real_estate': 0.20,
                'inventory': 0.10
            },
            'loan_to_value_ratio': 0.80,
            'recommended_loan_type': self._recommend_sba_loan_type(loan_amount, realistic_annual)
        }
        
        return {
            'monthly_projections': monthly_projections,
            'dscr': round(dscr, 2),
            'working_capital': working_capital,
            'cash_flow_timing': cash_flow_timing,
            'concentration_risk': concentration_risk,
            'compliance_metrics': compliance_metrics
        }
    
    def _calculate_loan_payment(self, principal: float, annual_rate: float, years: int) -> float:
        """Calculate monthly loan payment"""
        monthly_rate = annual_rate / 12
        num_payments = years * 12
        
        if monthly_rate == 0:
            return principal / num_payments
        
        payment = principal * (monthly_rate * (1 + monthly_rate) ** num_payments) / ((1 + monthly_rate) ** num_payments - 1)
        return round(payment, 2)
    
    def _get_month_name(self, month_num: int) -> str:
        """Get month name from number"""
        months = ['January', 'February', 'March', 'April', 'May', 'June',
                 'July', 'August', 'September', 'October', 'November', 'December']
        return months[month_num - 1]
    
    def _recommend_sba_loan_type(self, loan_amount: float, annual_revenue: float) -> str:
        """Recommend appropriate SBA loan type"""
        if loan_amount <= 50000:
            return "SBA Microloan"
        elif annual_revenue <= 500000:
            return "SBA 7(a) Standard"
        elif loan_amount >= 125000:
            return "SBA 504 (if real estate/equipment)"
        else:
            return "SBA 7(a) Standard"

    def _generate_monthly_projections_table(self, projection: RevenueProjection) -> str:
        """Generate formatted table of monthly projections for SBA template"""
        if not projection.monthly_cash_flow_projections:
            return "| 1 | 1 | $50,000 | 1.0 | 0.5 |\n| ... | ... | ... | ... | ... |"
        
        # Generate table rows for first 12 months
        table_rows = []
        for i, month_data in enumerate(projection.monthly_cash_flow_projections[:12]):
            row = f"| {month_data['month']} | {month_data['year']} | ${month_data['gross_revenue']:,.0f} | {month_data['seasonal_factor']:.2f} | {month_data['ramp_factor']:.2f} |"
            table_rows.append(row)
        
        # Add summary row
        table_rows.append("| ... | ... | ... | ... | ... |")
        table_rows.append("| 36 | 3 | [See full projections in data file] | | |")
        
        return "\n".join(table_rows)


def main():
    """Test the revenue projections analyzer"""
    analyzer = RevenueProjectionsAnalyzer()
    
    # Test coordinates (Madison, WI)
    lat, lon = 43.0731, -89.4014
    
    print("🧮 Testing Revenue Projections Analyzer")
    print("=" * 50)
    
    # Test revenue projections for different business types
    business_types = ['restaurant', 'hair_salon', 'auto_repair']
    
    for business_type in business_types:
        print(f"💰 Analyzing {business_type} revenue projections...")
        
        projection = analyzer.analyze_revenue_projections(
            business_type=business_type,
            address="123 Test St, Madison, WI",
            lat=lat,
            lon=lon
        )
        
        print(f"  Conservative: ${projection.conservative_annual:,.0f}")
        print(f"  Realistic: ${projection.realistic_annual:,.0f}")
        print(f"  Optimistic: ${projection.optimistic_annual:,.0f}")
        print(f"  Confidence: {projection.confidence_level:.0f}%")
        print()
    
    print("✅ Revenue projections analyzer test complete")


def main():
    """Test the revenue projections analyzer"""
    print("🚀 Testing Revenue Projections Analyzer...")
    
    analyzer = RevenueProjectionsAnalyzer()
    
    # Test cases
    test_cases = [
        ("Restaurant", "123 Main St, Madison, WI", 43.0731, -89.4014),
        ("Auto Repair Shop", "456 Oak Ave, Milwaukee, WI", 43.0389, -87.9065),
        ("Hair Salon", "789 Pine St, Green Bay, WI", 44.5133, -88.0133)
    ]
    
    for business_type, address, lat, lon in test_cases:
        print(f"\n📊 Testing {business_type} at {address}")
        
        projection = analyzer.analyze_revenue_projections(
            business_type=business_type,
            address=address,
            lat=lat,
            lon=lon
        )
        
        print(f"  Conservative: ${projection.conservative_annual:,.0f}")
        print(f"  Realistic: ${projection.realistic_annual:,.0f}")
        print(f"  Optimistic: ${projection.optimistic_annual:,.0f}")
        print(f"  Confidence: {projection.confidence_level:.0f}%")
        print(f"  DSCR: {projection.debt_service_coverage_ratio}")
        print(f"  SBA Loan Type: {projection.sba_compliance_metrics.get('recommended_loan_type', 'N/A')}")
        print()
    
    print("✅ Revenue projections analyzer test complete")


if __name__ == "__main__":
    main()