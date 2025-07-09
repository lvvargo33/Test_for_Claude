#!/usr/bin/env python3
"""
Financial Institution Analysis Generator for Section 8.1
======================================================

Provides SBA brokers and banks with detailed credit analysis, loan structuring
recommendations, and compliance assessments using existing financial data.
"""

import json
import os
from datetime import datetime
from typing import Dict, List, Any, Optional, Tuple
import logging

logger = logging.getLogger(__name__)


class FinancialInstitutionResult:
    """Container for financial institution analysis results"""
    
    def __init__(self, business_type: str, location: str):
        self.business_type = business_type
        self.location = location
        
        # Credit Analysis
        self.debt_service_coverage_ratio = 0.0
        self.loan_to_value_ratio = 0.0
        self.credit_risk_score = 0
        self.credit_risk_rating = ""
        self.collateral_adequacy_ratio = 0.0
        
        # SBA Compliance
        self.sba_eligibility_score = 0
        self.sba_recommended_program = ""
        self.sba_program_rationale = ""
        self.personal_guarantee_requirement = ""
        self.sba_fee_structure = ""
        
        # Loan Structuring
        self.recommended_loan_structure = ""
        self.optimal_loan_amount = 0
        self.recommended_term_length = ""
        self.collateral_requirements = ""
        self.down_payment_requirement = 0.0
        
        # Risk Assessment
        self.primary_risk_factors = []
        self.risk_mitigation_strategies = []
        self.regulatory_compliance_score = 0
        self.loan_approval_probability = 0.0
        
        # Documentation Requirements
        self.required_documentation = []
        self.financial_covenant_requirements = []
        self.monitoring_requirements = []
        
        # Institutional Metrics
        self.expected_loan_yield = 0.0
        self.regulatory_capital_impact = ""
        self.portfolio_diversification_impact = ""


class FinancialInstitutionAnalyzer:
    """Analyzes business opportunity from financial institution perspective"""
    
    def __init__(self):
        self.sba_industry_standards = self._load_sba_standards()
        self.credit_scoring_weights = self._load_credit_weights()
    
    def _load_sba_standards(self) -> Dict[str, Any]:
        """Load SBA program standards and requirements"""
        return {
            "7a": {
                "max_loan_amount": 5000000,
                "max_sba_guarantee": 3750000,
                "guarantee_percentage": 0.75,
                "max_term_working_capital": 10,
                "max_term_equipment": 10,
                "max_term_real_estate": 25,
                "min_down_payment": 0.10,
                "fee_structure": {
                    "guarantee_fee": 0.0175,
                    "servicing_fee": 0.005
                }
            },
            "504": {
                "max_project_size": 20000000,
                "sba_portion": 0.40,
                "bank_portion": 0.50,
                "borrower_portion": 0.10,
                "fixed_asset_requirement": 0.51,
                "max_term": 20,
                "fee_structure": {
                    "processing_fee": 0.005,
                    "funding_fee": 0.002
                }
            },
            "eligibility_criteria": {
                "size_standards": {
                    "restaurant": {"employees": 500, "revenue": 41500000},
                    "retail": {"employees": 500, "revenue": 27000000},
                    "manufacturing": {"employees": 500, "revenue": 35000000},
                    "service": {"employees": 500, "revenue": 35000000}
                },
                "credit_requirements": {
                    "min_credit_score": 650,
                    "debt_service_coverage": 1.15,
                    "collateral_coverage": 1.0
                }
            }
        }
    
    def _load_credit_weights(self) -> Dict[str, float]:
        """Load credit risk scoring weights"""
        return {
            "debt_service_coverage": 0.30,
            "collateral_coverage": 0.25,
            "credit_history": 0.20,
            "industry_risk": 0.15,
            "management_experience": 0.10
        }
    
    def generate_financial_institution_analysis(self, business_type: str, location: str,
                                              integrated_data: Dict[str, Any],
                                              recommendations_data: Dict[str, Any]) -> FinancialInstitutionResult:
        """Generate comprehensive financial institution analysis"""
        
        logger.info(f"Generating financial institution analysis for {business_type} in {location}")
        
        result = FinancialInstitutionResult(business_type, location)
        
        # Credit Analysis
        self._analyze_credit_metrics(result, integrated_data, recommendations_data)
        
        # SBA Compliance Assessment
        self._assess_sba_compliance(result, integrated_data, recommendations_data)
        
        # Loan Structuring Recommendations
        self._generate_loan_structure(result, integrated_data, recommendations_data)
        
        # Risk Assessment
        self._assess_lending_risks(result, integrated_data, recommendations_data)
        
        # Documentation Requirements
        self._generate_documentation_requirements(result, integrated_data)
        
        # Institutional Metrics
        self._calculate_institutional_metrics(result, integrated_data)
        
        return result
    
    def _analyze_credit_metrics(self, result: FinancialInstitutionResult,
                               integrated_data: Dict[str, Any],
                               recommendations_data: Dict[str, Any]):
        """Analyze key credit metrics"""
        
        # Get financial data
        projected_annual_revenue = integrated_data.get("projected_annual_revenue", 500000)
        total_startup_costs = integrated_data.get("total_startup_costs", 350000)
        monthly_operating_costs = integrated_data.get("monthly_operating_costs", 25000)
        
        # Calculate debt service coverage ratio
        annual_debt_service = self._calculate_annual_debt_service(total_startup_costs * 0.70)
        net_operating_income = projected_annual_revenue - (monthly_operating_costs * 12)
        
        if annual_debt_service > 0:
            result.debt_service_coverage_ratio = round(net_operating_income / annual_debt_service, 2)
        else:
            result.debt_service_coverage_ratio = 0.0
        
        # Calculate loan-to-value ratio
        collateral_value = integrated_data.get("real_estate_value", total_startup_costs * 0.80)
        loan_amount = total_startup_costs * 0.70
        result.loan_to_value_ratio = round(loan_amount / collateral_value, 2)
        
        # Calculate collateral adequacy ratio
        result.collateral_adequacy_ratio = round(collateral_value / loan_amount, 2)
        
        # Generate credit risk score (0-100)
        credit_factors = {
            "dscr": min(100, (result.debt_service_coverage_ratio - 1.0) * 100),
            "ltv": max(0, (1.0 - result.loan_to_value_ratio) * 100),
            "industry": self._get_industry_risk_score(result.business_type),
            "location": self._get_location_risk_score(result.location)
        }
        
        result.credit_risk_score = int(sum(credit_factors.values()) / len(credit_factors))
        
        # Credit risk rating
        if result.credit_risk_score >= 80:
            result.credit_risk_rating = "Excellent (A)"
        elif result.credit_risk_score >= 65:
            result.credit_risk_rating = "Good (B)"
        elif result.credit_risk_score >= 50:
            result.credit_risk_rating = "Fair (C)"
        else:
            result.credit_risk_rating = "Poor (D)"
    
    def _assess_sba_compliance(self, result: FinancialInstitutionResult,
                              integrated_data: Dict[str, Any],
                              recommendations_data: Dict[str, Any]):
        """Assess SBA program compliance and recommendations"""
        
        total_project_cost = integrated_data.get("total_startup_costs", 350000)
        real_estate_percentage = integrated_data.get("real_estate_cost", 0) / total_project_cost
        
        # SBA eligibility scoring
        eligibility_score = 0
        
        # Size standard check
        business_category = self._categorize_business_type(result.business_type)
        size_standards = self.sba_industry_standards["eligibility_criteria"]["size_standards"]
        
        if business_category in size_standards:
            # Assume business meets size standards for scoring
            eligibility_score += 25
        
        # Credit requirements
        credit_reqs = self.sba_industry_standards["eligibility_criteria"]["credit_requirements"]
        
        if result.debt_service_coverage_ratio >= credit_reqs["debt_service_coverage"]:
            eligibility_score += 25
        
        if result.collateral_adequacy_ratio >= credit_reqs["collateral_coverage"]:
            eligibility_score += 25
        
        # Assume credit score meets minimum for scoring
        eligibility_score += 25
        
        result.sba_eligibility_score = eligibility_score
        
        # Program recommendation
        if real_estate_percentage >= 0.51:
            result.sba_recommended_program = "SBA 504"
            result.sba_program_rationale = (
                f"Real estate component ({real_estate_percentage:.1%}) exceeds 51% threshold. "
                f"SBA 504 provides optimal financing for fixed asset acquisition with "
                f"favorable long-term rates and lower down payment requirements."
            )
        else:
            result.sba_recommended_program = "SBA 7(a)"
            result.sba_program_rationale = (
                f"Lower real estate component ({real_estate_percentage:.1%}) favors "
                f"working capital and equipment financing flexibility. SBA 7(a) "
                f"provides versatile financing structure for mixed-use projects."
            )
        
        # Personal guarantee requirement
        if total_project_cost > 350000:
            result.personal_guarantee_requirement = "Required for all owners with 20%+ ownership"
        else:
            result.personal_guarantee_requirement = "Standard personal guarantee required"
        
        # Fee structure
        if result.sba_recommended_program == "SBA 7(a)":
            fees = self.sba_industry_standards["7a"]["fee_structure"]
            result.sba_fee_structure = (
                f"Guarantee Fee: {fees['guarantee_fee']:.2%} of loan amount, "
                f"Servicing Fee: {fees['servicing_fee']:.2%} annually"
            )
        else:
            fees = self.sba_industry_standards["504"]["fee_structure"]
            result.sba_fee_structure = (
                f"Processing Fee: {fees['processing_fee']:.2%} of project cost, "
                f"Funding Fee: {fees['funding_fee']:.2%} of debenture amount"
            )
    
    def _generate_loan_structure(self, result: FinancialInstitutionResult,
                                integrated_data: Dict[str, Any],
                                recommendations_data: Dict[str, Any]):
        """Generate optimal loan structure recommendations"""
        
        total_project_cost = integrated_data.get("total_startup_costs", 350000)
        
        if result.sba_recommended_program == "SBA 7(a)":
            # SBA 7(a) structure
            sba_standards = self.sba_industry_standards["7a"]
            max_loan = min(total_project_cost * 0.90, sba_standards["max_loan_amount"])
            
            result.recommended_loan_structure = (
                f"**SBA 7(a) Loan Structure**\n"
                f"• Total Project Cost: ${total_project_cost:,}\n"
                f"• SBA Loan Amount: ${max_loan:,}\n"
                f"• SBA Guarantee: {sba_standards['guarantee_percentage']:.0%}\n"
                f"• Owner Equity: ${total_project_cost - max_loan:,}"
            )
            
            result.optimal_loan_amount = max_loan
            result.recommended_term_length = "10 years (working capital/equipment)"
            result.down_payment_requirement = (total_project_cost - max_loan) / total_project_cost
            
        else:
            # SBA 504 structure
            sba_standards = self.sba_industry_standards["504"]
            sba_portion = total_project_cost * sba_standards["sba_portion"]
            bank_portion = total_project_cost * sba_standards["bank_portion"]
            borrower_portion = total_project_cost * sba_standards["borrower_portion"]
            
            result.recommended_loan_structure = (
                f"**SBA 504 Loan Structure**\n"
                f"• Total Project Cost: ${total_project_cost:,}\n"
                f"• SBA Debenture (40%): ${sba_portion:,}\n"
                f"• Bank Loan (50%): ${bank_portion:,}\n"
                f"• Owner Equity (10%): ${borrower_portion:,}"
            )
            
            result.optimal_loan_amount = bank_portion
            result.recommended_term_length = "20 years (real estate), 10 years (equipment)"
            result.down_payment_requirement = borrower_portion / total_project_cost
        
        # Collateral requirements
        result.collateral_requirements = (
            f"Primary: Business assets and real estate being financed\n"
            f"Secondary: Personal guarantees from owners with 20%+ ownership\n"
            f"Tertiary: Additional collateral if coverage ratio below 1.25"
        )
    
    def _assess_lending_risks(self, result: FinancialInstitutionResult,
                             integrated_data: Dict[str, Any],
                             recommendations_data: Dict[str, Any]):
        """Assess primary lending risks and mitigation strategies"""
        
        # Primary risk factors
        risks = []
        
        if result.debt_service_coverage_ratio < 1.25:
            risks.append("Low debt service coverage ratio")
        
        if result.loan_to_value_ratio > 0.80:
            risks.append("High loan-to-value ratio")
        
        industry_risk = self._get_industry_risk_score(result.business_type)
        if industry_risk < 60:
            risks.append("High industry risk profile")
        
        # Market-specific risks
        market_risks = integrated_data.get("market_risks", [])
        risks.extend(market_risks)
        
        result.primary_risk_factors = risks
        
        # Mitigation strategies
        strategies = []
        
        if result.debt_service_coverage_ratio < 1.25:
            strategies.append("Require additional collateral or co-guarantor")
            strategies.append("Implement monthly cash flow monitoring")
        
        if result.loan_to_value_ratio > 0.80:
            strategies.append("Increase down payment requirement")
            strategies.append("Obtain additional collateral security")
        
        strategies.append("Require comprehensive business insurance")
        strategies.append("Implement quarterly financial reporting")
        
        result.risk_mitigation_strategies = strategies
        
        # Regulatory compliance score
        compliance_factors = {
            "capital_adequacy": 85,  # Assume adequate capital
            "asset_quality": max(0, result.credit_risk_score),
            "management_quality": 75,  # Assume adequate management
            "earnings_quality": min(100, result.debt_service_coverage_ratio * 40),
            "liquidity": 80  # Assume adequate liquidity
        }
        
        result.regulatory_compliance_score = int(sum(compliance_factors.values()) / len(compliance_factors))
        
        # Loan approval probability
        approval_factors = [
            result.credit_risk_score,
            result.sba_eligibility_score,
            result.regulatory_compliance_score
        ]
        
        result.loan_approval_probability = sum(approval_factors) / (len(approval_factors) * 100)
    
    def _generate_documentation_requirements(self, result: FinancialInstitutionResult,
                                           integrated_data: Dict[str, Any]):
        """Generate required documentation checklist"""
        
        basic_docs = [
            "Completed SBA loan application (Form 1919)",
            "Personal financial statements (all guarantors)",
            "Business tax returns (3 years)",
            "Personal tax returns (3 years, all guarantors)",
            "Financial projections (3 years)",
            "Business plan with market analysis",
            "Articles of incorporation/organization",
            "Operating agreements/bylaws",
            "Resume of key management personnel"
        ]
        
        # Add business-specific requirements
        if "restaurant" in result.business_type.lower():
            basic_docs.extend([
                "Food service license",
                "Liquor license (if applicable)",
                "Health department permits",
                "Equipment specifications and costs"
            ])
        elif "retail" in result.business_type.lower():
            basic_docs.extend([
                "Retail license",
                "Inventory management system documentation",
                "Supplier agreements",
                "Lease agreement with assignment clause"
            ])
        
        # Add SBA-specific requirements
        if result.sba_recommended_program == "SBA 504":
            basic_docs.extend([
                "Appraisal of real estate",
                "Environmental assessment (Phase I)",
                "Construction contracts and permits",
                "Architect/engineer certifications"
            ])
        
        result.required_documentation = basic_docs
        
        # Financial covenant requirements
        covenants = [
            f"Maintain debt service coverage ratio above {1.15}",
            f"Maintain minimum working capital of ${integrated_data.get('working_capital_requirement', 50000):,}",
            "Provide quarterly financial statements",
            "Maintain comprehensive business insurance",
            "Obtain lender consent for major capital expenditures"
        ]
        
        result.financial_covenant_requirements = covenants
        
        # Monitoring requirements
        monitoring = [
            "Monthly cash flow reports",
            "Quarterly profit & loss statements",
            "Annual audited financial statements",
            "Annual business insurance renewal",
            "Site visits (annual or as needed)"
        ]
        
        result.monitoring_requirements = monitoring
    
    def _calculate_institutional_metrics(self, result: FinancialInstitutionResult,
                                       integrated_data: Dict[str, Any]):
        """Calculate institutional-specific metrics"""
        
        # Expected loan yield
        base_rate = 0.045  # Assume 4.5% base rate
        risk_premium = (100 - result.credit_risk_score) * 0.0005  # Risk-based pricing
        sba_guarantee_discount = 0.01 if result.sba_recommended_program else 0.0
        
        result.expected_loan_yield = base_rate + risk_premium - sba_guarantee_discount
        
        # Regulatory capital impact
        if result.sba_recommended_program:
            result.regulatory_capital_impact = "Reduced (20% risk weight due to SBA guarantee)"
        else:
            result.regulatory_capital_impact = "Standard (100% risk weight)"
        
        # Portfolio diversification impact
        loan_size = result.optimal_loan_amount
        if loan_size > 1000000:
            result.portfolio_diversification_impact = "Large exposure - monitor concentration risk"
        elif loan_size > 500000:
            result.portfolio_diversification_impact = "Medium exposure - standard monitoring"
        else:
            result.portfolio_diversification_impact = "Small exposure - minimal concentration risk"
    
    def _calculate_annual_debt_service(self, loan_amount: float) -> float:
        """Calculate annual debt service for loan amount"""
        interest_rate = 0.065  # Assume 6.5% interest rate
        term_years = 10
        
        monthly_rate = interest_rate / 12
        num_payments = term_years * 12
        
        if monthly_rate > 0:
            monthly_payment = loan_amount * (monthly_rate * (1 + monthly_rate)**num_payments) / ((1 + monthly_rate)**num_payments - 1)
            return monthly_payment * 12
        else:
            return loan_amount / term_years
    
    def _get_industry_risk_score(self, business_type: str) -> int:
        """Get industry risk score (0-100, higher is better)"""
        industry_scores = {
            "restaurant": 45,
            "retail": 55,
            "service": 70,
            "healthcare": 80,
            "manufacturing": 60,
            "technology": 50,
            "construction": 40
        }
        
        business_lower = business_type.lower()
        for industry, score in industry_scores.items():
            if industry in business_lower:
                return score
        
        return 55  # Default moderate risk
    
    def _get_location_risk_score(self, location: str) -> int:
        """Get location risk score based on economic factors"""
        # This would typically use demographic and economic data
        # For now, return a moderate score
        return 65
    
    def _categorize_business_type(self, business_type: str) -> str:
        """Categorize business type for SBA size standards"""
        business_lower = business_type.lower()
        
        if any(word in business_lower for word in ["restaurant", "food", "cafe", "bar"]):
            return "restaurant"
        elif any(word in business_lower for word in ["retail", "store", "shop"]):
            return "retail"
        elif any(word in business_lower for word in ["manufacturing", "factory", "production"]):
            return "manufacturing"
        else:
            return "service"