"""
Data Models for Location Optimizer
=================================

Standardized data models for business data collection across all states.
Uses Pydantic for validation and type safety.
"""

from datetime import datetime, date
from typing import Optional, List, Dict, Any
from pydantic import BaseModel, Field, validator
from enum import Enum


class BusinessStatus(str, Enum):
    """Standard business status types"""
    ACTIVE = "active"
    INACTIVE = "inactive"
    PENDING = "pending"
    DISSOLVED = "dissolved"
    SUSPENDED = "suspended"


class BusinessType(str, Enum):
    """Standard business type categories"""
    RESTAURANT = "restaurant"
    RETAIL = "retail"
    PROFESSIONAL_SERVICES = "professional_services"
    PERSONAL_SERVICES = "personal_services"
    HEALTHCARE = "healthcare"
    AUTOMOTIVE = "automotive"
    FITNESS = "fitness"
    FRANCHISE = "franchise"
    OTHER = "other"


class DataSource(str, Enum):
    """Data source types"""
    STATE_REGISTRATION = "state_registration"
    SBA_LOANS = "sba_loans"
    BUSINESS_LICENSES = "business_licenses"
    ECONOMIC_DEVELOPMENT = "economic_development"


class BusinessEntity(BaseModel):
    """Standardized business entity model"""
    
    # Core identifiers
    business_id: str = Field(..., description="Unique business identifier")
    source_id: str = Field(..., description="Source system identifier")
    
    # Business information
    business_name: str = Field(..., description="Official business name")
    owner_name: Optional[str] = Field(None, description="Primary owner name")
    business_type: BusinessType = Field(..., description="Categorized business type")
    naics_code: Optional[str] = Field(None, description="NAICS industry code")
    entity_type: Optional[str] = Field(None, description="Legal entity type (LLC, Corp, etc)")
    
    # Status and dates
    status: BusinessStatus = Field(..., description="Current business status")
    registration_date: Optional[date] = Field(None, description="Business registration date")
    last_updated: Optional[date] = Field(None, description="Last update date from source")
    
    # Location information
    address_full: Optional[str] = Field(None, description="Complete address")
    city: str = Field(..., description="Business city")
    state: str = Field(..., description="Business state (2-letter code)")
    zip_code: Optional[str] = Field(None, description="ZIP code")
    county: Optional[str] = Field(None, description="County name")
    
    # Contact information
    phone: Optional[str] = Field(None, description="Business phone number")
    email: Optional[str] = Field(None, description="Business email")
    website: Optional[str] = Field(None, description="Business website")
    
    # Additional details
    business_description: Optional[str] = Field(None, description="Business description")
    employee_count: Optional[int] = Field(None, description="Number of employees")
    annual_revenue: Optional[float] = Field(None, description="Annual revenue estimate")
    
    # Metadata
    data_source: DataSource = Field(..., description="Original data source")
    source_url: Optional[str] = Field(None, description="Source URL")
    data_extraction_date: datetime = Field(default_factory=datetime.now, description="Data extraction timestamp")
    confidence_score: Optional[float] = Field(None, description="Data quality confidence score")
    
    @validator('state')
    def validate_state_code(cls, v):
        """Ensure state is 2-letter uppercase code"""
        if v and len(v) == 2:
            return v.upper()
        return v
    
    @validator('zip_code')
    def validate_zip_code(cls, v):
        """Clean and validate ZIP code"""
        if v:
            # Remove any non-digit characters and ensure 5 or 9 digits
            cleaned = ''.join(filter(str.isdigit, v))
            if len(cleaned) >= 5:
                return cleaned[:5]
        return v
    
    @validator('phone')
    def validate_phone(cls, v):
        """Clean phone number"""
        if v:
            # Keep only digits
            cleaned = ''.join(filter(str.isdigit, v))
            if len(cleaned) == 10:
                return f"({cleaned[:3]}) {cleaned[3:6]}-{cleaned[6:]}"
        return v


class SBALoanRecord(BaseModel):
    """SBA loan approval record"""
    
    # Loan identifiers
    loan_id: str = Field(..., description="Unique loan identifier")
    borrower_name: str = Field(..., description="Borrower business name")
    
    # Loan details
    loan_amount: float = Field(..., description="Approved loan amount")
    approval_date: date = Field(..., description="Loan approval date")
    program_type: str = Field(..., description="SBA program type (7a, 504, etc)")
    
    # Borrower information
    borrower_address: Optional[str] = Field(None, description="Borrower address")
    borrower_city: str = Field(..., description="Borrower city")
    borrower_state: str = Field(..., description="Borrower state")
    borrower_zip: Optional[str] = Field(None, description="Borrower ZIP code")
    
    # Business details
    naics_code: Optional[str] = Field(None, description="NAICS code")
    business_type: Optional[str] = Field(None, description="Business type description")
    jobs_supported: Optional[int] = Field(None, description="Jobs supported by loan")
    
    # Franchise information
    franchise_code: Optional[str] = Field(None, description="Franchise code if applicable")
    franchise_name: Optional[str] = Field(None, description="Franchise name")
    
    # Lender information
    lender_name: Optional[str] = Field(None, description="Lender institution name")
    
    # Metadata
    data_source: str = Field(default="SBA", description="Data source")
    data_extraction_date: datetime = Field(default_factory=datetime.now)
    
    @validator('borrower_state')
    def validate_state_code(cls, v):
        """Ensure state is 2-letter uppercase code"""
        if v and len(v) == 2:
            return v.upper()
        return v


class BusinessLicense(BaseModel):
    """Business license record"""
    
    # License identifiers
    license_id: str = Field(..., description="License identifier")
    business_name: str = Field(..., description="Licensed business name")
    
    # License details
    license_type: str = Field(..., description="Type of license")
    issue_date: Optional[date] = Field(None, description="License issue date")
    expiration_date: Optional[date] = Field(None, description="License expiration date")
    status: str = Field(..., description="License status")
    
    # Location information
    address: Optional[str] = Field(None, description="Business address")
    city: str = Field(..., description="Business city")
    state: str = Field(..., description="Business state")
    zip_code: Optional[str] = Field(None, description="ZIP code")
    
    # Metadata
    issuing_authority: Optional[str] = Field(None, description="License issuing authority")
    data_source: str = Field(..., description="Data source")
    data_extraction_date: datetime = Field(default_factory=datetime.now)
    
    @validator('state')
    def validate_state_code(cls, v):
        """Ensure state is 2-letter uppercase code"""
        if v and len(v) == 2:
            return v.upper()
        return v


class DataCollectionSummary(BaseModel):
    """Summary of data collection run"""
    
    collection_date: datetime = Field(default_factory=datetime.now)
    state: str = Field(..., description="State code")
    
    # Collection counts
    businesses_collected: int = Field(default=0)
    sba_loans_collected: int = Field(default=0)
    licenses_collected: int = Field(default=0)
    total_records: int = Field(default=0)
    
    # Success metrics
    success: bool = Field(default=False)
    errors_encountered: int = Field(default=0)
    processing_time_seconds: Optional[float] = Field(None)
    
    # Data quality metrics
    duplicate_records: int = Field(default=0)
    invalid_records: int = Field(default=0)
    confidence_scores: Optional[Dict[str, float]] = Field(None)
    
    # Source breakdown
    source_summary: Optional[Dict[str, int]] = Field(None, description="Records by source")
    
    def calculate_totals(self):
        """Calculate total records"""
        self.total_records = (
            self.businesses_collected + 
            self.sba_loans_collected + 
            self.licenses_collected
        )


class OpportunityScore(BaseModel):
    """Business opportunity scoring model"""
    
    business_id: str = Field(..., description="Business identifier")
    
    # Scoring components
    market_opportunity_score: float = Field(..., ge=0, le=100, description="Market opportunity score (0-100)")
    competition_density_score: float = Field(..., ge=0, le=100, description="Competition density score (0-100)")
    demographic_match_score: float = Field(..., ge=0, le=100, description="Demographic match score (0-100)")
    location_quality_score: float = Field(..., ge=0, le=100, description="Location quality score (0-100)")
    
    # Overall score
    overall_score: float = Field(..., ge=0, le=100, description="Overall opportunity score (0-100)")
    
    # Metadata
    score_date: datetime = Field(default_factory=datetime.now)
    model_version: str = Field(..., description="Scoring model version")
    
    def calculate_overall_score(self, weights: Dict[str, float] = None):
        """Calculate weighted overall score"""
        if weights is None:
            weights = {
                'market_opportunity': 0.3,
                'competition_density': 0.25,
                'demographic_match': 0.25,
                'location_quality': 0.2
            }
        
        self.overall_score = (
            self.market_opportunity_score * weights.get('market_opportunity', 0.3) +
            self.competition_density_score * weights.get('competition_density', 0.25) +
            self.demographic_match_score * weights.get('demographic_match', 0.25) +
            self.location_quality_score * weights.get('location_quality', 0.2)
        )
        
        return self.overall_score