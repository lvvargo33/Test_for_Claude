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
    CENSUS_DEMOGRAPHICS = "census_demographics"
    OPENSTREETMAP = "openstreetmap"


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
    
    # Geocoding information
    latitude: Optional[float] = Field(None, description="Latitude coordinate")
    longitude: Optional[float] = Field(None, description="Longitude coordinate")
    geocoding_confidence: Optional[float] = Field(None, description="Geocoding confidence score (0.0-1.0)")
    formatted_address: Optional[str] = Field(None, description="Standardized geocoded address")
    geocoding_date: Optional[datetime] = Field(None, description="Date when geocoding was performed")
    geocoding_source: Optional[str] = Field(None, description="Geocoding service used")
    
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


class CensusGeography(BaseModel):
    """Census geographic and demographic data model"""
    
    # Geographic identifiers
    geo_id: str = Field(..., description="Full Census geographic identifier")
    state_fips: str = Field(..., description="State FIPS code (e.g., '55' for Wisconsin)")
    county_fips: str = Field(..., description="County FIPS code (e.g., '55025' for Dane County)")
    tract_code: Optional[str] = Field(None, description="Census tract code")
    block_group: Optional[str] = Field(None, description="Block group code")
    
    # Geographic metadata
    geographic_level: str = Field(..., description="Geographic level (county, tract, block_group)")
    area_land_sqmi: Optional[float] = Field(None, description="Land area in square miles")
    area_water_sqmi: Optional[float] = Field(None, description="Water area in square miles")
    
    # Population data
    total_population: Optional[int] = Field(None, description="Total population (B01003_001E)")
    median_age: Optional[float] = Field(None, description="Median age (B01002_001E)")
    
    # Economic data
    median_household_income: Optional[int] = Field(None, description="Median household income (B19013_001E)")
    unemployment_count: Optional[int] = Field(None, description="Unemployed population (B23025_005E)")
    labor_force: Optional[int] = Field(None, description="Total labor force (B23025_002E)")
    unemployment_rate: Optional[float] = Field(None, description="Calculated unemployment rate (%)")
    
    # Education data
    bachelor_degree_count: Optional[int] = Field(None, description="Population with bachelor's degree (B15003_022E)")
    total_education_pop: Optional[int] = Field(None, description="Total population 25+ for education (B15003_001E)")
    bachelor_degree_pct: Optional[float] = Field(None, description="Calculated % with bachelor's degree")
    
    # Housing data
    total_housing_units: Optional[int] = Field(None, description="Total housing units (B25001_001E)")
    owner_occupied_units: Optional[int] = Field(None, description="Owner occupied units (B25003_002E)")
    total_occupied_units: Optional[int] = Field(None, description="Total occupied units (B25003_001E)")
    owner_occupied_pct: Optional[float] = Field(None, description="Calculated % owner occupied")
    
    # Transportation data
    total_commuters: Optional[int] = Field(None, description="Total commuters (B08303_001E)")
    commute_60_plus_min: Optional[int] = Field(None, description="Commute 60+ minutes (B08303_013E)")
    public_transport_count: Optional[int] = Field(None, description="Public transportation users (B08301_010E)")
    total_transport_pop: Optional[int] = Field(None, description="Total transportation population (B08301_001E)")
    avg_commute_time: Optional[float] = Field(None, description="Calculated average commute time")
    public_transport_pct: Optional[float] = Field(None, description="Calculated % using public transport")
    
    # Derived metrics
    population_density: Optional[float] = Field(None, description="Population per square mile")
    household_density: Optional[float] = Field(None, description="Housing units per square mile")
    
    # Population Estimates Program (PEP) data (2019 - most recent available via API)
    population_2019: Optional[int] = Field(None, description="2019 Population Estimate (POP)")
    population_density_2019: Optional[float] = Field(None, description="2019 Population Density per Square Mile (DENSITY)")
    population_2022: Optional[int] = Field(None, description="2022 Population Estimate (POP_2022)")
    population_2021: Optional[int] = Field(None, description="2021 Population Estimate (POP_2021)")
    population_2020: Optional[int] = Field(None, description="2020 Population Estimate (POP_2020)")
    
    # Population change data
    net_population_change_2022: Optional[int] = Field(None, description="Net Population Change 2021-2022 (NPOPCHG_2022)")
    net_population_change_2021: Optional[int] = Field(None, description="Net Population Change 2020-2021 (NPOPCHG_2021)")
    
    # Components of population change
    births_2022: Optional[int] = Field(None, description="Births 2022 (BIRTHS2022)")
    deaths_2022: Optional[int] = Field(None, description="Deaths 2022 (DEATHS2022)")
    net_migration_2022: Optional[int] = Field(None, description="Net Migration 2022 (NETMIG2022)")
    
    # Population rates
    birth_rate_2022: Optional[float] = Field(None, description="Birth Rate per 1000 population 2022 (RBIRTH2022)")
    death_rate_2022: Optional[float] = Field(None, description="Death Rate per 1000 population 2022 (RDEATH2022)")
    
    # Calculated population metrics
    population_growth_rate_2022: Optional[float] = Field(None, description="Calculated population growth rate 2021-2022 (%)")
    population_growth_rate_2021: Optional[float] = Field(None, description="Calculated population growth rate 2020-2021 (%)")
    avg_annual_growth_rate: Optional[float] = Field(None, description="Average annual growth rate 2020-2022 (%)")
    natural_increase_2022: Optional[int] = Field(None, description="Natural increase (births - deaths) 2022")
    
    # Metadata
    acs_year: int = Field(..., description="ACS data year (e.g., 2022)")
    pep_year: Optional[int] = Field(None, description="Population Estimates Program data year")
    data_source: str = Field(default="census_acs", description="Data source identifier")
    data_extraction_date: datetime = Field(default_factory=datetime.now, description="Data extraction timestamp")
    data_quality_score: Optional[float] = Field(None, description="Data completeness score (0-100)")
    
    @validator('state_fips')
    def validate_state_fips(cls, v):
        """Ensure state FIPS is 2 digits"""
        if v and len(v) == 2 and v.isdigit():
            return v
        return v
    
    @validator('county_fips')
    def validate_county_fips(cls, v):
        """Ensure county FIPS is 5 digits (state + county)"""
        if v and len(v) == 5 and v.isdigit():
            return v
        return v
    
    def calculate_derived_metrics(self):
        """Calculate derived demographic metrics"""
        # Calculate unemployment rate
        if self.unemployment_count and self.labor_force and self.labor_force > 0:
            self.unemployment_rate = round((self.unemployment_count / self.labor_force) * 100, 2)
            
        # Calculate education percentage
        if self.bachelor_degree_count and self.total_education_pop and self.total_education_pop > 0:
            self.bachelor_degree_pct = round((self.bachelor_degree_count / self.total_education_pop) * 100, 2)
            
        # Calculate housing percentages
        if self.owner_occupied_units and self.total_occupied_units and self.total_occupied_units > 0:
            self.owner_occupied_pct = round((self.owner_occupied_units / self.total_occupied_units) * 100, 2)
            
        # Calculate transportation percentages
        if self.public_transport_count and self.total_transport_pop and self.total_transport_pop > 0:
            self.public_transport_pct = round((self.public_transport_count / self.total_transport_pop) * 100, 2)
            
        # Calculate population density
        if self.total_population and self.area_land_sqmi and self.area_land_sqmi > 0:
            self.population_density = round(self.total_population / self.area_land_sqmi, 2)
            
        # Calculate household density
        if self.total_housing_units and self.area_land_sqmi and self.area_land_sqmi > 0:
            self.household_density = round(self.total_housing_units / self.area_land_sqmi, 2)
            
        # Calculate population growth rates
        if self.population_2022 and self.population_2021 and self.population_2021 > 0:
            self.population_growth_rate_2022 = round(((self.population_2022 - self.population_2021) / self.population_2021) * 100, 2)
            
        if self.population_2021 and self.population_2020 and self.population_2020 > 0:
            self.population_growth_rate_2021 = round(((self.population_2021 - self.population_2020) / self.population_2020) * 100, 2)
            
        # Calculate average annual growth rate 2020-2022
        if self.population_2022 and self.population_2020 and self.population_2020 > 0:
            total_growth = ((self.population_2022 - self.population_2020) / self.population_2020) * 100
            self.avg_annual_growth_rate = round(total_growth / 2, 2)  # Average over 2 years
            
        # Calculate natural increase (births - deaths)
        if self.births_2022 and self.deaths_2022:
            self.natural_increase_2022 = self.births_2022 - self.deaths_2022
    
    def calculate_data_quality_score(self):
        """Calculate data completeness score"""
        required_fields = [
            'total_population', 'median_household_income', 'total_housing_units'
        ]
        optional_fields = [
            'median_age', 'unemployment_rate', 'bachelor_degree_pct', 
            'owner_occupied_pct', 'public_transport_pct'
        ]
        
        # Count populated required fields
        required_score = sum(1 for field in required_fields if getattr(self, field) is not None)
        required_weight = (required_score / len(required_fields)) * 70  # 70% weight for required
        
        # Count populated optional fields
        optional_score = sum(1 for field in optional_fields if getattr(self, field) is not None)
        optional_weight = (optional_score / len(optional_fields)) * 30  # 30% weight for optional
        
        self.data_quality_score = round(required_weight + optional_weight, 1)
        return self.data_quality_score


class OSMBusinessEntity(BaseModel):
    """OpenStreetMap business entity model"""
    
    # OSM identifiers
    osm_id: str = Field(..., description="OpenStreetMap element ID")
    osm_type: str = Field(..., description="OSM element type (node, way, relation)")
    osm_version: Optional[int] = Field(None, description="OSM element version")
    osm_timestamp: Optional[datetime] = Field(None, description="OSM last modification timestamp")
    
    # Business information
    name: Optional[str] = Field(None, description="Business name")
    amenity: Optional[str] = Field(None, description="OSM amenity tag")
    shop: Optional[str] = Field(None, description="OSM shop tag")
    cuisine: Optional[str] = Field(None, description="Cuisine type for restaurants")
    brand: Optional[str] = Field(None, description="Brand name")
    operator: Optional[str] = Field(None, description="Operator/chain name")
    
    # Location information
    latitude: float = Field(..., description="Latitude coordinate")
    longitude: float = Field(..., description="Longitude coordinate")
    address_housenumber: Optional[str] = Field(None, description="House number")
    address_street: Optional[str] = Field(None, description="Street name")
    address_city: Optional[str] = Field(None, description="City name")
    address_state: Optional[str] = Field(None, description="State")
    address_postcode: Optional[str] = Field(None, description="Postal code")
    address_country: Optional[str] = Field(None, description="Country")
    
    # Contact information
    phone: Optional[str] = Field(None, description="Phone number")
    website: Optional[str] = Field(None, description="Website URL")
    email: Optional[str] = Field(None, description="Email address")
    
    # Business details
    opening_hours: Optional[str] = Field(None, description="Opening hours")
    wheelchair: Optional[str] = Field(None, description="Wheelchair accessibility")
    takeaway: Optional[str] = Field(None, description="Takeaway available")
    delivery: Optional[str] = Field(None, description="Delivery available")
    outdoor_seating: Optional[str] = Field(None, description="Outdoor seating available")
    
    # Classification
    business_type: BusinessType = Field(..., description="Categorized business type")
    franchise_indicator: bool = Field(default=False, description="Likely franchise business")
    
    # Metadata
    data_source: DataSource = Field(default=DataSource.OPENSTREETMAP, description="Data source")
    data_collection_date: datetime = Field(default_factory=datetime.now, description="Data collection timestamp")
    data_quality_score: Optional[float] = Field(None, description="Data completeness score (0-100)")
    
    @validator('osm_timestamp', pre=True)
    def parse_osm_timestamp(cls, v):
        """Parse OSM timestamp string to datetime"""
        if isinstance(v, str):
            try:
                return datetime.fromisoformat(v.replace('Z', '+00:00'))
            except:
                return None
        return v
    
    @validator('business_type', pre=True)
    def classify_business_type(cls, v, values):
        """Classify business type from OSM tags if not provided"""
        if v and v != 'other':
            return v
        
        amenity = values.get('amenity', '').lower()
        shop = values.get('shop', '').lower()
        
        # Food & Beverage
        if amenity in ['restaurant', 'fast_food', 'cafe', 'bar', 'pub', 'food_court']:
            return BusinessType.RESTAURANT
        if shop in ['bakery', 'butcher', 'fishmonger', 'greengrocer', 'alcohol']:
            return BusinessType.RESTAURANT
        
        # Retail
        if shop in ['supermarket', 'convenience', 'department_store', 'mall', 'clothes', 
                   'shoes', 'jewelry', 'electronics', 'mobile_phone', 'computer']:
            return BusinessType.RETAIL
        
        # Personal Services
        if amenity in ['beauty_salon', 'hairdresser']:
            return BusinessType.PERSONAL_SERVICES
        if shop in ['beauty', 'hairdresser', 'optician']:
            return BusinessType.PERSONAL_SERVICES
        
        # Automotive
        if amenity in ['fuel', 'car_rental', 'car_repair', 'car_wash']:
            return BusinessType.AUTOMOTIVE
        if shop in ['car', 'motorcycle', 'car_parts']:
            return BusinessType.AUTOMOTIVE
        
        # Fitness
        if amenity in ['gym', 'fitness_centre', 'swimming_pool', 'spa']:
            return BusinessType.FITNESS
        
        # Healthcare
        if amenity in ['pharmacy', 'hospital', 'clinic', 'dentist', 'veterinary']:
            return BusinessType.HEALTHCARE
        
        # Professional Services
        if amenity in ['bank', 'post_office', 'library']:
            return BusinessType.PROFESSIONAL_SERVICES
        
        return BusinessType.OTHER
    
    def calculate_data_quality_score(self):
        """Calculate data completeness score"""
        score = 0.0
        
        # Required fields (50 points total)
        if self.name:
            score += 20
        if self.latitude and self.longitude:
            score += 20
        if self.amenity or self.shop:
            score += 10
        
        # Address information (30 points total)
        if self.address_street:
            score += 10
        if self.address_city:
            score += 10
        if self.address_postcode:
            score += 5
        if self.address_housenumber:
            score += 5
        
        # Contact information (15 points total)
        if self.phone:
            score += 8
        if self.website:
            score += 4
        if self.email:
            score += 3
        
        # Business details (5 points total)
        if self.opening_hours:
            score += 3
        if self.brand:
            score += 2
        
        self.data_quality_score = round(score, 1)
        return self.data_quality_score


class OSMDataSummary(BaseModel):
    """Summary of OSM data collection run"""
    
    collection_date: datetime = Field(default_factory=datetime.now)
    area_name: str = Field(..., description="Area name (e.g., 'Dane County', 'Wisconsin')")
    bbox: str = Field(..., description="Bounding box used for collection")
    
    # Collection counts
    total_elements: int = Field(default=0, description="Total OSM elements returned")
    businesses_collected: int = Field(default=0, description="Valid businesses collected")
    franchises_identified: int = Field(default=0, description="Franchise businesses identified")
    
    # Business type breakdown
    business_type_counts: Optional[Dict[str, int]] = Field(None, description="Count by business type")
    
    # Data quality metrics
    avg_data_quality_score: Optional[float] = Field(None, description="Average data quality score")
    businesses_with_contact: int = Field(default=0, description="Businesses with contact info")
    businesses_with_address: int = Field(default=0, description="Businesses with full address")
    
    # Success metrics
    success: bool = Field(default=False)
    processing_time_seconds: Optional[float] = Field(None)
    api_requests_made: int = Field(default=0)
    
    # Coverage metrics
    cities_covered: int = Field(default=0, description="Number of unique cities")
    coverage_area_sqkm: Optional[float] = Field(None, description="Approximate coverage area")


class CensusDataSummary(BaseModel):
    """Summary of Census data collection run"""
    
    collection_date: datetime = Field(default_factory=datetime.now)
    state_fips: str = Field(..., description="State FIPS code")
    acs_year: int = Field(..., description="ACS data year")
    
    # Collection counts by geographic level
    counties_collected: int = Field(default=0)
    tracts_collected: int = Field(default=0)
    block_groups_collected: int = Field(default=0)
    total_geographies: int = Field(default=0)
    
    # Success metrics
    success: bool = Field(default=False)
    api_errors: int = Field(default=0)
    processing_time_seconds: Optional[float] = Field(None)
    
    # Data quality metrics
    complete_records: int = Field(default=0)
    partial_records: int = Field(default=0)
    avg_data_quality_score: Optional[float] = Field(None)
    
    # API usage tracking
    api_requests_made: int = Field(default=0)
    api_key_used: bool = Field(default=False)
    
    def calculate_totals(self):
        """Calculate total geographies collected"""
        self.total_geographies = (
            self.counties_collected + 
            self.tracts_collected + 
            self.block_groups_collected
        )