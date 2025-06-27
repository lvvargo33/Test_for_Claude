-- Google Places API Data Schema for Wisconsin Business Intelligence
-- Phase 1: Milwaukee, Dane, and Brown Counties
-- Optimized for location analysis and competitive intelligence

CREATE TABLE IF NOT EXISTS `raw_business_data.google_places_businesses` (
  -- Primary identifiers
  place_id STRING NOT NULL,
  google_place_id STRING,  -- Backup for place_id changes
  name STRING,
  
  -- Business status and classification
  business_status STRING,  -- OPERATIONAL, CLOSED_TEMPORARILY, CLOSED_PERMANENTLY
  place_types ARRAY<STRING>,  -- Google's business type classifications
  primary_type STRING,  -- Derived main business category
  business_category STRING,  -- Our standardized business category
  industry_sector STRING,  -- High-level grouping (Retail, Food Service, Professional, etc.)
  
  -- Location data
  formatted_address STRING,
  street_number STRING,
  route STRING,
  locality STRING,  -- City
  administrative_area_level_2 STRING,  -- County
  administrative_area_level_1 STRING,  -- State
  postal_code STRING,
  country STRING,
  
  -- Precise coordinates
  geometry_location_lat FLOAT64,
  geometry_location_lng FLOAT64,
  geometry_viewport_northeast_lat FLOAT64,
  geometry_viewport_northeast_lng FLOAT64,
  geometry_viewport_southwest_lat FLOAT64,
  geometry_viewport_southwest_lng FLOAT64,
  
  -- Wisconsin-specific location fields
  county_name STRING,  -- Milwaukee, Dane, Brown
  metro_area STRING,  -- Milwaukee MSA, Madison MSA, Green Bay MSA
  city_name STRING,  -- Standardized city name
  zip_code STRING,  -- Cleaned postal code
  
  -- Business contact and web presence
  formatted_phone_number STRING,
  international_phone_number STRING,
  website STRING,
  url STRING,  -- Google Maps URL
  
  -- Customer feedback and ratings
  rating FLOAT64,  -- 1.0 to 5.0
  user_ratings_total INT64,  -- Number of reviews
  price_level INT64,  -- 0-4 scale (0=free, 4=very expensive)
  
  -- Hours and accessibility
  opening_hours_open_now BOOLEAN,
  opening_hours_periods ARRAY<STRUCT<
    close STRUCT<day INT64, time STRING>,
    open STRUCT<day INT64, time STRING>
  >>,
  opening_hours_weekday_text ARRAY<STRING>,
  
  -- Additional place details
  vicinity STRING,
  plus_code_compound_code STRING,
  plus_code_global_code STRING,
  
  -- Photo references (for future image analysis)
  photos ARRAY<STRUCT<
    photo_reference STRING,
    height INT64,
    width INT64,
    html_attributions ARRAY<STRING>
  >>,
  
  -- Competitive analysis fields
  competitor_density_0_5_mile INT64,  -- Count within 0.5 miles
  competitor_density_1_mile INT64,    -- Count within 1 mile
  competitor_density_3_mile INT64,    -- Count within 3 miles
  nearest_competitor_distance_miles FLOAT64,
  market_saturation_score FLOAT64,   -- Calculated saturation metric
  
  -- Location quality metrics
  anchor_tenant_proximity_miles FLOAT64,  -- Distance to nearest major retailer
  shopping_center_type STRING,  -- Strip Mall, Shopping Center, Standalone, Downtown
  visibility_score INT64,  -- 1-10 based on location characteristics
  traffic_generator_count_1mile INT64,  -- Schools, offices, hospitals nearby
  
  -- Economic and demographic context
  median_household_income_area FLOAT64,  -- From census integration
  population_density_per_sq_mile FLOAT64,
  avg_commute_time_minutes FLOAT64,
  
  -- Search and collection metadata
  search_query STRING,  -- Original search query used
  search_radius_meters INT64,  -- Search radius used
  search_center_lat FLOAT64,  -- Search center coordinates
  search_center_lng FLOAT64,
  collection_method STRING,  -- 'nearby_search', 'text_search', 'place_details'
  api_response_status STRING,  -- 'OK', 'ZERO_RESULTS', etc.
  
  -- Data quality and freshness
  data_confidence_score FLOAT64,  -- 0-100 confidence in data accuracy
  last_verified TIMESTAMP,  -- When business was last confirmed active
  verification_method STRING,  -- How verification was done
  data_source_version STRING,  -- API version used
  
  -- Timestamps
  collection_date TIMESTAMP NOT NULL,
  last_updated TIMESTAMP,
  created_date TIMESTAMP DEFAULT CURRENT_TIMESTAMP(),
  
  -- Raw API response (for debugging and future enhancement)
  raw_api_response JSON
)
PARTITION BY DATE(collection_date)
CLUSTER BY county_name, business_category, city_name, place_types;

-- Create indexes for common query patterns
-- BigQuery doesn't use traditional indexes, but clustering handles this

-- Secondary table for business hours detail (normalized)
CREATE TABLE IF NOT EXISTS `raw_business_data.google_places_hours` (
  place_id STRING NOT NULL,
  day_of_week INT64,  -- 0=Sunday, 1=Monday, etc.
  day_name STRING,
  open_time STRING,   -- Format: "0800"
  close_time STRING,  -- Format: "2200"
  is_open_24hrs BOOLEAN,
  is_closed_all_day BOOLEAN,
  collection_date TIMESTAMP NOT NULL
)
PARTITION BY DATE(collection_date)
CLUSTER BY place_id, day_of_week;

-- Business type mapping table (for standardization)
CREATE TABLE IF NOT EXISTS `raw_business_data.business_type_mapping` (
  google_type STRING,
  standard_category STRING,
  industry_sector STRING,
  location_dependent BOOLEAN,  -- True for businesses where location matters
  franchise_potential BOOLEAN,  -- True for business types often franchised
  small_business_suitable BOOLEAN,  -- True for small business opportunities
  seasonal_variation STRING,  -- 'High', 'Medium', 'Low'
  description STRING,
  created_date TIMESTAMP DEFAULT CURRENT_TIMESTAMP()
);

-- Pre-populate business type mappings
INSERT INTO `raw_business_data.business_type_mapping` 
(google_type, standard_category, industry_sector, location_dependent, franchise_potential, small_business_suitable, seasonal_variation)
VALUES
-- Food & Dining
('restaurant', 'Restaurant', 'Food Service', TRUE, TRUE, TRUE, 'Medium'),
('meal_takeaway', 'Takeout/Delivery', 'Food Service', TRUE, TRUE, TRUE, 'Low'),
('meal_delivery', 'Delivery', 'Food Service', TRUE, TRUE, TRUE, 'Low'),
('bakery', 'Bakery', 'Food Service', TRUE, TRUE, TRUE, 'Medium'),
('cafe', 'Cafe/Coffee Shop', 'Food Service', TRUE, TRUE, TRUE, 'Low'),
('bar', 'Bar/Tavern', 'Food Service', TRUE, FALSE, TRUE, 'Medium'),
('food', 'Food Service', 'Food Service', TRUE, TRUE, TRUE, 'Medium'),

-- Retail
('clothing_store', 'Clothing Store', 'Retail', TRUE, TRUE, TRUE, 'High'),
('shoe_store', 'Shoe Store', 'Retail', TRUE, TRUE, TRUE, 'High'),
('jewelry_store', 'Jewelry Store', 'Retail', TRUE, TRUE, TRUE, 'High'),
('electronics_store', 'Electronics Store', 'Retail', TRUE, TRUE, TRUE, 'Medium'),
('furniture_store', 'Furniture Store', 'Retail', TRUE, TRUE, TRUE, 'Low'),
('home_goods_store', 'Home Goods', 'Retail', TRUE, TRUE, TRUE, 'Medium'),
('grocery_or_supermarket', 'Grocery Store', 'Retail', TRUE, TRUE, FALSE, 'Low'),
('convenience_store', 'Convenience Store', 'Retail', TRUE, TRUE, TRUE, 'Low'),
('gas_station', 'Gas Station', 'Retail', TRUE, TRUE, TRUE, 'Low'),

-- Personal Services
('hair_care', 'Hair Salon', 'Personal Services', TRUE, TRUE, TRUE, 'Low'),
('beauty_salon', 'Beauty Salon', 'Personal Services', TRUE, TRUE, TRUE, 'Low'),
('spa', 'Spa/Wellness', 'Personal Services', TRUE, TRUE, TRUE, 'Medium'),
('gym', 'Fitness Center', 'Personal Services', TRUE, TRUE, TRUE, 'High'),
('car_repair', 'Auto Repair', 'Personal Services', TRUE, TRUE, TRUE, 'Low'),
('car_wash', 'Car Wash', 'Personal Services', TRUE, TRUE, TRUE, 'Medium'),
('laundry', 'Laundry/Dry Cleaning', 'Personal Services', TRUE, TRUE, TRUE, 'Low'),

-- Professional Services
('lawyer', 'Legal Services', 'Professional Services', FALSE, FALSE, TRUE, 'Low'),
('accounting', 'Accounting', 'Professional Services', FALSE, TRUE, TRUE, 'High'),
('real_estate_agency', 'Real Estate', 'Professional Services', FALSE, TRUE, TRUE, 'High'),
('insurance_agency', 'Insurance', 'Professional Services', FALSE, TRUE, TRUE, 'Low'),

-- Healthcare
('hospital', 'Hospital', 'Healthcare', TRUE, FALSE, FALSE, 'Low'),
('doctor', 'Medical Practice', 'Healthcare', TRUE, FALSE, TRUE, 'Low'),
('dentist', 'Dental Practice', 'Healthcare', TRUE, FALSE, TRUE, 'Low'),
('pharmacy', 'Pharmacy', 'Healthcare', TRUE, TRUE, TRUE, 'Low'),
('veterinary_care', 'Veterinary', 'Healthcare', TRUE, FALSE, TRUE, 'Low'),

-- Entertainment & Recreation
('movie_theater', 'Movie Theater', 'Entertainment', TRUE, TRUE, FALSE, 'Medium'),
('bowling_alley', 'Bowling Alley', 'Entertainment', TRUE, TRUE, TRUE, 'Medium'),
('amusement_park', 'Amusement Park', 'Entertainment', TRUE, FALSE, FALSE, 'High'),

-- Automotive
('car_dealer', 'Auto Dealer', 'Automotive', TRUE, TRUE, FALSE, 'Low'),
('car_rental', 'Car Rental', 'Automotive', TRUE, TRUE, TRUE, 'Medium'),

-- Financial
('bank', 'Bank', 'Financial Services', TRUE, FALSE, FALSE, 'Low'),
('atm', 'ATM', 'Financial Services', TRUE, FALSE, FALSE, 'Low');

-- Create views for common analysis patterns

-- View: Location-dependent businesses only
CREATE OR REPLACE VIEW `analytics.location_dependent_businesses` AS
SELECT 
  pb.*,
  btm.location_dependent,
  btm.franchise_potential,
  btm.small_business_suitable
FROM `raw_business_data.google_places_businesses` pb
JOIN `raw_business_data.business_type_mapping` btm
ON pb.primary_type = btm.google_type
WHERE btm.location_dependent = TRUE;

-- View: Competitive analysis by county
CREATE OR REPLACE VIEW `analytics.competitive_density_by_county` AS
SELECT 
  county_name,
  business_category,
  COUNT(*) as business_count,
  AVG(rating) as avg_rating,
  AVG(user_ratings_total) as avg_review_count,
  AVG(competitor_density_1_mile) as avg_competitive_density,
  COUNT(DISTINCT city_name) as cities_with_presence
FROM `raw_business_data.google_places_businesses`
WHERE business_status = 'OPERATIONAL'
GROUP BY county_name, business_category;

-- View: Market opportunities (low competition areas)
CREATE OR REPLACE VIEW `analytics.market_opportunities` AS
SELECT 
  county_name,
  city_name,
  business_category,
  COUNT(*) as current_business_count,
  AVG(population_density_per_sq_mile) as population_density,
  AVG(median_household_income_area) as avg_income,
  AVG(competitor_density_1_mile) as avg_competition,
  CASE 
    WHEN COUNT(*) < 3 AND AVG(population_density_per_sq_mile) > 1000 THEN 'High Opportunity'
    WHEN COUNT(*) < 5 AND AVG(population_density_per_sq_mile) > 500 THEN 'Moderate Opportunity'
    ELSE 'Saturated'
  END as opportunity_level
FROM `raw_business_data.google_places_businesses`
WHERE business_status = 'OPERATIONAL'
GROUP BY county_name, city_name, business_category
HAVING COUNT(*) > 0;

-- Business density analysis view
CREATE OR REPLACE VIEW `analytics.business_density_analysis` AS
SELECT 
  county_name,
  business_category,
  COUNT(*) as total_businesses,
  COUNT(*) / (SELECT COUNT(DISTINCT city_name) FROM `raw_business_data.google_places_businesses` p2 WHERE p2.county_name = p1.county_name) as businesses_per_city,
  AVG(rating) as avg_rating,
  STDDEV(rating) as rating_variance,
  COUNT(CASE WHEN rating >= 4.0 THEN 1 END) / COUNT(*) as high_rated_percentage
FROM `raw_business_data.google_places_businesses` p1
WHERE business_status = 'OPERATIONAL'
  AND rating IS NOT NULL
GROUP BY county_name, business_category;