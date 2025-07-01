# Infrastructure Data Sources - Automation Feasibility Assessment

## Executive Summary

This document provides a comprehensive assessment of automation feasibility for 8 major infrastructure data categories: FCC Broadband, Parking, Cell Coverage, Transit, Airports, Electric Grid, Water, and Natural Gas. Each category is evaluated on automation potential, API quality, rate limits, data formats, authentication requirements, geographic querying capabilities, and implementation challenges.

---

## 1. FCC BROADBAND DATA

### Automation Score: 7/10

**API Endpoints:**
- FCC Broadband Data Collection (BDC) API: `https://broadbandmap.fcc.gov/nbm`
- Area and Census Block API: `https://geo.fcc.gov/api/census/`
- National Broadband Map API: `https://broadbandmap.fcc.gov/api/`

**Rate Limits:**
- BDCAPI supports up to 1,000 challenge IDs per request
- Built-in pauses between requests to avoid rate limiting
- Reasonable use policy for general APIs

**Data Format:** JSON, GeoJSON, CSV
**Authentication:** API key required for some endpoints, login required for preliminary data

**Sample API Call (Wisconsin):**
```bash
curl "https://geo.fcc.gov/api/census/area?lat=44.5&lon=-89.5&censusYear=2020&format=json"
```

**Geographic Querying:** ✅ Supports coordinate-based queries, census block lookups

**Update Frequency:** 
- BDC data: Bi-annual updates
- Coverage maps: Updated as carriers submit new data

**Challenges:**
- Some API endpoints have reported reliability issues
- Login required for detailed/preliminary data
- Limited historical data access

---

## 2. PARKING DATA

### Automation Score: 6/10

**API Endpoints:**
- SpotHero Developer API: `https://spothero.com/developers`
- ParkWhiz integration APIs (limited public access)
- Municipal parking APIs (varies by city)

**Rate Limits:**
- SpotHero: Commercial API rates (contact required)
- Municipal systems: Varies widely (typically 100-1000 requests/hour)

**Data Format:** JSON, REST APIs
**Authentication:** API keys, OAuth for commercial services

**Sample API Call (Milwaukee):**
```bash
curl -H "Authorization: Bearer {token}" \
"https://api.spothero.com/v1/search?lat=43.0389&lng=-87.9065&start_time=2024-01-01T10:00:00Z"
```

**Geographic Querying:** ✅ Location-based search with radius parameters

**Ad Hoc Retrieval:** Excellent for on-demand parking availability queries

**Update Frequency:** 
- Real-time availability updates
- Pricing updates: Dynamic (minutes)
- New locations: Weekly to monthly

**Challenges:**
- Commercial APIs require business agreements
- Municipal APIs highly fragmented
- Limited standardization across providers
- Wisconsin-specific coverage may be sparse outside major cities

---

## 3. CELL COVERAGE DATA

### Automation Score: 8/10

**API Endpoints:**
- OpenCellID API: `https://opencellid.org/api`
- Unwired Labs Location API: `https://unwiredlabs.com/api`
- CellMapper (community data, limited API)

**Rate Limits:**
- OpenCellID: 1000 requests/day (free tier), higher with registration
- Unwired Labs: 10,000 requests/month (free), commercial plans available

**Data Format:** JSON, CSV, XML
**Authentication:** API key required

**Sample API Call (Wisconsin):**
```bash
curl "https://opencellid.org/ajax/searchCell.php?mcc=310&mnc=410&lac=1234&cellid=5678&format=json&key=YOUR_API_KEY"
```

**Geographic Querying:** ✅ Coordinate-based queries, cell tower radius searches

**Ad Hoc Retrieval:** Excellent - real-time cell tower and coverage queries

**Update Frequency:**
- Community-driven updates: Continuous
- Commercial data: Weekly updates
- Coverage maps: Monthly updates

**Challenges:**
- Free tier limitations
- Data quality varies (community-sourced)
- Carrier-specific APIs not publicly available
- Coverage estimates may not reflect actual service quality

---

## 4. TRANSIT DATA (GTFS)

### Automation Score: 9/10

**API Endpoints:**
- GTFS Static feeds: Direct download URLs
- GTFS Realtime: `https://transitfeeds.com/api`
- OpenTripPlanner APIs: Various municipal deployments
- OneBusAway REST API: `http://api.pugetsound.onebusaway.org/api/`

**Rate Limits:**
- GTFS Static: No limits (file downloads)
- GTFS Realtime: Varies by agency (typically 1 request/minute minimum)
- Public APIs: 100-1000 requests/hour typical

**Data Format:** GTFS (Google format), JSON, Protocol Buffers
**Authentication:** Generally none for static data, API keys for real-time

**Sample API Call (Wisconsin Transit):**
```bash
# Download Wisconsin transit agency GTFS
curl "https://transitfeeds.com/p/madison-metro-transit/97/latest/download"

# Real-time updates (if available)
curl "https://api.example-transit.org/gtfs-realtime/trip-updates"
```

**Geographic Querying:** ✅ Route-based and stop-based queries

**Ad Hoc Retrieval:** Excellent for route planning and real-time updates

**Update Frequency:**
- Static GTFS: Weekly to monthly updates
- Realtime: 30-60 second intervals
- Service alerts: Immediate

**Wisconsin Transit Agencies:**
- Madison Metro Transit
- Milwaukee County Transit System
- Various smaller municipal systems

**Challenges:**
- Wisconsin has limited GTFS Realtime implementations
- Rural transit agencies may not provide digital feeds
- Data quality varies significantly between agencies

---

## 5. AIRPORT DATA (FAA)

### Automation Score: 8/10

**API Endpoints:**
- FAA ASPM System: `https://aspm.faa.gov/`
- Airport Status Web Service (ASWS): `https://soa.smext.faa.gov/asws/api/airport/status/`
- Aviation Weather API: `https://aviationweather.gov/api/data/`
- Airport Data Portal: `https://adip.faa.gov/`

**Rate Limits:**
- ASWS: No explicit limits documented
- Aviation Weather: 100 requests/minute maximum
- ASPM: Login required, reasonable use

**Data Format:** JSON, XML, Excel (ASPM downloads)
**Authentication:** Login required for detailed ASPM data, API keys for some services

**Sample API Calls (Wisconsin Airports):**
```bash
# General airport status
curl "https://soa.smext.faa.gov/asws/api/airport/status/MKE"

# Weather data for Milwaukee Mitchell
curl "https://aviationweather.gov/api/data/metar?ids=KMKE&format=json"

# Madison airport weather
curl "https://aviationweather.gov/api/data/metar?ids=KMSN&format=json"
```

**Wisconsin Major Airports:**
- MKE: Milwaukee Mitchell International
- MSN: Madison Dane County Regional
- GRB: Green Bay Austin Straubel International

**Geographic Querying:** ✅ Airport code-based queries, coordinate searches available

**Ad Hoc Retrieval:** Excellent for real-time status and weather

**Update Frequency:**
- Weather: Every 30 minutes (METAR)
- Flight delays: Real-time
- ASPM data: Hourly updates

**Challenges:**
- Login required for detailed operational data
- Historical data requires special access
- Some APIs have usage restrictions

---

## 6. ELECTRIC GRID / POWER OUTAGES

### Automation Score: 7/10

**API Endpoints:**
- PowerOutage.us API: `https://poweroutage.us/products` (Commercial)
- Individual utility APIs:
  - Wisconsin Public Service: Web scraping required
  - We Energies: Limited API access
- UtilityAPI: `https://utilityapi.com/` (Customer consent required)

**Rate Limits:**
- PowerOutage.us: Commercial rates, 10-minute update intervals
- Utility APIs: Varies significantly, typically 60 requests/hour

**Data Format:** JSON, GeoJSON, CSV
**Authentication:** API keys, utility customer consent for detailed data

**Sample API Call (Wisconsin):**
```bash
# PowerOutage.us (commercial API)
curl -H "Authorization: Bearer {token}" \
"https://api.poweroutage.us/outages?state=WI&format=json"

# WPS outage data (requires web scraping)
curl "https://www.wisconsinpublicservice.com/outagesummary/view/outagegrid"
```

**Geographic Querying:** ✅ State/county level, utility service area boundaries

**Ad Hoc Retrieval:** Good for current outages, limited for historical data

**Update Frequency:**
- Real-time outage maps: 10-15 minute intervals
- Historical data: Daily aggregation
- Utility infrastructure: Annual updates

**Wisconsin Coverage:**
- 94% of Wisconsin electric customers tracked by PowerOutage.us
- Major utilities: WPS, We Energies, WECA cooperatives

**Challenges:**
- Most utility APIs require customer consent
- Historical data requires commercial agreements
- Standardization varies between utilities
- Real-time infrastructure data is sensitive/restricted

---

## 7. WATER INFRASTRUCTURE

### Automation Score: 5/10

**API Endpoints:**
- Wisconsin DNR Surface Water Data Viewer: `https://dnr.wisconsin.gov/topic/SurfaceWater/swdv`
- Drinking Water System Portal: `https://apps.dnr.wi.gov/dwsportalpub`
- USGS Water Services: `https://waterservices.usgs.gov/`
- EPA SDWIS: State-level database access

**Rate Limits:**
- USGS: No explicit limits, reasonable use
- DNR portals: Web interface only, limited API access
- EPA data: Bulk download available

**Data Format:** JSON (USGS), HTML (DNR portals), CSV downloads
**Authentication:** Generally none for public data

**Sample API Calls (Wisconsin):**
```bash
# USGS water data for Wisconsin
curl "https://waterservices.usgs.gov/nwis/iv/?format=json&stateCd=wi&parameterCd=00060&siteStatus=active"

# Wisconsin water quality sites
curl "https://waterservices.usgs.gov/nwis/site/?format=json&stateCd=wi&siteType=ST&hasDataTypeCd=qw"
```

**Geographic Querying:** ✅ County/watershed based, coordinate searches

**Ad Hoc Retrieval:** Limited - mostly batch downloads or web interfaces

**Update Frequency:**
- Water quality monitoring: Weekly to monthly
- Infrastructure assessments: Annual
- Asset inventories: Updated as required by utilities

**Wisconsin Resources:**
- DNR maintains public water system database
- Asset management plans required for utilities
- GIS mapping requirements for water systems

**Challenges:**
- Most utility infrastructure data not publicly accessible via APIs
- DNR portals primarily web-based, limited programmatic access
- Asset management data held at utility level
- Standardization issues across municipal systems

---

## 8. NATURAL GAS INFRASTRUCTURE

### Automation Score: 4/10

**API Endpoints:**
- National Pipeline Mapping System (NPMS): `https://pvnpms.phmsa.dot.gov/`
- EIA Natural Gas Data: `https://api.eia.gov/`
- TC Energy pipeline data (limited public access)

**Rate Limits:**
- EIA API: 5000 requests/hour with registration
- NPMS: Web interface only, no direct API

**Data Format:** JSON (EIA), HTML/PDF (NPMS), Excel downloads
**Authentication:** API key for EIA data

**Sample API Calls (Wisconsin):**
```bash
# EIA natural gas consumption data for Wisconsin
curl "https://api.eia.gov/v2/natural-gas/cons/sum/data/?api_key=YOUR_KEY&frequency=annual&data[0]=value&facets[stateId][]=WI"

# Pipeline capacity data
curl "https://api.eia.gov/v2/natural-gas/pipelines/capacity/data/?api_key=YOUR_KEY&facets[stateId][]=WI"
```

**Geographic Querying:** Limited - state/county level primarily

**Ad Hoc Retrieval:** Poor - mostly requires manual data extraction

**Update Frequency:**
- EIA data: Monthly to annual updates
- Pipeline projects: Updated during construction/approval phases
- Safety data: Annual reporting cycles

**Wisconsin Infrastructure:**
- TC Energy Wisconsin Reliability Project
- Multiple local distribution companies
- Interstate pipeline network crossing state

**Challenges:**
- NPMS requires manual map searches, no API
- Pipeline location data restricted for security reasons
- Real-time flow data not publicly available
- Safety and operational data heavily regulated
- Most detailed infrastructure data requires special access

---

## IMPLEMENTATION RECOMMENDATIONS

### High Priority (Automation Score 7+)
1. **Cell Coverage (8/10)** - Implement OpenCellID integration for coverage analysis
2. **Transit (9/10)** - Develop GTFS feed collectors for Wisconsin agencies
3. **Airports (8/10)** - Integrate FAA APIs for airport status and weather
4. **Electric Grid (7/10)** - Partner with PowerOutage.us for commercial API access
5. **FCC Broadband (7/10)** - Build BDC API collector with retry logic

### Medium Priority (Automation Score 5-6)
1. **Parking (6/10)** - Focus on Milwaukee/Madison municipal APIs first
2. **Water Infrastructure (5/10)** - Combine USGS APIs with periodic DNR scraping

### Low Priority (Automation Score <5)
1. **Natural Gas (4/10)** - Manual collection recommended, limited API value

### Development Strategy

**Phase 1: High-Automation Sources**
- Build robust API collectors with error handling
- Implement rate limiting and retry logic
- Create standardized data models for each source

**Phase 2: Hybrid Automation**
- Combine APIs with controlled web scraping
- Develop municipal partnership programs
- Create manual override capabilities

**Phase 3: Enhanced Integration**
- Cross-reference infrastructure data for validation
- Build composite infrastructure scoring
- Develop real-time alerting for critical changes

### Technical Architecture

```python
# Recommended infrastructure collector framework
class InfrastructureCollector:
    def __init__(self, source_type, api_config):
        self.source_type = source_type
        self.rate_limiter = RateLimiter(api_config.rate_limit)
        self.retry_handler = RetryHandler(max_attempts=3)
        
    def collect_for_location(self, lat, lon, radius_miles=10):
        # Standardized collection interface
        pass
        
    def get_update_frequency(self):
        # Return optimal polling frequency
        pass
```

This assessment provides a roadmap for implementing automated infrastructure data collection, prioritizing sources with the highest automation potential while providing fallback strategies for manual collection where APIs are limited.