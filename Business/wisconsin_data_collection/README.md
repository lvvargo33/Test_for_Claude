# Location Optimizer - Wisconsin Data Collection System

A robust, scalable data collection system for identifying franchise opportunities and business leads in Wisconsin.

## 🎯 Overview

This system collects and analyzes business registration data, SBA loan approvals, and business licenses to identify high-value prospects for location optimization consulting services.

## 🏗️ Architecture

### New Improved Architecture (v2.0)

```
Data Sources → Collectors → Validation → BigQuery → Analytics → Reports
     ↓             ↓           ↓          ↓          ↓         ↓
   Config      Base Class   Pydantic    Optimized   Views    Prospects
   Registry    Framework    Models      Schema             
```

**Key Improvements:**
- ✅ **Scalable Architecture**: Abstract base classes for easy multi-state expansion
- ✅ **Data Validation**: Pydantic models with comprehensive validation
- ✅ **Error Handling**: Retry logic and graceful fallbacks
- ✅ **Optimized BigQuery**: Partitioned tables with clustering
- ✅ **Configuration Management**: YAML-based data source registry
- ✅ **Real Data Sources**: Framework for actual API integration

## 📁 Project Structure

```
wisconsin_data_collection/
├── README.md                 # This file
├── requirements.txt          # Python dependencies
├── data_sources.yaml         # Data source configuration
├── models.py                 # Pydantic data models
├── base_collector.py         # Abstract base collector class
├── wisconsin_collector.py    # Wisconsin-specific implementation
├── setup_bigquery.py         # BigQuery infrastructure setup
├── main.py                   # Main execution script
└── logs/                     # Log files
```

## 🚀 Quick Start

### 1. Install Dependencies

```bash
pip install -r requirements.txt
```

### 2. Setup BigQuery Infrastructure

```bash
python main.py --setup
```

### 3. Collect Wisconsin Data

```bash
python main.py --collect
```

### 4. Run Analysis

```bash
python main.py --analyze
```

### 5. Export Prospects

```bash
python main.py --export-prospects
```

### Complete Workflow

```bash
# Run everything in one command
python main.py --setup --collect --analyze --export-prospects
```

## 📊 Data Sources

### Wisconsin Data Sources

| Source | Type | Update Frequency | Status |
|--------|------|------------------|--------|
| **Wisconsin DFI** | Business Registrations | Daily | Framework Ready* |
| **SBA FOIA Data** | Loan Approvals | Quarterly | Framework Ready* |
| **Milwaukee Open Data** | Business Licenses | Weekly | Framework Ready* |
| **Madison Open Data** | Business Licenses | Weekly | Framework Ready* |
| **Green Bay** | Business Licenses | Manual | Framework Ready* |

*Framework includes sample data generation for development/testing

### Target Business Types

**High Priority:**
- Restaurants & Food Service
- Retail Stores
- Fitness Centers
- Personal Services
- Franchises

**NAICS Codes:** 722, 445, 448, 812, 541, 531

## 🎛️ Configuration

Edit `data_sources.yaml` to customize:
- Data source endpoints
- Update frequencies
- API credentials
- Business type targeting
- Quality scoring rules

```yaml
states:
  wisconsin:
    business_registrations:
      primary:
        url: "https://www.wcc.state.wi.us/search"
        type: "web_scraping"
        update_frequency: "daily"
```

## 📈 Usage Examples

### Basic Data Collection

```python
from wisconsin_collector import WisconsinDataCollector

# Initialize collector
collector = WisconsinDataCollector()

# Collect data
summary = collector.run_full_collection(days_back=30)
print(f"Collected {summary.total_records} records")
```

### Custom Analysis

```python
from google.cloud import bigquery

client = bigquery.Client(project="location-optimizer-1")

query = """
SELECT business_name, city, business_type, confidence_score
FROM `location-optimizer-1.raw_business_data.business_entities`
WHERE state = 'WI' 
  AND registration_date >= DATE_SUB(CURRENT_DATE(), INTERVAL 30 DAY)
  AND business_type = 'restaurant'
ORDER BY confidence_score DESC
"""

results = client.query(query).to_dataframe()
```

## 🔧 Command Line Interface

```bash
# Setup infrastructure
python main.py --setup

# Collect recent data (last 30 days)
python main.py --collect --days-back 30

# Run opportunity analysis
python main.py --analyze

# Export prospect list
python main.py --export-prospects --output-file my_prospects.csv

# Daily automated collection
python main.py --daily
```

## 📊 BigQuery Schema

### Business Entities Table
```sql
business_entities (
  business_id STRING,
  business_name STRING,
  business_type STRING,
  city STRING,
  state STRING,
  registration_date DATE,
  confidence_score FLOAT,
  data_extraction_date TIMESTAMP
)
PARTITION BY DATE(data_extraction_date)
CLUSTER BY state, business_type, city
```

### SBA Loans Table
```sql
sba_loan_approvals (
  loan_id STRING,
  borrower_name STRING,
  loan_amount NUMERIC,
  approval_date DATE,
  borrower_city STRING,
  borrower_state STRING,
  franchise_name STRING
)
PARTITION BY approval_date
CLUSTER BY borrower_state, program_type, franchise_name
```

## 🎯 Lead Scoring

### Confidence Score Calculation
- **Complete Record**: 100 points
- **Contact Information**: +20 points
- **Complete Address**: +15 points
- **NAICS Code**: +10 points
- **Business Description**: +5 points

### Lead Quality Grades
- **HIGH**: SBA loans >$200K, Complete franchise records
- **MEDIUM**: SBA loans >$100K, High-confidence new businesses
- **QUALIFIED**: Recent registrations in target industries
- **STANDARD**: Other qualifying businesses

## 🔄 Multi-State Expansion

The architecture is designed for easy expansion:

```python
# Add Illinois collector
class IllinoisDataCollector(BaseDataCollector):
    def collect_business_registrations(self, days_back):
        # Illinois-specific implementation
        pass
```

Update `data_sources.yaml`:
```yaml
states:
  illinois:
    business_registrations:
      primary:
        url: "https://www.ilsos.gov/corporatellc/"
```

## 📈 Performance & Costs

### Expected Performance
- **Data Freshness**: < 24 hours
- **Processing Time**: 2-4 hours per state
- **Success Rate**: > 99%
- **Records per Day**: 50-200 (Wisconsin)

### Estimated Monthly Costs
- **BigQuery Storage**: $50-200
- **BigQuery Queries**: $30-100
- **Cloud Functions**: $20-80
- **Total**: $100-380/month

## 🔍 Troubleshooting

### Common Issues

**1. BigQuery Authentication**
```bash
gcloud auth application-default login
export GOOGLE_APPLICATION_CREDENTIALS="path/to/credentials.json"
```

**2. Missing Dependencies**
```bash
pip install pyarrow google-cloud-bigquery
```

**3. Data Source Errors**
- Check `data_sources.yaml` configuration
- Verify API endpoints are accessible
- Review rate limiting settings

### Logs
Check logs in:
- `location_optimizer.log` - Main application logs
- `wisconsin_data_ingestion.log` - Legacy logs

## 📞 Support & Development

### Adding New Data Sources
1. Update `data_sources.yaml`
2. Implement collection method in state collector
3. Add data parsing logic
4. Test with sample data

### Custom Business Logic
1. Extend `BaseDataCollector` class
2. Override validation methods
3. Customize scoring algorithms
4. Add new BigQuery views

## 🚀 Production Deployment

### Automated Daily Collection
```bash
# Add to crontab
0 6 * * * /path/to/python /path/to/main.py --daily
```

### Google Cloud Scheduler
```yaml
name: wisconsin-daily-collection
schedule: "0 6 * * *"
target:
  httpTarget:
    uri: https://your-cloud-function-url
```

### Monitoring & Alerts
- Set up BigQuery query monitoring
- Configure Stackdriver alerts
- Monitor data freshness metrics

## 📋 Next Steps

1. **Immediate**: Test with real Wisconsin data sources
2. **Short-term**: Add Illinois as second state
3. **Medium-term**: Implement ML-based opportunity scoring
4. **Long-term**: Build client dashboard and automated reporting

---

## 🎉 Success Metrics

**Technical KPIs:**
- Data collection success rate > 99%
- Data freshness < 24 hours
- Query performance < 5 seconds

**Business KPIs:**
- Lead quality accuracy > 80%
- Client conversion rate > 60%
- Average project value growth
- Geographic expansion rate

The system is designed to scale from Wisconsin to 10+ states while maintaining high data quality and performance standards.