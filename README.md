# DataFlow Pro - Professional Data Engineering Framework

<p align="center">
  <a href="https://pypi.org/project/dataflow-pro/">
    <img src="https://img.shields.io/pypi/v/dataflow-pro.svg" alt="PyPI Version">
  </a>
  <a href="https://pypi.org/project/dataflow-pro/">
    <img src="https://img.shields.io/pypi/pyversions/dataflow-pro.svg" alt="Python Versions">
  </a>
  <a href="https://github.com/dataflow-pro/dataflow-pro/actions">
    <img src="https://github.com/dataflow-pro/dataflow-pro/workflows/CI/badge.svg" alt="Build Status">
  </a>
  <a href="https://codecov.io/gh/dataflow-pro/dataflow-pro">
    <img src="https://img.shields.io/codecov/c/github/dataflow-pro/dataflow-pro.svg" alt="Coverage">
  </a>
  <a href="https://pypi.org/project/dataflow-pro/">
    <img src="https://img.shields.io/pypi/l/dataflow-pro.svg" alt="License">
  </a>
</p>

> **DataFlow Pro** is a comprehensive, production-ready data engineering framework for building scalable ETL (Extract, Transform, Load) pipelines. Built with modern best practices, it supports multiple data sources, transformations, and destinations with enterprise-grade features including monitoring, data quality checks, and orchestration.

## ✨ Features

- **Multi-Source Data Extraction**
  - REST APIs with pagination and rate limiting
  - SQL databases (PostgreSQL, MySQL, SQLite)
  - File-based sources (CSV, JSON, Parquet, Excel, XML)
- **Robust Data Transformation**
  - Data cleaning (missing values, outliers, duplicates)
  - Data validation (schema, types, patterns)
  - Data enrichment (derived columns, lookups, encoding)
- **Multi-Target Data Loading**
  - Database batch inserts and upserts
  - S3 with multiple formats
  - Redis caching
- **Data Quality**
  - Great Expectations integration
  - Configurable validation rules
  - Quality metrics and reporting
- **Orchestration**
  - Apache Airflow DAGs
  - Incremental loading with watermarks
  - CDC patterns
- **Observability**
  - Structured logging
  - Prometheus metrics
  - Retry with exponential backoff

## 📦 Installation

```bash
pip install dataflow-pro
```

### With Development Dependencies

```bash
pip install dataflow-pro[dev]
```

### With All Dependencies

```bash
pip install dataflow-pro[all]
```

## 🚀 Quick Start

### Basic ETL Pipeline

```python
from dataflow_pro.extractors import APIExtractor
from dataflow_pro.transformers import DataCleaner, DataValidator
from dataflow_pro.loaders import DatabaseLoader

# Extract
api_extractor = APIExtractor(base_url="https://api.example.com")
data = api_extractor.extract(endpoint="users")

# Transform
cleaner = DataCleaner()
cleaned_data = cleaner.clean(data)

validator = DataValidator()
validator.required("email")
result = validator.validate(cleaned_data)

# Load
loader = DatabaseLoader(table_name="users")
loader.load(result.data)
```

### Using the CLI

```bash
# Run a pipeline
dataflow-pro run --config pipeline.yaml

# Validate data
dataflow-pro validate --input data.csv

# Check data quality
dataflow-pro quality --expectations suite.yaml
```

## 📁 Project Structure

```
dataflow-pro/
├── src/
│   └── dataflow_pro/
│       ├── __init__.py
│       ├── config.py
│       ├── cli.py
│       ├── extractors/
│       │   ├── __init__.py
│       │   ├── api_extractor.py
│       │   ├── database_extractor.py
│       │   └── file_extractor.py
│       ├── transformers/
│       │   ├── __init__.py
│       │   ├── data_cleaner.py
│       │   ├── data_validator.py
│       │   └── data_enricher.py
│       ├── loaders/
│       │   ├── __init__.py
│       │   ├── database_loader.py
│       │   ├── s3_loader.py
│       │   └── cache_loader.py
│       ├── data_quality/
│       │   └── __init__.py
│       └── utils/
│           ├── __init__.py
│           ├── logger.py
│           ├── metrics.py
│           ├── retry_handler.py
│           └── config.py
├── tests/
│   ├── __init__.py
│   ├── conftest.py
│   ├── unit/
│   │   ├── test_extractors.py
│   │   ├── test_transformers.py
│   │   ├── test_loaders.py
│   │   └── test_utils.py
│   └── integration/
├── dags/
│   ├── etl_pipeline.py
│   └── incremental_loader.py
├── monitoring/
│   ├── prometheus.yml
│   └── dashboards/
├── Dockerfile
├── docker-compose.yml
├── pyproject.toml
├── requirements.txt
├── setup.py
├── Makefile
└── README.md
```

## 🔧 Configuration

### Environment Variables

```bash
# Database
DATABASE_URL=postgresql://user:pass@localhost:5432/db

# Redis
REDIS_HOST=localhost
REDIS_PORT=6379

# AWS S3
AWS_ACCESS_KEY_ID=your_key
AWS_SECRET_ACCESS_KEY=your_secret
S3_BUCKET=your-bucket
AWS_REGION=us-east-1
```

### YAML Configuration

```yaml
# config.yaml
pipeline:
  name: etl_pipeline
  batch_size: 10000
  
source:
  type: api
  url: https://api.example.com
  rate_limit: 100
  pagination: cursor
  
transform:
  - type: clean
    missing_values: fill_default
    remove_duplicates: true
    
  - type: validate
    required:
      - email
    types:
      age: integer
      
target:
  type: database
  table: users
  strategy: upsert
```

## 🔨 API Reference

### Extractors

```python
# API Extractor
from dataflow_pro.extractors import APIExtractor

extractor = APIExtractor(
    base_url="https://api.example.com",
    rate_limit_calls=100,
    rate_limit_period=60,
)
result = extractor.extract(endpoint="users")
```

```python
# Database Extractor
from dataflow_pro.extractors import DatabaseExtractor

extractor = DatabaseExtractor(database_url="postgresql://...")
result = extractor.extract("SELECT * FROM users")

# Incremental extraction
result = extractor.extract_incremental(
    table_name="users",
    columns=["id", "name", "email"],
    incremental_column="updated_at",
    last_value="2024-01-01",
)
```

```python
# File Extractor
from dataflow_pro.extractors import FileExtractor

extractor = FileExtractor()
result = extractor.extract("data.csv")
```

### Transformers

```python
# Data Cleaner
from dataflow_pro.transformers import DataCleaner

cleaner = DataCleaner()
result = cleaner.clean(data)
```

```python
# Data Validator
from dataflow_pro.transformers import DataValidator

validator = DataValidator()
validator.required("email")
validator.check_type("age", int)
validator.check_range("age", min_value=0, max_value=120)
validator.check_pattern("email", r"^[\w.-]+@[\w.-]+\.\w+$")

result = validator.validate(data)
```

```python
# Data Enricher
from dataflow_pro.transformers import DataEnricher

enricher = DataEnricher()
enricher.add_derived_column("full_name", func=lambda r: f"{r['first']} {r['last']}")
enricher.add_label_encoding("status")
enricher.add_binning("age", [18, 30, 60], ["minor", "adult", "senior"])

result = enricher.enrich(data)
```

### Loaders

```python
# Database Loader
from dataflow_pro.loaders import DatabaseLoader

loader = DatabaseLoader(table_name="users")
result = loader.load(data, strategy="upsert")
```

```python
# S3 Loader
from dataflow_pro.loaders import S3Loader

loader = S3Loader(bucket_name="my-bucket")
result = loader.load_batched(data, "data/output", format="parquet")
```

```python
# Cache Loader
from dataflow_pro.loaders import CacheLoader

loader = CacheLoader()
loader.set_hash("user:1", {"name": "John"})
result = loader.load(data, key_prefix="users")
```

## 🐳 Docker

### Build Image

```bash
docker build -t dataflow-pro:latest .
```

### Run with Docker Compose

```bash
# Start all services
docker-compose up -d

# View logs
docker-compose logs -f

# Stop services
docker-compose down
```

### Services

| Service | Port | Description |
|--------|------|-------------|
| PostgreSQL | 5432 | Database |
| Redis | 6379 | Cache |
| LocalStack | 4566 | S3 (local) |
| Airflow | 8080 | Web UI |
| Flower | 5555 | Monitoring |
| Prometheus | 9090 | Metrics |
| Grafana | 3000 | Dashboards |

## 🧪 Testing

```bash
# Run all tests
pytest

# Run unit tests
pytest tests/unit/

# Run with coverage
pytest --cov=dataflow_pro --cov-report=html

# Docker-based tests
docker-compose run --rm test
```

## 📝 Logging

```python
from dataflow_pro.utils import get_logger

logger = get_logger(__name__)
logger.info("Processing data", extra={"pipeline": "etl", "rows": 1000})
```

## 📊 Metrics

```python
from dataflow_pro.utils import get_metrics

metrics = get_metrics()

# Counters
metrics.increment_counter("rows_processed", 1000)

# Gauges
metrics.set_gauge("queue_size", 50)

# Timings
metrics.record_timing("query_duration", 0.5)
```

## 🔄 Retry Handler

```python
from dataflow_pro.utils import RetryHandler, retry_on_exception

@retry_on_exception(max_attempts=3, backoff="exponential")
def fetch_data():
    return api.get()
```

## 🎯 Data Quality

```python
from dataflow_pro.data_quality import create_quality_suite

suite = create_quality_suite()
result = suite.validate_data(data)
```

## 🤝 Contributing

1. Fork the repository
2. Create a feature branch (`git checkout -b feature/amazing-feature`)
3. Commit your changes (`git commit -m 'Add amazing feature'`)
4. Push to the branch (`git push origin feature/amazing-feature`)
5. Open a Pull Request

### Development Setup

```bash
# Clone the repository
git clone https://github.com/dataflow-pro/dataflow-pro.git
cd dataflow-pro

# Install dependencies
pip install -e ".[dev]"

# Run tests
pytest

# Install pre-commit hooks
pre-commit install
```

## 📄 License

This project is licensed under the MIT License - see the [LICENSE](LICENSE) file for details.

## 🙏 Acknowledgments

- [Apache Airflow](https://airflow.apache.org/) - Orchestration
- [Great Expectations](https://greatexpectations.io/) - Data quality
- [SQLAlchemy](https://www.sqlalchemy.org/) - Database ORM
- [Pandas](https://pandas.pydata.org/) - Data analysis

---

<p align="center">Built with ❤️ by Data Engineers, for Data Engineers</p>