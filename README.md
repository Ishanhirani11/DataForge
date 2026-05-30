# DataForge 🔧

<div align="center">

A powerful, **config-driven ETL (Extract, Transform, Load)** pipeline framework that enables data processing without writing code. Transform, clean, validate, and export data using simple YAML configurations.

[![Python](https://img.shields.io/badge/Python-3.8+-blue?logo=python&logoColor=white)](https://python.org)
[![License](https://img.shields.io/badge/License-MIT-green?logo=open-source-initiative)](LICENSE)
[![GitHub](https://img.shields.io/badge/GitHub-Ishanhirani11-black?logo=github)](https://github.com/Ishanhirani11/DataForge)
[![Status](https://img.shields.io/badge/Status-Active-brightgreen)](https://github.com/Ishanhirani11/DataForge)

[Features](#-features) • [Quick Start](#-quick-start) • [Examples](#-example-workflows) • [Documentation](#-documentation) • [Contributing](#-contributing)

</div>

---

## Table of Contents

- [Features](#-features)
- [Why DataForge?](#-why-dataforge)
- [Quick Start](#-quick-start)
- [Project Structure](#-project-structure)
- [Configuration Guide](#-configuration-guide)
- [CLI Commands](#-cli-commands)
- [Supported Formats](#-supported-formats)
- [Example Workflows](#-example-workflows)
- [Development](#-development)
- [Troubleshooting](#-troubleshooting)
- [Best Practices](#-best-practices)
- [Contributing](#-contributing)
- [License](#-license)

---

## 🌟 Features

✨ **Core Capabilities:**
- **Zero-Code Pipelines** — Define entire ETL workflows using YAML configuration files
- **Multi-Format Support** — Work with CSV, Excel, JSON, and Parquet files seamlessly
- **Data Cleaning** — Handle missing values, remove duplicates, normalize data automatically
- **Advanced Filtering** — Filter rows using simple or complex conditions (AND/OR logic)
- **Data Validation** — Enforce data quality rules with pattern matching and range checks
- **Data Enrichment** — Label encode categories, enrich datasets with derived fields
- **Multiple Loaders** — Export to file systems, databases (PostgreSQL), S3, Redis, or cache
- **CLI Interface** — Easy command-line tools for running pipelines and validating data
- **Extensible Architecture** — Plugin-based extractors, transformers, and loaders
- **Error Handling** — Built-in retry logic and comprehensive error reporting
- **Performance Metrics** — Track pipeline execution time, rows processed, and quality metrics
- **Logging** — Detailed logs for debugging and auditing data transformations

---

## ❓ Why DataForge?

| Problem | Solution |
|---------|----------|
| **Need to write code for simple ETL tasks** | Just write YAML, no Python required |
| **Manual data cleaning is time-consuming** | Automated cleaning in seconds |
| **Hard to maintain data pipelines** | Version-controlled YAML configs |
| **Data quality issues go undetected** | Built-in validation rules |
| **Integration with multiple data sources** | Support for files, databases, APIs |
| **Scaling data operations** | Pluggable architecture for custom needs |

---

## 🚀 Quick Start

### Prerequisites

- Python 3.8 or higher
- pip or conda

### Installation

```bash
# Clone the repository
git clone https://github.com/Ishanhirani11/DataForge.git
cd DataForge

# Create virtual environment (recommended)
python -m venv venv
source venv/bin/activate  # On Windows: venv\Scripts\activate

# Install in development mode
pip install -e .

# Or install from requirements
pip install -r requirements.txt
```

### Run Your First Pipeline

```bash
# 1. Run the sample pipeline
dataforge run --config config/sample_pipeline.yaml

# 2. Check the output
ls -la data/output/

# 3. View the logs
tail -f dataforge.log
```

---

## 📁 Project Structure

```
DataForge/
├── config/
│   └── sample_pipeline.yaml         # ← Start with this example
├── data/
│   ├── .gitkeep                     # Placeholder for data directory
│   └── output/                      # Generated pipeline outputs
├── src/dataforge/
│   ├── __init__.py
│   ├── cli.py                       # Command-line interface
│   ├── pipeline.py                  # Core ETL engine
│   ├── extractors/                  # Data readers
│   │   ├── __init__.py
│   │   ├── file_extractor.py       # CSV, Excel, JSON, Parquet
│   │   ├── database_extractor.py   # PostgreSQL, MySQL
│   │   └── api_extractor.py        # REST API sources
│   ├── transformers/                # Data processors
│   │   ├── __init__.py
│   │   ├── data_cleaner.py         # Clean & normalize data
│   │   ├── data_validator.py       # Validate data quality
│   │   └── data_enricher.py        # Enrich with new fields
│   ├── loaders/                     # Data writers
│   │   ├── __init__.py
│   │   ├── database_loader.py      # PostgreSQL, MySQL
│   │   ├── s3_loader.py            # AWS S3 integration
│   │   └── cache_loader.py         # Redis cache
│   ├── data_quality/                # Quality assurance tools
│   │   └── __init__.py
│   ├── config/
│   │   └── __init__.py
│   └── utils/                       # Helper utilities
│       ├── __init__.py
│       ├── logger.py               # Structured logging
│       ├── metrics.py              # Performance metrics
│       └── retry_handler.py        # Retry logic with backoff
├── .env.example                     # Environment variables template
├── .gitignore                       # Git ignore rules
├── LICENSE                          # MIT License
├── README.md                        # This file
├── requirements.txt                 # Python dependencies
└── setup.py                         # Package configuration
```

---

## 📋 Configuration Guide

### Basic Structure

Create a YAML file in the `config/` directory:

```yaml
pipeline:
  name: my_pipeline
  description: Process customer data
  timeout: 3600  # seconds

source:
  type: file                          # file | database | api
  path: data/customers.csv
  format: csv                         # csv | excel | json | parquet
  sheet_name: Sheet1                  # Excel only

transform:
  clean:
    missing_value_strategy: fill_default  # drop | fill_mean | fill_median
    missing_value_default: "Unknown"
    remove_duplicates: true
    numeric_columns:
      - age
      - salary

  filter:
    or:                               # Match ANY condition
      - column: country
        operator: equals
        value: India
      - column: status
        operator: equals
        value: Active

  validate:
    required: [id, email, name]       # Required columns
    patterns:
      email: "^[\\w\\.-]+@[\\w\\.-]+\\.\\w+$"
      phone: "^\\d{10}$"
    ranges:
      age:
        min: 18
        max: 100
      salary:
        min: 0
        max: 1000000

  enrich:
    label_encode:
      - country
      - department

target:
  type: file                          # file | database | s3
  path: data/output/processed.csv
  format: csv                         # csv | json
```

### Configuration Options

**Source Types:**
- `file` — Local CSV, Excel, JSON, Parquet files
- `database` — PostgreSQL, MySQL, SQLite connections
- `api` — REST API endpoints with pagination support

**Transform Operators:**
- `equals` — Exact match comparison
- `contains` — Substring match
- `regex` — Regular expression matching
- `gt` / `gte` — Greater than / Greater or equal
- `lt` / `lte` — Less than / Less or equal

**Target Types:**
- `file` — Local filesystem (CSV, JSON)
- `database` — PostgreSQL, MySQL, SQLite
- `s3` — AWS S3 bucket
- `redis` — Redis cache

---

## 🔧 CLI Commands

```bash
# Run a pipeline
dataforge run --config config/sample_pipeline.yaml
dataforge run --config config/my_pipeline.yaml --verbose

# Validate data without transforming
dataforge validate --input data/customers.csv --format csv

# Check data quality metrics
dataforge quality --input data/customers.csv --format csv

# Show pipeline execution status
dataforge status --config config/sample_pipeline.yaml

# Generate sample data for testing
dataforge sample --output data/sample.csv --rows 1000 --format csv

# List available pipelines
dataforge list
```

---

## 📊 Supported Formats

| Format | Extract | Load | Encoding | Notes |
|--------|---------|------|----------|-------|
| **CSV** | ✅ | ✅ | UTF-8, ASCII | Fast, universal format |
| **Excel (.xlsx)** | ✅ | ❌ | — | Multi-sheet support |
| **JSON** | ✅ | ✅ | UTF-8 | Nested structures supported |
| **Parquet** | ✅ | ❌ | — | Columnar, compressed, fast |
| **PostgreSQL** | ✅ | ✅ | — | Connection pooling, SSL |
| **MySQL** | ✅ | ✅ | — | Full compatibility |
| **SQLite** | ✅ | ✅ | — | Local database |
| **S3** | ❌ | ✅ | — | AWS integration, partitioning |
| **Redis** | ❌ | ✅ | — | In-memory caching |

---

## 🔑 Environment Configuration

```bash
# Copy the template
cp .env.example .env

# Edit with your credentials
nano .env
```

**Required Environment Variables:**

```env
# Database Connections
DATABASE_URL=postgresql://user:password@localhost:5432/dataforge
MYSQL_URL=mysql://user:password@localhost:3306/dataforge
SQLITE_PATH=./data/database.db

# Cache & Session Storage
REDIS_URL=redis://localhost:6379/0

# AWS Credentials (S3 loader only)
AWS_ACCESS_KEY_ID=your_access_key
AWS_SECRET_ACCESS_KEY=your_secret_key
AWS_REGION=us-east-1

# Application Settings
ENVIRONMENT=development          # development | production
LOG_LEVEL=INFO                   # DEBUG | INFO | WARNING | ERROR
LOG_FILE=dataforge.log
```

---

## 📝 Example Workflows

### Example 1: Clean Customer Data

**Scenario:** You have raw customer data with missing values and duplicates.

```yaml
pipeline:
  name: clean_customers
  description: Clean raw customer dataset

source:
  type: file
  path: data/raw_customers.csv
  format: csv

transform:
  clean:
    missing_value_strategy: fill_default
    missing_value_default: "N/A"
    remove_duplicates: true
    numeric_columns: [age, income]

target:
  type: file
  path: data/output/clean_customers.csv
  format: csv
```

**Run it:**
```bash
dataforge run --config config/clean_customers.yaml
```

### Example 2: Filter & Validate Sales Data

**Scenario:** Process sales data, filter by region and validate amounts.

```yaml
pipeline:
  name: validate_sales
  description: Filter high-value sales and validate

source:
  type: file
  path: data/sales.csv
  format: csv

transform:
  filter:
    or:
      - column: amount
        operator: gt
        value: 1000
      - column: region
        operator: equals
        value: "APAC"
  
  validate:
    required: [order_id, customer_id, amount, date]
    patterns:
      order_id: "^ORD[0-9]{6}$"
    ranges:
      amount:
        min: 0
        max: 999999

target:
  type: file
  path: data/output/valid_sales.csv
  format: csv
```

### Example 3: Multi-stage Pipeline with Enrichment

```yaml
pipeline:
  name: enrich_user_data
  description: Enrich user data with encoding

source:
  type: file
  path: data/users.csv
  format: csv

transform:
  clean:
    missing_value_strategy: fill_mean
    numeric_columns: [age, tenure]
  
  filter:
    or:
      - column: country
        operator: contains
        value: "India"
      - column: status
        operator: equals
        value: "active"
  
  validate:
    required: [user_id, email, country]
    patterns:
      email: "^[\\w\\.-]+@[\\w\\.-]+\\.[a-z]{2,}$"
    ranges:
      age:
        min: 18
        max: 100
  
  enrich:
    label_encode:
      - country
      - region
      - subscription_type

target:
  type: file
  path: data/output/enriched_users.csv
  format: csv
```

---

## 🛠️ Development

### Setup Development Environment

```bash
# Clone and navigate
git clone https://github.com/Ishanhirani11/DataForge.git
cd DataForge

# Create virtual environment
python -m venv venv
source venv/bin/activate  # On Windows: venv\Scripts\activate

# Install in development mode with dev dependencies
pip install -e ".[dev]"
pip install -r requirements.txt

# Install pre-commit hooks (optional)
pre-commit install
```

### Running Tests

```bash
# Run all tests
pytest

# Run with verbose output
pytest -v

# Run specific test file
pytest tests/test_pipeline.py

# Generate coverage report
pytest --cov=src/dataforge --cov-report=html tests/
```

### Code Quality

```bash
# Format code
black src/ tests/

# Lint code
pylint src/dataforge/
flake8 src/

# Type checking
mypy src/dataforge/
```

---

## 🔍 Troubleshooting

### Common Issues

**1. Module not found error**
```bash
# Solution: Reinstall in editable mode
pip install -e .
```

**2. Permission denied when running CLI**
```bash
# Solution: Add execute permission
chmod +x dataforge
```

**3. Database connection failed**
```bash
# Solution: Check DATABASE_URL in .env
# Format: postgresql://user:password@host:port/database
env | grep DATABASE_URL
```

**4. Data validation failed with no clear error**
```bash
# Solution: Run with verbose logging
dataforge run --config config/my_pipeline.yaml --verbose
```

**5. Memory error on large files**
```bash
# Solution: Process in chunks (update your pipeline config)
# Use batch processing for files > 1GB
```

**6. Encoding issues with special characters**
```bash
# Solution: Specify encoding in config
source:
  type: file
  encoding: utf-8  # or iso-8859-1, cp1252
```

### Getting Help

1. **Check logs:** `tail -f dataforge.log`
2. **Enable debug mode:** Set `LOG_LEVEL=DEBUG` in `.env`
3. **Validate config:** `dataforge validate --config config/my_pipeline.yaml`
4. **Open an issue:** [GitHub Issues](https://github.com/Ishanhirani11/DataForge/issues)

---

## 💡 Best Practices

### 1. Configuration Management
```yaml
# ✅ Good: Parameterized paths
source:
  path: ${INPUT_DIR}/data.csv

# ❌ Bad: Hardcoded paths
source:
  path: /home/user/Documents/data.csv
```

### 2. Data Validation
```yaml
# ✅ Always validate required fields
validate:
  required: [id, email, name]  # Catch bad data early
```

### 3. Error Handling
```yaml
# ✅ Use try-catch in enrichment
enrich:
  label_encode: [country]
  on_error: skip  # or: fail, warn
```

### 4. Pipeline Organization
```
config/
├── prod/
│   ├── customers.yaml
│   └── orders.yaml
├── staging/
│   └── customers_test.yaml
└── dev/
    └── local_test.yaml
```

### 5. Logging & Monitoring
```yaml
# ✅ Enable detailed logging
pipeline:
  log_level: DEBUG
  log_file: logs/my_pipeline.log
```

---

## 🤝 Contributing

We welcome contributions! Here's how:

1. **Fork** the repository
2. **Create** a feature branch (`git checkout -b feature/AmazingFeature`)
3. **Commit** changes (`git commit -m 'Add AmazingFeature'`)
4. **Push** to branch (`git push origin feature/AmazingFeature`)
5. **Open** a Pull Request

### Development Guidelines
- Follow PEP 8 style guide
- Add tests for new features
- Update documentation
- Keep commits atomic and descriptive

---

## 📚 Additional Resources

- **[Configuration Reference](docs/CONFIG.md)** — Detailed YAML options
- **[API Reference](docs/API.md)** — Python API documentation
- **[Troubleshooting Guide](docs/TROUBLESHOOTING.md)** — Common issues
- **[Contributing Guide](CONTRIBUTING.md)** — How to contribute

---

## 📄 License

This project is licensed under the **MIT License** — see the [LICENSE](LICENSE) file for details.

```
MIT License

Copyright (c) 2026 Ishan Hirani

Permission is hereby granted, free of charge, to any person obtaining a copy
of this software and associated documentation files...
```

---

## 👤 Author & Maintainer

**Ishan Hirani**
- GitHub: [@Ishanhirani11](https://github.com/Ishanhirani11)
- Repository: [DataForge](https://github.com/Ishanhirani11/DataForge)
- Email: [your-email@example.com](mailto:your-email@example.com)

---

## 🙏 Acknowledgments

- Built with ❤️ using Python 3.8+
- Inspired by [Apache Airflow](https://airflow.apache.org/) and [dbt](https://www.getdbt.com/)
- Thanks to all [contributors](CONTRIBUTING.md)

---

## 📮 Support & Feedback

Have questions or feedback?

- 🐛 **Bug Reports** — [Open Issue](https://github.com/Ishanhirani11/DataForge/issues)
- 💡 **Feature Requests** — [Discussions](https://github.com/Ishanhirani11/DataForge/discussions)
- ❓ **Questions** — [Q&A Discussion](https://github.com/Ishanhirani11/DataForge/discussions/categories/q-a)
- 📧 **Email** — [your-email@example.com](mailto:your-email@example.com)

---

<div align="center">

**Made with ❤️ by [Ishan Hirani](https://github.com/Ishanhirani11)**

If this project helped you, please consider giving it a ⭐ on GitHub!

[Back to Top](#dataforge-)

</div>
