# DataForge 🔧

> A powerful, config-driven ETL (Extract, Transform, Load) pipeline framework that enables data processing without writing code. Transform, clean, validate, and export data using simple YAML configurations.

[![Python](https://img.shields.io/badge/Python-3.8+-blue?logo=python)](https://python.org)
[![License](https://img.shields.io/badge/License-MIT-green)](LICENSE)
[![GitHub](https://img.shields.io/badge/GitHub-Ishanhirani11-black?logo=github)](https://github.com/Ishanhirani11/DataForge)

---

## 🌟 Features

- **Zero-Code Pipelines** — Define entire ETL workflows using YAML configuration files
- **Multi-Format Support** — Work with CSV, Excel, JSON, and Parquet files
- **Data Cleaning** — Handle missing values, remove duplicates, normalize data
- **Advanced Filtering** — Filter rows using simple or complex conditions (AND/OR logic)
- **Data Validation** — Enforce data quality rules with pattern matching and range checks
- **Data Enrichment** — Label encode categories, enrich datasets with derived fields
- **Multiple Loaders** — Export to file systems, databases (PostgreSQL), S3, Redis, or cache
- **CLI Interface** — Easy command-line tools for running pipelines and validating data
- **Extensible Architecture** — Plugin-based extractors, transformers, and loaders

---

## 🚀 Quick Start

### Installation

```bash
# Clone the repository
git clone https://github.com/Ishanhirani11/DataForge.git
cd DataForge

# Install in development mode
pip install -e .

# Or install from requirements
pip install -r requirements.txt
```

### Run Your First Pipeline

```bash
# Run the sample pipeline
dataforge run --config config/sample_pipeline.yaml

# Output will be saved to the output/ directory
```

---

## 📁 Project Structure

```
DataForge/
├── config/                          # Pipeline configuration files
│   └── sample_pipeline.yaml         # ← Example configuration
├── src/dataforge/
│   ├── cli.py                       # Command-line interface
│   ├── pipeline.py                  # Core ETL engine
│   ├── extractors/                  # Data readers
│   │   ├── file_extractor.py       # CSV, Excel, JSON, Parquet
│   │   ├── database_extractor.py   # PostgreSQL, MySQL
│   │   └── api_extractor.py        # REST API sources
│   ├── transformers/                # Data processors
│   │   ├── data_cleaner.py         # Clean & normalize
│   │   ├── data_validator.py       # Quality checks
│   │   └── data_enricher.py        # Enrich with new fields
│   ├── loaders/                     # Data writers
│   │   ├── database_loader.py      # PostgreSQL, MySQL
│   │   ├── s3_loader.py            # AWS S3
│   │   └── cache_loader.py         # Redis cache
│   ├── data_quality/                # Quality assurance tools
│   └── utils/                       # Helpers & utilities
│       ├── logger.py               # Logging
│       ├── metrics.py              # Performance metrics
│       └── retry_handler.py        # Retry logic
├── data/                            # Input/output data
│   └── output/                      # Generated outputs
├── logs/                            # Application logs
├── tests/                           # Unit tests
├── .env.example                     # Environment template
├── requirements.txt                 # Python dependencies
├── setup.py                         # Package setup
└── LICENSE                          # MIT License
```

---

## 📋 Configuration Guide

Create a YAML file in the `config/` directory with the following structure:

```yaml
pipeline:
  name: my_data_pipeline
  description: Process customer data

source:
  type: file                          # file | database | api
  path: data/customers.csv
  format: csv                         # csv | excel | json | parquet
  sheet_name: Sheet1                  # (Excel only)

transform:
  clean:
    missing_value_strategy: fill_default  # drop | fill_mean | fill_median
    missing_value_default: "Unknown"
    remove_duplicates: true
    numeric_columns:
      - age
      - salary

  filter:                             # Keep rows matching conditions
    or:                               # OR logic (match any condition)
      - column: country
        operator: equals              # equals | contains | regex | gt | lt
        value: India
      - column: status
        operator: equals
        value: Active

  validate:                           # Data quality rules
    required:                         # Required columns
      - id
      - email
      - name
    patterns:                         # Regex patterns
      email: "^[\\w\\.-]+@[\\w\\.-]+\\.\\w+$"
      phone: "^\\d{10}$"
    ranges:                           # Numeric ranges
      age:
        min: 18
        max: 100
      salary:
        min: 0
        max: 1000000

  enrich:                             # Add/transform columns
    label_encode:                     # Convert categories to numbers
      - country
      - department

target:
  type: file                          # file | database | s3
  path: output/processed_customers.csv
  format: csv                         # csv | json
```

---

## 🔧 CLI Commands

```bash
# Run a pipeline
dataforge run --config config/sample_pipeline.yaml

# Validate a data file
dataforge validate --input data/customers.csv --format csv

# Check data quality
dataforge quality --input data/customers.csv

# Show pipeline status
dataforge status --config config/sample_pipeline.yaml

# Generate sample data
dataforge sample --output data/sample.csv --rows 100
```

---

## 📊 Supported Formats

| Format | Extract | Load | Notes |
|--------|---------|------|-------|
| **CSV** | ✅ | ✅ | Fast, universal format |
| **Excel (.xlsx)** | ✅ | ❌ | Multi-sheet support |
| **JSON** | ✅ | ✅ | Nested structures supported |
| **Parquet** | ✅ | ❌ | Columnar, compressed |
| **PostgreSQL** | ✅ | ✅ | Connection pooling |
| **MySQL** | ✅ | ✅ | Full compatibility |
| **S3** | ❌ | ✅ | AWS integration |
| **Redis** | ❌ | ✅ | Caching & sessions |

---

## 🔑 Environment Configuration

```bash
# Copy the template
cp .env.example .env

# Edit with your credentials
nano .env
```

**Environment Variables:**

```env
# Database
DATABASE_URL=postgresql://user:password@localhost:5432/dataforge
MYSQL_URL=mysql://user:password@localhost:3306/dataforge

# Cache & Session
REDIS_URL=redis://localhost:6379/0

# AWS (S3 loader only)
AWS_ACCESS_KEY_ID=your_access_key
AWS_SECRET_ACCESS_KEY=your_secret_key
AWS_REGION=us-east-1

# Application
ENVIRONMENT=development          # development | production
LOG_LEVEL=INFO                   # DEBUG | INFO | WARNING | ERROR
```

---

## 📝 Example Workflows

### 1. Clean Customer Data

```yaml
pipeline:
  name: clean_customers

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
  path: output/clean_customers.csv
  format: csv
```

### 2. Filter & Validate Sales Data

```yaml
pipeline:
  name: validate_sales

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
    required: [order_id, customer_id, amount]
    ranges:
      amount:
        min: 0
        max: 999999

target:
  type: file
  path: output/valid_sales.csv
  format: csv
```

---

## 🛠️ Development

### Setup Development Environment

```bash
# Create virtual environment
python -m venv venv
source venv/bin/activate

# Install dependencies
pip install -r requirements.txt

# Run tests
python -m pytest tests/

# Check code quality
pylint src/dataforge/
```

### Running Tests

```bash
# Run all tests
pytest

# Run specific test file
pytest tests/test_pipeline.py

# Run with coverage
pytest --cov=src/dataforge tests/
```

---

## 📚 Documentation

- [Configuration Reference](docs/CONFIG.md) — Detailed YAML configuration options
- [API Reference](docs/API.md) — Python API documentation
- [Troubleshooting](docs/TROUBLESHOOTING.md) — Common issues and solutions
- [Contributing Guide](CONTRIBUTING.md) — How to contribute

---

## 🤝 Contributing

Contributions are welcome! Please:

1. Fork the repository
2. Create a feature branch (`git checkout -b feature/AmazingFeature`)
3. Commit changes (`git commit -m 'Add AmazingFeature'`)
4. Push to branch (`git push origin feature/AmazingFeature`)
5. Open a Pull Request

---

## 📄 License

This project is licensed under the **MIT License** — see the [LICENSE](LICENSE) file for details.

---

## 👤 Author

**Ishan Hirani**
- GitHub: [@Ishanhirani11](https://github.com/Ishanhirani11)
- Repository: [DataForge](https://github.com/Ishanhirani11/DataForge)

---

## 🙏 Acknowledgments

- Built with Python 3.8+
- Inspired by Apache Airflow and dbt
- Community feedback and contributions

---

## 📮 Support

Found a bug? Have a suggestion? 
- Open an [Issue](https://github.com/Ishanhirani11/DataForge/issues)
- Start a [Discussion](https://github.com/Ishanhirani11/DataForge/discussions)

---

**Happy Data Processing! 🎉**
