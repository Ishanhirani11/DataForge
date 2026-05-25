# DataForge 🔧

> Config-driven ETL pipeline framework — clean, filter, validate and export data using only a YAML file.

[![Python](https://img.shields.io/badge/Python-3.11+-blue?logo=python)](https://python.org)
[![License](https://img.shields.io/badge/License-MIT-green)](LICENSE)

---

## How It Works

```
Input File  →  Clean  →  Filter  →  Validate  →  Output File
(CSV/Excel/JSON)                                  (CSV/JSON)
```

No code changes needed. Just create a YAML config and run one command.

---

## Quick Start

```bash
# 1. Clone and install
git clone https://github.com/ipatel09/data-forge.git
cd data-forge
pip install -e .

# 2. Run the sample pipeline
dataforge run --config config/sample_pipeline.yaml
```

---

## Project Structure

```
data-forge/
├── config/                          # Your pipeline YAML files
│   ├── sample_pipeline.yaml         # ← Start here
│   ├── cars_pipeline.yaml
│   └── mundra_mahuva_dehydrated_pipeline.yaml
├── data/
│   ├── sample_users.csv             # Sample input data
│   └── output/                      # Generated outputs (gitignored)
├── src/dataforge/
│   ├── pipeline.py                  # Core pipeline engine
│   ├── cli.py                       # CLI entry point
│   ├── extractors/                  # CSV, Excel, JSON, Parquet readers
│   ├── transformers/                # Cleaner, Validator, Enricher
│   └── loaders/                     # PostgreSQL, S3, Redis loaders
├── requirements.txt
└── .env.example
```

---

## Pipeline YAML Reference

```yaml
pipeline:
  name: my_pipeline

source:
  type: file
  path: data/sample_users.csv      # supports csv, excel, json, parquet
  format: csv
  sheet_name: Sheet1               # only for excel

transform:
  clean:
    missing_value_strategy: fill_default   # or: drop, fill_mean, fill_median
    missing_value_default: unknown
    remove_duplicates: true
    numeric_columns:
      - age
      - salary

  filter:                          # keep rows matching ANY condition (OR logic)
    or:
      - column: country
        operator: contains         # contains | equals | regex
        value: India
      - column: description
        operator: contains
        value: dehydrated onion

  validate:                        # only valid rows go to output
    required:
      - id
      - name
      - email
    patterns:
      email: "^[^@\\s]+@[^@\\s]+\\.[^@\\s]+$"
    ranges:
      age:
        min: 18
        max: 80

  enrich:
    label_encode:                  # convert text categories to numbers
      - country

target:
  type: file
  path: data/output/result.csv
  format: csv                      # csv or json
```

---

## Sample Data → Sample Output

**Input** (`data/sample_users.csv`):

| id | name | email | age | country | salary |
|----|------|-------|-----|---------|--------|
| 1 | Alice Johnson | alice@example.com | 29 | India | 55000 |
| 2 | Bob Smith | bob@example.com | 34 | USA | 72000 |
| 3 | Carol White | *(missing)* | 27 | UK | 48000 |
| 4 | David Lee | david@example.com | 41 | Canada | 91000 |
| 5 | Eva Brown | eva@example.com | 22 | Australia | 38000 |
| 6 | Frank Das | frank@example.com | *(missing)* | India | 61000 |
| 7 | Grace Kim | grace@example.com | 31 | USA | 83000 |
| 8 | Henry Ford | henry@example.com | 55 | UK | 105000 |
| 9 | Iris Patel | iris@example.com | 26 | India | 44000 |
| 10 | James Roy | james@example.com | 38 | Canada | 77000 |

**Pipeline applied** (`config/sample_pipeline.yaml`):
- Clean: fill missing values, remove duplicates
- Filter: keep only India or USA rows
- Validate: require id, name, email; valid email pattern

**Output** (`data/output/clean_sample_users.csv`):

| id | name | email | age | country | salary |
|----|------|-------|-----|---------|--------|
| 1 | Alice Johnson | alice@example.com | 29.0 | India | 55000.0 |
| 2 | Bob Smith | bob@example.com | 34.0 | USA | 72000.0 |
| 6 | Frank Das | frank@example.com | unknown | India | 61000.0 |
| 7 | Grace Kim | grace@example.com | 31.0 | USA | 83000.0 |
| 9 | Iris Patel | iris@example.com | 26.0 | India | 44000.0 |

5 rows out of 10 — UK, Canada, Australia rows filtered out; Carol White dropped (missing email fails validation).

---

## CLI Commands

```bash
dataforge run      --config config/my_pipeline.yaml   # Run a pipeline
dataforge validate --input data/myfile.csv            # Validate a file
dataforge quality  --expectations suite.json          # Check data quality
```

---

## Supported Formats

| Format | Read | Write |
|--------|------|-------|
| CSV | ✅ | ✅ |
| Excel (.xlsx) | ✅ | — |
| JSON | ✅ | ✅ |
| Parquet | ✅ | — |

---

## Environment Setup

```bash
cp .env.example .env
# Edit .env with your database/AWS credentials if using DB or S3 loaders
```

| Variable | Description |
|----------|-------------|
| `DATABASE_URL` | PostgreSQL connection string |
| `REDIS_URL` | Redis connection string |
| `ENVIRONMENT` | `development` / `production` |
| `AWS_ACCESS_KEY_ID` | For S3 loader only |
| `AWS_SECRET_ACCESS_KEY` | For S3 loader only |

---

## License

MIT
