# DataForge 🔧

> Enterprise-grade ETL/ELT pipeline framework for production data workflows.

[![Python](https://img.shields.io/badge/Python-3.11+-blue?logo=python)](https://python.org)
[![Docker](https://img.shields.io/badge/Docker-ready-2496ED?logo=docker)](https://docker.com)
[![PostgreSQL](https://img.shields.io/badge/PostgreSQL-15-336791?logo=postgresql)](https://postgresql.org)
[![Redis](https://img.shields.io/badge/Redis-7-DC382D?logo=redis)](https://redis.io)

---

## What is DataForge?

DataForge is a production-ready Python framework that automates the full **Extract → Transform → Load** pipeline. It handles messy real-world data — cleaning it, validating it, and loading it into your database, S3, or cache — reliably and at scale.

```
Raw Data  →  Extract  →  Clean  →  Validate  →  Enrich  →  Load  →  Clean Data
(CSV/DB/API)                                                    (DB/S3/Redis)
```

---

## Features

| Feature | Description |
|---|---|
| **Multi-source extraction** | CSV, JSON, Parquet, Excel, XML, PostgreSQL, MySQL, REST APIs |
| **Data cleaning** | Missing values, duplicates, outliers, type coercion |
| **Validation** | Required fields, type checks, regex patterns, range checks |
| **Enrichment** | Label encoding, normalization, binning, derived columns |
| **Multi-target loading** | PostgreSQL (upsert), AWS S3 (partitioned), Redis cache |
| **Data quality** | Custom expectation suites (JSON-based rules) |
| **Retry logic** | Exponential backoff with jitter for transient failures |
| **Metrics** | Prometheus-compatible metrics with in-memory fallback |
| **Config-driven** | Define pipelines in YAML — no code changes needed |

---

## Project Structure

```
dataforge/
├── src/dataforge/
│   ├── extractors/        # CSV, JSON, Parquet, DB, API extractors
│   ├── transformers/      # Cleaner, Validator, Enricher
│   ├── loaders/           # PostgreSQL, S3, Redis loaders
│   ├── data_quality/      # Expectation suite runner
│   ├── config/            # Pydantic settings (env-based)
│   ├── utils/             # Logger, metrics, retry handler
│   ├── pipeline.py        # Main pipeline orchestrator
│   └── cli.py             # CLI entry point
├── config/                # Pipeline YAML configs
├── docker-compose.yml     # PostgreSQL + Redis + App
├── Dockerfile
├── deploy.sh
└── requirements.txt
```

---

## Quick Start

### 1. Clone & Configure

```bash
git clone https://github.com/g00562/dataflow-pro.git
cd dataflow-pro
cp .env.example .env   # edit with your credentials
```

### 2. Deploy

```bash
./deploy.sh
```

### 3. Run a Pipeline

```bash
# Create your pipeline config
cp config/pipeline.example.yaml config/my_pipeline.yaml

# Run it
docker-compose exec app dataforge run --config /app/config/my_pipeline.yaml
```

---

## Pipeline Configuration

Pipelines are defined in YAML — no Python required:

```yaml
pipeline:
  name: user_data_pipeline

source:
  type: file
  path: /data/users.csv
  format: csv

transform:
  clean:
    missing_value_strategy: fill_default
    missing_value_default: unknown
    remove_duplicates: true
    numeric_columns: [age]
  validate:
    required: [id, name, email]
    patterns:
      email: "^[^@\\s]+@[^@\\s]+\\.[^@\\s]+$"
    ranges:
      age: {min: 18, max: 120}

target:
  type: file
  path: /data/output/clean_users.json
  format: json
```

---

## CLI Commands

```bash
dataforge run       --config pipeline.yaml        # Run a pipeline
dataforge validate  --input data.csv              # Validate a file
dataforge quality   --expectations suite.json     # Check data quality
dataforge worker                                  # Start worker (health check)
```

---

## Architecture

```
┌─────────────┐    ┌─────────────┐    ┌─────────────┐
│  PostgreSQL  │    │    Redis    │    │     App     │
│  (port 5432) │    │ (port 6379) │    │  DataForge  │
└─────────────┘    └─────────────┘    └─────────────┘
       │                  │                  │
       └──────────────────┴──────────────────┘
                    Docker Network
```

---

## Environment Variables

| Variable | Description | Default |
|---|---|---|
| `DATABASE_URL` | PostgreSQL connection string | — |
| `REDIS_URL` | Redis connection string | — |
| `ENVIRONMENT` | `development` / `production` | `development` |
| `LOG_LEVEL` | `DEBUG` / `INFO` / `WARNING` | `INFO` |
| `AWS_ACCESS_KEY_ID` | AWS key (for S3 loader) | — |
| `AWS_SECRET_ACCESS_KEY` | AWS secret (for S3 loader) | — |

---

## Tech Stack

- **Python 3.11+** — Core language
- **SQLAlchemy 2.x** — Database ORM & connection pooling
- **Pydantic v2** — Config validation & settings management
- **pandas** — Data manipulation
- **boto3** — AWS S3 integration
- **redis-py** — Redis cache integration
- **tenacity** — Retry logic
- **Docker** — Containerization

---

## Operations

```bash
# View logs
docker-compose logs -f app

# Stop services
docker-compose down

# Backup database
docker-compose exec postgres pg_dump -U dataforge dataforge > backup.sql

# Restart app
docker-compose restart app
```

---

## License

MIT
