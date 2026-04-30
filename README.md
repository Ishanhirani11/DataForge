# DataForge - Production Data Pipeline

Enterprise-grade ETL/ELT pipeline framework for production data workflows.

## Quick Start

### 1. Configure Environment
```bash
nano .env
```

### 2. Deploy
```bash
./deploy.sh
```

### 3. Verify
```bash
docker-compose ps
docker-compose logs -f app
```

## Architecture

- **PostgreSQL**: Primary data store
- **Redis**: Caching layer
- **Application**: Python-based ETL worker

## Configuration

### Database Connection
Edit `.env`:
```
DATABASE_URL=postgresql://user:pass@host:5432/dbname
```

### Pipeline Configuration
```bash
cp config/pipeline.example.yaml config/my_pipeline.yaml
```

### Run Pipeline
```bash
docker-compose exec app dataforge run --config /app/config/my_pipeline.yaml
```

## Operations

### View Logs
```bash
docker-compose logs -f app
```

### Stop / Restart
```bash
docker-compose down
docker-compose restart app
```

### Database Backup
```bash
docker-compose exec postgres pg_dump -U dataforge dataforge > backup.sql
```
