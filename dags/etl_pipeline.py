"""
Main ETL Pipeline DAG for Apache Airflow.

This DAG demonstrates a complete ETL pipeline with:
- Data extraction from multiple sources
- Data transformation with validation
- Data loading to multiple targets
- Data quality checks
- Error handling and retry logic
- Monitoring and metrics
"""

from datetime import datetime, timedelta
from typing import Dict, List, Any, Optional
import logging

# Airflow imports
from airflow import DAG
from airflow.operators.python import PythonOperator, BranchPythonOperator
from airflow.operators.dummy import DummyOperator
from airflow.operators.bash import BashOperator
from airflow.providers.postgres.operators.postgres import PostgresOperator
from airflow.providers.http.operators.http import SimpleHttpOperator
from airflow.providers.http.sensors.http import HttpSensor
from airflow.models import Variable
from airflow.utils.task_group import TaskGroup
from airflow.utils.dates import days_ago

# Try to import our modules
try:
    from dataflow_pro.extractors import (
        APIExtractor,
        DatabaseExtractor,
        FileExtractor,
    )
    from dataflow_pro.transformers import (
        DataCleaner,
        DataValidator,
        DataEnricher,
    )
    from dataflow_pro.loaders import (
        DatabaseLoader,
        S3Loader,
    )
    from dataflow_pro.utils import get_logger, get_metrics
    MODULES_AVAILABLE = True
except ImportError:
    MODULES_AVAILABLE = False
    get_logger = logging.getLogger


# DAG Configuration
DEFAULT_ARGS = {
    "owner": "data-engineering",
    "depends_on_past": False,
    "email": ["dataengineering@example.com"],
    "email_on_failure": True,
    "email_on_retry": False,
    "retries": 3,
    "retry_delay": timedelta(minutes=5),
    "retry_exponential_backoff": True,
    "max_retry_delay": timedelta(minutes=30),
}

DAG_CONFIG = {
    "dag_id": "etl_pipeline",
    "default_args": DEFAULT_ARGS,
    "description": "Main ETL Pipeline for Data Processing",
    "schedule_interval": "0 2 * * *",  # Daily at 2 AM
    "start_date": days_ago(1),
    "catchup": False,
    "tags": ["etl", "data-pipeline", "production"],
    "max_active_runs": 1,
    "dagrun_timeout": timedelta(hours=2),
}


# Pipeline Configuration
PIPELINE_CONFIG = {
    "batch_size": 10000,
    "source": {
        "type": "api",  # or "database", "file"
        "api_url": "https://api.example.com/v1/data",
    },
    "target": {
        "database": "data_warehouse",
        "s3_bucket": "dataflow-pro-processed",
    },
    "data_quality": {
        "enabled": True,
        "threshold": 0.95,
    },
}


def setup_logging(**context):
    """Setup logging for pipeline."""
    logger = logging.getLogger("etl_pipeline")
    logger.setLevel(logging.INFO)
    
    # Log execution info
    dag_run = context.get("dag_run")
    if dag_run:
        logger.info(f"Pipeline run: {dag_run.run_id}")
    
    return logger


def extract_data(**context) -> Dict[str, Any]:
    """
    Extract data from source.
    
    Returns:
        Dict with extracted data and metadata
    """
    logger = logging.getLogger("etl_pipeline")
    logger.info("Starting data extraction")
    
    config = PIPELINE_CONFIG.get("source", {})
    
    if MODULES_AVAILABLE:
        extractor = APIExtractor(
            base_url=config.get("api_url", ""),
        )
        
        # Extract data
        result = extractor.extract()
        
        logger.info(f"Extracted {result.rows_extracted} rows")
        
        return {
            "data": result.data,
            "rows_extracted": result.rows_extracted,
            "source": "api",
        }
    
    # Mock data for demo
    return {
        "data": [
            {"id": 1, "name": "John", "email": "john@example.com"},
            {"id": 2, "name": "Jane", "email": "jane@example.com"},
        ],
        "rows_extracted": 2,
        "source": "mock",
    }


def transform_data(**context) -> Dict[str, Any]:
    """
    Transform and clean data.
    
    Returns:
        Dict with transformed data and metadata
    """
    logger = logging.getLogger("etl_pipeline")
    logger.info("Starting data transformation")
    
    # Get data from previous task
    ti = context["ti"]
    extraction_result = ti.xcom_pull(task_ids="extract_data")
    
    data = extraction_result.get("data", [])
    
    if MODULES_AVAILABLE:
        # Clean data
        cleaner = DataCleaner()
        cleaning_result = cleaner.clean(data)
        
        # Validate data
        validator = DataValidator()
        validation_result = validator.validate(cleaning_result.data)
        
        logger.info(
            f"Transformed: {validation_result.rows_valid} valid, "
            f"{validation_result.rows_invalid} invalid"
        )
        
        return {
            "data": validation_result.data,
            "rows_valid": validation_result.rows_valid,
            "rows_invalid": validation_result.rows_invalid,
        }
    
    return {
        "data": data,
        "rows_valid": len(data),
        "rows_invalid": 0,
    }


def load_to_database(**context) -> Dict[str, Any]:
    """
    Load data to database.
    
    Returns:
        Dict with load result
    """
    logger = logging.getLogger("etl_pipeline")
    logger.info("Loading data to database")
    
    # Get data from previous task
    ti = context["ti"]
    transform_result = ti.xcom_pull(task_ids="transform_data")
    
    data = transform_result.get("data", [])
    
    if MODULES_AVAILABLE:
        loader = DatabaseLoader(table_name="processed_data")
        
        result = loader.load(data, strategy="insert")
        
        logger.info(f"Loaded {result.rows_loaded} rows to database")
        
        return {
            "rows_loaded": result.rows_loaded,
            "target": "database",
        }
    
    return {
        "rows_loaded": len(data),
        "target": "database",
    }


def load_to_s3(**context) -> Dict[str, Any]:
    """
    Load data to S3.
    
    Returns:
        Dict with load result
    """
    logger = logging.getLogger("etl_pipeline")
    logger.info("Loading data to S3")
    
    # Get data from previous task
    ti = context["ti"]
    transform_result = ti.xcom_pull(task_ids="transform_data")
    
    data = transform_result.get("data", [])
    
    if MODULES_AVAILABLE:
        loader = S3Loader()
        
        result = loader.load(data, "processed/data.parquet")
        
        logger.info(f"Loaded {result.files_written} files to S3")
        
        return {
            "files_written": result.files_written,
            "target": "s3",
        }
    
    return {
        "files_written": 1,
        "target": "s3",
    }


def check_data_quality(**context) -> Dict[str, Any]:
    """
    Check data quality.
    
    Returns:
        Dict with quality check results
    """
    logger = logging.getLogger("etl_pipeline")
    logger.info("Checking data quality")
    
    # Get data from previous tasks
    ti = context["ti"]
    transform_result = ti.xcom_pull(task_ids="transform_data")
    
    rows_valid = transform_result.get("rows_valid", 0)
    rows_invalid = transform_result.get("rows_invalid", 0)
    total = rows_valid + rows_invalid
    
    quality_score = rows_valid / total if total > 0 else 0
    
    logger.info(f"Data quality score: {quality_score:.2%}")
    
    # Check threshold
    threshold = PIPELINE_CONFIG.get("data_quality", {}).get("threshold", 0.95)
    
    if quality_score < threshold:
        raise ValueError(f"Data quality {quality_score:.2%} below threshold {threshold:.2%}")
    
    return {
        "quality_score": quality_score,
        "passed": quality_score >= threshold,
    }


def on_failure(context):
    """Handle task failure."""
    logger = logging.getLogger("etl_pipeline")
    logger.error(f"Pipeline failed: {context.get('task_instance')}")


def on_success(context):
    """Handle task success."""
    logger = logging.getLogger("etl_pipeline")
    logger.info(f"Pipeline succeeded: {context.get('task_instance')}")


# Create DAG
with DAG(**DAG_CONFIG) as dag:
    # Start task
    start = DummyOperator(
        task_id="start",
    )
    
    # Setup logging
    setup = PythonOperator(
        task_id="setup_logging",
        python_callable=setup_logging,
        provide_context=True,
    )
    
    # Extract
    extract = PythonOperator(
        task_id="extract_data",
        python_callable=extract_data,
        provide_context=True,
        on_success_callback=on_success,
    )
    
    # Transform
    transform = PythonOperator(
        task_id="transform_data",
        python_callable=transform_data,
        provide_context=True,
    )
    
    # Data quality check
    quality_check = PythonOperator(
        task_id="check_data_quality",
        python_callable=check_data_quality,
        provide_context=True,
    )
    
    # Load to database
    load_db = PythonOperator(
        task_id="load_to_database",
        python_callable=load_to_database,
        provide_context=True,
    )
    
    # Load to S3
    load_s3 = PythonOperator(
        task_id="load_to_s3",
        python_callable=load_to_s3,
        provide_context=True,
    )
    
    # End task
    end = DummyOperator(
        task_id="end",
        trigger_rule="all_done",
    )
    
    # Define workflow
    start >> setup >> extract >> transform >> quality_check
    
    # Parallel loading
    [load_db, load_s3] >> end
    
    # Error handling
    quality_check.set_downstream(end)


# For testing purposes
if __name__ == "__main__":
    from airflow.utils.state import State
    from airflow.executors.serialized_dag import SerializedDAG
    
    print(f"DAG: {dag.dag_id}")
    print(f"Schedule: {dag.schedule_interval}")
    print(f"Tasks: {len(dag.tasks)}")
    
    for task in dag.tasks:
        print(f"  - {task.task_id} ({task.__class__.__name__})")