"""
Incremental Data Loader DAG for Apache Airflow.

This DAG demonstrates incremental data loading with:
- Watermark tracking
- Change Data Capture (CDC) patterns
- Backfill capabilities
- Idempotency
"""

from datetime import datetime, timedelta
from typing import Dict, Any, Optional

# Airflow imports
from airflow import DAG
from airflow.operators.python import PythonOperator, PythonVirtualenvOperator
from airflow.operators.dummy import DummyOperator
from airflow.operators.branch import BaseBranchOperator
from airflow.models import Variable
from airflow.utils.dates import days_ago
from airflow.utils.task_group import TaskGroup


from airflow import Dataset


DEFAULT_ARGS = {
    "owner": "data-engineering",
    "depends_on_past": False,
    "email": ["dataengineering@example.com"],
    "email_on_failure": True,
    "email_on_retry": False,
    "retries": 3,
    "retry_delay": timedelta(minutes=5),
}

DAG_CONFIG = {
    "dag_id": "incremental_loader",
    "default_args": DEFAULT_ARGS,
    "description": "Incremental Data Loader with Watermark Tracking",
    "schedule_interval": "*/15 * * * *",  # Every 15 minutes
    "start_date": days_ago(1),
    "catchup": False,
    "tags": ["incremental", "cdc", "streaming"],
}


def get_watermark(task_id: str) -> Optional[datetime]:
    """Get last processed watermark."""
    try:
        watermark = Variable.get(f"watermark_{task_id}")
        return datetime.fromisoformat(watermark)
    except:
        return None


def save_watermark(task_id: str, watermark: datetime) -> None:
    """Save watermark."""
    Variable.set(f"watermark_{task_id}", watermark.isoformat())


def extract_incremental(**context) -> Dict[str, Any]:
    """Extract data incrementally since last watermark."""
    import logging
    logger = logging.getLogger("incremental_loader")
    
    task_id = "extract_incremental"
    
    # Get last watermark
    last_watermark = get_watermark(task_id)
    
    logger.info(f"Last watermark: {last_watermark}")
    
    # Mock extraction with watermark
    current_watermark = datetime.utcnow()
    
    mock_data = [
        {"id": 1, "name": "New Record", "created_at": current_watermark.isoformat()}
    ]
    
    return {
        "data": mock_data,
        "rows_extracted": len(mock_data),
        "last_watermark": last_watermark.isoformat() if last_watermark else None,
        "current_watermark": current_watermark.isoformat(),
    }


def detect_changes(**context) -> str:
    """Detect if there are changes to process."""
    ti = context["ti"]
    result = ti.xcom_pull(task_ids="extract_incremental")
    
    if result and result.get("rows_extracted", 0) > 0:
        return "process_changes"
    else:
        return "skip_processing"


def process_changes(**context) -> Dict[str, Any]:
    """Process changed data."""
    import logging
    logger = logging.getLogger("incremental_loader")
    
    ti = context["ti"]
    result = ti.xcom_pull(task_ids="extract_incremental")
    
    data = result.get("data", [])
    
    logger.info(f"Processing {len(data)} changed records")
    
    # Process each record
    processed = 0
    for record in data:
        # Here you would apply business logic
        logger.info(f"Processing: {record}")
        processed += 1
    
    # Update watermark
    current_watermark = result.get("current_watermark")
    if current_watermark:
        save_watermark("extract_incremental", datetime.fromisoformat(current_watermark))
    
    return {
        "processed": processed,
        "watermark": current_watermark,
    }


def skip_processing(**context) -> Dict[str, Any]:
    """Skip processing when no changes."""
    import logging
    logger = logging.getLogger("incremental_loader")
    logger.info("No changes to process, skipping")
    
    return {"processed": 0, "skipped": True}


def run_backfill(**context) -> Dict[str, Any]:
    """Run backfill for historical data."""
    import logging
    logger = logging.getLogger("incremental_loader")
    
    # Get parameters
    params = context["params"]
    
    start_date = params.get("start_date")
    end_date = params.get("end_date")
    
    logger.info(f"Running backfill from {start_date} to {end_date}")
    
    # Mock backfill
    return {
        "backfill": True,
        "start_date": start_date,
        "end_date": end_date,
    }


# Create DAG
with DAG(**DAG_CONFIG) as dag:
    # Start
    start = DummyOperator(task_id="start")
    
    # Extract
    extract = PythonOperator(
        task_id="extract_incremental",
        python_callable=extract_incremental,
        provide_context=True,
    )
    
    # Branch
    branch = PythonOperator(
        task_id="detect_changes",
        python_callable=detect_changes,
        provide_context=True,
    )
    
    # Process changes
    process = PythonOperator(
        task_id="process_changes",
        python_callable=process_changes,
        provide_context=True,
    )
    
    # Skip
    skip = PythonOperator(
        task_id="skip_processing",
        python_callable=skip_processing,
        provide_context=True,
    )
    
    # End
    end = DummyOperator(task_id="end")
    
    # Workflow
    start >> extract >> branch
    
    branch >> skip >> end
    branch >> process >> end


# Backfill DAG
BACKFILL_DAG_CONFIG = {
    "dag_id": "incremental_backfill",
    "default_args": DEFAULT_ARGS,
    "description": "Backfill Historical Data",
    "schedule_interval": None,  # Manual trigger
    "start_date": days_ago(30),
    "tags": ["backfill", "historical"],
}

with DAG(**BACKFILL_DAG_CONFIG) as backfill_dag:
    start = DummyOperator(task_id="start")
    
    backfill = PythonOperator(
        task_id="run_backfill",
        python_callable=run_backfill,
        provide_context=True,
       op_kwargs={
            "start_date": "2024-01-01",
            "end_date": "2024-12-31",
        },
    )
    
    end = DummyOperator(task_id="end")
    
    start >> backfill >> end


__all__ = ["dag", "backfill_dag"]