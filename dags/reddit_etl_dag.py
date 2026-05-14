"""
Reddit ETL DAG — TaskFlow API + LocalExecutor
==============================================
End-to-end pipeline: Reddit API -> S3 (raw) -> PySpark -> S3 (curated) -> Redshift.

Uses Airflow's modern TaskFlow API (@dag, @task decorators) instead of the
legacy PythonOperator. This produces cleaner, more Pythonic DAG code.


"""

import json
import logging
import os
from datetime import datetime, timedelta

from airflow.decorators import dag, task
from airflow.providers.amazon.aws.hooks.s3 import S3Hook
from airflow.exceptions import AirflowFailException

from src.extract.reddit_client import RedditExtractor
from src.transform.spark_job import run_spark_transform
from src.load.redshift_loader import load_to_redshift

logger = logging.getLogger(__name__)

# ---------------------------------------------------------------------------
# CONFIG
# ---------------------------------------------------------------------------
SUBREDDITS = ["dataengineering", "machinelearning"]
BUCKET = os.getenv("AWS_S3_BUCKET", "your-reddit-etl-bucket")
REDSHIFT_IAM_ROLE = os.getenv(
    "REDSHIFT_IAM_ROLE", "arn:aws:iam::123456789012:role/RedshiftS3Role"
)


# =============================================================================
# TASK 1: EXTRACT (decorator style)
# =============================================================================
@task
def extract_reddit_data(ds: str) -> list:
    """
    Fetch posts from Reddit and land raw JSONL to S3.
    
    Args:
        ds: Execution date string (e.g., "2026-05-14") — passed automatically by Airflow
    
    Returns:
        List of metadata dicts. Airflow passes this via XCom to the next task.
    """
    extractor = RedditExtractor(
        client_id=os.getenv("REDDIT_CLIENT_ID", ""),
        client_secret=os.getenv("REDDIT_CLIENT_SECRET", ""),
        user_agent="portfolio-etl/0.1",
    )

    s3_hook = S3Hook(aws_conn_id="aws_default")
    all_meta = []

    for sub in SUBREDDITS:
        logger.info("Extracting r/%s for %s", sub, ds)
        posts = extractor.fetch_subreddit_posts(sub, limit=50, sort_by="hot")

        if not posts:
            logger.warning("No posts for r/%s", sub)
            continue

        # Partitioned S3 key: raw/subreddit/year=/month=/day=/
        key = (
            f"raw/reddit/{sub}/"
            f"year={ds[:4]}/month={ds[5:7]}/day={ds[8:10]}/posts.jsonl"
        )
        body = "\n".join([json.dumps(p) for p in posts])
        s3_hook.load_string(
            string_data=body,
            key=key,
            bucket_name=BUCKET,
            replace=True,
        )

        uri = f"s3a://{BUCKET}/{key}"
        logger.info("Landed %d posts -> %s", len(posts), uri)

        meta = {
            "subreddit": sub,
            "s3_uri": uri,
            "s3_key": key,
            "bucket": BUCKET,
            "record_count": len(posts),
            "date": ds,
        }
        all_meta.append(meta)

    return all_meta


# =============================================================================
# TASK 2: TRANSFORM (decorator style)
# =============================================================================
@task
def transform_reddit_data(extract_meta: list, ds: str) -> dict:
    """
    Read raw JSONL from S3, run PySpark, write curated Parquet.
    
    Args:
        extract_meta: Output from extract_reddit_data (passed via XCom automatically)
        ds: Execution date string
    
    Returns:
        Dict with transform results. Passed via XCom to the next task.
    """
    if not extract_meta:
        raise AirflowFailException("No metadata from extract task. Failing.")

    s3_input_uris = [m["s3_uri"] for m in extract_meta]
    s3_output_uri = (
        f"s3a://{BUCKET}/"
        f"curated/reddit/year={ds[:4]}/month={ds[5:7]}/day={ds[8:10]}/"
    )

    result = run_spark_transform(s3_input_uris, s3_output_uri)
    logger.info(
        "Transform: raw=%d curated=%d -> %s",
        result["raw_count"],
        result["curated_count"],
        result["output_uri"],
    )
    return result


# =============================================================================
# TASK 3: LOAD (decorator style)
# =============================================================================
@task
def load_to_redshift_task(transform_meta: dict) -> dict:
    """
    COPY curated Parquet from S3 into Redshift.
    
    Args:
        transform_meta: Output from transform_reddit_data (passed via XCom automatically)
    
    Returns:
        Dict with load status.
    """
    if not transform_meta:
        raise AirflowFailException("No transform metadata. Failing.")

    result = load_to_redshift(
        s3_uri=transform_meta["output_uri"],
        iam_role=REDSHIFT_IAM_ROLE,
        redshift_host=os.getenv("REDSHIFT_HOST", ""),
        redshift_port=int(os.getenv("REDSHIFT_PORT", "5439")),
        redshift_db=os.getenv("REDSHIFT_DB", "dev"),
        redshift_user=os.getenv("REDSHIFT_USER", "awsuser"),
        redshift_password=os.getenv("REDSHIFT_PASSWORD", ""),
    )
    logger.info("Redshift load OK: %s", result)
    return result


# =============================================================================
# DAG DEFINITION (TaskFlow decorator)
# =============================================================================
@dag(
    dag_id="reddit_etl_pipeline",
    description="Reddit API -> S3 -> PySpark -> Redshift (TaskFlow API)",
    schedule_interval="@daily",
    start_date=datetime(2024, 1, 1),
    catchup=False,
    max_active_runs=1,
    default_args={
        "owner": "portfolio",
        "depends_on_past": False,
        "email_on_failure": False,
        "retries": 2,
        "retry_delay": timedelta(minutes=5),
        "execution_timeout": timedelta(hours=1),
    },
    tags=["reddit", "etl", "portfolio", "learning", "taskflow"],
)
def reddit_etl_pipeline():
    """
    Define the task dependency graph using TaskFlow.
    
    The >> operator is replaced by direct function calls:
    - extract_reddit_data() returns data
    - transform_reddit_data(extract_result) receives it
    - load_to_redshift_task(transform_result) receives it
    
    Airflow handles XCom passing automatically between decorated tasks.
    """
    # Task 1: Extract from Reddit -> S3
    extract_result = extract_reddit_data(ds="{{ ds }}")
    
    # Task 2: Transform with PySpark
    # extract_result is automatically passed via XCom
    transform_result = transform_reddit_data(extract_result, ds="{{ ds }}")
    
    # Task 3: Load to Redshift
    # transform_result is automatically passed via XCom
    load_result = load_to_redshift_task(transform_result)
    
    # TaskFlow handles the dependency graph automatically:
    # extract_result >> transform_result >> load_result


# Instantiate the DAG
reddit_etl_dag = reddit_etl_pipeline()