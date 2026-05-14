"""
PySpark Transformation Layer
============================
Reads raw JSONL from S3, cleans / deduplicates / enriches, writes curated
Parquet back to S3 partitioned by subreddit.

Runs in local[*] mode (all CPU cores on the scheduler machine).
No standalone Spark cluster needed.
"""

import logging
from typing import Dict

from pyspark.sql import SparkSession
from pyspark.sql.functions import (
    col,
    to_timestamp,
    regexp_replace,
    lower,
    current_date,
    when,
    lit,
)

logger = logging.getLogger(__name__)


def run_spark_transform(s3_input_uris: list, s3_output_uri: str) -> Dict:
    """
    Execute the PySpark transformation pipeline in local mode.

    Args:
        s3_input_uris: List of S3 URIs to raw JSONL files
        s3_output_uri: S3 URI where curated Parquet will be written

    Returns:
        dict with record counts and output location.
    """
    spark = (
        SparkSession.builder.appName("RedditETLTransform")
        .master("local[*]")  # Use all CPU cores on this machine
        .config("spark.sql.adaptive.enabled", "true")
        .config("spark.sql.adaptive.coalescePartitions.enabled", "true")
        .getOrCreate()
    )

    logger.info("Reading raw JSONL from %d paths", len(s3_input_uris))

    # -----------------------------------------------------------------------
    # 1. READ RAW JSONL
    # -----------------------------------------------------------------------
    raw_df = spark.read.json(s3_input_uris)
    raw_count = raw_df.count()

    if raw_count == 0:
        raise ValueError("Raw data loaded but 0 records parsed. Check input paths.")

    logger.info("Loaded %d raw records", raw_count)

    # -----------------------------------------------------------------------
    # 2. CLEAN & ENRICH
    # -----------------------------------------------------------------------
    clean_df = (
        raw_df
        .withColumn("created_ts", to_timestamp(col("created_utc")))
        .withColumn(
            "title_clean",
            regexp_replace(lower(col("title")), r"[^\w\s]", ""),
        )
        .withColumn(
            "engagement_ratio",
            col("score") / (col("num_comments") + lit(1)),
        )
        .withColumn(
            "high_engagement",
            when(col("engagement_ratio") > 10, lit(True)).otherwise(lit(False)),
        )
        .withColumn("processed_date", current_date())
        .filter(col("score") >= 0)
        .filter(col("created_utc").isNotNull())
        .dropDuplicates(["id"])
    )

    curated_count = clean_df.count()
    logger.info("Curated dataset: %d records (dropped %d)", curated_count, raw_count - curated_count)

    # -----------------------------------------------------------------------
    # 3. WRITE CURATED PARQUET
    # -----------------------------------------------------------------------
    clean_df.write.mode("overwrite").partitionBy("subreddit").parquet(s3_output_uri)

    logger.info("Wrote curated Parquet to %s", s3_output_uri)

    spark.stop()

    return {
        "raw_count": raw_count,
        "curated_count": curated_count,
        "output_uri": s3_output_uri,
    }