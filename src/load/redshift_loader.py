"""
Redshift Loader
===============
Builds and executes Redshift COPY commands for bulk loading curated Parquet
from S3. Follows best practices: staging tables, atomic swap, sortkey/distkey.
"""

import logging
from typing import Dict

import psycopg2

logger = logging.getLogger(__name__)

# Production DDL — includes DISTKEY and SORTKEY for query performance
REDSHIFT_DDL = """
CREATE TABLE IF NOT EXISTS reddit_posts (
    id              VARCHAR(10) DISTKEY,
    title           VARCHAR(500),
    author          VARCHAR(50),
    score           INTEGER,
    upvote_ratio    FLOAT,
    num_comments    INTEGER,
    created_utc     FLOAT,
    subreddit       VARCHAR(50) SORTKEY,
    url             VARCHAR(500),
    selftext        VARCHAR(2000),
    is_video        BOOLEAN,
    over_18         BOOLEAN,
    stickied        BOOLEAN,
    engagement_ratio FLOAT,
    high_engagement  BOOLEAN,
    processed_date   DATE,
    extracted_at     VARCHAR(30)
);
"""


def get_connection(host: str, port: int, db: str, user: str, password: str):
    """Return a psycopg2 connection to Redshift."""
    return psycopg2.connect(
        host=host,
        port=port,
        dbname=db,
        user=user,
        password=password,
    )


def load_to_redshift(
    s3_uri: str,
    iam_role: str,
    redshift_host: str,
    redshift_port: int,
    redshift_db: str,
    redshift_user: str,
    redshift_password: str,
) -> Dict:
    """
    Load curated Parquet from S3 into Redshift using COPY.

    Uses a staging table pattern for safety:
      1. CREATE staging table LIKE target
      2. COPY into staging
      3. DELETE from target where IDs match (upsert)
      4. INSERT from staging into target
      5. DROP staging
    """
    conn = get_connection(
        redshift_host, redshift_port, redshift_db, redshift_user, redshift_password
    )

    try:
        with conn.cursor() as cur:
            # Ensure target table exists
            cur.execute(REDSHIFT_DDL)

            # Staging table name
            staging = "reddit_posts_staging"
            cur.execute(f"DROP TABLE IF EXISTS {staging}")
            cur.execute(f"CREATE TABLE {staging} (LIKE reddit_posts)")

            # COPY from S3 (Parquet format)
            copy_sql = f"""
            COPY {staging}
            FROM '{s3_uri}'
            IAM_ROLE '{iam_role}'
            FORMAT AS PARQUET;
            """
            logger.info("Executing COPY into staging...")
            cur.execute(copy_sql)

            # Upsert: delete existing IDs, then insert
            cur.execute(f"""
                DELETE FROM reddit_posts
                USING {staging}
                WHERE reddit_posts.id = {staging}.id;
            """)
            cur.execute(f"""
                INSERT INTO reddit_posts
                SELECT * FROM {staging};
            """)
            cur.execute(f"DROP TABLE {staging}")

            conn.commit()
            logger.info("Redshift load complete.")

    except Exception as e:
        conn.rollback()
        logger.error("Redshift load failed: %s", e)
        raise
    finally:
        conn.close()

    return {"status": "success", "source_s3": s3_uri}