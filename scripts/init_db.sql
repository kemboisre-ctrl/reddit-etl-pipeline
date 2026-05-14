-- =============================================================================
-- REDSHIFT DDL — Reddit Posts Table
-- =============================================================================


CREATE TABLE IF NOT EXISTS reddit_posts (
    id              VARCHAR(10) DISTKEY,      -- Even distribution for parallel joins
    title           VARCHAR(500),
    author          VARCHAR(50),
    score           INTEGER,
    upvote_ratio    FLOAT,
    num_comments    INTEGER,
    created_utc     FLOAT,
    subreddit       VARCHAR(50) SORTKEY,     -- Sort for time-range + subreddit filters
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

-- Verify
SELECT COUNT(*) FROM reddit_posts;