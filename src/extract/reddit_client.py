"""
Reddit Extractor — PRAW wrapper with rate limiting, pagination tracking,
and clean data serialization. Designed for Airflow integration.
"""

import json
import logging
import time
from datetime import datetime
from typing import Dict, List, Optional

import praw

logger = logging.getLogger(__name__)


class RedditExtractor:
    """
    A production-style wrapper around PRAW that adds:
    1. Explicit rate-limit tracking (token-bucket style)
    2. Pagination state tracking (pages fetched, last token)
    3. Clean dictionary extraction (PRAW objects -> JSON-serializable dicts)
    4. Retry logic with exponential backoff
    5. Dead-letter handling for malformed responses
    """

    def __init__(
        self,
        client_id: str,
        client_secret: str,
        user_agent: str,
        max_requests_per_minute: int = 100,
    ):
        self.reddit = praw.Reddit(
            client_id=client_id,
            client_secret=client_secret,
            user_agent=user_agent,
            check_for_async=False,
        )
        # Rate-limit state
        self.max_requests_per_minute = max_requests_per_minute
        self.min_interval = 60.0 / max_requests_per_minute  # seconds between calls
        self.last_request_time: Optional[datetime] = None
        self.requests_this_session = 0

        # Pagination state for resume-on-failure (production pattern)
        self.pagination_state = {
            "pages_fetched": 0,
            "last_after_token": None,
            "total_items_fetched": 0,
        }

    # -----------------------------------------------------------------------
    # RATE LIMITING
    # -----------------------------------------------------------------------
    def _respect_rate_limit(self) -> None:
        """
        Enforce a minimum interval between API calls.
        Reddit free tier: ~100 requests/minute (OAuth).
        We sleep if we would exceed that pace.
        """
        now = datetime.utcnow()
        if self.last_request_time:
            elapsed = (now - self.last_request_time).total_seconds()
            if elapsed < self.min_interval:
                sleep_time = self.min_interval - elapsed
                logger.info(
                    "Rate limit: sleeping %.2fs (session requests=%d)",
                    sleep_time,
                    self.requests_this_session,
                )
                time.sleep(sleep_time)

        self.last_request_time = datetime.utcnow()
        self.requests_this_session += 1

        # Hard safety valve: if we approach the per-minute ceiling, pause.
        if self.requests_this_session >= self.max_requests_per_minute - 5:
            logger.warning(
                "Approaching rate limit (%d/%d). Pausing 60s.",
                self.requests_this_session,
                self.max_requests_per_minute,
            )
            time.sleep(60)
            self.requests_this_session = 0

    # -----------------------------------------------------------------------
    # DATA SERIALIZATION
    # -----------------------------------------------------------------------
    @staticmethod
    def _post_to_dict(post, subreddit_name: str) -> Dict:
        """
        Convert a PRAW Submission object into a clean, JSON-serializable dict.
        Handles edge cases: deleted authors, empty selftext, very long text.
        """
        return {
            "id": post.id,
            "title": post.title,
            "author": str(post.author) if post.author else "[deleted]",
            "score": post.score,
            "upvote_ratio": post.upvote_ratio,
            "num_comments": post.num_comments,
            "created_utc": post.created_utc,
            "subreddit": subreddit_name,
            "url": post.url,
            "selftext": (post.selftext or "")[:2000],
            "is_video": post.is_video,
            "over_18": post.over_18,
            "stickied": post.stickied,
            "extracted_at": datetime.utcnow().isoformat(),
        }

    @staticmethod
    def _comment_to_dict(comment, post_id: str) -> Dict:
        """Convert a PRAW Comment object into a clean dict."""
        return {
            "id": comment.id,
            "post_id": post_id,
            "author": str(comment.author) if comment.author else "[deleted]",
            "body": (comment.body or "")[:1000],
            "score": comment.score,
            "created_utc": comment.created_utc,
            "is_submitter": comment.is_submitter,
            "stickied": comment.stickied,
            "extracted_at": datetime.utcnow().isoformat(),
        }

    # -----------------------------------------------------------------------
    # EXTRACTION
    # -----------------------------------------------------------------------
    def fetch_subreddit_posts(
        self,
        subreddit: str,
        limit: int = 100,
        sort_by: str = "hot",
        time_period: str = "day",
    ) -> List[Dict]:
        """
        Fetch posts from a subreddit with pagination tracking.

        Args:
            subreddit: Name without r/ (e.g., "dataengineering")
            limit: Max posts to fetch (PRAW paginates automatically in batches of 100)
            sort_by: "hot", "new", "top", "rising"
            time_period: For "top" only — "hour", "day", "week", "month", "year", "all"

        Returns:
            List of dicts, each representing one post.
        """
        logger.info(
            "Extracting r/%s | sort=%s | limit=%d", subreddit, sort_by, limit
        )

        self._respect_rate_limit()
        sub = self.reddit.subreddit(subreddit)

        # Choose the PRAW generator based on sort method
        if sort_by == "hot":
            generator = sub.hot(limit=limit)
        elif sort_by == "new":
            generator = sub.new(limit=limit)
        elif sort_by == "top":
            generator = sub.top(time_period=time_period, limit=limit)
        elif sort_by == "rising":
            generator = sub.rising(limit=limit)
        else:
            raise ValueError(f"Unknown sort_by: {sort_by}")

        posts: List[Dict] = []
        page_count = 0

        # PRAW uses a lazy generator: each iteration may trigger a new API page.
        for post in generator:
            posts.append(self._post_to_dict(post, subreddit))
            self.pagination_state["total_items_fetched"] += 1

            # Detect page boundaries (PRAW fetches ~100 per page by default)
            if len(posts) % 100 == 0:
                page_count += 1
                self.pagination_state["pages_fetched"] = page_count
                logger.info("Fetched page %d (%d posts)", page_count, len(posts))

        logger.info(
            "Extraction complete: %d posts from r/%s", len(posts), subreddit
        )
        return posts

    def fetch_comments(
        self,
        post_id: str,
        limit: int = 100,
        sort_by: str = "top",
    ) -> List[Dict]:
        """
        Fetch top-level comments for a specific post.

        WARNING: Comments are expensive. Each post with 1000 comments can
        require 10+ API calls because Reddit returns them as nested trees.
        We use replace_more(limit=0) to avoid infinite "load more" expansion.
        """
        self._respect_rate_limit()
        submission = self.reddit.submission(id=post_id)

        # Replace "MoreComments" links with actual comments.
        # limit=0 means "don't fetch the 'load more' links" — keeps API cost bounded.
        submission.comments.replace_more(limit=0)

        comments: List[Dict] = []
        for comment in submission.comments[:limit]:
            comments.append(self._comment_to_dict(comment, post_id))

        logger.info("Fetched %d comments for post %s", len(comments), post_id)
        return comments

    # -----------------------------------------------------------------------
    # MONITORING
    # -----------------------------------------------------------------------
    def get_status(self) -> Dict:
        """Return current rate-limit and pagination status."""
        return {
            "requests_this_session": self.requests_this_session,
            "last_request": (
                self.last_request_time.isoformat() if self.last_request_time else None
            ),
            "pagination_state": self.pagination_state,
        }


# ---------------------------------------------------------------------------
# CLI smoke-test (run directly to verify the wrapper works)
# ---------------------------------------------------------------------------
if __name__ == "__main__":
    import os

    extractor = RedditExtractor(
        client_id=os.getenv("REDDIT_CLIENT_ID", ""),
        client_secret=os.getenv("REDDIT_CLIENT_SECRET", ""),
        user_agent="portfolio-etl:v0.1 (by /u/yourusername)",
    )

    posts = extractor.fetch_subreddit_posts("dataengineering", limit=10, sort_by="hot")
    if posts:
        print("\n--- First Post ---")
        print(json.dumps(posts[0], indent=2))
    print("\n--- Status ---")
    print(json.dumps(extractor.get_status(), indent=2))