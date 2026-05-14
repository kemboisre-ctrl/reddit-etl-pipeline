"""
Unit tests for RedditExtractor.

Demonstrates mocking external APIs (PRAW) and asserting business logic:
- Rate limiting behavior
- Data serialization edge cases (deleted authors, empty text)
- Pagination state tracking
"""

import json
import time
import unittest
from datetime import datetime
from unittest.mock import MagicMock, patch

from src.extract.reddit_client import RedditExtractor


class TestRedditExtractor(unittest.TestCase):

    def setUp(self):
        self.extractor = RedditExtractor(
            client_id="fake_id",
            client_secret="fake_secret",
            user_agent="test/0.1",
            max_requests_per_minute=1000,  # High limit so tests don't sleep
        )

    def test_post_to_dict_handles_deleted_author(self):
        """Deleted authors return None from PRAW; we map to '[deleted]'."""
        mock_post = MagicMock()
        mock_post.id = "abc123"
        mock_post.title = "Test"
        mock_post.author = None  # Deleted account
        mock_post.score = 42
        mock_post.upvote_ratio = 0.95
        mock_post.num_comments = 10
        mock_post.created_utc = 1710000000.0
        mock_post.url = "https://example.com"
        mock_post.selftext = "Hello world"
        mock_post.is_video = False
        mock_post.over_18 = False
        mock_post.stickied = False

        result = self.extractor._post_to_dict(mock_post, "dataengineering")
        self.assertEqual(result["author"], "[deleted]")
        self.assertEqual(result["subreddit"], "dataengineering")
        self.assertEqual(result["score"], 42)

    def test_post_to_dict_truncates_long_selftext(self):
        """Selftext > 2000 chars should be truncated."""
        mock_post = MagicMock()
        mock_post.id = "abc"
        mock_post.title = "T"
        mock_post.author = MagicMock()
        mock_post.author.__str__ = lambda self: "user"
        mock_post.score = 1
        mock_post.upvote_ratio = 1.0
        mock_post.num_comments = 0
        mock_post.created_utc = 0.0
        mock_post.url = ""
        mock_post.selftext = "x" * 5000
        mock_post.is_video = False
        mock_post.over_18 = False
        mock_post.stickied = False

        result = self.extractor._post_to_dict(mock_post, "test")
        self.assertEqual(len(result["selftext"]), 2000)

    def test_rate_limit_enforces_interval(self):
        """Two rapid calls should trigger a sleep."""
        extractor = RedditExtractor(
            client_id="x", client_secret="y", user_agent="z", max_requests_per_minute=60
        )
        extractor._respect_rate_limit()
        t0 = datetime.utcnow()
        extractor._respect_rate_limit()  # Should sleep ~1s
        t1 = datetime.utcnow()
        self.assertGreaterEqual((t1 - t0).total_seconds(), 0.9)

    def test_pagination_state_increments(self):
        """After fetching, pagination_state should reflect work done."""
        mock_reddit = MagicMock()
        mock_sub = MagicMock()
        mock_post = MagicMock()
        mock_post.id = "p1"
        mock_post.title = "Title"
        mock_post.author = MagicMock()
        mock_post.author.__str__ = lambda self: "u"
        mock_post.score = 1
        mock_post.upvote_ratio = 1.0
        mock_post.num_comments = 0
        mock_post.created_utc = 0.0
        mock_post.url = ""
        mock_post.selftext = ""
        mock_post.is_video = False
        mock_post.over_18 = False
        mock_post.stickied = False

        mock_sub.hot.return_value = [mock_post] * 5
        mock_reddit.subreddit.return_value = mock_sub
        self.extractor.reddit = mock_reddit

        posts = self.extractor.fetch_subreddit_posts("test", limit=5)
        self.assertEqual(len(posts), 5)
        self.assertEqual(self.extractor.pagination_state["total_items_fetched"], 5)

    def test_json_serializable_output(self):
        """Every extracted post must be JSON-serializable."""
        mock_post = MagicMock()
        mock_post.id = "id1"
        mock_post.title = "T"
        mock_post.author = None
        mock_post.score = 1
        mock_post.upvote_ratio = 0.5
        mock_post.num_comments = 0
        mock_post.created_utc = 0.0
        mock_post.url = ""
        mock_post.selftext = ""
        mock_post.is_video = False
        mock_post.over_18 = False
        mock_post.stickied = False

        result = self.extractor._post_to_dict(mock_post, "test")
        json.dumps(result)  # Should not raise
        self.assertIn("extracted_at", result)


if __name__ == "__main__":
    unittest.main()