"""Tests for coordination constants and helper functions."""

from __future__ import annotations

import pytest

from src.core.aws.database.constants import (
    SENTINEL_TIMESTAMP,
    is_crawl_in_progress,
    is_scrape_eligible,
    is_scrape_in_progress,
    is_sentinel,
)


class TestSentinelTimestamp:
    """Tests for SENTINEL_TIMESTAMP constant."""

    def test_sentinel_timestamp_value(self):
        """Verify sentinel timestamp has expected value."""
        assert SENTINEL_TIMESTAMP == "1970-01-01T00:00:00Z"


class TestIsSentinel:
    """Tests for is_sentinel function."""

    def test_none_is_sentinel(self):
        """None should be considered a sentinel value."""
        assert is_sentinel(None) is True

    def test_sentinel_timestamp_is_sentinel(self):
        """The SENTINEL_TIMESTAMP constant should be recognized as sentinel."""
        assert is_sentinel(SENTINEL_TIMESTAMP) is True
        assert is_sentinel("1970-01-01T00:00:00Z") is True

    def test_real_timestamp_is_not_sentinel(self):
        """Real timestamps should not be considered sentinel values."""
        assert is_sentinel("2026-01-15T10:00:00Z") is False
        assert is_sentinel("2025-12-31T23:59:59Z") is False

    def test_empty_string_is_not_sentinel(self):
        """Empty string should not be considered a sentinel value."""
        assert is_sentinel("") is False

    def test_arbitrary_string_is_not_sentinel(self):
        """Arbitrary strings should not be considered sentinel values."""
        assert is_sentinel("not-a-timestamp") is False
        assert is_sentinel("1970-01-01") is False  # Missing time component


class TestIsCrawlInProgress:
    """Tests for is_crawl_in_progress function."""

    def test_both_none_not_in_progress(self):
        """When both timestamps are None, crawl is not in progress."""
        assert is_crawl_in_progress(None, None) is False

    def test_both_sentinel_not_in_progress(self):
        """When both timestamps are sentinel, crawl is not in progress."""
        assert is_crawl_in_progress(SENTINEL_TIMESTAMP, SENTINEL_TIMESTAMP) is False

    def test_start_sentinel_end_none_not_in_progress(self):
        """When start is sentinel and end is None, crawl is not in progress."""
        assert is_crawl_in_progress(SENTINEL_TIMESTAMP, None) is False

    def test_start_real_end_none_is_in_progress(self):
        """When start is real and end is None, crawl is in progress."""
        assert is_crawl_in_progress("2026-01-15T10:00:00Z", None) is True

    def test_start_real_end_sentinel_is_in_progress(self):
        """When start is real and end is sentinel, crawl is in progress."""
        assert is_crawl_in_progress("2026-01-15T10:00:00Z", SENTINEL_TIMESTAMP) is True

    def test_start_newer_than_end_is_in_progress(self):
        """When start timestamp is newer than end, crawl is in progress."""
        assert (
            is_crawl_in_progress(
                "2026-01-15T12:00:00Z",  # Start (newer)
                "2026-01-15T10:00:00Z",  # End (older)
            )
            is True
        )

    def test_end_newer_than_start_not_in_progress(self):
        """When end timestamp is newer than start, crawl is not in progress."""
        assert (
            is_crawl_in_progress(
                "2026-01-15T10:00:00Z",  # Start (older)
                "2026-01-15T12:00:00Z",  # End (newer)
            )
            is False
        )

    def test_start_equals_end_not_in_progress(self):
        """When timestamps are equal, crawl is not in progress."""
        timestamp = "2026-01-15T10:00:00Z"
        assert is_crawl_in_progress(timestamp, timestamp) is False

    def test_start_none_end_real_not_in_progress(self):
        """When start is None but end is real, crawl is not in progress."""
        assert is_crawl_in_progress(None, "2026-01-15T10:00:00Z") is False


class TestIsScrapeInProgress:
    """Tests for is_scrape_in_progress function."""

    def test_both_none_not_in_progress(self):
        """When both timestamps are None, scrape is not in progress."""
        assert is_scrape_in_progress(None, None) is False

    def test_both_sentinel_not_in_progress(self):
        """When both timestamps are sentinel, scrape is not in progress."""
        assert is_scrape_in_progress(SENTINEL_TIMESTAMP, SENTINEL_TIMESTAMP) is False

    def test_start_sentinel_end_none_not_in_progress(self):
        """When start is sentinel and end is None, scrape is not in progress."""
        assert is_scrape_in_progress(SENTINEL_TIMESTAMP, None) is False

    def test_start_real_end_none_is_in_progress(self):
        """When start is real and end is None, scrape is in progress."""
        assert is_scrape_in_progress("2026-01-15T10:00:00Z", None) is True

    def test_start_real_end_sentinel_is_in_progress(self):
        """When start is real and end is sentinel, scrape is in progress."""
        assert is_scrape_in_progress("2026-01-15T10:00:00Z", SENTINEL_TIMESTAMP) is True

    def test_start_newer_than_end_is_in_progress(self):
        """When start timestamp is newer than end, scrape is in progress."""
        assert (
            is_scrape_in_progress(
                "2026-01-15T12:00:00Z",  # Start (newer)
                "2026-01-15T10:00:00Z",  # End (older)
            )
            is True
        )

    def test_end_newer_than_start_not_in_progress(self):
        """When end timestamp is newer than start, scrape is not in progress."""
        assert (
            is_scrape_in_progress(
                "2026-01-15T10:00:00Z",  # Start (older)
                "2026-01-15T12:00:00Z",  # End (newer)
            )
            is False
        )

    def test_start_equals_end_not_in_progress(self):
        """When timestamps are equal, scrape is not in progress."""
        timestamp = "2026-01-15T10:00:00Z"
        assert is_scrape_in_progress(timestamp, timestamp) is False

    def test_start_none_end_real_not_in_progress(self):
        """When start is None but end is real, scrape is not in progress."""
        assert is_scrape_in_progress(None, "2026-01-15T10:00:00Z") is False


class TestIsScrapeEligible:
    """Tests for is_scrape_eligible function."""

    def test_never_crawled_not_eligible(self):
        """Shop that has never been crawled is not eligible for scraping."""
        assert is_scrape_eligible(None, None) is False
        assert is_scrape_eligible(SENTINEL_TIMESTAMP, SENTINEL_TIMESTAMP) is False

    def test_crawl_finished_never_scraped_is_eligible(self):
        """Shop with finished crawl and never scraped is eligible."""
        assert (
            is_scrape_eligible(
                last_crawled_end="2026-01-15T10:00:00Z",
                last_scraped_end=None,
            )
            is True
        )
        assert (
            is_scrape_eligible(
                last_crawled_end="2026-01-15T10:00:00Z",
                last_scraped_end=SENTINEL_TIMESTAMP,
            )
            is True
        )

    def test_crawl_newer_than_scrape_is_eligible(self):
        """Shop where crawl is newer than scrape is eligible."""
        assert (
            is_scrape_eligible(
                last_crawled_end="2026-01-15T12:00:00Z",  # Newer
                last_scraped_end="2026-01-15T10:00:00Z",  # Older
            )
            is True
        )

    def test_scrape_newer_than_crawl_not_eligible(self):
        """Shop where scrape is newer than crawl is not eligible."""
        assert (
            is_scrape_eligible(
                last_crawled_end="2026-01-15T10:00:00Z",  # Older
                last_scraped_end="2026-01-15T12:00:00Z",  # Newer
            )
            is False
        )

    def test_scrape_in_progress_not_eligible(self):
        """Shop with scrape in progress is not eligible for new scrape."""
        assert (
            is_scrape_eligible(
                last_crawled_end="2026-01-15T12:00:00Z",
                last_scraped_end="2026-01-15T10:00:00Z",  # Old scrape
                last_scraped_start="2026-01-15T11:00:00Z",  # New start > end
            )
            is False
        )

    def test_scrape_completed_is_eligible_if_crawl_newer(self):
        """Shop with completed scrape is eligible if crawl is newer."""
        assert (
            is_scrape_eligible(
                last_crawled_end="2026-01-15T14:00:00Z",  # Newest
                last_scraped_end="2026-01-15T12:00:00Z",  # Scrape end
                last_scraped_start="2026-01-15T11:00:00Z",  # Scrape start
            )
            is True
        )

    def test_equal_timestamps_not_eligible(self):
        """Shop where crawl and scrape timestamps are equal is not eligible."""
        timestamp = "2026-01-15T10:00:00Z"
        assert (
            is_scrape_eligible(
                last_crawled_end=timestamp,
                last_scraped_end=timestamp,
            )
            is False
        )

    def test_crawl_none_not_eligible(self):
        """Shop where crawl end is None is not eligible (crawl in progress)."""
        assert (
            is_scrape_eligible(
                last_crawled_end=None,
                last_scraped_end="2026-01-15T10:00:00Z",
            )
            is False
        )

    def test_crawl_sentinel_not_eligible(self):
        """Shop where crawl end is sentinel is not eligible (never crawled)."""
        assert (
            is_scrape_eligible(
                last_crawled_end=SENTINEL_TIMESTAMP,
                last_scraped_end="2026-01-15T10:00:00Z",
            )
            is False
        )


class TestIsScrapeEligibleEdgeCases:
    """Edge case tests for is_scrape_eligible function."""

    def test_scrape_start_none_checks_only_end_timestamps(self):
        """When scrape_start is None, only end timestamps are compared."""
        # Crawl newer than scrape - should be eligible
        assert (
            is_scrape_eligible(
                last_crawled_end="2026-01-15T12:00:00Z",
                last_scraped_end="2026-01-15T10:00:00Z",
                last_scraped_start=None,
            )
            is True
        )

    def test_scrape_start_sentinel_ignores_in_progress_check(self):
        """When scrape_start is sentinel, in-progress check returns False."""
        assert (
            is_scrape_eligible(
                last_crawled_end="2026-01-15T12:00:00Z",
                last_scraped_end=SENTINEL_TIMESTAMP,
                last_scraped_start=SENTINEL_TIMESTAMP,
            )
            is True
        )

    @pytest.mark.parametrize(
        "crawled_end,scraped_end,scraped_start,expected",
        [
            # New shop, never scraped
            ("2026-01-15T10:00:00Z", None, None, True),
            ("2026-01-15T10:00:00Z", SENTINEL_TIMESTAMP, None, True),
            # Crawl finished, scrape in progress
            ("2026-01-15T10:00:00Z", None, "2026-01-15T11:00:00Z", False),
            # Crawl finished, scrape completed, crawl newer
            (
                "2026-01-15T14:00:00Z",
                "2026-01-15T12:00:00Z",
                "2026-01-15T11:00:00Z",
                True,
            ),
            # Crawl finished, scrape completed, scrape newer
            (
                "2026-01-15T10:00:00Z",
                "2026-01-15T12:00:00Z",
                "2026-01-15T11:00:00Z",
                False,
            ),
            # Never crawled
            (None, None, None, False),
            (SENTINEL_TIMESTAMP, SENTINEL_TIMESTAMP, SENTINEL_TIMESTAMP, False),
        ],
    )
    def test_eligibility_scenarios(
        self, crawled_end, scraped_end, scraped_start, expected
    ):
        """Test various eligibility scenarios with parameterized inputs."""
        result = is_scrape_eligible(
            last_crawled_end=crawled_end,
            last_scraped_end=scraped_end,
            last_scraped_start=scraped_start,
        )
        assert result is expected


class TestCoordinationIntegration:
    """Integration tests combining multiple coordination functions."""

    def test_typical_shop_lifecycle(self):
        """Test a typical shop lifecycle through various states."""
        # Step 1: New shop - never crawled, never scraped
        crawled_end = None
        scraped_end = None
        scraped_start = None

        assert is_sentinel(crawled_end)
        assert is_sentinel(scraped_end)
        assert is_scrape_in_progress(scraped_start, scraped_end) is False
        assert is_scrape_eligible(crawled_end, scraped_end) is False

        # Step 2: Crawl completed
        crawled_end = "2026-01-15T10:00:00Z"

        assert is_sentinel(crawled_end) is False
        assert is_scrape_eligible(crawled_end, scraped_end) is True

        # Step 3: Scrape started
        scraped_start = "2026-01-15T11:00:00Z"

        assert is_scrape_in_progress(scraped_start, scraped_end) is True
        assert is_scrape_eligible(crawled_end, scraped_end, scraped_start) is False

        # Step 4: Scrape completed
        scraped_end = "2026-01-15T12:00:00Z"

        assert is_scrape_in_progress(scraped_start, scraped_end) is False
        assert (
            is_scrape_eligible(crawled_end, scraped_end) is False
        )  # Already up to date

        # Step 5: New crawl completed
        crawled_end = "2026-01-15T14:00:00Z"

        assert is_scrape_eligible(crawled_end, scraped_end) is True

    def test_concurrent_scrape_prevention(self):
        """Test that in-progress scrape prevents new scrape from being eligible."""
        crawled_end = "2026-01-15T14:00:00Z"  # New crawl
        scraped_end = "2026-01-15T10:00:00Z"  # Old scrape finished
        scraped_start = "2026-01-15T12:00:00Z"  # New scrape started (> end)

        # Scrape is in progress
        assert is_scrape_in_progress(scraped_start, scraped_end) is True

        # Even though crawl is newer, not eligible due to in-progress scrape
        assert is_scrape_eligible(crawled_end, scraped_end, scraped_start) is False
