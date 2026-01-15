"""Coordination constants and helper functions for crawl/scrape orchestration."""

from __future__ import annotations

from typing import Optional

# Sentinel timestamp for "never processed"
SENTINEL_TIMESTAMP = "1970-01-01T00:00:00Z"


def is_sentinel(timestamp: Optional[str]) -> bool:
    """Check if a timestamp is the sentinel value.

    Args:
        timestamp: Timestamp string to check.

    Returns:
        True if timestamp is the sentinel value or None.
    """
    return timestamp is None or timestamp == SENTINEL_TIMESTAMP


def is_crawl_in_progress(
    last_crawled_start: Optional[str], last_crawled_end: Optional[str]
) -> bool:
    """Check if a crawl is currently in progress.

    A crawl is in progress when:
    - last_crawled_start is a real timestamp (not None, not SENTINEL)
    - last_crawled_start > last_crawled_end (start is newer than end)

    Args:
        last_crawled_start: When the crawl started.
        last_crawled_end: When the crawl ended.

    Returns:
        True if crawl is in progress, False otherwise.
    """
    if is_sentinel(last_crawled_start):
        return False

    if is_sentinel(last_crawled_end):
        return True  # Started but never ended

    # Both are real timestamps - compare them
    return last_crawled_start > last_crawled_end


def is_scrape_in_progress(
    last_scraped_start: Optional[str], last_scraped_end: Optional[str]
) -> bool:
    """Check if a scrape is currently in progress.

    A scrape is in progress when:
    - last_scraped_start is a real timestamp (not None, not SENTINEL)
    - last_scraped_start > last_scraped_end (start is newer than end)

    Args:
        last_scraped_start: When the scrape started.
        last_scraped_end: When the scrape ended.

    Returns:
        True if scrape is in progress, False otherwise.
    """
    if is_sentinel(last_scraped_start):
        return False

    if is_sentinel(last_scraped_end):
        return True  # Started but never ended

    # Both are real timestamps - compare them
    return last_scraped_start > last_scraped_end


def is_scrape_eligible(
    last_crawled_end: Optional[str],
    last_scraped_end: Optional[str],
    last_scraped_start: Optional[str] = None,
) -> bool:
    """Check if a shop is eligible for scraping.

    A shop is eligible if:
    1. Crawl has finished (last_crawled_end is a real timestamp)
    2. Crawl is newer than the last scrape (last_crawled_end > last_scraped_end)
    3. No scrape is currently in progress

    Args:
        last_crawled_end: When the last crawl finished.
        last_scraped_end: When the last scrape finished.
        last_scraped_start: Optional, when current scrape started.

    Returns:
        True if eligible for scraping, False otherwise.
    """
    # 1. Crawl must be finished (not None, not SENTINEL)
    if is_sentinel(last_crawled_end):
        return False

    # 2. If scrape is in progress, not eligible
    if last_scraped_start and is_scrape_in_progress(
        last_scraped_start, last_scraped_end
    ):
        return False

    # 3. Crawl must be newer than scrape
    # If never scraped (SENTINEL), crawl is always newer
    if is_sentinel(last_scraped_end):
        return True

    # Both are real timestamps - crawl must be newer
    return last_crawled_end > last_scraped_end
