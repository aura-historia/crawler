from typing import Tuple

from crawl4ai import (
    CrawlerRunConfig,
    CacheMode,
    RateLimiter,
    MemoryAdaptiveDispatcher,
    BrowserConfig,
)
from crawl4ai.content_scraping_strategy import LXMLWebScrapingStrategy

from src.core.algorithms.bfs_no_cycle_deep_crawl_strategy import (
    BFSNoCycleDeepCrawlStrategy,
)


def crawl_config() -> CrawlerRunConfig:
    """Crawl URLs starting from the given URL using BFSNoCycleDeepCrawlStrategy."""
    strategy = BFSNoCycleDeepCrawlStrategy(
        max_depth=1000,
        include_external=False,
        exclude_extensions=[
            "jpg",
            "jpeg",
            "png",
            "gif",
            "webp",
            "svg",  # Images
            "mp4",
            "avi",
            "mov",
            "wmv",
            "flv",  # Videos
            "pdf",
            "doc",
            "docx",
            "xls",
            "xlsx",  # Documents
            "zip",
            "rar",
            "tar",
            "gz",  # Archives
            "css",
            "js",
            "ico",
            "woff",
            "woff2",
            "ttf",  # Assets
        ],
        exclude_patterns=["*wishlist*", "*cart*", "*login*", "*signup*"],
    )

    config = CrawlerRunConfig(
        cache_mode=CacheMode.BYPASS,
        deep_crawl_strategy=strategy,
        scraping_strategy=LXMLWebScrapingStrategy(),
        verbose=False,
        stream=True,
        check_robots_txt=True,
    )

    return config


def crawl_dispatcher() -> MemoryAdaptiveDispatcher:
    """Create a SemaphoreDispatcher for crawling."""
    dispatcher = MemoryAdaptiveDispatcher(
        memory_threshold_percent=80.0,
        check_interval=0.5,
        rate_limiter=RateLimiter(),
    )

    return dispatcher


def build_product_scraper_components() -> Tuple[BrowserConfig, CrawlerRunConfig]:
    """
    Build and return the crawler configuration objects for the product scraper.
    """
    browser_config = BrowserConfig(headless=True)
    run_config = CrawlerRunConfig(cache_mode=CacheMode.BYPASS, stream=True)

    return browser_config, run_config
