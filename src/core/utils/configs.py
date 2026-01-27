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
        max_depth=100,  # Deep enough to reach all pages on most websites
        max_pages=999999,  # Effectively unlimited - crawl entire website
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
    """Create a MemoryAdaptiveDispatcher with proper rate limiting."""
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
    excluded_tags = [
        "nav",
        "footer",
        "header",
        "navbar",
        "navigation",
        "site-header",
        "site-footer",
        "aside",
        "form",
        "button",
        "input",
        "noscript",
        "script",
        "style",
        "canvas",
        "video",
        "audio",
        "advertisement",
        "ads",
        "cookie-banner",
        "popup",
        "subscribe-modal",
        "rcb-banner",
        "rcb-overlay",
    ]

    run_config = CrawlerRunConfig(
        cache_mode=CacheMode.BYPASS,
        check_robots_txt=True,
        page_timeout=60000,
        excluded_tags=excluded_tags,
        verbose=False,
    )
    browser_config = BrowserConfig(
        headless=True,
    )

    return browser_config, run_config
