from src.core.utils import spider_config
from crawl4ai import CacheMode
import math


def test_crawl_config_basic():
    """crawl_config returns an object with expected top-level settings."""
    cfg = spider_config.crawl_config()

    assert cfg is not None
    assert hasattr(cfg, "deep_crawl_strategy")
    assert hasattr(cfg, "scraping_strategy")
    assert cfg.cache_mode == CacheMode.BYPASS
    assert getattr(cfg, "stream", None) is True
    assert getattr(cfg, "check_robots_txt", None) is True


def test_deep_crawl_strategy_settings():
    """The deep crawl strategy contains sensible defaults (max_depth, includes/excludes)."""
    cfg = spider_config.crawl_config()
    strat = cfg.deep_crawl_strategy

    assert strat is not None
    assert hasattr(strat, "max_depth")
    assert getattr(strat, "max_depth") == 100  # Updated for full website crawling
    assert hasattr(strat, "max_pages")
    assert (
        getattr(strat, "max_pages") == 999999
    )  # Effectively unlimited for full crawling
    assert hasattr(strat, "include_external")
    assert getattr(strat, "include_external") is False

    # exclude patterns/extensions may be stored under different attribute names
    excl_patterns = getattr(strat, "exclude_patterns", None) or getattr(
        strat, "_exclude_patterns", None
    )
    assert isinstance(excl_patterns, (list, tuple))
    assert len(excl_patterns) >= 1

    excl_ext = getattr(strat, "exclude_extensions", None) or getattr(
        strat, "_exclude_extensions", None
    )
    assert excl_ext is not None
    # normalize and ensure some common extensions are present
    normalized = [e.lstrip(".").lower() for e in excl_ext]
    assert "jpg" in normalized or "jpeg" in normalized


def test_crawl_dispatcher_settings():
    """crawl_dispatcher returns a dispatcher that exposes expected attributes."""
    dispatcher = spider_config.crawl_dispatcher()

    assert dispatcher is not None
    assert hasattr(dispatcher, "memory_threshold_percent")
    assert math.isclose(
        getattr(dispatcher, "memory_threshold_percent"), 80.0, rel_tol=1e-9
    )
    assert hasattr(dispatcher, "rate_limiter")
    assert getattr(dispatcher, "rate_limiter") is not None
