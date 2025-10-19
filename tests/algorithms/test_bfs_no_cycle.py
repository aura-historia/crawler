import asyncio
import pytest
from typing import Dict, List, Optional, Union, AsyncGenerator
from crawl4ai import CrawlerRunConfig
from crawl4ai.utils import normalize_url_for_deep_crawl
from src.core.algorithms.bfs_no_cycle_deep_crawl_strategy import (
    BFSNoCycleDeepCrawlStrategy,
)


class DummyResult:
    def __init__(
        self,
        url: str,
        success: bool = True,
        internal_links: Optional[List[Dict]] = None,
        external_links: Optional[List[Dict]] = None,
    ):
        self.url = url
        self.success = success
        # links format matches what strategy expects: dict with 'internal' and 'external' lists of dicts
        self.links = {
            "internal": internal_links or [],
            "external": external_links or [],
        }
        self.metadata = {}


class MockCrawler:
    """A simple mock of AsyncWebCrawler that responds to arun_many.
    It maps input URLs to DummyResult instances predefined in `responses`.
    """

    def __init__(self, responses: Dict[str, DummyResult]):
        # responses: dict url -> DummyResult
        self.responses = responses

    async def arun_many(
        self, urls: List[str], config: CrawlerRunConfig
    ) -> Union[List[DummyResult], AsyncGenerator]:
        # For batch mode return list of DummyResult
        await asyncio.sleep(0)  # ensure this is async
        results = []
        # Use existing normalization utility
        for url in urls:
            # direct match
            if url in self.responses:
                results.append(self.responses[url])
                continue

            # try normalized form
            try:
                norm = normalize_url_for_deep_crawl(url, url)
                if norm in self.responses:
                    results.append(self.responses[norm])
                    continue
            except (ValueError, TypeError, KeyError):
                pass

            # try trailing slash variants
            if url.endswith("/"):
                alt = url.rstrip("/")
            else:
                alt = url + "/"

            if alt in self.responses:
                results.append(self.responses[alt])
                continue

            # fallback: unknown -> unsuccessful result
            results.append(DummyResult(url, success=False))

        # If streaming requested, return an async generator that yields results
        if getattr(config, "stream", False):

            async def gen():
                for result in results:
                    await asyncio.sleep(0)
                    yield result

            return gen()

        return results


class Cfg:
    """Minimal config object with clone method as expected by strategy."""

    def __init__(self, stream: bool = False, deep_crawl_strategy=None):
        self.stream = stream
        self.deep_crawl_strategy = deep_crawl_strategy

    def clone(self, deep_crawl_strategy=None, stream: bool = False) -> "Cfg":
        cfg = Cfg(stream=stream)
        cfg.deep_crawl_strategy = deep_crawl_strategy
        return cfg


@pytest.mark.asyncio
async def test_bfs_no_cycle_avoids_cycles_and_returns_all_links():
    # Setup a small graph with a cycle: A -> B -> C -> A and A -> D
    url_a = "https://example.com/"
    url_b = "https://example.com/b"
    url_c = "https://example.com/c"
    url_d = "https://example.com/d"

    responses = {
        url_a: DummyResult(url_a, internal_links=[{"href": url_b}, {"href": url_d}]),
        url_b: DummyResult(url_b, internal_links=[{"href": url_c}]),
        url_c: DummyResult(url_c, internal_links=[{"href": url_a}]),
        url_d: DummyResult(url_d, internal_links=[]),
    }

    mock_crawler = MockCrawler(responses)

    strategy = BFSNoCycleDeepCrawlStrategy(
        max_depth=10, include_external=False, max_pages=100
    )

    cfg = Cfg(stream=False, deep_crawl_strategy=strategy)

    result = await strategy._arun_batch(
        start_url=url_a, crawler=mock_crawler, config=cfg
    )

    # result is a list of discovered normalized URLs
    assert isinstance(result, list)
    # Should contain A, B, C, D exactly once
    # Normalize expected URLs the same way the strategy does
    expected = {
        normalize_url_for_deep_crawl(url, url_a) for url in [url_a, url_b, url_c, url_d]
    }
    assert set(result) == expected
    assert len(result) == 4
