import asyncio
import pytest
from typing import Dict, List, Optional, Any
from crawl4ai.utils import normalize_url_for_deep_crawl
from src.core.utils.url_filters import (
    ExtensionExcludeBFSStrategy,
    MultipleExtensionExcludeBFSStrategy,
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
        self.links = {
            "internal": internal_links or [],
            "external": external_links or [],
        }
        self.metadata = {}


class MockCrawler:
    """A simple mock of AsyncWebCrawler that responds to arun_many."""

    def __init__(self, responses: Dict[str, DummyResult]):
        self.responses = responses

    async def arun_many(self, urls: List[str], config: Any = None) -> List[DummyResult]:
        await asyncio.sleep(0)
        results = []
        for url in urls:
            if url in self.responses:
                results.append(self.responses[url])
                continue

            try:
                norm = normalize_url_for_deep_crawl(url, url)
                if norm in self.responses:
                    results.append(self.responses[norm])
                    continue
            except (ValueError, TypeError, KeyError):
                pass

            if url.endswith("/"):
                alt = url.rstrip("/")
            else:
                alt = url + "/"

            if alt in self.responses:
                results.append(self.responses[alt])
            else:
                results.append(DummyResult(url, success=False))
        return results


class DummyConfig:
    def __init__(self):
        self.stream = False
        self.deep_crawl_strategy = None

    def clone(self, **kwargs: Any) -> "DummyConfig":
        new_config = DummyConfig()
        new_config.stream = kwargs.get("stream", False)
        new_config.deep_crawl_strategy = kwargs.get("deep_crawl_strategy", None)
        return new_config


@pytest.mark.asyncio
async def test_extension_exclude_strategy_filters_images():
    """Test that ExtensionExcludeBFSStrategy filters out image extensions."""
    strategy = ExtensionExcludeBFSStrategy(
        exclude_extensions=["jpg", "png", "gif"], max_depth=2, include_external=False
    )

    responses = {
        "https://example.com/": DummyResult(
            "https://example.com/",
            success=True,
            internal_links=[
                {"href": "https://example.com/page1"},
                {"href": "https://example.com/image.jpg"},
                {"href": "https://example.com/photo.png"},
                {"href": "https://example.com/page2"},
            ],
        ),
        "https://example.com/page1": DummyResult(
            "https://example.com/page1", success=True
        ),
        "https://example.com/page2": DummyResult(
            "https://example.com/page2", success=True
        ),
    }

    crawler = MockCrawler(responses)
    config = DummyConfig()

    discovered = await strategy.arun(
        start_url="https://example.com/",
        crawler=crawler,  # type: ignore[arg-type]
        config=config,  # type: ignore[arg-type]
    )

    # Should include base URL and page1, page2 but NOT image.jpg or photo.png
    # Note: URLs are normalized, so 'https://example.com/' becomes 'https://example.com'
    assert "https://example.com" in discovered
    assert "https://example.com/page1" in discovered
    assert "https://example.com/page2" in discovered
    assert "https://example.com/image.jpg" not in discovered
    assert "https://example.com/photo.png" not in discovered


@pytest.mark.asyncio
async def test_extension_exclude_strategy_filters_documents():
    """Test that ExtensionExcludeBFSStrategy filters out document extensions."""
    strategy = ExtensionExcludeBFSStrategy(
        exclude_extensions=["pdf", "doc", "docx", "zip"],
        max_depth=2,
        include_external=False,
    )

    responses = {
        "https://example.com/": DummyResult(
            "https://example.com/",
            success=True,
            internal_links=[
                {"href": "https://example.com/products"},
                {"href": "https://example.com/manual.pdf"},
                {"href": "https://example.com/report.docx"},
                {"href": "https://example.com/archive.zip"},
            ],
        ),
        "https://example.com/products": DummyResult(
            "https://example.com/products", success=True
        ),
    }

    crawler = MockCrawler(responses)
    config = DummyConfig()

    discovered = await strategy.arun(
        start_url="https://example.com/",
        crawler=crawler,  # type: ignore[arg-type]
        config=config,  # type: ignore[arg-type]
    )

    # Should include base URL and products but NOT pdf, docx, zip
    assert "https://example.com" in discovered
    assert "https://example.com/products" in discovered
    assert "https://example.com/manual.pdf" not in discovered
    assert "https://example.com/report.docx" not in discovered
    assert "https://example.com/archive.zip" not in discovered


@pytest.mark.asyncio
async def test_extension_exclude_case_insensitive():
    """Test that extension filtering is case-insensitive."""
    strategy = ExtensionExcludeBFSStrategy(
        exclude_extensions=["JPG", "PDF"], max_depth=2, include_external=False
    )

    responses = {
        "https://example.com/": DummyResult(
            "https://example.com/",
            success=True,
            internal_links=[
                {"href": "https://example.com/image.jpg"},
                {"href": "https://example.com/IMAGE.JPG"},
                {"href": "https://example.com/doc.pdf"},
                {"href": "https://example.com/DOC.PDF"},
                {"href": "https://example.com/page"},
            ],
        ),
        "https://example.com/page": DummyResult(
            "https://example.com/page", success=True
        ),
    }

    crawler = MockCrawler(responses)
    config = DummyConfig()

    discovered = await strategy.arun(
        start_url="https://example.com/",
        crawler=crawler,  # type: ignore[arg-type]
        config=config,  # type: ignore[arg-type]
    )

    # Should only include base URL and page
    assert len(discovered) == 2
    assert "https://example.com" in discovered
    assert "https://example.com/page" in discovered


@pytest.mark.asyncio
async def test_extension_exclude_with_leading_dot():
    """Test that extensions with leading dots are handled correctly."""
    strategy = ExtensionExcludeBFSStrategy(
        exclude_extensions=[".jpg", "png"],  # Mixed: with and without dot
        max_depth=2,
        include_external=False,
    )

    responses = {
        "https://example.com/": DummyResult(
            "https://example.com/",
            success=True,
            internal_links=[
                {"href": "https://example.com/photo.jpg"},
                {"href": "https://example.com/image.png"},
                {"href": "https://example.com/page"},
            ],
        ),
        "https://example.com/page": DummyResult(
            "https://example.com/page", success=True
        ),
    }

    crawler = MockCrawler(responses)
    config = DummyConfig()

    discovered = await strategy.arun(
        start_url="https://example.com/",
        crawler=crawler,  # type: ignore[arg-type]
        config=config,  # type: ignore[arg-type]
    )

    # Both jpg and png should be filtered regardless of leading dot in config
    assert "https://example.com" in discovered
    assert "https://example.com/page" in discovered
    assert "https://example.com/photo.jpg" not in discovered
    assert "https://example.com/image.png" not in discovered


@pytest.mark.asyncio
async def test_extension_exclude_no_extensions():
    """Test that strategy works without excluded extensions."""
    strategy = ExtensionExcludeBFSStrategy(
        exclude_extensions=None, max_depth=2, include_external=False
    )

    responses = {
        "https://example.com/": DummyResult(
            "https://example.com/",
            success=True,
            internal_links=[
                {"href": "https://example.com/image.jpg"},
                {"href": "https://example.com/page"},
            ],
        ),
        "https://example.com/image.jpg": DummyResult(
            "https://example.com/image.jpg", success=True
        ),
        "https://example.com/page": DummyResult(
            "https://example.com/page", success=True
        ),
    }

    crawler = MockCrawler(responses)
    config = DummyConfig()

    discovered = await strategy.arun(
        start_url="https://example.com/",
        crawler=crawler,  # type: ignore[arg-type]
        config=config,  # type: ignore[arg-type]
    )

    # Should include all URLs when no extensions are excluded
    assert "https://example.com" in discovered
    assert "https://example.com/image.jpg" in discovered
    assert "https://example.com/page" in discovered


@pytest.mark.asyncio
async def test_multiple_extension_exclude_with_patterns():
    """Test MultipleExtensionExcludeBFSStrategy with both extensions and patterns."""
    strategy = MultipleExtensionExcludeBFSStrategy(
        exclude_extensions=["jpg", "png"],
        exclude_patterns=["*/download/*", "*/media/*"],
        max_depth=2,
        include_external=False,
    )

    responses = {
        "https://example.com/": DummyResult(
            "https://example.com/",
            success=True,
            internal_links=[
                {"href": "https://example.com/products"},
                {"href": "https://example.com/image.jpg"},
                {"href": "https://example.com/download/file.pdf"},
                {"href": "https://example.com/media/video.mp4"},
                {"href": "https://example.com/about"},
            ],
        ),
        "https://example.com/products": DummyResult(
            "https://example.com/products", success=True
        ),
        "https://example.com/about": DummyResult(
            "https://example.com/about", success=True
        ),
    }

    crawler = MockCrawler(responses)
    config = DummyConfig()

    discovered = await strategy.arun(
        start_url="https://example.com/",
        crawler=crawler,  # type: ignore[arg-type]
        config=config,  # type: ignore[arg-type]
    )

    # Should include base, products, about
    assert "https://example.com" in discovered
    assert "https://example.com/products" in discovered
    assert "https://example.com/about" in discovered

    # Should exclude image (extension) and download/media (patterns)
    assert "https://example.com/image.jpg" not in discovered
    assert "https://example.com/download/file.pdf" not in discovered
    assert "https://example.com/media/video.mp4" not in discovered


@pytest.mark.asyncio
async def test_multiple_extension_exclude_pattern_matching():
    """Test that pattern matching works correctly with wildcards."""
    strategy = MultipleExtensionExcludeBFSStrategy(
        exclude_extensions=[],
        exclude_patterns=["*/admin/*", "*/api/*", "*/temp*"],
        max_depth=2,
        include_external=False,
    )

    responses = {
        "https://example.com/": DummyResult(
            "https://example.com/",
            success=True,
            internal_links=[
                {"href": "https://example.com/products"},
                {"href": "https://example.com/admin/dashboard"},
                {"href": "https://example.com/api/users"},
                {"href": "https://example.com/temporary"},
                {"href": "https://example.com/temp-file"},
            ],
        ),
        "https://example.com/products": DummyResult(
            "https://example.com/products", success=True
        ),
    }

    crawler = MockCrawler(responses)
    config = DummyConfig()

    discovered = await strategy.arun(
        start_url="https://example.com/",
        crawler=crawler,  # type: ignore[arg-type]
        config=config,  # type: ignore[arg-type]
    )

    # Should only include base and products
    assert "https://example.com" in discovered
    assert "https://example.com/products" in discovered

    # Should exclude admin, api, and temp* patterns
    assert "https://example.com/admin/dashboard" not in discovered
    assert "https://example.com/api/users" not in discovered
    assert "https://example.com/temporary" not in discovered
    assert "https://example.com/temp-file" not in discovered


@pytest.mark.asyncio
async def test_can_process_url_directly():
    """Test the can_process_url method directly."""
    strategy = ExtensionExcludeBFSStrategy(
        exclude_extensions=["jpg", "pdf"], max_depth=3, include_external=False
    )

    # URLs that should be processed
    assert await strategy.can_process_url("https://example.com/page", 0) is True
    assert await strategy.can_process_url("https://example.com/page", 1) is True
    assert await strategy.can_process_url("https://example.com/products", 2) is True
    assert await strategy.can_process_url("https://example.com/products", 3) is True

    # URLs that should be filtered (extensions)
    assert await strategy.can_process_url("https://example.com/image.jpg", 1) is False
    assert await strategy.can_process_url("https://example.com/doc.pdf", 1) is False
    assert await strategy.can_process_url("https://example.com/photo.JPG", 1) is False

    # Test max_depth boundary: In BFS implementation, links at depth > max_depth are excluded
    # The check in code is: if next_depth > self.max_depth: continue
    # So with max_depth=3, depth 4 would be rejected when trying to add links FROM depth 3
    # But can_process_url itself might not enforce this the same way
    # Let's test that extensions are still filtered at any depth
    assert await strategy.can_process_url("https://example.com/image.jpg", 10) is False


@pytest.mark.asyncio
async def test_multiple_extension_exclude_empty_filters():
    """Test MultipleExtensionExcludeBFSStrategy with no filters."""
    strategy = MultipleExtensionExcludeBFSStrategy(
        exclude_extensions=None,
        exclude_patterns=None,
        max_depth=2,
        include_external=False,
    )

    responses = {
        "https://example.com/": DummyResult(
            "https://example.com/",
            success=True,
            internal_links=[
                {"href": "https://example.com/page1"},
                {"href": "https://example.com/image.jpg"},
            ],
        ),
        "https://example.com/page1": DummyResult(
            "https://example.com/page1", success=True
        ),
        "https://example.com/image.jpg": DummyResult(
            "https://example.com/image.jpg", success=True
        ),
    }

    crawler = MockCrawler(responses)
    config = DummyConfig()

    discovered = await strategy.arun(
        start_url="https://example.com/",
        crawler=crawler,  # type: ignore[arg-type]
        config=config,  # type: ignore[arg-type]
    )

    # Should include all URLs when no filters are set
    assert len(discovered) == 3
    assert "https://example.com" in discovered
    assert "https://example.com/page1" in discovered
    assert "https://example.com/image.jpg" in discovered
