import asyncio
import json
from unittest.mock import patch, mock_open
import pytest
from typing import Dict, List, Optional, Any

from crawl4ai.utils import normalize_url_for_deep_crawl
from src.core.utils.spider import main


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
    """Mock AsyncWebCrawler for testing."""

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

    async def __aenter__(self):
        return self

    async def __aexit__(self, exc_type, exc_val, exc_tb):
        pass


@pytest.mark.asyncio
async def test_spider_main_excludes_image_extensions():
    """Test that spider excludes common image file extensions."""
    responses = {
        "https://example.com/": DummyResult(
            "https://example.com/",
            success=True,
            internal_links=[
                {"href": "https://example.com/products"},
                {"href": "https://example.com/image.jpg"},
                {"href": "https://example.com/photo.png"},
                {"href": "https://example.com/logo.svg"},
            ],
        ),
        "https://example.com/products": DummyResult(
            "https://example.com/products", success=True
        ),
    }

    mock_crawler = MockCrawler(responses)

    with patch("src.core.utils.spider.AsyncWebCrawler", return_value=mock_crawler):
        with patch("builtins.open", mock_open()) as mock_file:
            with patch("pathlib.Path.mkdir"):
                with patch("builtins.print"):
                    await main("https://example.com/")

                    # Check that file was written
                    mock_file.assert_called()
                    handle = mock_file.return_value
                    written_data = "".join(
                        call.args[0] for call in handle.write.call_args_list
                    )

                    # Parse the written JSON
                    discovered = json.loads(written_data)

                    # Should include base URL and products
                    assert "https://example.com" in discovered
                    assert "https://example.com/products" in discovered

                    # Should NOT include images
                    assert "https://example.com/image.jpg" not in discovered
                    assert "https://example.com/photo.png" not in discovered
                    assert "https://example.com/logo.svg" not in discovered


@pytest.mark.asyncio
async def test_spider_main_excludes_video_extensions():
    """Test that spider excludes common video file extensions."""
    responses = {
        "https://example.com/": DummyResult(
            "https://example.com/",
            success=True,
            internal_links=[
                {"href": "https://example.com/about"},
                {"href": "https://example.com/video.mp4"},
                {"href": "https://example.com/promo.avi"},
            ],
        ),
        "https://example.com/about": DummyResult(
            "https://example.com/about", success=True
        ),
    }

    mock_crawler = MockCrawler(responses)

    with patch("src.core.utils.spider.AsyncWebCrawler", return_value=mock_crawler):
        with patch("builtins.open", mock_open()) as mock_file:
            with patch("pathlib.Path.mkdir"):
                with patch("builtins.print"):
                    await main("https://example.com/")

                    handle = mock_file.return_value
                    written_data = "".join(
                        call.args[0] for call in handle.write.call_args_list
                    )
                    discovered = json.loads(written_data)

                    # Should include base URL and about
                    assert "https://example.com" in discovered
                    assert "https://example.com/about" in discovered

                    # Should NOT include videos
                    assert "https://example.com/video.mp4" not in discovered
                    assert "https://example.com/promo.avi" not in discovered


@pytest.mark.asyncio
async def test_spider_main_excludes_document_extensions():
    """Test that spider excludes common document file extensions."""
    responses = {
        "https://example.com/": DummyResult(
            "https://example.com/",
            success=True,
            internal_links=[
                {"href": "https://example.com/contact"},
                {"href": "https://example.com/manual.pdf"},
                {"href": "https://example.com/catalog.docx"},
                {"href": "https://example.com/prices.xlsx"},
            ],
        ),
        "https://example.com/contact": DummyResult(
            "https://example.com/contact", success=True
        ),
    }

    mock_crawler = MockCrawler(responses)

    with patch("src.core.utils.spider.AsyncWebCrawler", return_value=mock_crawler):
        with patch("builtins.open", mock_open()) as mock_file:
            with patch("pathlib.Path.mkdir"):
                with patch("builtins.print"):
                    await main("https://example.com/")

                    handle = mock_file.return_value
                    written_data = "".join(
                        call.args[0] for call in handle.write.call_args_list
                    )
                    discovered = json.loads(written_data)

                    # Should include base URL and contact
                    assert "https://example.com" in discovered
                    assert "https://example.com/contact" in discovered

                    # Should NOT include documents
                    assert "https://example.com/manual.pdf" not in discovered
                    assert "https://example.com/catalog.docx" not in discovered
                    assert "https://example.com/prices.xlsx" not in discovered


@pytest.mark.asyncio
async def test_spider_main_excludes_archive_extensions():
    """Test that spider excludes common archive file extensions."""
    responses = {
        "https://example.com/": DummyResult(
            "https://example.com/",
            success=True,
            internal_links=[
                {"href": "https://example.com/shop"},
                {"href": "https://example.com/backup.zip"},
                {"href": "https://example.com/data.tar.gz"},
            ],
        ),
        "https://example.com/shop": DummyResult(
            "https://example.com/shop", success=True
        ),
    }

    mock_crawler = MockCrawler(responses)

    with patch("src.core.utils.spider.AsyncWebCrawler", return_value=mock_crawler):
        with patch("builtins.open", mock_open()) as mock_file:
            with patch("pathlib.Path.mkdir"):
                with patch("builtins.print"):
                    await main("https://example.com/")

                    handle = mock_file.return_value
                    written_data = "".join(
                        call.args[0] for call in handle.write.call_args_list
                    )
                    discovered = json.loads(written_data)

                    # Should include base URL and shop
                    assert "https://example.com" in discovered
                    assert "https://example.com/shop" in discovered

                    # Should NOT include archives
                    assert "https://example.com/backup.zip" not in discovered
                    assert "https://example.com/data.tar.gz" not in discovered


@pytest.mark.asyncio
async def test_spider_main_excludes_asset_extensions():
    """Test that spider excludes common asset file extensions (CSS, JS, fonts)."""
    responses = {
        "https://example.com/": DummyResult(
            "https://example.com/",
            success=True,
            internal_links=[
                {"href": "https://example.com/services"},
                {"href": "https://example.com/style.css"},
                {"href": "https://example.com/app.js"},
                {"href": "https://example.com/font.woff2"},
                {"href": "https://example.com/favicon.ico"},
            ],
        ),
        "https://example.com/services": DummyResult(
            "https://example.com/services", success=True
        ),
    }

    mock_crawler = MockCrawler(responses)

    with patch("src.core.utils.spider.AsyncWebCrawler", return_value=mock_crawler):
        with patch("builtins.open", mock_open()) as mock_file:
            with patch("pathlib.Path.mkdir"):
                with patch("builtins.print"):
                    await main("https://example.com/")

                    handle = mock_file.return_value
                    written_data = "".join(
                        call.args[0] for call in handle.write.call_args_list
                    )
                    discovered = json.loads(written_data)

                    # Should include base URL and services
                    assert "https://example.com" in discovered
                    assert "https://example.com/services" in discovered

                    # Should NOT include assets
                    assert "https://example.com/style.css" not in discovered
                    assert "https://example.com/app.js" not in discovered
                    assert "https://example.com/font.woff2" not in discovered
                    assert "https://example.com/favicon.ico" not in discovered


@pytest.mark.asyncio
async def test_spider_main_saves_to_correct_file():
    """Test that spider saves discovered URLs to the correct output file."""
    responses = {
        "https://example.com/": DummyResult(
            "https://example.com/",
            success=True,
            internal_links=[{"href": "https://example.com/page1"}],
        ),
        "https://example.com/page1": DummyResult(
            "https://example.com/page1", success=True
        ),
    }

    mock_crawler = MockCrawler(responses)

    with patch("src.core.utils.spider.AsyncWebCrawler", return_value=mock_crawler):
        with patch("builtins.open", mock_open()) as mock_file:
            with patch("pathlib.Path.mkdir") as mock_mkdir:
                with patch("builtins.print"):
                    await main("https://example.com/")

                    # Check that data directory was created
                    mock_mkdir.assert_called_once_with(exist_ok=True)

                    # Check that file was opened with correct path
                    call_args = mock_file.call_args[0]
                    output_path = call_args[0]
                    assert str(output_path).endswith("crawled_url_filtered.json")


@pytest.mark.asyncio
async def test_spider_main_prints_status_messages():
    """Test that spider prints appropriate status messages during execution."""
    responses = {
        "https://example.com/": DummyResult(
            "https://example.com/", success=True, internal_links=[]
        ),
    }

    mock_crawler = MockCrawler(responses)

    with patch("src.core.utils.spider.AsyncWebCrawler", return_value=mock_crawler):
        with patch("builtins.open", mock_open()):
            with patch("pathlib.Path.mkdir"):
                with patch("builtins.print") as mock_print:
                    await main("https://example.com/")

                    # Check that key messages were printed
                    print_calls = [str(call) for call in mock_print.call_args_list]
                    printed_text = " ".join(print_calls)

                    assert "Starting deep crawl" in printed_text or any(
                        "Starting" in str(call) for call in print_calls
                    )
                    assert any("Done" in str(call) for call in print_calls)
                    assert any("URLs saved" in str(call) for call in print_calls)


@pytest.mark.asyncio
async def test_spider_main_handles_multiple_pages():
    """Test that spider correctly crawls and saves multiple pages."""
    responses = {
        "https://example.com/": DummyResult(
            "https://example.com/",
            success=True,
            internal_links=[
                {"href": "https://example.com/page1"},
                {"href": "https://example.com/page2"},
                {"href": "https://example.com/page3"},
            ],
        ),
        "https://example.com/page1": DummyResult(
            "https://example.com/page1", success=True
        ),
        "https://example.com/page2": DummyResult(
            "https://example.com/page2", success=True
        ),
        "https://example.com/page3": DummyResult(
            "https://example.com/page3", success=True
        ),
    }

    mock_crawler = MockCrawler(responses)

    with patch("src.core.utils.spider.AsyncWebCrawler", return_value=mock_crawler):
        with patch("builtins.open", mock_open()) as mock_file:
            with patch("pathlib.Path.mkdir"):
                with patch("builtins.print"):
                    await main("https://example.com/")

                    handle = mock_file.return_value
                    written_data = "".join(
                        call.args[0] for call in handle.write.call_args_list
                    )
                    discovered = json.loads(written_data)

                    # Should have found all 4 pages
                    assert len(discovered) == 4
                    assert "https://example.com" in discovered
                    assert "https://example.com/page1" in discovered
                    assert "https://example.com/page2" in discovered
                    assert "https://example.com/page3" in discovered


@pytest.mark.asyncio
async def test_spider_main_case_insensitive_extension_filtering():
    """Test that extension filtering is case-insensitive."""
    responses = {
        "https://example.com/": DummyResult(
            "https://example.com/",
            success=True,
            internal_links=[
                {"href": "https://example.com/page"},
                {"href": "https://example.com/IMAGE.JPG"},
                {"href": "https://example.com/photo.PNG"},
                {"href": "https://example.com/Doc.PDF"},
            ],
        ),
        "https://example.com/page": DummyResult(
            "https://example.com/page", success=True
        ),
    }

    mock_crawler = MockCrawler(responses)

    with patch("src.core.utils.spider.AsyncWebCrawler", return_value=mock_crawler):
        with patch("builtins.open", mock_open()) as mock_file:
            with patch("pathlib.Path.mkdir"):
                with patch("builtins.print"):
                    await main("https://example.com/")

                    handle = mock_file.return_value
                    written_data = "".join(
                        call.args[0] for call in handle.write.call_args_list
                    )
                    discovered = json.loads(written_data)

                    # Should include only base and page
                    assert "https://example.com" in discovered
                    assert "https://example.com/page" in discovered

                    # Should exclude files regardless of case
                    assert "https://example.com/IMAGE.JPG" not in discovered
                    assert "https://example.com/photo.PNG" not in discovered
                    assert "https://example.com/Doc.PDF" not in discovered
