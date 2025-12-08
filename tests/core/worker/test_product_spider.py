import asyncio
import json
from unittest.mock import Mock, AsyncMock, patch
import pytest

from src.core.worker.product_spider import (
    parse_shop_message,
    crawl_and_classify_urls,
    handle_shop_message,
    process_message_batch,
)


def setup_mock_arun(results_to_yield):
    """Creates a mock async generator function for crawler.arun."""

    async def mock_generator():
        for result in results_to_yield:
            yield result

    async def mock_arun(*_args, **_kwargs):
        await asyncio.sleep(0)
        return mock_generator()

    return mock_arun


def create_mock_messages(count):
    """Creates a list of mock messages with unique domains."""
    messages = []
    for i in range(count):
        msg = Mock()
        msg.body = json.dumps({"domain": f"example{i}.com"})
        messages.append(msg)
    return messages


class TestParseShopMessage:
    """Tests for parse_shop_message function."""

    def test_parse_valid_message_with_domain_only(self):
        """Test parsing a valid message with only domain (constructs start_url automatically)."""
        message = Mock()
        message.body = json.dumps({"domain": "example.com"})

        domain, start_url = parse_shop_message(message)

        assert domain == "example.com"
        assert start_url == "https://example.com"

    def test_parse_message_with_https_prefix(self):
        """Test parsing a message where domain includes https:// prefix."""
        message = Mock()
        message.body = json.dumps({"domain": "https://example.com"})

        domain, start_url = parse_shop_message(message)

        assert domain == "example.com"
        assert start_url == "https://example.com"

    def test_parse_message_with_http_prefix(self):
        """Test parsing a message where domain includes http:// prefix."""
        message = Mock()
        message.body = json.dumps({"domain": "http://example.com"})

        domain, start_url = parse_shop_message(message)

        assert domain == "example.com"
        assert start_url == "https://example.com"

    def test_parse_message_with_trailing_slash(self):
        """Test parsing a message where domain has trailing slash."""
        message = Mock()
        message.body = json.dumps({"domain": "example.com/"})

        domain, start_url = parse_shop_message(message)

        assert domain == "example.com"
        assert start_url == "https://example.com"

    def test_parse_message_missing_domain(self):
        """Test parsing a message without domain."""
        message = Mock()
        message.body = json.dumps({"other_field": "value"})

        domain, start_url = parse_shop_message(message)

        assert domain is None
        assert start_url is None

    def test_parse_invalid_json(self):
        """Test parsing invalid JSON."""
        message = Mock()
        message.body = "not valid json"

        domain, start_url = parse_shop_message(message)

        assert domain is None
        assert start_url is None


class TestCrawlAndClassifyUrls:
    """Tests for crawl_and_classify_urls function."""

    @pytest.mark.asyncio
    async def test_crawl_and_classify_success(self):
        """Test successful crawling and classification."""
        crawler = Mock()

        result1 = Mock(success=True, url="https://example.com/product1")
        result2 = Mock(success=True, url="https://example.com/product2")

        crawler.arun = setup_mock_arun([result1, result2])

        classifier = Mock()
        classifier.classify_url = Mock(
            side_effect=[
                (True, 0.95),  # First URL is a product
                (False, 0.60),  # Second URL is not a product
            ]
        )

        db = Mock()
        db.batch_write_url_entries = Mock()

        run_config = Mock()
        shutdown_event = asyncio.Event()

        processed = await crawl_and_classify_urls(
            crawler=crawler,
            start_url="https://example.com",
            domain="example.com",
            classifier=classifier,
            db=db,
            shutdown_event=shutdown_event,
            run_config=run_config,
            batch_size=2,
        )

        assert processed == 2
        assert classifier.classify_url.call_count == 2
        assert db.batch_write_url_entries.call_count == 1

    @pytest.mark.asyncio
    async def test_crawl_with_failed_results(self):
        """Test crawling with some failed results."""
        crawler = Mock()

        result1 = Mock(success=True, url="https://example.com/product1")
        result2 = Mock(
            success=False,
            url="https://example.com/error",
            error_message="Connection timeout",
        )

        crawler.arun = setup_mock_arun([result1, result2])

        classifier = Mock()
        classifier.classify_url = Mock(return_value=(True, 0.95))

        db = Mock()
        db.batch_write_url_entries = Mock()

        run_config = Mock()
        shutdown_event = asyncio.Event()

        processed = await crawl_and_classify_urls(
            crawler=crawler,
            start_url="https://example.com",
            domain="example.com",
            classifier=classifier,
            db=db,
            shutdown_event=shutdown_event,
            run_config=run_config,
            batch_size=10,
        )

        assert processed == 2
        assert classifier.classify_url.call_count == 1
        assert db.batch_write_url_entries.call_count == 1

    @pytest.mark.asyncio
    async def test_crawl_with_shutdown(self):
        """Test crawling interrupted by shutdown event."""
        crawler = Mock()

        result1 = Mock(success=True, url="https://example.com/product1")

        crawler.arun = setup_mock_arun([result1])

        classifier = Mock()
        classifier.classify_url = Mock(return_value=(True, 0.95))

        db = Mock()
        db.batch_write_url_entries = Mock()

        run_config = Mock()
        shutdown_event = asyncio.Event()
        shutdown_event.set()

        processed = await crawl_and_classify_urls(
            crawler=crawler,
            start_url="https://example.com",
            domain="example.com",
            classifier=classifier,
            db=db,
            shutdown_event=shutdown_event,
            run_config=run_config,
            batch_size=10,
        )

        assert processed == 0

    @pytest.mark.asyncio
    async def test_crawl_batching_behavior(self):
        """Test that URLs are batched correctly before writing to database."""
        crawler = Mock()

        results = []
        for i in range(5):
            result = Mock(success=True, url=f"https://example.com/page{i}")
            results.append(result)

        crawler.arun = setup_mock_arun(results)

        classifier = Mock()
        classifier.classify_url = Mock(return_value=(True, 0.95))

        db = Mock()
        db.batch_write_url_entries = Mock()

        run_config = Mock()
        shutdown_event = asyncio.Event()

        processed = await crawl_and_classify_urls(
            crawler=crawler,
            start_url="https://example.com",
            domain="example.com",
            classifier=classifier,
            db=db,
            shutdown_event=shutdown_event,
            run_config=run_config,
            batch_size=2,
        )

        assert processed == 5
        assert db.batch_write_url_entries.call_count == 3


class TestHandleShopMessage:
    """Tests for handle_shop_message function."""

    @pytest.mark.asyncio
    async def test_handle_valid_message(self):
        """Test handling a valid shop message."""
        message = Mock()
        message.body = json.dumps({"domain": "example.com"})

        classifier = Mock()
        db = Mock()
        shutdown_event = asyncio.Event()

        with (
            patch(
                "src.core.worker.product_spider.AsyncWebCrawler"
            ) as mock_crawler_class,
            patch(
                "src.core.worker.product_spider.crawl_and_classify_urls",
                new_callable=AsyncMock,
                return_value=10,
            ) as mock_crawl,
            patch(
                "src.core.worker.product_spider.asyncio.to_thread",
                new_callable=AsyncMock,
            ) as mock_thread,
            patch("src.core.worker.product_spider.crawl_config"),
            patch("src.core.worker.product_spider.BrowserConfig"),
        ):
            mock_crawler_instance = AsyncMock()
            mock_crawler_class.return_value.__aenter__.return_value = (
                mock_crawler_instance
            )

            await handle_shop_message(
                message=message,
                classifier=classifier,
                db=db,
                shutdown_event=shutdown_event,
                batch_size=50,
            )

            mock_crawl.assert_called_once()
            assert mock_thread.call_count == 1

    @pytest.mark.asyncio
    async def test_handle_invalid_message(self):
        """Test handling an invalid message."""
        message = Mock()
        message.body = json.dumps({"invalid": "data"})

        classifier = Mock()
        db = Mock()
        shutdown_event = asyncio.Event()

        with patch(
            "src.core.worker.product_spider.asyncio.to_thread", new_callable=AsyncMock
        ) as mock_delete:
            await handle_shop_message(
                message=message,
                classifier=classifier,
                db=db,
                shutdown_event=shutdown_event,
                batch_size=50,
            )

            mock_delete.assert_called_once()

    @pytest.mark.asyncio
    async def test_handle_message_with_error(self):
        """Test handling a message where crawling fails."""
        message = Mock()
        message.body = json.dumps({"domain": "example.com"})

        classifier = Mock()
        db = Mock()
        shutdown_event = asyncio.Event()

        with (
            patch(
                "src.core.worker.product_spider.AsyncWebCrawler"
            ) as mock_crawler_class,
            patch(
                "src.core.worker.product_spider.crawl_and_classify_urls",
                new_callable=AsyncMock,
                side_effect=RuntimeError("Crawl error"),
            ),
            patch(
                "src.core.worker.product_spider.asyncio.to_thread",
                new_callable=AsyncMock,
            ) as mock_thread,
            patch("src.core.worker.product_spider.crawl_config"),
            patch("src.core.worker.product_spider.BrowserConfig"),
        ):
            mock_crawler_instance = AsyncMock()
            mock_crawler_class.return_value.__aenter__.return_value = (
                mock_crawler_instance
            )

            await handle_shop_message(
                message=message,
                classifier=classifier,
                db=db,
                shutdown_event=shutdown_event,
                batch_size=50,
            )

            assert mock_thread.call_count == 0


class TestProcessMessageBatch:
    """Tests for process_message_batch function."""

    @pytest.mark.asyncio
    async def test_process_empty_batch(self):
        """Test processing an empty message batch."""
        classifier = Mock()
        db = Mock()
        shutdown_event = asyncio.Event()

        await process_message_batch(
            messages=[],
            classifier=classifier,
            db=db,
            shutdown_event=shutdown_event,
            batch_size=50,
        )

    @pytest.mark.asyncio
    async def test_process_multiple_messages(self):
        """Test processing multiple messages in parallel."""
        messages = create_mock_messages(3)

        classifier = Mock()
        db = Mock()
        shutdown_event = asyncio.Event()

        with patch(
            "src.core.worker.product_spider.handle_shop_message", new_callable=AsyncMock
        ) as mock_handle:
            await process_message_batch(
                messages=messages,
                classifier=classifier,
                db=db,
                shutdown_event=shutdown_event,
                batch_size=50,
            )

            assert mock_handle.call_count == 3

    @pytest.mark.asyncio
    async def test_process_batch_with_shutdown(self):
        """Test processing messages when shutdown is triggered."""
        messages = create_mock_messages(3)

        classifier = Mock()
        db = Mock()
        shutdown_event = asyncio.Event()
        shutdown_event.set()

        with patch(
            "src.core.worker.product_spider.handle_shop_message", new_callable=AsyncMock
        ) as mock_handle:
            await process_message_batch(
                messages=messages,
                classifier=classifier,
                db=db,
                shutdown_event=shutdown_event,
                batch_size=50,
            )

            assert mock_handle.call_count == 0

    @pytest.mark.asyncio
    async def test_process_batch_handles_exceptions(self):
        """Test that exceptions in one message don't affect others."""
        messages = create_mock_messages(3)

        classifier = Mock()
        db = Mock()
        shutdown_event = asyncio.Event()

        async def mock_handle_side_effect(message, *_args, **_kwargs):
            await asyncio.sleep(0)
            if message == messages[1]:
                raise RuntimeError("Processing error")

        with patch(
            "src.core.worker.product_spider.handle_shop_message",
            new_callable=AsyncMock,
            side_effect=mock_handle_side_effect,
        ) as mock_handle:
            await process_message_batch(
                messages=messages,
                classifier=classifier,
                db=db,
                shutdown_event=shutdown_event,
                batch_size=50,
            )

            assert mock_handle.call_count == 3
