import asyncio
import json
from unittest.mock import AsyncMock, Mock, patch

import pytest

from src.core.worker.product_spider import (
    parse_shop_message,
    crawl_and_classify_urls,
    handle_shop_message,
    worker,
)


def setup_mock_arun(results_to_yield):
    """
    Create a mock async generator function for crawler.arun.

    Parameters:
        results_to_yield (list): List of mock result objects to yield.

    Returns:
        callable: Async function that returns an async generator.
    """

    async def mock_generator():
        for result in results_to_yield:
            yield result

    async def mock_arun(*_args, **_kwargs):
        await asyncio.sleep(0)
        return mock_generator()

    return mock_arun


def create_mock_messages(count):
    """
    Create a list of mock SQS messages with unique domains.

    Parameters:
        count (int): Number of mock messages to create.

    Returns:
        list: List of Mock objects representing SQS messages.
    """
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
        """Test handling a valid shop message with successful crawl."""
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
                return_value=10,  # Successfully processed 10 URLs
            ) as mock_crawl,
            patch(
                "src.core.worker.product_spider.asyncio.to_thread",
                new_callable=AsyncMock,
            ) as mock_thread,
            patch("src.core.worker.product_spider.crawl_config"),
            patch("src.core.worker.product_spider.BrowserConfig"),
            patch("src.core.worker.product_spider.delete_message") as mock_delete,
        ):
            mock_thread.side_effect = lambda func, *args, **kwargs: func(
                *args, **kwargs
            )
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
            # Should be called twice: once at start, once at end (when processed_count > 1)
            assert db.update_shop_metadata.call_count == 2
            # Message should be deleted because processed_count > 1
            mock_delete.assert_called_once_with(message)

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
            ) as mock_crawl,
            patch(
                "src.core.worker.product_spider.asyncio.to_thread",
                new_callable=AsyncMock,
            ) as mock_thread,
            patch("src.core.worker.product_spider.crawl_config"),
            patch("src.core.worker.product_spider.BrowserConfig"),
            patch("src.core.worker.product_spider.delete_message") as mock_delete,
        ):
            mock_thread.side_effect = lambda func, *args, **kwargs: func(
                *args, **kwargs
            )
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
            # Should be called once at start before error occurs
            db.update_shop_metadata.assert_called_once()
            # Message should NOT be deleted on error
            mock_delete.assert_not_called()

    @pytest.mark.asyncio
    async def test_handle_message_no_urls_found(self):
        """Test handling a message when no URLs are found during crawl."""
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
                return_value=0,  # No URLs found
            ) as mock_crawl,
            patch(
                "src.core.worker.product_spider.asyncio.to_thread",
                new_callable=AsyncMock,
            ) as mock_thread,
            patch("src.core.worker.product_spider.crawl_config"),
            patch("src.core.worker.product_spider.BrowserConfig"),
            patch("src.core.worker.product_spider.delete_message") as mock_delete,
        ):
            mock_thread.side_effect = lambda func, *args, **kwargs: func(
                *args, **kwargs
            )
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
            # Should be called once at start (but not at end since processed_count <= 1)
            db.update_shop_metadata.assert_called_once()
            # Message should NOT be deleted when no URLs found (processed_count = 0)
            mock_delete.assert_not_called()

    @pytest.mark.asyncio
    async def test_handle_message_single_url_found(self):
        """Test handling a message when only 1 URL is found (boundary case)."""
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
                return_value=1,  # Only 1 URL found (boundary case)
            ) as mock_crawl,
            patch(
                "src.core.worker.product_spider.asyncio.to_thread",
                new_callable=AsyncMock,
            ) as mock_thread,
            patch("src.core.worker.product_spider.crawl_config"),
            patch("src.core.worker.product_spider.BrowserConfig"),
            patch("src.core.worker.product_spider.delete_message") as mock_delete,
        ):
            mock_thread.side_effect = lambda func, *args, **kwargs: func(
                *args, **kwargs
            )
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
            # Should be called once at start (but not at end since processed_count <= 1)
            db.update_shop_metadata.assert_called_once()
            # Message should NOT be deleted when processed_count <= 1
            mock_delete.assert_not_called()


class TestWorker:
    """Tests for worker function using generic_worker."""

    @pytest.mark.asyncio
    async def test_worker_processes_messages(self):
        """Test that worker processes shop messages correctly."""
        worker_id = 1
        queue = Mock()
        classifier = Mock()
        db = Mock()
        batch_size = 50
        message = Mock()
        message.body = json.dumps({"domain": "example.com"})

        with (
            patch(
                "src.core.worker.product_spider.generic_worker", new_callable=AsyncMock
            ) as mock_generic_worker,
            patch(
                "src.core.worker.product_spider.shutdown_event"
            ) as mock_shutdown_event,
        ):
            mock_shutdown_event.is_set.return_value = False

            await worker(worker_id, queue, classifier, db, batch_size)

            mock_generic_worker.assert_called_once()
            call_kwargs = mock_generic_worker.call_args[1]
            assert call_kwargs["worker_id"] == worker_id
            assert call_kwargs["queue"] == queue
            assert call_kwargs["max_messages"] == 1
            assert call_kwargs["wait_time"] == 20

    @pytest.mark.asyncio
    async def test_worker_handler_calls_handle_shop_message(self):
        """Test that worker's message handler delegates to handle_shop_message."""
        worker_id = 1
        queue = Mock()
        classifier = Mock()
        db = Mock()
        batch_size = 50
        message = Mock()
        message.body = json.dumps({"domain": "example.com"})

        handler_captured = None

        async def capture_handler(*args, **kwargs):  # NOSONAR
            nonlocal handler_captured
            handler_captured = kwargs.get("message_handler")

        with (
            patch(
                "src.core.worker.product_spider.generic_worker",
                side_effect=capture_handler,
            ),
            patch(
                "src.core.worker.product_spider.handle_shop_message",
                new_callable=AsyncMock,
            ) as mock_handle,
            patch(
                "src.core.worker.product_spider.shutdown_event"
            ) as mock_shutdown_event,
        ):
            mock_shutdown_event.is_set.return_value = False

            await worker(worker_id, queue, classifier, db, batch_size)

            # Test the handler
            await handler_captured(message)

            mock_handle.assert_called_once_with(
                message, classifier, db, mock_shutdown_event, batch_size
            )
