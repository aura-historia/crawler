import asyncio
import json
import pytest
from unittest.mock import AsyncMock, Mock, patch
from typing import cast
from crawl4ai import AsyncWebCrawler
from src.core.worker import product_scraper

from src.core.worker.product_scraper import (
    process_result_async,
    batch_sender,
    scrape,
    handle_domain_message,
    main,
    worker,
    log_metrics_if_needed,
    process_single_url,
)


class FakeResult:
    """Mock crawl result object for testing AsyncWebCrawler responses."""

    def __init__(
        self,
        success=True,
        markdown="# Test Page",
        url="https://example.com",
        error_message=None,
    ):
        self.success = success
        self.markdown = markdown
        self.url = url
        self.error_message = error_message


class FakeCrawler:
    """Mock AsyncWebCrawler for testing without making real HTTP requests."""

    def __init__(self, results=None, raise_on=None):
        self._results = results or []
        self.raise_on = raise_on or set()

    async def arun(self, url, **_kwargs):
        await asyncio.sleep(0)
        if url in self.raise_on:
            raise RuntimeError("crawl fail")
        if self._results:
            return self._results.pop(0)
        return FakeResult(url=url)

    async def __aenter__(self):
        return self

    async def __aexit__(self, exc_type, exc, tb):
        return False


@pytest.fixture
def mock_qwen_extract():
    """Mock qwen_extract function."""
    from src.core.scraper.schemas.extracted_product import ExtractedProduct

    async def fake_qwen_extract(markdown):
        await asyncio.sleep(0)
        return ExtractedProduct(
            shop_item_id="test-123",
            title="Test Product",
            description="Test description",
            language="en",
            priceEstimateMinAmount=1000,
            priceEstimateMinCurrency="EUR",
            state="AVAILABLE",
            images=["https://example.com/image.jpg"],
        )

    with patch(
        "src.core.worker.product_scraper.qwen_extract", new=fake_qwen_extract
    ) as mock:
        yield mock


@pytest.fixture
def mock_send_items():
    """Mock send_items function."""
    with patch(
        "src.core.worker.product_scraper.send_items", new_callable=AsyncMock
    ) as mock:
        yield mock


@pytest.fixture
def mock_update_hash():
    """Mock update_hash function to return True by default."""
    with patch(
        "src.core.worker.product_scraper.update_hash",
        new_callable=AsyncMock,
        return_value=True,
    ) as mock:
        yield mock


class TestProcessResult:
    """Tests for process_result_async function."""

    @pytest.mark.asyncio
    async def test_process_result_success(self, mock_qwen_extract, mock_update_hash):
        """Test successful result processing."""
        result = FakeResult(
            success=True, markdown="# Test Product", url="https://example.com/product"
        )
        extracted = await process_result_async(result, "example.com")
        assert extracted is not None
        assert isinstance(extracted, dict)
        assert extracted["title"]["text"] == "Test Product"
        assert extracted["priceEstimateMin"]["amount"] == 1000
        assert extracted["priceEstimateMin"]["currency"] == "EUR"
        assert extracted["state"] == "AVAILABLE"
        assert extracted["shopsProductId"] == "test-123"

    @pytest.mark.asyncio
    async def test_process_result_failure(self):
        """Test processing a failed result."""
        result = FakeResult(success=False, error_message="Connection timeout")

        extracted = await process_result_async(result, "example.com")

        assert extracted is None

    @pytest.mark.asyncio
    async def test_process_result_qwen_error(self, mock_update_hash):
        """Test processing when qwen_extract raises an exception."""
        result = FakeResult(success=True, markdown="# Test", url="https://example.com")

        with patch(
            "src.core.worker.product_scraper.qwen_extract",
            side_effect=Exception("LLM error"),
        ):
            extracted = await process_result_async(result, "example.com")

            assert extracted is None

    @pytest.mark.asyncio
    async def test_process_result_empty_response(self, mock_update_hash):
        """Test processing when qwen_extract returns empty data."""
        result = FakeResult(success=True, markdown="# Test", url="https://example.com")

        def fake_qwen_extract(_markdown):
            return json.dumps({})

        with patch(
            "src.core.worker.product_scraper.qwen_extract", new=fake_qwen_extract
        ):
            extracted = await process_result_async(result, "example.com")

            # Implementation returns None for empty response
            assert extracted is None

    @pytest.mark.asyncio
    async def test_process_result_no_hash_change(self, mock_update_hash):
        """Test processing when hash hasn't changed."""
        result = FakeResult(success=True, markdown="# Test", url="https://example.com")

        # Override the fixture to return False for this test
        mock_update_hash.return_value = False

        extracted = await process_result_async(result, "example.com")

        # Implementation returns None when hash hasn't changed
        assert extracted is None


class TestBatchSender:
    """Tests for batch_sender function."""

    @pytest.mark.asyncio
    async def test_batch_sender_single_batch(self, mock_send_items):
        """Test batch sender with items filling exactly one batch."""
        q = asyncio.Queue()

        sent_batches = []
        mock_send_items.side_effect = lambda batch: sent_batches.append(list(batch))

        await q.put({"url": "https://example.com/1"})
        await q.put({"url": "https://example.com/2"})
        await q.put(None)

        await batch_sender(q, batch_size=2)

        assert mock_send_items.call_count == 1
        assert len(sent_batches) == 1
        assert len(sent_batches[0]) == 2

    @pytest.mark.asyncio
    async def test_batch_sender_multiple_batches(self, mock_send_items):
        """Test batch sender with items requiring multiple batches."""
        q = asyncio.Queue()

        sent_batches = []
        mock_send_items.side_effect = lambda batch: sent_batches.append(list(batch))

        for i in range(5):
            await q.put({"url": f"https://example.com/{i}"})
        await q.put(None)

        await batch_sender(q, batch_size=2)

        # With 5 items and batch_size=2, we expect 3 sends: [2, 2, 1]
        assert mock_send_items.call_count == 3
        assert len(sent_batches) == 3
        assert len(sent_batches[0]) == 2
        assert len(sent_batches[1]) == 2
        assert len(sent_batches[2]) == 1

    @pytest.mark.asyncio
    async def test_batch_sender_empty_queue(self, mock_send_items):
        """Test batch sender with empty queue."""
        q = asyncio.Queue()
        await q.put(None)

        await batch_sender(q, batch_size=2)

        assert mock_send_items.call_count == 0


class TestLogMetricsIfNeeded:
    """Tests for _log_metrics_if_needed function."""

    def test_log_metrics_at_interval(self):
        """Test that metrics are logged at the configured interval."""

        # Set processed to a multiple of LOG_METRICS_INTERVAL
        product_scraper._metrics["processed"] = 50
        product_scraper.LOG_METRICS_INTERVAL = 50

        with patch("src.core.worker.product_scraper.logger") as mock_logger:
            log_metrics_if_needed("example.com")

            mock_logger.info.assert_called_once()
            call_args = mock_logger.info.call_args
            assert "Metrics:" in call_args[0][0]

    def test_no_log_between_intervals(self):
        """Test that metrics are not logged between intervals."""
        from src.core.worker import product_scraper

        # Set processed to a non-multiple of LOG_METRICS_INTERVAL
        product_scraper._metrics["processed"] = 49
        product_scraper.LOG_METRICS_INTERVAL = 50

        with patch("src.core.worker.product_scraper.logger") as mock_logger:
            log_metrics_if_needed("example.com")

            mock_logger.info.assert_not_called()


class TestProcessSingleUrl:
    """Tests for _process_single_url function."""

    @pytest.mark.asyncio
    async def test_process_single_url_success(
        self, mock_qwen_extract, mock_send_items, mock_update_hash
    ):
        """Test successful processing of a single URL."""
        from src.core.worker import product_scraper

        crawler = FakeCrawler(results=[FakeResult(url="https://example.com/1")])
        result_queue = asyncio.Queue()

        # Reset metrics
        product_scraper._metrics["processed"] = 0
        product_scraper._metrics["extracted"] = 0

        await process_single_url(
            "https://example.com/1",
            cast(AsyncWebCrawler, cast(object, crawler)),
            "example.com",
            {},
            result_queue,
        )

        assert product_scraper._metrics["processed"] == 1
        assert product_scraper._metrics["extracted"] == 1
        assert result_queue.qsize() == 1
        item = await result_queue.get()
        assert isinstance(item, dict)
        assert item["title"]["text"] == "Test Product"

    @pytest.mark.asyncio
    async def test_process_single_url_timeout(self):
        """Test processing a URL that times out."""
        from src.core.worker import product_scraper

        async def slow_arun(*args, **kwargs):
            await asyncio.sleep(100)  # Simulate timeout

        crawler = Mock()
        crawler.arun = slow_arun
        result_queue = asyncio.Queue()

        # Reset metrics
        product_scraper._metrics["processed"] = 0
        product_scraper._metrics["timeout"] = 0

        with patch("src.core.worker.product_scraper.REQUEST_TIMEOUT", 0.1):
            await process_single_url(
                "https://example.com/slow",
                cast(AsyncWebCrawler, cast(object, crawler)),
                "example.com",
                {},
                result_queue,
            )

        assert product_scraper._metrics["processed"] == 1
        assert product_scraper._metrics["timeout"] == 1
        assert result_queue.qsize() == 0

    @pytest.mark.asyncio
    async def test_process_single_url_error(self):
        """Test processing a URL that raises an exception."""
        from src.core.worker import product_scraper

        crawler = FakeCrawler(raise_on={"https://example.com/error"})
        result_queue = asyncio.Queue()

        # Reset metrics
        product_scraper._metrics["processed"] = 0
        product_scraper._metrics["error"] = 0

        await process_single_url(
            "https://example.com/error",
            cast(AsyncWebCrawler, cast(object, crawler)),
            "example.com",
            {},
            result_queue,
        )

        assert product_scraper._metrics["processed"] == 1
        assert product_scraper._metrics["error"] == 1
        assert result_queue.qsize() == 0


class TestScrape:
    """Tests for scrape function."""

    @pytest.mark.asyncio
    async def test_scrape_success(
        self, mock_qwen_extract, mock_send_items, mock_update_hash
    ):
        """Test successful scraping of URLs."""
        from typing import cast
        from crawl4ai import AsyncWebCrawler

        crawler = FakeCrawler(
            results=[
                FakeResult(url="https://example.com/1"),
                FakeResult(url="https://example.com/2"),
            ]
        )

        urls = ["https://example.com/1", "https://example.com/2"]
        shutdown_event = asyncio.Event()

        count = await scrape(
            cast(AsyncWebCrawler, cast(object, crawler)),
            "example.com",
            urls,
            shutdown_event,
            run_config={},
            batch_size=2,
        )

        assert count == 2
        assert mock_send_items.call_count >= 1

    @pytest.mark.asyncio
    async def test_scrape_with_crawl_errors(
        self, mock_qwen_extract, mock_send_items, mock_update_hash
    ):
        """Test scraping with some URLs failing to crawl."""

        crawler = FakeCrawler(
            results=[FakeResult(url="https://example.com/1")],
            raise_on={"https://example.com/2"},
        )

        urls = ["https://example.com/1", "https://example.com/2"]
        shutdown_event = asyncio.Event()

        count = await scrape(
            cast(AsyncWebCrawler, cast(object, crawler)),
            "example.com",
            urls,
            shutdown_event,
            run_config={},
            batch_size=10,
        )

        assert count == 2

    @pytest.mark.asyncio
    async def test_scrape_with_shutdown(
        self, mock_qwen_extract, mock_send_items, mock_update_hash
    ):
        """Test scraping interrupted by shutdown event."""

        crawler = FakeCrawler(
            results=[
                FakeResult(url="https://example.com/1"),
                FakeResult(url="https://example.com/2"),
            ]
        )

        urls = [
            "https://example.com/1",
            "https://example.com/2",
            "https://example.com/3",
        ]
        shutdown_event = asyncio.Event()
        shutdown_event.set()

        count = await scrape(
            cast(AsyncWebCrawler, cast(object, crawler)),
            "example.com",
            urls,
            shutdown_event,
            run_config={},
            batch_size=10,
        )

        assert count == 0


class TestHandleDomainMessage:
    """Tests for handle_domain_message function."""

    @pytest.mark.asyncio
    async def test_handle_domain_message_success(
        self, mock_qwen_extract, mock_send_items, mock_update_hash
    ):
        """Test successful handling of a domain message."""
        message = Mock()
        message.body = json.dumps({"domain": "example.com"})

        db = Mock()
        db.get_all_product_urls_by_domain = Mock(
            return_value=[
                "https://example.com/product1",
                "https://example.com/product2",
            ]
        )

        queue = Mock()
        shutdown_event = asyncio.Event()

        # Create a fake heartbeat task
        async def dummy_heartbeat():
            pass

        fake_heartbeat_task = asyncio.create_task(dummy_heartbeat())
        with (
            patch(
                "src.core.worker.product_scraper.visibility_heartbeat",
                return_value=fake_heartbeat_task,
            ),
            patch(
                "src.core.worker.product_scraper.parse_message_body",
                return_value=("example.com", None),
            ),
            patch("src.core.worker.product_scraper.delete_message") as mock_delete,
            patch("src.core.worker.product_scraper.send_message"),
            patch(
                "src.core.worker.product_scraper.build_product_scraper_components",
                return_value=({}, {}),
            ),
            patch(
                "src.core.worker.product_scraper.AsyncWebCrawler",
                return_value=FakeCrawler(),
            ),
            patch(
                "src.core.worker.product_scraper.asyncio.to_thread",
                new_callable=AsyncMock,
            ) as mock_thread,
        ):
            mock_thread.side_effect = lambda func, *args, **kwargs: func(
                *args, **kwargs
            )

            await handle_domain_message(
                message, db, shutdown_event, queue, batch_size=10
            )

            db.get_all_product_urls_by_domain.assert_called_once_with("example.com")
            # Should be called twice: once at start, once at end
            assert db.update_shop_metadata.call_count == 2
            mock_delete.assert_called_once_with(message)

    @pytest.mark.asyncio
    async def test_handle_domain_message_no_domain(self):
        """Test handling message without domain."""
        message = Mock()
        db = Mock()
        db.get_all_product_urls_by_domain = Mock(return_value=[])
        queue = Mock()
        shutdown_event = asyncio.Event()

        with (
            patch(
                "src.core.worker.product_scraper.parse_message_body",
                return_value=(None, None),
            ),
            patch("src.core.worker.product_scraper.delete_message") as mock_delete,
            patch(
                "src.core.worker.product_scraper.asyncio.to_thread",
                new_callable=AsyncMock,
            ) as mock_thread,
            patch(
                "src.core.worker.product_scraper.visibility_heartbeat",
                return_value=asyncio.create_task(asyncio.sleep(0)),
            ),
        ):
            mock_thread.side_effect = lambda func, *args, **kwargs: func(
                *args, **kwargs
            )

            await handle_domain_message(
                message, db, shutdown_event, queue, batch_size=10
            )

            mock_delete.assert_called_once_with(message)

    @pytest.mark.asyncio
    async def test_handle_domain_message_no_urls(self):
        """Test handling message when no product URLs exist."""
        message = Mock()
        db = Mock()
        db.get_all_product_urls_by_domain = Mock(return_value=[])
        queue = Mock()
        shutdown_event = asyncio.Event()

        with (
            patch(
                "src.core.worker.product_scraper.parse_message_body",
                return_value=("example.com", None),
            ),
            patch("src.core.worker.product_scraper.delete_message") as mock_delete,
            patch(
                "src.core.worker.product_scraper.asyncio.to_thread",
                new_callable=AsyncMock,
            ) as mock_thread,
            patch(
                "src.core.worker.product_scraper.visibility_heartbeat",
                return_value=asyncio.create_task(asyncio.sleep(0)),
            ),
        ):
            mock_thread.side_effect = lambda func, *args, **kwargs: func(
                *args, **kwargs
            )

            await handle_domain_message(
                message, db, shutdown_event, queue, batch_size=10
            )

            mock_delete.assert_called_once_with(message)


class TestMain:
    """Tests for main function."""

    @pytest.mark.asyncio
    async def test_main_queue_failure(self):
        """`main` returns immediately when SQS queue lookup fails."""
        from botocore.exceptions import ClientError

        queue_error = ClientError({"Error": {}}, "GetQueue")

        with (
            patch(
                "src.core.worker.product_scraper.get_queue", side_effect=queue_error
            ) as mock_get_queue,
            patch("src.core.worker.product_scraper.DynamoDBOperations") as mock_db,
            patch("src.core.worker.product_scraper.run_worker_pool") as mock_run_pool,
        ):
            await main(n_workers=1, batch_size=5)

            mock_get_queue.assert_called_once()
            mock_db.assert_not_called()
            mock_run_pool.assert_not_called()

    @pytest.mark.asyncio
    async def test_main_shutdown_flow(self):
        """`main` starts workers using run_worker_pool and shuts down gracefully."""
        queue = Mock()
        db = Mock()

        async def fake_run_worker_pool(*args, **kwargs):
            # Simulate worker pool running briefly then shutting down
            await asyncio.sleep(0.1)

        with (
            patch(
                "src.core.worker.product_scraper.get_queue", return_value=queue
            ) as mock_get_queue,
            patch(
                "src.core.worker.product_scraper.DynamoDBOperations", return_value=db
            ) as mock_db_cls,
            patch(
                "src.core.worker.product_scraper.run_worker_pool",
                new_callable=AsyncMock,
                side_effect=fake_run_worker_pool,
            ) as mock_run_pool,
        ):
            import src.core.worker.product_scraper as ps

            # Reset shutdown event for this test
            ps.shutdown_event = asyncio.Event()

            await main(n_workers=2, batch_size=5)

            assert mock_get_queue.call_count == 1
            mock_db_cls.assert_called_once()
            # Should call run_worker_pool once with correct parameters
            mock_run_pool.assert_called_once()
            call_kwargs = mock_run_pool.call_args[1]
            assert call_kwargs["n_workers"] == 2
            assert call_kwargs["shutdown_event"] == ps.shutdown_event
            assert call_kwargs["shutdown_timeout"] == 90


class TestUpdateHash:
    """Tests for the update_hash function (hash update logic)."""

    @pytest.mark.asyncio
    async def test_update_hash_new_hash(self, monkeypatch):
        """Should update hash if no old entry exists."""

        db_ops = Mock()

        # Import URLEntry for hash calculation
        url_entry_class = __import__(
            "src.core.aws.database.models", fromlist=["URLEntry"]
        ).URLEntry

        async def fake_to_thread(func, *args, **kwargs):  # NOSONAR
            if func is db_ops.get_url_entry:
                return None
            if func is db_ops.update_url_hash:
                return True
            return None

        monkeypatch.setattr(product_scraper.asyncio, "to_thread", fake_to_thread)
        monkeypatch.setattr(product_scraper, "db_operations", db_ops)
        monkeypatch.setattr(product_scraper, "URLEntry", url_entry_class)

        markdown = "# Test Product\nPrice: 9.99\nState: in_stock"
        result = await product_scraper.update_hash(
            markdown, "example.com", "https://example.com/1"
        )
        assert result is True

    @pytest.mark.asyncio
    async def test_update_hash_no_change(self, monkeypatch):
        """Should not update hash if hash is unchanged."""
        from src.core.worker import product_scraper

        db_ops = Mock()

        markdown = "# Test Product\nPrice: 9.99\nState: in_stock"

        # Import URLEntry to calculate hash
        url_entry_class = __import__(
            "src.core.aws.database.models", fromlist=["URLEntry"]
        ).URLEntry

        class DummyEntry:
            hash = url_entry_class.calculate_hash(markdown)

        async def fake_to_thread(func, *args, **kwargs):  # NOSONAR
            if func is db_ops.get_url_entry:
                return DummyEntry()
            if func is db_ops.update_url_hash:
                raise AssertionError("Should not be called")
            return None

        monkeypatch.setattr(product_scraper.asyncio, "to_thread", fake_to_thread)
        monkeypatch.setattr(product_scraper, "db_operations", db_ops)
        monkeypatch.setattr(product_scraper, "URLEntry", url_entry_class)

        result = await product_scraper.update_hash(
            markdown, "example.com", "https://example.com/1"
        )
        assert result is False

    @pytest.mark.asyncio
    async def test_update_hash_update_error(self, monkeypatch):
        """Should log error and return False if update fails."""
        from src.core.worker import product_scraper

        db_ops = Mock()
        get_url_entry = db_ops.get_url_entry
        update_url_hash = db_ops.update_url_hash

        class DummyEntry:
            hash = None

        async def fake_to_thread(func, *args, **kwargs):  # NOSONAR
            if func is get_url_entry:
                return DummyEntry()
            if func is update_url_hash:
                raise RuntimeError("db error")
            return None

        monkeypatch.setattr(product_scraper.asyncio, "to_thread", fake_to_thread)
        monkeypatch.setattr(product_scraper, "db_operations", db_ops)
        monkeypatch.setattr(
            product_scraper,
            "URLEntry",
            __import__("src.core.aws.database.models", fromlist=["URLEntry"]).URLEntry,
        )

        markdown = "# Test Product\nPrice: 9.99\nState: in_stock"
        result = await product_scraper.update_hash(
            markdown, "example.com", "https://example.com/1"
        )
        assert result is False


class TestWorker:
    """Tests for worker function using generic_worker."""

    @pytest.mark.asyncio
    async def test_worker_calls_generic_worker(self):
        """Test that worker sets up and calls generic_worker correctly."""
        worker_id = 1
        queue = Mock()
        db = Mock()
        batch_size = 10

        with patch(
            "src.core.worker.product_scraper.generic_worker", new_callable=AsyncMock
        ) as mock_generic_worker:
            await worker(worker_id, queue, db, batch_size)

            mock_generic_worker.assert_called_once()
            call_kwargs = mock_generic_worker.call_args[1]
            assert call_kwargs["worker_id"] == worker_id
            assert call_kwargs["queue"] == queue
            assert "message_handler" in call_kwargs
            assert call_kwargs["max_messages"] == 1
            assert call_kwargs["wait_time"] == 20
