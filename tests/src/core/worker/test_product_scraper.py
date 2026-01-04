import asyncio
import json
import pytest
from unittest.mock import AsyncMock, Mock, patch

from src.core.worker.product_scraper import (
    process_result,
    batch_sender,
    scrape,
    handle_domain_message,
    main,
    worker,
)


class FakeResult:
    """Mock crawl result object for testing AsyncWebCrawler responses."""

    def __init__(
        self,
        success=True,
        html="<html></html>",
        url="https://example.com",
        error_message=None,
    ):
        self.success = success
        self.html = html
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
def mock_extruct_extract():
    """Mock extruct.extract function."""
    with patch("src.core.worker.product_scraper.extruct_extract") as mock:
        mock.return_value = {
            "microdata": [],
            "opengraph": [],
            "json-ld": [],
            "rdfa": [],
        }
        yield mock


@pytest.fixture
def mock_extract_standard():
    """Mock extract_standard function."""

    async def fake_extract(_extracted_raw, url, preferred=None, **_extras):
        await asyncio.sleep(0)
        return {
            "url": url,
            "standard": True,
            "preferred": preferred,
            "price": {"amount": 1.0},
            "state": "AVAILABLE",
        }

    with patch(
        "src.core.worker.product_scraper.extract_standard", new=fake_extract
    ) as mock:
        yield mock


@pytest.fixture
def mock_get_base_url():
    """Mock get_base_url function."""
    with patch("src.core.worker.product_scraper.get_base_url") as mock:
        mock.return_value = "https://example.com"
        yield mock


@pytest.fixture
def mock_send_items():
    """Mock send_items function."""
    with patch(
        "src.core.worker.product_scraper.send_items", new_callable=AsyncMock
    ) as mock:
        yield mock


class TestProcessResult:
    """Tests for process_result function."""

    @pytest.mark.asyncio
    async def test_process_result_success(
        self, mock_extruct_extract, mock_extract_standard, mock_get_base_url
    ):
        """Test successful result processing."""
        result = FakeResult(
            success=True, html="<html>test</html>", url="https://example.com/product"
        )

        extracted = await process_result(result)

        assert extracted is not None
        assert extracted["url"] == "https://example.com/product"
        assert extracted["standard"] is True
        mock_extruct_extract.assert_called_once()

    @pytest.mark.asyncio
    async def test_process_result_failure(self):
        """Test processing a failed result."""
        result = FakeResult(success=False, error_message="Connection timeout")

        extracted = await process_result(result)

        assert extracted is None

    @pytest.mark.asyncio
    async def test_process_result_no_html(self):
        """Test processing result without HTML."""
        result = FakeResult(success=True, html="", url="https://example.com")

        extracted = await process_result(result)

        assert extracted is None

    @pytest.mark.asyncio
    async def test_process_result_no_url(self):
        """Test processing result without URL."""
        result = FakeResult(success=True, html="<html>test</html>", url="")

        extracted = await process_result(result)

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


class TestScrape:
    """Tests for scrape function."""

    @pytest.fixture(autouse=True)
    def patch_db_operations_for_scrape(self, monkeypatch):
        import src.core.worker.product_scraper as ps

        class DummyEntry:
            hash = None

        mock_db_ops = Mock()
        mock_db_ops.get_url_entry = Mock(return_value=DummyEntry())
        mock_db_ops.update_url_hash = Mock(return_value=True)

        async def fake_to_thread(func, *args, **kwargs):  # NOSONAR
            return func(*args, **kwargs)

        monkeypatch.setattr(ps.asyncio, "to_thread", fake_to_thread)
        monkeypatch.setattr(ps, "db_operations", mock_db_ops)
        yield

    @pytest.mark.asyncio
    async def test_scrape_success(
        self,
        mock_extruct_extract,
        mock_extract_standard,
        mock_get_base_url,
        mock_send_items,
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
        self,
        mock_extruct_extract,
        mock_extract_standard,
        mock_get_base_url,
        mock_send_items,
    ):
        """Test scraping with some URLs failing to crawl."""
        from typing import cast
        from crawl4ai import AsyncWebCrawler

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
        self,
        mock_extruct_extract,
        mock_extract_standard,
        mock_get_base_url,
        mock_send_items,
    ):
        """Test scraping interrupted by shutdown event."""
        from typing import cast
        from crawl4ai import AsyncWebCrawler

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

    @pytest.fixture(autouse=True)
    def patch_db_operations_for_scrape(self, monkeypatch):
        import src.core.worker.product_scraper as ps

        class DummyEntry:
            hash = None

        mock_db_ops = Mock()
        mock_db_ops.get_url_entry = Mock(return_value=DummyEntry())
        mock_db_ops.update_url_hash = Mock(return_value=True)

        async def fake_to_thread(func, *args, **kwargs):  # NOSONAR
            return func(*args, **kwargs)

        monkeypatch.setattr(ps.asyncio, "to_thread", fake_to_thread)
        monkeypatch.setattr(ps, "db_operations", mock_db_ops)
        yield

    @pytest.mark.asyncio
    async def test_handle_domain_message_success(
        self,
        mock_extruct_extract,
        mock_extract_standard,
        mock_get_base_url,
        mock_send_items,
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
        from src.core.worker import product_scraper

        db_ops = Mock()
        get_url_entry = db_ops.get_url_entry
        update_url_hash = db_ops.update_url_hash

        async def fake_to_thread(func, *args, **kwargs):  # NOSONAR
            if func is get_url_entry:
                return None
            if func is update_url_hash:
                return True

        monkeypatch.setattr(product_scraper.asyncio, "to_thread", fake_to_thread)
        monkeypatch.setattr(product_scraper, "db_operations", db_ops)
        monkeypatch.setattr(
            product_scraper,
            "URLEntry",
            __import__("src.core.aws.database.models", fromlist=["URLEntry"]).URLEntry,
        )

        extracted = {"state": "in_stock", "price": {"amount": 9.99}}
        result = await product_scraper.update_hash(
            extracted, "example.com", "https://example.com/1"
        )
        assert result is True

    @pytest.mark.asyncio
    async def test_update_hash_no_change(self, monkeypatch):
        """Should not update hash if hash is unchanged."""
        from src.core.worker import product_scraper

        db_ops = Mock()
        get_url_entry = db_ops.get_url_entry
        update_url_hash = db_ops.update_url_hash

        class DummyEntry:
            hash = __import__(
                "src.core.aws.database.models", fromlist=["URLEntry"]
            ).URLEntry.calculate_hash("in_stock", 9.99)

        async def fake_to_thread(func, *args, **kwargs):  # NOSONAR
            if func is get_url_entry:
                return DummyEntry()
            if func is update_url_hash:
                raise AssertionError("Should not be called")
            return None

        monkeypatch.setattr(product_scraper.asyncio, "to_thread", fake_to_thread)
        monkeypatch.setattr(product_scraper, "db_operations", db_ops)
        monkeypatch.setattr(
            product_scraper,
            "URLEntry",
            __import__("src.core.aws.database.models", fromlist=["URLEntry"]).URLEntry,
        )

        extracted = {"state": "in_stock", "price": {"amount": 9.99}}
        result = await product_scraper.update_hash(
            extracted, "example.com", "https://example.com/1"
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

        extracted = {"state": "in_stock", "price": {"amount": 9.99}}
        result = await product_scraper.update_hash(
            extracted, "example.com", "https://example.com/1"
        )
        assert result is False


class TestWorker:
    """Tests for worker function using generic_worker."""

    @pytest.mark.asyncio
    async def test_worker_processes_messages(self):
        """Test that worker processes domain messages correctly."""
        worker_id = 1
        queue = Mock()
        db = Mock()
        batch_size = 10
        message = Mock()
        message.body = json.dumps({"domain": "example.com"})

        with (
            patch(
                "src.core.worker.product_scraper.generic_worker",
                new_callable=AsyncMock,
            ) as mock_generic_worker,
            patch(
                "src.core.worker.product_scraper.shutdown_event"
            ) as mock_shutdown_event,
        ):
            mock_shutdown_event.is_set.return_value = False

            await worker(worker_id, queue, db, batch_size)

            mock_generic_worker.assert_called_once()
            call_kwargs = mock_generic_worker.call_args[1]
            assert call_kwargs["worker_id"] == worker_id
            assert call_kwargs["queue"] == queue
            assert call_kwargs["max_messages"] == 1
            assert call_kwargs["wait_time"] == 20

    @pytest.mark.asyncio
    async def test_worker_handler_calls_handle_domain_message(self):
        """Test that worker's message handler delegates to handle_domain_message."""
        worker_id = 1
        queue = Mock()
        db = Mock()
        batch_size = 10
        message = Mock()
        message.body = json.dumps({"domain": "example.com"})

        handler_captured = None

        async def capture_handler(*args, **kwargs):  # NOSONAR
            nonlocal handler_captured
            handler_captured = kwargs.get("message_handler")

        with (
            patch(
                "src.core.worker.product_scraper.generic_worker",
                side_effect=capture_handler,
            ),
            patch(
                "src.core.worker.product_scraper.handle_domain_message",
                new_callable=AsyncMock,
            ) as mock_handle,
            patch(
                "src.core.worker.product_scraper.shutdown_event"
            ) as mock_shutdown_event,
        ):
            mock_shutdown_event.is_set.return_value = False

            await worker(worker_id, queue, db, batch_size)

            # Test the handler
            await handler_captured(message)

            mock_handle.assert_called_once_with(
                message, db, mock_shutdown_event, queue, batch_size
            )
