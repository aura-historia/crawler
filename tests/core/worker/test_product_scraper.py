import asyncio
import importlib
import signal
import sys
import types

import pytest


class DummyMessage:
    def __init__(self, body="{}"):
        self.body = body


@pytest.fixture
def product_scraper(monkeypatch):
    monkeypatch.setenv("SQS_QUEUE_NAME", "test-queue")

    from src.core.sqs import queue_wrapper

    fake_queue = object()
    monkeypatch.setattr(queue_wrapper, "get_queue", lambda _: fake_queue)

    fake_crawl4ai = types.ModuleType("crawl4ai")

    class PlaceholderCrawler:
        def __init__(self, config=None):
            """This is a dummy class for testing purposes"""
            self.config = config

        async def __aenter__(self):
            return self

        async def __aexit__(self, exc_type, exc, tb):
            return False

        async def arun_many(self, urls=None, config=None, dispatcher=None):
            if urls:
                for url in urls:
                    yield types.SimpleNamespace(success=False, html=None, url=url)

    class _SimpleAttr:
        def __init__(self, *_, **__):
            """This is a dummy class for testing purposes"""

    fake_crawl4ai.AsyncWebCrawler = PlaceholderCrawler
    fake_crawl4ai.CrawlerRunConfig = _SimpleAttr
    fake_crawl4ai.CacheMode = types.SimpleNamespace(BYPASS="bypass")
    fake_crawl4ai.RateLimiter = _SimpleAttr
    fake_crawl4ai.MemoryAdaptiveDispatcher = _SimpleAttr
    fake_crawl4ai.BrowserConfig = _SimpleAttr

    scraping_module = types.ModuleType("crawl4ai.content_scraping_strategy")

    class LXMLWebScrapingStrategy:
        def __init__(self, *_, **__):
            """This is a dummy class for testing purposes"""

    scraping_module.LXMLWebScrapingStrategy = LXMLWebScrapingStrategy

    monkeypatch.setitem(
        sys.modules, "crawl4ai.content_scraping_strategy", scraping_module
    )
    monkeypatch.setitem(sys.modules, "crawl4ai", fake_crawl4ai)

    crawler_module = types.ModuleType(
        "src.core.algorithms.bfs_no_cycle_deep_crawl_strategy"
    )

    class DummyStrategy:
        def __init__(self, *_, **__):
            """This is a dummy class for testing purposes"""

    class DummyCrawler:
        pass

    class DummyRunConfig:
        def clone(self, *_, **__):
            return self

    crawler_module.BFSNoCycleDeepCrawlStrategy = DummyStrategy
    crawler_module.BFSDeepCrawlStrategy = DummyStrategy
    crawler_module.AsyncWebCrawler = DummyCrawler
    crawler_module.CrawlerRunConfig = DummyRunConfig

    monkeypatch.setitem(
        sys.modules,
        "src.core.algorithms.bfs_no_cycle_deep_crawl_strategy",
        crawler_module,
    )

    import src.core.worker.product_scraper as module

    module = importlib.reload(module)
    module.queue = fake_queue
    module.AsyncWebCrawler = PlaceholderCrawler
    module.current_message = None
    module.shutdown_event = asyncio.Event()
    return module


@pytest.mark.asyncio
async def test_process_result_success_returns_standardized_data(
    product_scraper, monkeypatch
):
    result = types.SimpleNamespace(
        success=True, html="<html></html>", url="https://example.com/product"
    )

    captured = {}

    def fake_extruct(html, base_url, syntaxes):
        captured["base"] = base_url
        captured["syntaxes"] = list(syntaxes)
        return {"raw": True}

    async def fake_extract_standard(raw, url, preferred):
        await asyncio.sleep(0)
        return {"url": url, "raw": raw, "preferred": preferred}

    monkeypatch.setattr(product_scraper, "extruct_extract", fake_extruct)
    monkeypatch.setattr(product_scraper, "extract_standard", fake_extract_standard)

    data = await product_scraper.process_result(result)

    assert data["url"] == result.url
    assert captured["base"] == result.url
    assert captured["syntaxes"] == ["microdata", "opengraph", "json-ld", "rdfa"]


@pytest.mark.asyncio
async def test_process_result_returns_none_when_invalid(product_scraper):
    missing_html = types.SimpleNamespace(
        success=True, html=None, url="https://example.com"
    )
    failed = types.SimpleNamespace(
        success=False, html="<html></html>", url="https://example.com"
    )

    assert await product_scraper.process_result(missing_html) is None
    assert await product_scraper.process_result(failed) is None


@pytest.mark.asyncio
async def test_batch_sender_flushes_batches(product_scraper, monkeypatch):
    sent_batches = []

    async def fake_send(items):
        await asyncio.sleep(0)
        sent_batches.append(list(items))

    monkeypatch.setattr(product_scraper, "send_items", fake_send)

    q = asyncio.Queue()
    payloads = [{"id": 1}, {"id": 2}, {"id": 3}]
    for item in payloads:
        await q.put(item)
    await q.put(None)

    await product_scraper.batch_sender(q, batch_size=2)

    assert sent_batches == [payloads[:2], payloads[2:]]


@pytest.mark.asyncio
async def test_crawl_streaming_processes_urls(product_scraper, monkeypatch):
    sent_batches = []

    async def fake_send(batch):
        await asyncio.sleep(0)
        sent_batches.append([item["url"] for item in batch])

    async def fake_process(result):
        await asyncio.sleep(0)
        return {"url": result.url}

    class StubCrawler:
        def __init__(self, config=None):
            self.config = config

        async def __aenter__(self):
            return self

        async def __aexit__(self, exc_type, exc, tb):
            return False

        async def arun_many(self, urls, config=None, dispatcher=None):
            for url in urls:
                yield types.SimpleNamespace(success=True, html="<html></html>", url=url)

    monkeypatch.setattr(product_scraper, "send_items", fake_send)
    monkeypatch.setattr(product_scraper, "process_result", fake_process)
    monkeypatch.setattr(product_scraper, "AsyncWebCrawler", StubCrawler)
    monkeypatch.setattr(
        product_scraper,
        "build_product_scraper_components",
        lambda: ("browser", "run", "dispatcher"),
    )

    await product_scraper.crawl_streaming(["https://a", "https://b"], batch_size=1)

    assert sent_batches == [["https://a"], ["https://b"]]


@pytest.mark.asyncio
async def test_crawl_streaming_halts_when_interrupted(product_scraper, monkeypatch):
    product_scraper.shutdown_event = asyncio.Event()
    product_scraper.shutdown_event.set()

    async def fake_process(result):
        raise AssertionError("process_result should not be called when interrupted")

    async def fake_batch_sender(q, batch_size):
        # This should not be called if the producer loop is correctly interrupted.
        item = await q.get()
        if item is not None:
            raise AssertionError("batch_sender should not have received items")

    # We patch create_task to control the execution of the consumer
    original_create_task = asyncio.create_task
    consumer_tasks = []

    def fake_create_task(coro):
        # We want to test the producer loop, so we only create the consumer task
        # to be awaited, but we don't want it to run freely.
        task = original_create_task(coro)
        if "batch_sender" in coro.__name__:
            consumer_tasks.append(task)
        return task

    monkeypatch.setattr(product_scraper.asyncio, "create_task", fake_create_task)
    monkeypatch.setattr(product_scraper, "process_result", fake_process)
    monkeypatch.setattr(product_scraper, "batch_sender", fake_batch_sender)
    monkeypatch.setattr(
        product_scraper,
        "build_product_scraper_components",
        lambda: ("browser", "run", "dispatcher"),
    )

    await product_scraper.crawl_streaming(["https://only"], batch_size=1)

    # The consumer task should have been cancelled or finished without processing.
    assert len(consumer_tasks) == 1
    assert consumer_tasks[0].done()

    product_scraper.shutdown_event = asyncio.Event()


@pytest.mark.asyncio
async def test_handle_domain_message_without_domain_deletes(
    product_scraper, monkeypatch
):
    message = DummyMessage("payload")
    deletes = []

    monkeypatch.setattr(product_scraper, "parse_message_body", lambda _: (None, None))
    monkeypatch.setattr(
        product_scraper, "delete_message", lambda msg: deletes.append(msg)
    )

    db = types.SimpleNamespace(get_product_urls_by_domain=lambda _: [])

    await product_scraper.handle_domain_message(message, db, batch_size=3)

    assert deletes == [message]


@pytest.mark.asyncio
async def test_handle_domain_message_processes_and_requeues(
    product_scraper, monkeypatch
):
    message = DummyMessage("body")

    monkeypatch.setattr(
        product_scraper, "parse_message_body", lambda _: ("example.com", "https://b")
    )
    db = types.SimpleNamespace(
        get_product_urls_by_domain=lambda _: ["https://a", "https://b", "https://c"]
    )

    crawled = {}

    async def fake_crawl(urls, batch_size):
        await asyncio.sleep(0)
        crawled["urls"] = list(urls)
        crawled["batch"] = batch_size

    sent_bodies = []
    deletes = []

    monkeypatch.setattr(product_scraper, "crawl_streaming", fake_crawl)
    monkeypatch.setattr(
        product_scraper, "send_message", lambda _queue, body: sent_bodies.append(body)
    )
    monkeypatch.setattr(
        product_scraper, "delete_message", lambda msg: deletes.append(msg)
    )

    await product_scraper.handle_domain_message(message, db, batch_size=5)

    assert crawled["urls"] == ["https://b", "https://c"]
    assert crawled["batch"] == 5
    assert sent_bodies == [message.body]
    assert deletes == [message]
    assert product_scraper.current_message is None


@pytest.mark.asyncio
async def test_process_message_batch_logs_exceptions(product_scraper, monkeypatch):
    messages = [DummyMessage("one"), DummyMessage("bad")]
    monkeypatch.setattr(product_scraper, "receive_messages", lambda *_, **__: messages)

    calls = []

    async def fake_handle(msg, db, batch_size):
        await asyncio.sleep(0)
        calls.append((msg, batch_size))
        if msg.body == "bad":
            raise RuntimeError("boom")

    logged = []

    monkeypatch.setattr(product_scraper, "handle_domain_message", fake_handle)
    monkeypatch.setattr(
        product_scraper.logger,
        "exception",
        lambda message, exc: logged.append((message, str(exc))),
    )

    await product_scraper.process_message_batch(object(), n_shops=2, batch_size=4)

    assert calls == [(messages[0], 4), (messages[1], 4)]
    assert any("boom" in entry[1] for entry in logged)


def test_signal_handler_requeues_and_signals_shutdown(product_scraper, monkeypatch):
    product_scraper.current_message = DummyMessage("body")
    product_scraper.shutdown_event = asyncio.Event()

    sent_bodies = []

    def fake_send_message(queue, body):
        sent_bodies.append(body)

    monkeypatch.setattr(product_scraper, "send_message", fake_send_message)

    product_scraper.signal_handler(signal.SIGTERM, None)

    assert product_scraper.shutdown_event.is_set()
    assert sent_bodies == ["body"]


@pytest.mark.asyncio
async def test_main_runs_until_shutdown(product_scraper, monkeypatch):
    creations = []

    class DummyDB:
        def __init__(self):
            creations.append(True)

    async def watcher_stub(check_interval=30):
        while not product_scraper.shutdown_event.is_set():
            await asyncio.sleep(0.01)

    msg_iter = iter([[DummyMessage("work")]])

    def fake_receive(queue, max_number, wait_time):
        return next(msg_iter, [])

    async def fake_process_batch(db, n_shops, batch_size):
        await asyncio.sleep(0)
        fake_process_batch.calls.append((n_shops, batch_size))
        product_scraper.shutdown_event.set()

    fake_process_batch.calls = []

    monkeypatch.setattr(product_scraper, "DynamoDBOperations", DummyDB)
    monkeypatch.setattr(product_scraper.signal, "signal", lambda *args, **kwargs: None)
    monkeypatch.setattr(product_scraper, "spot_termination_watcher", watcher_stub)
    monkeypatch.setattr(product_scraper, "receive_messages", fake_receive)
    monkeypatch.setattr(product_scraper, "process_message_batch", fake_process_batch)

    import pytest

    # The watcher task will be cancelled during shutdown; `main` re-raises
    # asyncio.CancelledError after cleaning up the watcher. The test should
    # therefore expect that exception but still assert the side-effects.
    with pytest.raises(asyncio.CancelledError):
        await product_scraper.main(n_shops=2, batch_size=6)

    assert fake_process_batch.calls == [(2, 6)]
    assert creations
