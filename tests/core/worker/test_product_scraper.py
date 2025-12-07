import asyncio
import json
import pytest
from unittest.mock import AsyncMock
from typing import cast
import aiohttp

import src.core.worker.product_scraper as ps

pytestmark = pytest.mark.asyncio


# small async helper to make async tests use await (silence IDE warnings)
async def _ensure_async_noop():
    await asyncio.sleep(0)


class FakeResp:
    def __init__(self, status=200, text_value=None, json_value=None):
        self.status = status
        self._text = text_value or "tok"
        self._json = json_value or {"action": "terminate", "time": "now"}

    async def text(self):
        await asyncio.sleep(0)
        return self._text

    async def json(self):
        await asyncio.sleep(0)
        return self._json


class FakeSession:
    def __init__(
        self, put_resp=None, get_resp=None, raise_on_put=False, raise_on_get=False
    ):
        self._put_resp = put_resp or FakeResp()
        self._get_resp = get_resp or FakeResp()
        self._raise_on_put = raise_on_put
        self._raise_on_get = raise_on_get

    class _Ctx:
        def __init__(self, resp, raise_exc=False):
            self.resp = resp
            self._raise = raise_exc

        async def __aenter__(self):
            if self._raise:
                raise RuntimeError("boom")
            return self.resp

        async def __aexit__(self, exc_type, exc, tb):
            return False

    def put(self, *args, **kwargs):
        return FakeSession._Ctx(self._put_resp, self._raise_on_put)

    def get(self, *args, **kwargs):
        return FakeSession._Ctx(self._get_resp, self._raise_on_get)


class FakeResult:
    def __init__(
        self, success=True, html="<html></html>", url="http://a", error_message=None
    ):
        self.success = success
        self.html = html
        self.url = url
        self.error_message = error_message


class FakeCrawler:
    def __init__(self, results=None, raise_on=None):
        self._results = results or []
        self.raise_on = raise_on or set()

    async def arun(self, url, config=None):
        await asyncio.sleep(0)
        if url in self.raise_on:
            raise RuntimeError("crawl fail")
        # pop front
        if self._results:
            return self._results.pop(0)
        return FakeResult()

    async def __aenter__(self):
        return self

    async def __aexit__(self, exc_type, exc, tb):
        return False


@pytest.fixture(autouse=True)
def patch_defaults(monkeypatch):
    # Prevent real network/DB/SQS calls by default
    monkeypatch.setattr(
        ps, "extruct_extract", lambda html, base_url, syntaxes: {"raw": True}
    )

    async def fake_extract_standard(extracted_raw, url, preferred=None):
        await asyncio.sleep(0)
        return {"standard": True, "url": url}

    monkeypatch.setattr(ps, "extract_standard", fake_extract_standard)
    monkeypatch.setattr(ps, "get_base_url", lambda html, url: "http://base")

    monkeypatch.setattr(ps, "send_items", AsyncMock())
    monkeypatch.setattr(ps, "send_message", lambda q, body: None)
    monkeypatch.setattr(ps, "delete_message", lambda msg: None)
    monkeypatch.setattr(ps, "receive_messages", lambda q, n, v: [])
    monkeypatch.setattr(ps, "parse_message_body", lambda m: (None, None))
    monkeypatch.setattr(ps, "get_queue", lambda name: object())

    # Fake DynamoDBOperations
    class FakeDB:
        def get_product_urls_by_domain(self, domain):
            return []

    monkeypatch.setattr(ps, "DynamoDBOperations", lambda: FakeDB())


async def test_signal_handler_sets_event():
    ev = asyncio.Event()
    await _ensure_async_noop()
    ps.shutdown_event = ev
    ps.signal_handler(2, None)
    assert ev.is_set()


async def test_get_metadata_token_success(monkeypatch):
    session = FakeSession(put_resp=FakeResp(status=200, text_value="tokval"))
    monkeypatch.setenv("EC2_TOKEN_URL", "http://token")
    token = await ps._get_metadata_token(
        cast(aiohttp.ClientSession, cast(object, session))
    )
    assert token == "tokval"


async def test_get_metadata_token_non200(monkeypatch):
    session = FakeSession(put_resp=FakeResp(status=404))
    monkeypatch.setenv("EC2_TOKEN_URL", "http://token")
    token = await ps._get_metadata_token(
        cast(aiohttp.ClientSession, cast(object, session))
    )
    assert token is None


async def test_get_metadata_token_exception(monkeypatch):
    # raise by making put ctx raise
    session = FakeSession(put_resp=FakeResp(), raise_on_put=True)
    monkeypatch.setenv("EC2_TOKEN_URL", "http://token")
    token = await ps._get_metadata_token(
        cast(aiohttp.ClientSession, cast(object, session))
    )
    assert token is None


async def test_check_spot_termination_sets_event(monkeypatch):
    session = FakeSession(
        put_resp=FakeResp(status=200, text_value="tok"),
        get_resp=FakeResp(status=200, json_value={"action": "terminate", "time": "t"}),
    )
    monkeypatch.setenv("EC2_METADATA_URL", "http://meta")
    ev = asyncio.Event()
    await ps._check_spot_termination_notice(
        cast(aiohttp.ClientSession, cast(object, session)), ev
    )
    assert ev.is_set()


async def test_check_spot_termination_token_none(monkeypatch):
    # make token fetcher return None
    async def _get_metadata_token_no(session):
        await asyncio.sleep(0)
        return None

    monkeypatch.setattr(ps, "_get_metadata_token", _get_metadata_token_no)
    session = FakeSession()
    ev = asyncio.Event()
    await ps._check_spot_termination_notice(
        cast(aiohttp.ClientSession, cast(object, session)), ev
    )
    assert not ev.is_set()


async def test_check_spot_termination_jsondecode(monkeypatch):
    # make get_resp.json raise JSONDecodeError
    class BadResp(FakeResp):
        async def json(self):
            raise json.JSONDecodeError("msg", "doc", 0)

    session = FakeSession(
        put_resp=FakeResp(status=200, text_value="tok"), get_resp=BadResp(status=200)
    )
    monkeypatch.setenv("EC2_METADATA_URL", "http://meta")
    ev = asyncio.Event()
    await ps._check_spot_termination_notice(
        cast(aiohttp.ClientSession, cast(object, session)), ev
    )
    assert not ev.is_set()


async def test_watch_spot_termination_exits_when_event_set(monkeypatch):
    # Patch _check_spot_termination_notice to set the event directly
    async def setter(session, event):
        await asyncio.sleep(0)
        event.set()

    monkeypatch.setattr(ps, "_check_spot_termination_notice", setter)
    ev = asyncio.Event()
    # run watcher (it should exit after first check)
    await ps.watch_spot_termination(ev, check_interval=1)
    assert ev.is_set()


async def test_process_result_failure_cases(monkeypatch):
    r = FakeResult(success=False, error_message="err")
    assert await ps.process_result(r) is None

    # Use empty strings to satisfy static type expectations while still being falsy
    r2 = FakeResult(success=True, html="", url="")
    assert await ps.process_result(r2) is None


async def test_process_result_success(monkeypatch):
    r = FakeResult(success=True, html="<html>o</html>", url="http://ok")
    # extruct_extract & extract_standard patched by fixture
    res = await ps.process_result(r)
    assert res == {"standard": True, "url": "http://ok"}


async def test_batch_sender_flush_and_batch(monkeypatch):
    q = asyncio.Queue()
    # put 3 items and None; batch_size=2 => send_items called twice
    await q.put({"a": 1})
    await q.put({"a": 2})
    await q.put({"a": 3})
    await q.put(None)

    await ps.batch_sender(q, batch_size=2)
    # send_items is AsyncMock; check either await_count (if available) or call_count
    count = getattr(
        ps.send_items, "await_count", getattr(ps.send_items, "call_count", 0)
    )
    assert count == 2


async def test_scrape_counts_and_send(monkeypatch):
    # create crawler that returns 3 successful results
    results = [FakeResult(), FakeResult(), FakeResult()]
    crawler = FakeCrawler(results=list(results))
    urls = ["u1", "u2", "u3"]
    ev = asyncio.Event()
    count = await ps.scrape(
        cast(ps.AsyncWebCrawler, cast(object, crawler)),
        urls,
        ev,
        run_config={},
        batch_size=2,
    )
    assert count == 3
    # ensure send_items called at least once
    count2 = getattr(
        ps.send_items, "await_count", getattr(ps.send_items, "call_count", 0)
    )
    assert count2 >= 1


async def test_scrape_handles_crawl_exception(monkeypatch):
    # crawler raises for one url
    crawler = FakeCrawler(results=[FakeResult()], raise_on={"bad"})
    urls = ["good", "bad", "good2"]
    ev = asyncio.Event()
    count = await ps.scrape(
        cast(ps.AsyncWebCrawler, cast(object, crawler)),
        urls,
        ev,
        run_config={},
        batch_size=10,
    )
    assert count == 3


async def test_scrape_handles_process_exception(monkeypatch):
    # monkeypatch process_result to raise on second
    async def pr(r):
        await asyncio.sleep(0)
        if getattr(r, "url", "").endswith("2"):
            raise RuntimeError("proc fail")
        return {"url": r.url}

    monkeypatch.setattr(ps, "process_result", pr)
    results = [FakeResult(url="http://1"), FakeResult(url="http://2")]
    crawler = FakeCrawler(results=list(results))
    urls = ["u1", "u2"]
    ev = asyncio.Event()
    count = await ps.scrape(
        cast(ps.AsyncWebCrawler, cast(object, crawler)),
        urls,
        ev,
        run_config={},
        batch_size=10,
    )
    assert count == 2


async def test_scrape_respects_shutdown(monkeypatch):
    # set shutdown while scraping
    async def slow_arun(url, config=None):
        await asyncio.sleep(0.02)
        return FakeResult()

    class SCrawler(FakeCrawler):
        async def arun(self, url, config=None):
            return await slow_arun(url, config)

    crawler = SCrawler(results=[FakeResult(), FakeResult(), FakeResult()])
    urls = ["u1", "u2", "u3"]
    ev = asyncio.Event()

    async def setter():
        await asyncio.sleep(0.01)
        ev.set()

    t_setter = asyncio.create_task(setter())
    count = await ps.scrape(
        cast(ps.AsyncWebCrawler, cast(object, crawler)),
        urls,
        ev,
        run_config={},
        batch_size=10,
    )
    # ensure background task reference is kept
    if not t_setter.done():
        t_setter.cancel()
    assert count < 3


async def test_handle_domain_message_parse_none_triggers_delete(monkeypatch):
    # parse_message_body returns (None, None) via fixture
    msg = object()
    db = ps.DynamoDBOperations()
    q = object()
    # delete_message patched to lambda -> no error
    await ps.handle_domain_message(
        msg, cast(ps.DynamoDBOperations, db), asyncio.Event(), q, batch_size=2
    )


async def test_handle_domain_message_no_urls_deletes(monkeypatch):
    # make parse_message_body return domain
    monkeypatch.setattr(ps, "parse_message_body", lambda m: ("d", None))

    class DB:
        def get_product_urls_by_domain(self, domain):
            return []

    db = DB()
    msg = object()
    q = object()
    await ps.handle_domain_message(
        msg,
        cast(ps.DynamoDBOperations, cast(object, db)),
        asyncio.Event(),
        q,
        batch_size=2,
    )


async def test_handle_domain_message_requeue_on_exception(monkeypatch):
    # parse returns domain with some urls
    monkeypatch.setattr(ps, "parse_message_body", lambda m: ("d", None))

    class DB:
        def get_product_urls_by_domain(self, domain):
            return ["a", "b", "c"]

    db = DB()
    msg = object()
    # make build components return trivial configs
    monkeypatch.setattr(ps, "build_product_scraper_components", lambda: ({}, {}))

    # create a crawler whose __aenter__ raises to simulate fatal processing error
    class RaisingCrawler(FakeCrawler):
        async def __aenter__(self):
            raise RuntimeError("enter fail")

    monkeypatch.setattr(ps, "AsyncWebCrawler", lambda config=None: RaisingCrawler())

    requeued = []

    def fake_send_message(q, body):
        requeued.append(body)

    monkeypatch.setattr(ps, "send_message", fake_send_message)
    # delete_message is patched to lambda via fixture

    await ps.handle_domain_message(
        msg,
        cast(ps.DynamoDBOperations, cast(object, db)),
        asyncio.Event(),
        object(),
        batch_size=1,
    )
    # after exception, it should have tried to requeue at least once
    assert requeued


async def test_process_message_batch_runs_handlers(monkeypatch):
    called = []

    async def fake_handle(m, db, ev, q, batch_size):
        await asyncio.sleep(0)
        called.append(m)

    monkeypatch.setattr(ps, "handle_domain_message", fake_handle)
    await ps.process_message_batch(
        [1, 2, 3], ps.DynamoDBOperations(), asyncio.Event(), object(), batch_size=2
    )
    assert called == [1, 2, 3]


async def test_main_get_queue_failure(monkeypatch):
    from botocore.exceptions import ClientError

    def bad_get_queue(name):
        raise ClientError({"Error": {}}, "Op")

    monkeypatch.setattr(ps, "get_queue", bad_get_queue)
    # run main; it should exit cleanly
    await ps.main(n_shops=1, batch_size=1)


async def test_main_shutdown_cancels_watcher(monkeypatch):
    # patch get_queue to return something
    monkeypatch.setattr(ps, "get_queue", lambda n: object())

    class DB:
        def get_product_urls_by_domain(self, domain):
            return []

    monkeypatch.setattr(ps, "DynamoDBOperations", lambda: DB())

    # make receive_messages return empty (so loop continues) and then set shutdown
    seq = [[], []]

    def recv(q, n, v):
        if seq:
            return seq.pop(0)
        return []

    monkeypatch.setattr(ps, "receive_messages", recv)

    # patch watcher to a simple coroutine that waits until event is set
    async def fake_watch(ev, check_interval=1):
        # wait until main sets shutdown_event; then return
        await asyncio.wait_for(ev.wait(), timeout=2)

    monkeypatch.setattr(ps, "watch_spot_termination", fake_watch)

    # schedule shutdown after a short delay
    async def trigger_shutdown():
        await asyncio.sleep(0.05)
        ps.shutdown_event.set()

    t_shutdown = asyncio.create_task(trigger_shutdown())
    # main is expected to propagate watcher cancellation -> CancelledError
    with pytest.raises(asyncio.CancelledError):
        await ps.main(n_shops=1, batch_size=1)
    if not t_shutdown.done():
        t_shutdown.cancel()


async def test_handle_three_domains_no_shutdown(monkeypatch):
    # Three messages for three domains with various URL counts; no shutdown
    messages = [object(), object(), object()]
    domains = ["dom1", "dom2", "dom3"]

    # parse_message_body will return corresponding domain for each message
    def parse(m):
        return domains[messages.index(m)], None

    monkeypatch.setattr(ps, "parse_message_body", parse)

    # DB returns different number of URLs per domain
    class DB:
        def get_product_urls_by_domain(self, domain):
            return {
                "dom1": ["u1", "u2"],
                "dom2": ["u3"],
                "dom3": ["u4", "u5", "u6"],
            }[domain]

    db = DB()

    # build_product_scraper_components and AsyncWebCrawler should produce results for each url
    monkeypatch.setattr(ps, "build_product_scraper_components", lambda: ({}, {}))

    # crawler that yields a FakeResult per URL requested
    class SimpleCrawler(FakeCrawler):
        def __init__(self):
            super().__init__()

        async def arun(self, url, config=None):
            # return a FakeResult with the same url to help tracing
            return FakeResult(url=url)

    monkeypatch.setattr(ps, "AsyncWebCrawler", lambda config=None: SimpleCrawler())

    sent = []

    def fake_send_items(batch):
        sent.append(list(batch))

    monkeypatch.setattr(ps, "send_items", AsyncMock(side_effect=fake_send_items))

    deleted = []
    monkeypatch.setattr(ps, "delete_message", lambda m: deleted.append(m))

    ev = asyncio.Event()  # not set

    # run processing for 3 messages; batch_size=2 will produce ceil((2+1+3)/2)=3 sends
    await ps.process_message_batch(messages, db, ev, object(), batch_size=2)

    # verify that all messages resulted in delete_message calls
    assert len(deleted) == 3

    # total URLs = 6 -> with batch_size=2 should produce at least 3 sends
    # verify batch contents are from the expected domains (urls preserved)
    all_sent = [
        item.get("url") if isinstance(item, dict) else item for b in sent for item in b
    ]
    assert set(all_sent) == {"u1", "u2", "u3", "u4", "u5", "u6"}
    # ensure no batch exceeds the batch_size and at least the minimal number of batches were sent
    assert all(len(b) <= 2 for b in sent)
    assert len(sent) >= 3


async def test_handle_three_domains_with_shutdown(monkeypatch):
    # Three domains but shutdown will be triggered during processing of second domain
    messages = [object(), object(), object()]
    domains = ["d1", "d2", "d3"]

    def parse(m):
        return (domains[messages.index(m)], None)

    monkeypatch.setattr(ps, "parse_message_body", parse)

    class DB:
        def get_product_urls_by_domain(self, domain):
            return {
                "d1": ["a1", "a2"],
                "d2": ["b1", "b2", "b3"],
                "d3": ["c1"],
            }[domain]

    db = DB()

    monkeypatch.setattr(ps, "build_product_scraper_components", lambda: ({}, {}))

    # Crawler returns results but we'll trigger shutdown in the middle
    class SlowCrawler(FakeCrawler):
        async def arun(self, url, config=None):
            # slow to allow us to set shutdown between domains
            await asyncio.sleep(0.01)
            return FakeResult(url=url)

    monkeypatch.setattr(ps, "AsyncWebCrawler", lambda config=None: SlowCrawler())

    sent = []

    def fake_send_items(batch):
        sent.append(list(batch))

    monkeypatch.setattr(ps, "send_items", AsyncMock(side_effect=fake_send_items))

    deleted = []
    monkeypatch.setattr(ps, "delete_message", lambda m: deleted.append(m))

    ev = asyncio.Event()

    # schedule shutdown shortly after processing starts
    async def trigger():
        await asyncio.sleep(0.02)
        ev.set()

    t_trigger = asyncio.create_task(trigger())

    # process messages concurrently - expect that not all domains complete
    await ps.process_message_batch(messages, db, ev, object(), batch_size=2)

    # ensure trigger task is cancelled after run
    if not t_trigger.done():
        t_trigger.cancel()
    # At least first domain should have been deleted; others may or may not
    assert len(deleted) >= 1
    # Some batches may have been sent but total sent URLs should be <= total available
    all_sent = [
        item.get("url") if isinstance(item, dict) else item for b in sent for item in b
    ]
    assert set(all_sent).issubset({"a1", "a2", "b1", "b2", "b3", "c1"})


async def test_single_domain_batching_behavior(monkeypatch):
    # Single domain with 5 URLs and batch_size=2 should produce 3 sends
    messages = [object()]
    monkeypatch.setattr(ps, "parse_message_body", lambda m: ("only", None))

    class DB:
        def get_product_urls_by_domain(self, domain):
            return [f"p{i}" for i in range(5)]

    db = DB()
    monkeypatch.setattr(ps, "build_product_scraper_components", lambda: ({}, {}))

    class FastCrawler(FakeCrawler):
        async def arun(self, url, config=None):
            return FakeResult(url=url)

    monkeypatch.setattr(ps, "AsyncWebCrawler", lambda config=None: FastCrawler())

    sent = []

    def fake_send_items(batch):
        sent.append(list(batch))

    monkeypatch.setattr(ps, "send_items", AsyncMock(side_effect=fake_send_items))
    deleted = []
    monkeypatch.setattr(ps, "delete_message", lambda m: deleted.append(m))

    await ps.process_message_batch(
        messages, db, asyncio.Event(), object(), batch_size=2
    )

    assert len(deleted) == 1
    assert len(sent) == 3
    assert {
        item.get("url") if isinstance(item, dict) else item for b in sent for item in b
    } == {"p0", "p1", "p2", "p3", "p4"}
