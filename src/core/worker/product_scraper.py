import asyncio
import json
import os
import signal
from datetime import datetime
from typing import Any, Dict, List, Optional

from botocore.exceptions import ClientError
from dotenv import load_dotenv
from extruct import extract as extruct_extract
from w3lib.html import get_base_url
from src.core.aws.database.operations import DynamoDBOperations
from src.core.aws.sqs.message_wrapper import (
    receive_messages,
    send_message,
    delete_message,
    parse_message_body,
)
from src.core.aws.sqs.queue_wrapper import get_queue
from src.core.utils.logger import logger
from src.core.utils.send_items import send_items
from src.core.utils.spider_config import build_product_scraper_components
from src.core.aws.spot.spot_termination_watcher import (
    watch_spot_termination,
    signal_handler,
)
from src.core.utils.standards_extractor import extract_standard
from crawl4ai import AsyncWebCrawler

load_dotenv()


ScrapedData = Dict[str, Any]

QUEUE_NAME = os.getenv("SQS_QUEUE_NAME")

shutdown_event: asyncio.Event = asyncio.Event()


async def process_result(result: Any) -> Optional[ScrapedData]:
    """
    Normalize the structured data contained in a single crawl result.

    Args:
        result: Object returned by `AsyncWebCrawler.arun` containing `success`,
            `html`, and `url` attributes.

    Returns:
        A ScrapedData dict when the crawl succeeded and extraction worked,
        otherwise ``None`` (e.g., network failure, missing html/url, or
        unsupported syntax).
    """
    if not getattr(result, "success", False):
        logger.debug(
            "Result indicates failure: %s", getattr(result, "error_message", None)
        )
        return None

    html = getattr(result, "html", None)
    url = getattr(result, "url", None)
    if not html or not url:
        logger.debug("Skipping result without html or url: %s", url)
        return None

    base_url = get_base_url(html, url)
    syntaxes = ["microdata", "opengraph", "json-ld", "rdfa"]
    extracted_raw = extruct_extract(html, base_url=base_url, syntaxes=syntaxes)

    standardized = await extract_standard(extracted_raw, url, preferred=syntaxes)

    logger.info(f"Extracted raw data: {standardized}")

    return standardized


async def batch_sender(q: asyncio.Queue, batch_size: int) -> None:
    """Drain `q` and send accumulated items via `send_items` until a ``None`` sentinel arrives."""
    batch: List[ScrapedData] = []
    while True:
        item = await q.get()
        if item is None:
            if batch:
                logger.info(f"Sending Items remaining: {batch}")
                await send_items(batch)
            break

        batch.append(item)
        if len(batch) >= batch_size:
            await send_items(batch)
            batch.clear()


async def scrape(
    crawler: AsyncWebCrawler,
    urls: List[str],
    shutdown_event: asyncio.Event,
    run_config: Any,
    batch_size: int = 500,
) -> int:
    """
    Crawl the given URLs sequentially, enqueue extracted products, and flush them in batches.

    Args:
        crawler: Initialized `AsyncWebCrawler` instance (context-managed by caller).
        urls: URLs to fetch.
        shutdown_event: Cooperative cancellation flag checked before each request.
        run_config: Dict passed to `crawler.arun(url, config=run_config)`.
        batch_size: Number of extracted items passed to `send_items` at once.

    Returns:
        Count of URLs attempted (successful or not).
    """
    processed_count = 0
    q: asyncio.Queue = asyncio.Queue()
    consumer_task = asyncio.create_task(batch_sender(q, batch_size))

    try:
        for url in urls:
            if shutdown_event.is_set():
                logger.info("Interrupted during crawl_streaming")
                break

            try:
                result = await crawler.arun(url, config=run_config)
            except Exception as crawl_error:
                logger.exception("Error crawling %s: %s", url, crawl_error)
                processed_count += 1
                continue

            try:
                extracted = await process_result(result)
                if extracted:
                    await q.put(extracted)
            except Exception as proc_error:
                logger.exception("Processing error for %s: %s", url, proc_error)

            processed_count += 1

    except Exception as outer_error:
        logger.exception("Unexpected error in crawl_streaming: %s", outer_error)

    finally:
        await q.put(None)

        try:
            await consumer_task
        except Exception as ce:
            logger.exception("Error in batch sender: %s", ce)

    return processed_count


async def handle_domain_message(
    message: Any,
    db: DynamoDBOperations,
    shutdown_event: asyncio.Event,
    queue: Any,
    batch_size: int = 10,
) -> None:
    """
    Resolve a domain message, crawl its product URLs, and requeue unfinished work if interrupted.

    Workflow:
        1. Parse ``domain``/``next`` from the message body.
        2. Fetch URLs from DynamoDB and resume at ``next`` if present.
        3. Build crawler components, run `scrape`, and stream items.
        4. On shutdown or errors, requeue the remaining URLs before deleting the original message.
    """

    domain, next_url = parse_message_body(message)
    if not domain:
        await asyncio.to_thread(delete_message, message)
        return

    logger.info("Processing domain: %s", domain)
    product_urls = await asyncio.to_thread(db.get_product_urls_by_domain, domain)

    start_index = 0
    if next_url:
        try:
            start_index = product_urls.index(next_url)
        except ValueError:
            pass

    urls_to_crawl = product_urls[start_index:]
    if not urls_to_crawl:
        await asyncio.to_thread(delete_message, message)
        return

    logger.info(urls_to_crawl)
    logger.info(f"Found {len(urls_to_crawl)} URLs to crawl")

    items_processed = 0

    async def requeue_remaining(reason: str) -> bool:
        current_absolute_index = start_index + items_processed
        if current_absolute_index >= len(product_urls):
            return False

        next_url_candidate = product_urls[current_absolute_index]
        body = json.dumps({"domain": domain, "next": next_url_candidate})

        logger.info(body)

        try:
            await asyncio.to_thread(send_message, queue, body)
            logger.info("Requeued domain %s due to %s", domain, reason)
            return True
        except Exception as exc:
            logger.exception("Failed to requeue: %s", exc)
            return False

    try:
        browser_config, run_config = build_product_scraper_components()

        async with AsyncWebCrawler(config=browser_config) as crawler:
            items_processed = await scrape(
                crawler=crawler,
                urls=urls_to_crawl,
                shutdown_event=shutdown_event,
                run_config=run_config,
                batch_size=batch_size,
            )

        logger.info("Items processed for domain %s: %d", domain, items_processed)

        if shutdown_event.is_set():
            if await requeue_remaining("shutdown signal"):
                await asyncio.to_thread(delete_message, message)
            return

        # Update shop metadata with last scraped date
        await asyncio.to_thread(
            db.update_shop_metadata,
            domain=domain,
            last_scraped=datetime.now().isoformat(),
        )

        await asyncio.to_thread(delete_message, message)
    except Exception as e:
        logger.exception("Error handling domain %s: %s", domain, e)
        if await requeue_remaining("processing error"):
            await asyncio.to_thread(delete_message, message)


async def process_message_batch(
    messages: List[Any],
    db: DynamoDBOperations,
    shutdown_event: asyncio.Event,
    queue: Any,
    batch_size: int,
) -> None:
    """Spawn one task per SQS message and wait for all to finish, logging per-task failures."""

    if not messages:
        return

    # Process messages concurrently (one task per message)
    tasks = [
        asyncio.create_task(
            handle_domain_message(m, db, shutdown_event, queue, batch_size=batch_size)
        )
        for m in messages
    ]

    results = await asyncio.gather(*tasks, return_exceptions=True)
    for r in results:
        if isinstance(r, Exception):
            logger.exception("Error while handling message: %s", r)


async def main(n_shops: int = 5, batch_size: int = 10) -> None:
    """
    Main worker loop to poll SQS and process domain messages.
    """
    try:
        queue = get_queue(QUEUE_NAME)
    except ClientError as e:
        logger.error(f"Failed to get SQS queue '{QUEUE_NAME}': {e}")
        return

    db = DynamoDBOperations()

    loop = asyncio.get_running_loop()
    for sig in (signal.SIGINT, signal.SIGTERM):
        try:
            loop.add_signal_handler(sig, signal_handler, sig, shutdown_event)
        except NotImplementedError:
            signal.signal(sig, lambda s, f: shutdown_event.set())

    watcher_task = asyncio.create_task(watch_spot_termination(shutdown_event))

    logger.info("Worker started. Listening for messages...")

    while not shutdown_event.is_set():
        try:
            messages = await asyncio.to_thread(receive_messages, queue, n_shops, 1)

            if not messages:
                try:
                    await asyncio.wait_for(shutdown_event.wait(), timeout=1.0)
                except asyncio.TimeoutError:
                    pass
                continue

            await process_message_batch(messages, db, shutdown_event, queue, batch_size)
        except Exception as r:
            logger.exception("Error in main loop: %s", r)

    # Shutdown watcher cleanly
    if not watcher_task.done():
        watcher_task.cancel()
        try:
            await watcher_task
        except asyncio.CancelledError:
            logger.debug("Watcher task cancelled during shutdown; continuing shutdown")
            raise


if __name__ == "__main__":
    asyncio.run(main(n_shops=1))
