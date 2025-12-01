import asyncio
import logging
import os
import signal
from typing import Any, Dict, List, Optional
from botocore.exceptions import ClientError
from dotenv import load_dotenv
from extruct import extract as extruct_extract
from w3lib.html import get_base_url
from src.core.database.operations import DynamoDBOperations
from src.core.sqs.message_wrapper import (
    receive_messages,
    send_message,
    delete_message,
    parse_message_body,
)
from src.core.sqs.queue_wrapper import get_queue, create_queue
from src.core.utils.send_items import send_items
from src.core.utils.spider import build_product_scraper_components
from src.core.utils.standards_extractor import extract_standard
from crawl4ai import AsyncWebCrawler

load_dotenv()

logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)

ScrapedData = Dict[str, Any]

QUEUE_NAME = os.getenv("SQS_QUEUE_NAME")
queue = get_queue(QUEUE_NAME)
if not queue:
    queue = create_queue(QUEUE_NAME)
    logger.info(f"Queue {QUEUE_NAME} not found.")
    raise ResourceWarning

current_message: Optional[Any] = None

shutdown_event = asyncio.Event()


def signal_handler(signum: int, frame) -> None:
    """Signal handler that marks the worker as interrupted and re-queues
    the currently-processing SQS message (if any).

    The `frame` argument is required by the Python `signal` API even if
    unused.
    """
    global current_message, shutdown_event
    logger.info("Signal %s received. Finishing current task and shutting down.", signum)

    if shutdown_event is not None:
        try:
            shutdown_event.set()
        except Exception:
            logger.debug("Failed to signal shutdown_event to the loop")

    if current_message:
        try:
            send_message(queue, current_message.body)
            logger.info(
                "Re-queued message due to signal: %s",
                getattr(current_message, "body", "<no-body>"),
            )
        except ClientError as e:
            logger.exception(
                "Failed to re-queue message on signal (ClientError): %s", e
            )


async def spot_termination_watcher(check_interval: int = 30):
    while True:
        await asyncio.sleep(check_interval)


async def process_result(result: Any) -> Optional[ScrapedData]:
    """
    Process a single crawl result and extract a structured product representation.

    Args:
        result: The crawl result object returned by `AsyncWebCrawler.arun_many`.

    Returns:
        A dictionary with structured data (ScrapedData) or None if extraction
        failed or the result signals an unsuccessful fetch.
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

    return standardized


async def batch_sender(q: asyncio.Queue, batch_size: int) -> None:
    """
    Background consumer that reads items from `q` and sends them in batches.
    """
    batch: List[ScrapedData] = []
    while True:
        item = await q.get()
        if item is None:
            if batch:
                await send_items(batch)
            break

        batch.append(item)
        if len(batch) >= batch_size:
            await send_items(batch)
            batch.clear()


async def crawl_streaming(urls: List[str], batch_size: int = 500) -> None:
    """
    Crawl the given list of `urls` in streaming mode and send extracted items
    to the backend in batches.

    Args:
        urls: Sequence of URLs to crawl.
        batch_size: Number of items to accumulate before sending to the backend.
    """
    q: asyncio.Queue = asyncio.Queue()
    consumer_task = asyncio.create_task(batch_sender(q, batch_size))

    browser_config, run_config, dispatcher = build_product_scraper_components()

    async with AsyncWebCrawler(config=browser_config) as crawler:
        # `arun_many` yields results as they become available
        result_generator = await crawler.arun_many(
            urls=urls, config=run_config, dispatcher=dispatcher
        )
        async for result in result_generator:
            if shutdown_event.is_set():
                logger.info(
                    "Interrupted during crawl_streaming; stopping producer loop"
                )
                break

            extracted = await process_result(result)
            if extracted:
                await q.put(extracted)
    await q.put(None)
    await consumer_task


async def handle_domain_message(
    message: Any, db: DynamoDBOperations, batch_size: int = 10
) -> None:
    """
    Handle a single domain-processing message from the queue.

    This function fetches the product URLs for the domain (from DynamoDB),
    computes a start position if a `next` key is present, and runs
    `crawl_streaming` for the remaining URLs. On interruption, it will attempt
    to re-queue the message for future processing.

    Args:
        message: SQS message object containing the domain to process.
        db: DynamoDBOperations instance used to retrieve product URLs.
        batch_size: Batch size to pass to `crawl_streaming`.
    """
    global current_message

    domain, next_url = parse_message_body(message)
    if not domain:
        logger.error(
            "Message does not contain a domain: %s", getattr(message, "body", None)
        )
        delete_message(message)
        return

    logger.info("Processing domain: %s", domain)

    product_urls = db.get_product_urls_by_domain(domain)

    start_index = 0
    if next_url:
        try:
            start_index = product_urls.index(next_url)
        except ValueError:
            logger.warning(
                "next_url %s not found in product URLs for domain %s. Starting from the beginning.",
                next_url,
                domain,
            )

    urls_to_crawl = product_urls[start_index:]

    if not urls_to_crawl:
        logger.info("No URLs to crawl for domain: %s", domain)
        delete_message(message)
        return

    current_message = message

    await crawl_streaming(urls_to_crawl, batch_size=batch_size)
    send_message(queue, message.body)
    current_message = None
    delete_message(message)


async def process_message_batch(
    db: DynamoDBOperations, n_shops: int, batch_size: int
) -> None:
    """Poll SQS once and process the returned messages."""
    messages = receive_messages(queue, max_number=n_shops, wait_time=10)

    if not messages:
        return

    # Process messages concurrently (one task per message)
    tasks = [
        asyncio.create_task(handle_domain_message(m, db, batch_size=batch_size))
        for m in messages
    ]

    results = await asyncio.gather(*tasks, return_exceptions=True)
    for r in results:
        if isinstance(r, Exception):
            logger.exception("Error while handling message: %s", r)


async def main(n_shops: int = 1, batch_size: int = 10) -> None:
    """
    Main worker loop to poll SQS and process domain messages.
    """

    db = DynamoDBOperations()

    # Register signal handlers so we can gracefully shut down on SIGINT/SIGTERM
    try:
        signal.signal(signal.SIGINT, signal_handler)
        signal.signal(signal.SIGTERM, signal_handler)
    except Exception as e:
        logger.debug("Could not register signal handlers: %s", e)

    watcher_task = asyncio.create_task(spot_termination_watcher())

    while not shutdown_event.is_set():
        try:
            messages = receive_messages(queue, max_number=n_shops, wait_time=10)

            if not messages:
                try:
                    await asyncio.wait_for(shutdown_event.wait(), timeout=1.0)
                except asyncio.TimeoutError:
                    pass
                continue

            await process_message_batch(db, n_shops, batch_size)
        except Exception as e:
            logger.exception("An unexpected runtime error occurred in main loop: %s", e)

    # Shutdown watcher cleanly
    if not watcher_task.done():
        watcher_task.cancel()
        try:
            await watcher_task
        except asyncio.CancelledError:
            logger.debug("Watcher task cancelled during shutdown")
            raise

    logger.info("Worker shutting down.")


if __name__ == "__main__":
    asyncio.run(main(1))
