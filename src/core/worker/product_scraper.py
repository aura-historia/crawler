import asyncio
import json
import logging
import os
import signal
from typing import Any, Dict, List, Optional
import aiohttp
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
from src.core.sqs.queue_wrapper import get_queue
from src.core.utils.send_items import send_items
from src.core.utils.spider import build_product_scraper_components
from src.core.utils.standards_extractor import extract_standard
from crawl4ai import AsyncWebCrawler

load_dotenv()

logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)

ScrapedData = Dict[str, Any]

QUEUE_NAME = os.getenv("SQS_QUEUE_NAME")

shutdown_event: asyncio.Event = asyncio.Event()


# Helper to run blocking calls without freezing the loop
async def run_sync(func, *args):
    loop = asyncio.get_running_loop()
    return await loop.run_in_executor(None, func, *args)


def signal_handler(signum: int, frame) -> None:
    """Signal handler that marks the worker as interrupted and re-queues
    the currently-processing SQS message (if any).

    The `frame` argument is required by the Python `signal` API even if
    unused.
    """
    logger.info("Signal %s received. Finishing current task and shutting down.", signum)

    if shutdown_event is not None:
        try:
            shutdown_event.set()
        except Exception:
            logger.debug("Failed to signal shutdown_event to the loop")


async def _get_metadata_token(session: aiohttp.ClientSession) -> Optional[str]:
    """Fetches a metadata token from the EC2 metadata service."""
    token_url = os.getenv("EC2_TOKEN_URL")
    headers = {"X-aws-ec2-metadata-token-ttl-seconds": "21600"}
    try:
        async with session.put(token_url, headers=headers, timeout=2) as token_resp:
            if token_resp.status == 200:
                return await token_resp.text()
            else:
                logger.debug(f"Failed to get metadata token: {token_resp.status}")
                return None
    except Exception as e:
        logger.debug(f"Error fetching metadata token: {e}")
        return None


async def _check_spot_termination_notice(
    session: aiohttp.ClientSession, event: asyncio.Event
):
    """Checks for a spot termination notice and sets the event if found."""
    metadata_url = os.getenv("EC2_METADATA_URL")
    try:
        token = await _get_metadata_token(session)
        if not token:
            return

        headers = {"X-aws-ec2-metadata-token": token}
        async with session.get(metadata_url, headers=headers, timeout=2) as resp:
            if resp.status == 200:
                try:
                    data = await resp.json()
                    action = data.get("action")
                    if action in ["terminate", "stop"]:
                        logger.warning(
                            f"Spot {action} imminent! Time: {data.get('time')}"
                        )
                        event.set()
                except json.JSONDecodeError:
                    logger.debug("Failed to decode instance-action JSON")
    except asyncio.CancelledError:
        logger.info("Spot termination watcher cancelled.")
        raise
    except Exception as e:
        logger.debug(f"Error checking spot status: {e}")


async def watch_spot_termination(event: asyncio.Event, check_interval: int = 5) -> None:
    """
    Poll for spot termination notice using the modern 'instance-action' endpoint.
    """
    async with aiohttp.ClientSession() as session:
        while not event.is_set():
            try:
                # Wait for the check_interval or until the event is set.
                await asyncio.wait_for(event.wait(), timeout=check_interval)
            except asyncio.TimeoutError:
                # Timeout expired, time to check for the notice.
                if not event.is_set():
                    await _check_spot_termination_notice(session, event)
            except asyncio.CancelledError:
                logger.info("Spot termination watcher task cancelled.")
                raise

        logger.info("Spot termination watcher finished.")


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

    logger.info(f"Extracted raw data: {standardized}")

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
    Crawl the given list of `urls` in streaming mode and send extracted items
    to the backend in batches.

    Args:
        crawler: An instance of AsyncWebCrawler to use for crawling.
        urls: Sequence of URLs to crawl.
        shutdown_event: An asyncio.Event that signals when to stop processing.
        run_config: The configuration for the crawler run.
        batch_size: Number of items to accumulate before sending to the backend.
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
    Handle a single domain-processing message from the queue.

    This function fetches the product URLs for the domain (from DynamoDB),
    computes a start position if a `next` key is present, and runs
    `crawl_streaming` for the remaining URLs. On interruption, it will attempt
    to re-queue the message for future processing.

    Args:
        message: SQS message object containing the domain to process.
        db: DynamoDBOperations instance used to retrieve product URLs.
        shutdown_event: An asyncio.Event that signals when to stop processing.
        queue: The SQS queue object for re-queuing.
        batch_size: Batch size to pass to `crawl_streaming`.
    """

    domain, next_url = parse_message_body(message)
    if not domain:
        await run_sync(delete_message, message)
        return

    logger.info("Processing domain: %s", domain)
    product_urls = await run_sync(db.get_product_urls_by_domain, domain)

    start_index = 0
    if next_url:
        try:
            start_index = product_urls.index(next_url)
        except ValueError:
            pass

    urls_to_crawl = product_urls[start_index:]
    if not urls_to_crawl:
        await run_sync(delete_message, message)
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
            await run_sync(send_message, queue, body)
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
                await run_sync(delete_message, message)
            return

        await run_sync(delete_message, message)
    except Exception as e:
        logger.exception("Error handling domain %s: %s", domain, e)
        if await requeue_remaining("processing error"):
            await run_sync(delete_message, message)


async def process_message_batch(
    messages: List[Any],
    db: DynamoDBOperations,
    shutdown_event: asyncio.Event,
    queue: Any,
    batch_size: int,
) -> None:
    """Poll SQS once and process the returned messages."""

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
            loop.add_signal_handler(sig, signal_handler, sig, None)
        except NotImplementedError:
            signal.signal(sig, lambda s, f: shutdown_event.set())

    watcher_task = asyncio.create_task(watch_spot_termination(shutdown_event))

    logger.info("Worker started. Listening for messages...")

    while not shutdown_event.is_set():
        try:
            messages = await run_sync(receive_messages, queue, n_shops, 1)

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
