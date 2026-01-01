import asyncio
import json
import os
from datetime import datetime
from typing import Any, Dict, List, Optional

from dotenv import load_dotenv
from extruct import extract as extruct_extract
from w3lib.html import get_base_url
from src.core.aws.database.operations import DynamoDBOperations, db_operations
from src.core.aws.sqs.message_wrapper import (
    send_message,
    delete_message,
    parse_message_body,
    visibility_heartbeat,
)
from src.core.aws.sqs.queue_wrapper import get_queue
from src.core.utils.logger import logger
from src.core.utils.send_items import send_items
from src.core.utils.spider_config import build_product_scraper_components
from src.core.utils.standards_extractor import extract_standard
from src.core.worker.base_worker import generic_worker, run_worker_pool
from crawl4ai import AsyncWebCrawler
from src.core.aws.database.models import URLEntry

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
    domain: str,
    urls: List[str],
    shutdown_event: asyncio.Event,
    run_config: Any,
    batch_size: int = 500,
) -> int:
    """
    Crawl the given URLs sequentially, enqueue extracted products, and flush them in batches.

    Args:
        crawler: Initialized `AsyncWebCrawler` instance (context-managed by caller).
        domain: Domain being processed.
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
                    hash_changed = await update_hash(extracted, domain, url)
                    if hash_changed:
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


async def update_hash(extracted, domain, url) -> bool:
    """
    Update the hash of a URL entry in the database based on status and price.

    Args:
        extracted: Extracted structured data from the webpage.
        domain: The domain of the URL.
        url: The URL being processed.
    Returns:
        True if the hash was updated, False otherwise.
    """

    status = extracted.get("state")
    price = extracted.get("price").get("amount")

    print(status, price)

    new_hash = URLEntry.calculate_hash(status, price)
    old_entry = await asyncio.to_thread(db_operations.get_url_entry, domain, url)
    old_hash = old_entry.hash if old_entry else None
    if (not old_hash and new_hash) or (old_hash != new_hash):
        try:
            await asyncio.to_thread(
                db_operations.update_url_hash, domain, url, new_hash
            )
            return True
        except Exception as e:
            logger.error(f"Failed to update hash for {url}: {e}")

    return False


async def handle_domain_message(
    message: Any,
    db: DynamoDBOperations,
    shutdown_event: asyncio.Event,
    queue: Any,
    batch_size: int = 10,
) -> None:
    """Handles the full lifecycle of a single domain message."""
    domain, next_url = parse_message_body(message)
    if not domain:
        await asyncio.to_thread(delete_message, message)
        return

    logger.info("Processing domain: %s", domain)
    stop_event = asyncio.Event()
    heartbeat_task = visibility_heartbeat(message, stop_event)

    try:
        scrape_start_time = datetime.now().isoformat()
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

        items_processed = 0

        async def requeue_remaining(reason: str) -> bool:
            current_absolute_index = start_index + items_processed
            if current_absolute_index >= len(product_urls):
                return False

            next_url_candidate = product_urls[current_absolute_index]
            body = json.dumps({"domain": domain, "next": next_url_candidate})
            try:
                await asyncio.to_thread(send_message, queue, body)
                logger.info(
                    "Requeued domain %s at %s due to %s",
                    domain,
                    next_url_candidate,
                    reason,
                )
                return True
            except Exception as exc:
                logger.exception("Failed to requeue: %s", exc)
                return False

        try:
            browser_config, run_config = build_product_scraper_components()
            async with AsyncWebCrawler(config=browser_config) as crawler:
                items_processed = await scrape(
                    crawler=crawler,
                    domain=domain,
                    urls=urls_to_crawl,
                    shutdown_event=shutdown_event,
                    run_config=run_config,
                    batch_size=batch_size,
                )

            if shutdown_event.is_set():
                if await requeue_remaining("shutdown signal"):
                    await asyncio.to_thread(delete_message, message)
                return

            await asyncio.to_thread(
                db.update_shop_metadata,
                domain=domain,
                last_scraped_start=scrape_start_time,
                last_scraped_end=datetime.now().isoformat(),
            )
            await asyncio.to_thread(delete_message, message)
        except Exception as e:
            logger.exception("Error handling domain %s: %s", domain, e)
            if await requeue_remaining("processing error"):
                await asyncio.to_thread(delete_message, message)
    finally:
        stop_event.set()
        await heartbeat_task


async def worker(
    worker_id: int, queue: Any, db: DynamoDBOperations, batch_size: int
) -> None:
    """Worker function for processing domain messages from SQS queue.

    Args:
        worker_id (int): Unique identifier for this worker instance.
        queue (Any): SQS queue object to poll messages from.
        db (DynamoDBOperations): Database operations instance.
        batch_size (int): Number of items to batch before sending.
    """

    async def handler(message: Any) -> None:
        await handle_domain_message(message, db, shutdown_event, queue, batch_size)

    await generic_worker(
        worker_id=worker_id,
        queue=queue,
        shutdown_event=shutdown_event,
        message_handler=handler,
        max_messages=1,
        wait_time=20,
    )


async def main(n_workers: int = 2, batch_size: int = 10) -> None:
    """Main entry point managing the graceful exit of event-driven workers.

    Args:
        n_workers (int): Number of concurrent workers. Defaults to 2.
        batch_size (int): Items to batch before sending. Defaults to 10.
    """
    try:
        queue = get_queue(QUEUE_NAME)
        db = DynamoDBOperations()
        logger.info("Environment initialized successfully.")
    except Exception as e:
        logger.error(f"Initialization failed: {e}")
        return

    # Worker factory function
    async def create_worker(worker_id: int) -> None:
        await worker(worker_id, queue, db, batch_size)

    await run_worker_pool(
        n_workers=n_workers,
        shutdown_event=shutdown_event,
        worker_factory=create_worker,
        shutdown_timeout=90.0,
    )


if __name__ == "__main__":
    asyncio.run(main(n_workers=10, batch_size=10))
