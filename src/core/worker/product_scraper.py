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
from src.core.utils.spider_config import (
    build_product_scraper_components,
    crawl_dispatcher,
)
from src.core.worker.base_worker import generic_worker, run_worker_pool
from crawl4ai import AsyncWebCrawler
from src.core.aws.database.models import URLEntry
from src.core.scraper.qwen import extract as qwen_extract

load_dotenv()

ScrapedData = Dict[str, Any]

QUEUE_NAME = os.getenv("SQS_QUEUE_NAME")

REQUEST_TIMEOUT = int(os.getenv("REQUEST_TIMEOUT", "50"))
LOG_METRICS_INTERVAL = int(os.getenv("LOG_METRICS_INTERVAL", "50"))

_metrics = {
    "processed": 0,
    "success": 0,
    "timeout": 0,
    "error": 0,
    "extracted": 0,
    "sent_batches": 0,
}

shutdown_event: asyncio.Event = asyncio.Event()


async def process_result_async(result: Any) -> Optional[ScrapedData]:
    """
    Async version of process_result that uses async qwen extraction.
    """
    if not getattr(result, "success", False):
        return None
    markdown = result.markdown

    try:
        qwen_out = await qwen_extract(markdown[:40000])
    except Exception as e:
        logger.exception("qwen.extract failed: %s", e, extra={"url": getattr(result, "url", None)})
        return None

    parsed = json.loads(qwen_out)

    if not parsed:
        return None

    data: ScrapedData = {
        "shopsProductId": parsed.get("shop_item_id") or result.url,
        "title": {"text": parsed.get("title") or "", "language": parsed.get("language") or "UNKNOWN"},
        "description": {"text": parsed.get("description") or "", "language": parsed.get("language") or "UNKNOWN"},
        "price": {"amount": int(parsed.get("current_price")) or 0, "currency": parsed.get("currency") or "UNKNOWN"},
        "state": parsed.get("state") or "UNKNOWN",
        "images": parsed.get("images") or [],
        "url": result.url,
    }
    logger.info(data)

    return data


async def batch_sender(q: asyncio.Queue, batch_size: int) -> None:
    """Drain `q` and send accumulated items via `send_items` until a ``None`` sentinel arrives."""
    batch: List[ScrapedData] = []
    while True:
        item = await q.get()
        if item is None:
            if batch:
                try:
                    await send_items(batch)
                    _metrics["sent_batches"] += 1
                except Exception as e:
                    logger.exception("Failed to send final batch: %s", e)
            break

        batch.append(item)
        if len(batch) >= batch_size:
            try:
                await send_items(batch)
                _metrics["sent_batches"] += 1
            except Exception as e:
                logger.exception("Failed to send batch: %s", e)
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
    Crawl and extract URLs sequentially with inline processing.

    Each worker:
    1. Fetches HTML
    2. Waits for vLLM extraction response
    3. Queues result immediately
    """
    processed_count = 0
    q: asyncio.Queue = asyncio.Queue()
    consumer_task = asyncio.create_task(batch_sender(q, batch_size))

    async def process_url(url: str):
        """Fetch HTML and extract inline - each worker waits for its response"""
        nonlocal processed_count

        if shutdown_event.is_set():
            return

        try:
            result = await asyncio.wait_for(
                crawler.arun(url, config=run_config, dispatcher=crawl_dispatcher()),
                timeout=REQUEST_TIMEOUT
            )

            if result and getattr(result, "success", False):
                extracted = await process_result_async(result)
                if extracted:
                    _metrics["extracted"] += 1
                    await q.put(extracted)

        except Exception as e:
            _metrics["error"] += 1
            logger.exception("Error processing %s: %s", url, e, extra={"domain": domain})
        finally:
            processed_count += 1
            _metrics["processed"] += 1

            # Log metrics periodically
            if _metrics["processed"] % LOG_METRICS_INTERVAL == 0:
                logger.info(
                    "Metrics: processed=%s, extracted=%s, timeouts=%s, errors=%s, sent_batches=%s",
                    _metrics["processed"],
                    _metrics["extracted"],
                    _metrics["timeout"],
                    _metrics["error"],
                    _metrics["sent_batches"],
                    extra={"domain": domain},
                )

    # Process URLs one by one
    for url in urls:
        if shutdown_event.is_set():
            logger.info("Shutdown detected, stopping scrape loop.")
            break
        await process_url(url)

    await q.put(None)
    try:
        await consumer_task
    except Exception as ce:
        logger.exception("Error in batch sender: %s", ce, extra={"domain": domain})

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
    price_obj = extracted.get("price") or {}
    price = price_obj.get("amount")

    if not status or not price:
        logger.warning(f"Missing status or price for {url}", extra={"domain": domain})
        return False

    logger.debug(f"Status: {status}, Price: {price} for {url}")

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
            logger.error(
                f"Failed to update hash for {url}: {e}", extra={"domain": domain}
            )
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

    logger.info("Processing domain: %s", domain)
    stop_event = asyncio.Event()
    heartbeat_task = visibility_heartbeat(message, stop_event)

    try:
        product_urls = await asyncio.to_thread(
            db.get_all_product_urls_by_domain, domain
        )

        start_index = 0
        if next_url:
            try:
                start_index = product_urls.index(next_url)
            except ValueError:
                pass
        else:
            await asyncio.to_thread(
                db.update_shop_metadata,
                domain=domain,
                last_scraped_start=datetime.now().isoformat(),
                last_scraped_end=None,
            )

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
    asyncio.run(main(n_workers=2, batch_size=50))
