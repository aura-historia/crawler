import asyncio
import json
import os
import signal
from datetime import datetime
from typing import Any, List, Optional

from dotenv import load_dotenv

from src.core.classifier.url_classifier import URLBertClassifier
from src.core.aws.database.operations import DynamoDBOperations
from src.core.aws.database.models import URLEntry
from src.core.aws.sqs.message_wrapper import (
    receive_messages,
    delete_message,
    visibility_heartbeat,
)
from src.core.aws.sqs.queue_wrapper import get_queue
from src.core.utils.logger import logger
from src.core.utils.spider_config import crawl_config, crawl_dispatcher
from src.core.aws.spot.spot_termination_watcher import (
    watch_spot_termination,
    signal_handler,
)
from crawl4ai import AsyncWebCrawler, BrowserConfig


load_dotenv()

QUEUE_NAME = os.getenv("SQS_PRODUCT_SPIDER_QUEUE_NAME")
shutdown_event: asyncio.Event = asyncio.Event()


async def crawl_and_classify_urls(
    crawler: AsyncWebCrawler,
    start_url: str,
    domain: str,
    classifier: URLBertClassifier,
    db: DynamoDBOperations,
    shutdown_event: asyncio.Event,
    run_config: Any,
    batch_size: int = 50,
) -> int:
    """
    Crawl a website starting from start_url using BFS algorithm,
    classify each discovered URL, and save to database.

    Args:
        crawler: AsyncWebCrawler instance
        start_url: Starting URL for the crawl
        domain: Domain being crawled
        classifier: URLBertClassifier instance
        db: DynamoDBOperations instance
        shutdown_event: Event to signal shutdown
        run_config: Crawler configuration
        batch_size: Number of URLs to batch before writing to DB

    Returns:
        Number of URLs processed
    """
    processed_count = 0
    url_batch: List[URLEntry] = []

    try:
        async for result in await crawler.arun(
            start_url, config=run_config, dispatcher=crawl_dispatcher()
        ):
            if shutdown_event.is_set():
                logger.info("Shutdown event received, stopping crawl")
                break

            if not result.success:
                logger.debug(
                    f"Crawl failed for {result.url}: {getattr(result, 'error_message', 'Unknown error')}"
                )
                processed_count += 1
                continue

            url = result.url

            try:
                # Classify the URL
                is_product_bool, confidence = classifier.classify_url(url)

                logger.info(
                    f"URL: {url} | is_product: {is_product_bool} | confidence: {confidence:.3f}"
                )

                # Create URL entry with type field
                url_entry = URLEntry(
                    domain=domain,
                    url=url,
                    type="product" if is_product_bool else "other",
                )

                url_batch.append(url_entry)

                # Batch write to database
                if len(url_batch) >= batch_size:
                    logger.info(f"Writing batch of {len(url_batch)} URLs to database")
                    await asyncio.to_thread(db.batch_write_url_entries, url_batch)
                    url_batch.clear()

                processed_count += 1

            except Exception as e:
                logger.exception(f"Error processing URL {url}: {e}")
                processed_count += 1
                continue

    except Exception as e:
        logger.exception(f"Error during crawl: {e}")

    finally:
        # Write remaining URLs
        if url_batch:
            logger.info(f"Writing final batch of {len(url_batch)} URLs to database")
            try:
                await asyncio.to_thread(db.batch_write_url_entries, url_batch)
            except Exception as e:
                logger.exception(f"Error writing final batch: {e}")

    return processed_count


def parse_shop_message(message: Any) -> tuple[Optional[str], Optional[str]]:
    """
    Parse a shop message from SQS.

    Expected format: {"domain": "example.com"}
    The start_url is constructed as https://{domain}

    Args:
        message: SQS message object

    Returns:
        Tuple of (domain, start_url) or (None, None) if parsing fails
    """
    try:
        body = json.loads(message.body)
        domain = body.get("domain")

        if not domain:
            logger.error(f"Invalid message format - missing domain: {body}")
            return None, None

        clean_domain = domain.replace("https://", "").replace("http://", "").strip("/")
        start_url = f"https://{clean_domain}"

        return clean_domain, start_url

    except json.JSONDecodeError as e:
        logger.error(f"Failed to parse message body: {e}")
        return None, None
    except Exception as e:
        logger.error(f"Error parsing message: {e}")
        return None, None


async def handle_shop_message(
    message: Any,
    classifier: URLBertClassifier,
    db: DynamoDBOperations,
    shutdown_event: asyncio.Event,
    batch_size: int = 50,
) -> None:
    domain, start_url = parse_shop_message(message)

    if not domain or not start_url:
        await asyncio.to_thread(delete_message, message)
        return

    logger.info(f"Processing shop: {domain}")
    stop_event = asyncio.Event()
    heartbeat_task = visibility_heartbeat(message, stop_event)
    crawl_start_time = datetime.now().isoformat()

    try:
        await asyncio.to_thread(
            db.update_shop_metadata,
            domain=domain,
            last_crawled_start=crawl_start_time,
        )

        browser_config = BrowserConfig(headless=True)
        run_config = crawl_config()

        # 2. Wrap crawler in an inner try to catch shutdown cleanup noise
        try:
            async with AsyncWebCrawler(config=browser_config) as crawler:
                await crawl_and_classify_urls(
                    crawler=crawler,
                    start_url=start_url,
                    domain=domain,
                    classifier=classifier,
                    db=db,
                    shutdown_event=shutdown_event,
                    run_config=run_config,
                    batch_size=batch_size,
                )
        except (Exception, asyncio.CancelledError) as e:
            if shutdown_event.is_set():
                logger.debug(f"Interrupted crawl cleanup for {domain}: {e}")
            else:
                raise e

        if shutdown_event.is_set():
            logger.warning(
                f"Crawl for {domain} interrupted by shutdown signal. Skipping deletion."
            )
            return

        crawl_end_time = datetime.now().isoformat()
        await asyncio.to_thread(
            db.update_shop_metadata,
            domain=domain,
            last_crawled_end=crawl_end_time,
        )

        await asyncio.to_thread(delete_message, message)
        logger.info(f"Successfully processed and deleted message for {domain}")

    except Exception as e:
        logger.exception(f"Error handling shop {domain}: {e}")
    finally:
        if heartbeat_task:
            stop_event.set()
            await heartbeat_task


async def worker(
    worker_id: int,
    queue: Any,
    classifier: URLBertClassifier,
    db: DynamoDBOperations,
    batch_size: int,
):
    """
    Independent worker loop. Pulls 1 message, processes it fully,
    and repeats until shutdown.
    """
    logger.info(f"Worker-{worker_id} started and ready for tasks.")

    while not shutdown_event.is_set():
        try:
            if shutdown_event.is_set():
                break

            messages = await asyncio.to_thread(receive_messages, queue, 1, 20)
            if not messages:
                continue

            message = messages[0]
            logger.info(f"Worker-{worker_id} picked up a new domain.")

            if shutdown_event.is_set():
                logger.info(
                    f"Worker-{worker_id} discarding fetched message due to shutdown."
                )
                break

            # handle_shop_message manages its own heartbeat and crawl
            await handle_shop_message(
                message, classifier, db, shutdown_event, batch_size
            )

        except Exception as e:
            logger.exception(
                f"Worker-{worker_id} encountered an error: {e}", exc_info=True
            )

    logger.info(f"Worker-{worker_id} shutting down.")


async def main(n_workers: int = 3, batch_size: int = 50) -> None:
    """
    Entry point that initializes resources and spawns a worker pool.
    """
    try:
        queue = get_queue(QUEUE_NAME)
        db = DynamoDBOperations()
        logger.info("Loading URL classifier...")
        classifier = URLBertClassifier()
        logger.info("Environment initialized successfully.")
    except Exception as e:
        logger.critical(f"Initialization failed: {e}")
        return

    loop = asyncio.get_running_loop()
    for sig in (signal.SIGINT, signal.SIGTERM):
        try:
            loop.add_signal_handler(sig, signal_handler, sig, shutdown_event)
        except NotImplementedError:
            signal.signal(sig, lambda s, f: shutdown_event.set())

    watcher_task = asyncio.create_task(watch_spot_termination(shutdown_event))

    # Spawn worker pool
    logger.info(f"Starting {n_workers} concurrent workers...")
    workers = [
        asyncio.create_task(worker(i, queue, classifier, db, batch_size))
        for i in range(n_workers)
    ]

    # Wait for shutdown signal
    await shutdown_event.wait()
    logger.info(
        "Shutdown initiated. Waiting for active workers to finish current task..."
    )

    # Wait for workers to complete their current job (with a timeout)
    await asyncio.gather(*workers, return_exceptions=True)

    watcher_task.cancel()
    logger.info("Process finished.")


if __name__ == "__main__":
    # In AWS ECS/Fargate, adjust n_workers based on available vCPU/RAM
    asyncio.run(main(n_workers=3, batch_size=50))
