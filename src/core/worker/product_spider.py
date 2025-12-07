import asyncio
import json
import os
import signal
from typing import Any, List, Optional

from aiohttp import ClientError
from dotenv import load_dotenv

from src.core.classifier.url_classifier import URLBertClassifier
from src.core.database.operations import DynamoDBOperations
from src.core.database.models import URLEntry
from src.core.sqs.message_wrapper import (
    receive_messages,
    delete_message,
)
from src.core.sqs.queue_wrapper import get_queue, create_queue
from src.core.utils.logger import logger
from src.core.utils.spider import crawl_config, crawl_dispatcher
from src.core.utils.spot_termination_watcher import (
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
                is_product, confidence = classifier.classify_url(url)

                logger.info(
                    f"URL: {url} | is_product: {is_product} | confidence: {confidence:.3f}"
                )

                # Create URL entry
                url_entry = URLEntry(
                    domain=domain,
                    url=url,
                    is_product=is_product,
                    standards_used=[],
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
    """
    Handle a single shop-crawling message from the queue.

    This function extracts the domain from the message (e.g., {"domain": "example.com"}),
    constructs the start URL as https://{domain},
    crawls the website using BFS algorithm, classifies discovered URLs,
    and saves them to the database.

    If an error occurs, the message is NOT deleted and will automatically
    reappear in the queue after the visibility timeout expires.

    Args:
        message: SQS message object containing shop domain (format: {"domain": "example.com"})
        classifier: URLBertClassifier instance
        db: DynamoDBOperations instance
        shutdown_event: Event to signal shutdown
        batch_size: Batch size for database writes
    """
    domain, start_url = parse_shop_message(message)

    if not domain or not start_url:
        await asyncio.to_thread(delete_message, message)
        return

    logger.info(f"Processing shop: {domain}, starting from: {start_url}")

    try:
        # Build crawler configuration
        browser_config = BrowserConfig(headless=True)
        run_config = crawl_config()

        # Create crawler and run
        async with AsyncWebCrawler(config=browser_config) as crawler:
            urls_processed = await crawl_and_classify_urls(
                crawler=crawler,
                start_url=start_url,
                domain=domain,
                classifier=classifier,
                db=db,
                shutdown_event=shutdown_event,
                run_config=run_config,
                batch_size=batch_size,
            )

        logger.info(f"Processed {urls_processed} URLs for domain {domain}")

        await asyncio.to_thread(delete_message, message)

    except Exception as e:
        logger.exception(f"Error handling shop {domain}: {e}")
        logger.warning(
            f"Message not deleted for {domain} - will reappear in queue after visibility timeout"
        )


async def process_message_batch(
    messages: List[Any],
    classifier: URLBertClassifier,
    db: DynamoDBOperations,
    shutdown_event: asyncio.Event,
    batch_size: int,
) -> None:
    """
    Process a batch of messages from SQS in parallel.

    Each shop is crawled concurrently, allowing N shops to be processed simultaneously.

    Args:
        messages: List of SQS messages
        classifier: URLBertClassifier instance
        db: DynamoDBOperations instance
        shutdown_event: Event to signal shutdown
        batch_size: Batch size for database writes
    """
    if not messages:
        return

    logger.info(f"Processing {len(messages)} shops in parallel")

    # Create tasks for parallel processing
    tasks = []
    for message in messages:
        if shutdown_event.is_set():
            logger.info("Shutdown event received, stopping message processing")
            break

        task = asyncio.create_task(
            handle_shop_message(message, classifier, db, shutdown_event, batch_size)
        )
        tasks.append(task)

    # Wait for all tasks to complete
    if tasks:
        results = await asyncio.gather(*tasks, return_exceptions=True)

        # Log any exceptions that occurred
        for i, result in enumerate(results):
            if isinstance(result, Exception):
                logger.exception(f"Error handling message {i}: {result}")


async def main(n_shops: int = 5, batch_size: int = 50) -> None:
    """
    Main worker loop to poll SQS and process shop-crawling messages.

    Args:
        n_shops: Number of messages to receive per poll (max)
        batch_size: Number of URLs to batch before writing to database
    """
    try:
        create_queue(QUEUE_NAME)
        queue = get_queue(QUEUE_NAME)
    except ClientError as e:
        logger.error(f"Failed to get SQS queue '{QUEUE_NAME}': {e}")
        return

    db = DynamoDBOperations()

    logger.info("Loading URL classifier...")
    try:
        classifier = URLBertClassifier()
        logger.info("URL classifier loaded successfully")
    except Exception as e:
        logger.error(f"Failed to load URL classifier: {e}")
        return

    loop = asyncio.get_running_loop()
    for sig in (signal.SIGINT, signal.SIGTERM):
        try:
            loop.add_signal_handler(sig, signal_handler, sig, shutdown_event)
        except NotImplementedError:
            signal.signal(sig, lambda s, f: shutdown_event.set())

    # Start spot termination watcher
    watcher_task = asyncio.create_task(watch_spot_termination(shutdown_event))

    logger.info("Product spider worker started. Listening for shop messages...")

    while not shutdown_event.is_set():
        try:
            # Receive messages from queue
            messages = await asyncio.to_thread(receive_messages, queue, n_shops, 1)

            if not messages:
                # No messages, wait a bit
                try:
                    await asyncio.wait_for(shutdown_event.wait(), timeout=5.0)
                except asyncio.TimeoutError:
                    pass
                continue

            # Process the messages
            await process_message_batch(
                messages, classifier, db, shutdown_event, batch_size
            )

        except Exception as e:
            logger.exception(f"Error in main loop: {e}")

    logger.info("Product spider worker shutting down...")
    watcher_task.cancel()
    try:
        await watcher_task
    except asyncio.CancelledError:
        logger.debug("Watcher task cancelled successfully")
        raise


if __name__ == "__main__":
    asyncio.run(main(n_shops=3, batch_size=50))
