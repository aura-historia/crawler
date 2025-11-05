"""
Training data generator for classifier.

This module extracts product/non-product labels by crawling shop pages and
parsing structured data. It processes work in bounded batches to avoid
overloading remote servers and local resources.
"""

import asyncio
import csv
import io
from pathlib import Path
import logging
from typing import List, Dict, Set, Iterable
import json

import aiofiles
from crawl4ai import AsyncWebCrawler
from extruct import extract as extruct_extract
from w3lib.html import get_base_url

from src.core.utils.spider import crawl_config, crawl_dispatcher
from src.core.utils.standards_extractor import extract_standard

# Setup logging
logging.basicConfig(
    level=logging.INFO, format="%(asctime)s - %(levelname)s - %(message)s"
)

# Batch size for processing shops concurrently
SHOP_BATCH_SIZE = 10


def load_antique_shop_urls(json_path: Path | None = None) -> List[str]:
    """Load all URLs from the antique shops JSON file.

    The repository's JSON file may include a leading comment line ("// ...").
    This function strips any lines that start with '//' before parsing to make
    it robust to that format.

    Args:
        json_path: Optional Path to the JSON file. If omitted, the function
                   locates the file relative to this module at
                   src/core/shops_finder/data/antique_shops_urls_domains.json.

    Returns:
        A list of URL strings extracted from the file. If the file is missing
        or malformed, a FileNotFoundError or ValueError is raised.
    """
    if json_path is None:
        # from src/core/classifier/training_data -> go up to 'core'
        json_path = (
            Path(__file__).parent.parent.parent
            / "shops_finder"
            / "data"
            / "antique_shops_urls_domains.json"
        )

    if not json_path.exists():
        raise FileNotFoundError(f"Could not find antique shops JSON at {json_path}")

    raw = json_path.read_text(encoding="utf-8")
    # Remove lines that are single-line comments starting with // to allow parsing
    cleaned = "\n".join(
        line for line in raw.splitlines() if not line.strip().startswith("//")
    )

    try:
        payload = json.loads(cleaned)
    except Exception as exc:
        raise ValueError(f"Failed to parse JSON file {json_path}: {exc}") from exc

    urls = [
        item.get("url")
        for item in payload.get("urls_and_domains", [])
        if item.get("url")
    ]
    return urls


def _chunked(iterable: Iterable, size: int) -> List[List]:
    """Split an iterable into chunks of `size`.

    Args:
        iterable: Any iterable to chunk.
        size: Maximum size of each chunk.

    Returns:
        A list of lists where each inner list is up to `size` elements.
    """
    it = list(iterable)
    return [it[i : i + size] for i in range(0, len(it), size)]


async def process_crawl_result(result) -> Dict:
    """Process a CrawlResult and determine whether it contains product data.

    This function extracts structured data (json-ld, microdata, rdfa, opengraph)
    from the crawl result's HTML and runs the repository's standard extractor.

    Args:
        result: A CrawlResult object from crawl4ai.

    Returns:
        A dict with keys: 'url', 'label' ('product', 'non-product', 'failed'),
        and 'status' (either 'success', '429', or an error message).
    """
    try:
        url = result.url

        # Check for rate limiting
        if result.success and "429 Too Many Requests" in (result.html or ""):
            logging.warning(f"RATE LIMIT: Server returned 429 for {url}")
            return {"url": url, "label": "failed", "status": "429"}

        # Process successful crawls
        if result.success and result.html:
            # Ensure html is a string (sometimes it can be a list)
            html_content = result.html
            if isinstance(html_content, list):
                html_content = html_content[0] if html_content else ""

            if not html_content or not isinstance(html_content, str):
                return {"url": url, "label": "failed", "status": "Invalid HTML content"}

            base_url = get_base_url(html_content, result.url)
            structured = extruct_extract(
                html_content,
                base_url=base_url,
                syntaxes=["json-ld", "microdata", "rdfa", "opengraph"],
            )

            extracted_data = await extract_standard(structured, result.url)
            if extracted_data:
                return {"url": url, "label": "product", "status": "success"}
            return {"url": url, "label": "non-product", "status": "success"}

        # Handle failed crawls
        logging.error(
            f"FAILED to crawl {url}: {getattr(result, 'error_message', 'unknown')}"
        )
        return {
            "url": url,
            "label": "failed",
            "status": getattr(result, "error_message", "unknown"),
        }

    except asyncio.CancelledError:
        raise
    except Exception as exc:
        logging.error(f"Exception during processing {result.url}: {exc}")
        return {"url": result.url, "label": "failed", "status": str(exc)}


async def stream_and_process_shop(
    start_url: str,
    queue: asyncio.Queue,
    existing_urls: Set[str],
):
    """Stream crawl results from a shop URL and process them in real-time.

    This function uses crawl_urls to stream CrawlResults from start_url,
    processes each result to determine if it's a product page, and puts
    the results into a queue for writing.

    Args:
        start_url: The base page to scan for product links.
        queue: An asyncio.Queue where results dicts will be placed.
        existing_urls: A set of URLs already present in the CSV (to skip).
    """
    logging.info(f"Starting streaming crawl for: {start_url}")

    try:
        # crawl_urls now returns an async generator of CrawlResults
        async with AsyncWebCrawler() as crawler:
            # Use strategy directly to get all discovered URLs
            async for result in await crawler.arun(
                start_url, config=crawl_config(), dispatcher=crawl_dispatcher()
            ):
                if result.url in existing_urls:
                    logging.debug(f"Skipping duplicate URL: {result.url}")
                    continue

                # Process the result and put it in the queue
                processed = await process_crawl_result(result)
                await queue.put(processed)

                # Add to existing_urls to avoid processing duplicates in the same run
                existing_urls.add(result.url)

    except Exception as exc:
        logging.error(f"Error during streaming crawl of {start_url}: {exc}")


async def writer_task(queue: asyncio.Queue, csv_file_path: Path):
    """Consume results from a queue and append them to a CSV file.

    Uses an asynchronous file API (aiofiles) to avoid blocking the event loop.

    Args:
        queue: An asyncio.Queue that receives result dicts from crawlers.
        csv_file_path: Destination CSV file path.

    Returns:
        A tuple with counts: (product_count, non_product_count, failed_count).
    """
    fieldnames = ["url", "label", "status"]
    file_exists = csv_file_path.exists()

    processed_count = 0
    product_count = 0
    non_product_count = 0
    failed_count = 0

    # Open the file asynchronously and write rows as they arrive. We use io.StringIO
    # + csv.writer to produce correctly escaped CSV text, then write via aiofiles.
    async with aiofiles.open(csv_file_path, "a", encoding="utf-8", newline="") as afp:
        # Write header if needed
        if not file_exists:
            sio = io.StringIO()
            writer = csv.writer(sio)
            writer.writerow(fieldnames)
            await afp.write(sio.getvalue())
            logging.info(f"Created new CSV file with headers at {csv_file_path}")

        while True:
            result = await queue.get()
            if result is None:  # Sentinel value to stop
                queue.task_done()
                break

            # Determine the label: if failed, mark as "failed", otherwise use the actual label
            csv_label = result.get("label")

            # Get the status (either "success" or the error message)
            csv_status = result.get("status", "unknown")

            sio = io.StringIO()
            writer = csv.writer(sio)
            writer.writerow([result.get("url"), csv_label, csv_status])
            await afp.write(sio.getvalue())

            processed_count += 1
            if result.get("label") == "product":
                product_count += 1
            elif result.get("label") == "non-product":
                non_product_count += 1
            else:  # failed
                failed_count += 1

            if processed_count % 20 == 0:
                logging.info(
                    f"Progress: {processed_count} URLs processed. "
                    f"Products: {product_count}, Non-Products: {non_product_count}, Failed: {failed_count}"
                )

            queue.task_done()

    logging.info("Writer task finished.")
    return product_count, non_product_count, failed_count


async def _read_existing_urls(csv_file_path: Path) -> Set[str]:
    """Read existing URLs from the CSV file asynchronously.

    Args:
        csv_file_path: Path to CSV file.

    Returns:
        A set of URLs found in the CSV file. Returns an empty set if file missing or unreadable.
    """
    if not csv_file_path.exists():
        return set()

    try:
        async with aiofiles.open(csv_file_path, "r", encoding="utf-8") as afp:
            content = await afp.read()
        reader = csv.DictReader(io.StringIO(content))
        return {row["url"] for row in reader if "url" in row}
    except Exception as exc:
        logging.error(f"Could not read existing CSV asynchronously: {exc}")
        return set()


async def shop_worker(
    shop_queue: asyncio.Queue,
    result_queue: asyncio.Queue,
    existing_urls: Set[str],
    worker_id: int,
):
    """Worker that continuously processes shops from a queue.

    As soon as one shop is finished, this worker picks up the next shop
    from the queue, ensuring maximum throughput.

    Args:
        shop_queue: Queue containing shop URLs to process
        result_queue: Queue to put crawl results into
        existing_urls: Set of already processed URLs
        worker_id: ID of this worker for logging
    """
    while True:
        try:
            shop_url = await shop_queue.get()
            if shop_url is None:  # Sentinel to stop
                shop_queue.task_done()
                break

            logging.info(f"[Worker {worker_id}] Starting shop: {shop_url}")
            await stream_and_process_shop(shop_url, result_queue, existing_urls)
            logging.info(f"[Worker {worker_id}] Completed shop: {shop_url}")

            shop_queue.task_done()
        except Exception as exc:
            logging.error(f"[Worker {worker_id}] Error processing shop: {exc}")
            shop_queue.task_done()


async def crawl_batch_parallel(start_urls: List[str]):
    """Orchestrate streaming crawl for multiple start URLs with centralized writing.

    This function uses a worker pool approach where SHOP_BATCH_SIZE workers
    continuously process shops. When a worker finishes a shop, it immediately
    picks up the next one from the queue, maximizing throughput.

    Args:
        start_urls: A list of base pages (shops) to scan for product pages.
    """
    logging.info(f"Starting STREAMING crawl for {len(start_urls)} shop URLs")
    logging.info(f"Using {SHOP_BATCH_SIZE} concurrent workers")

    csv_file_path = Path(__file__).parent / "training_data.csv"
    csv_file_path.parent.mkdir(parents=True, exist_ok=True)

    # Read existing URLs from CSV (asynchronously)
    existing_urls = await _read_existing_urls(csv_file_path)
    logging.info(f"Found {len(existing_urls)} existing URLs in CSV.")

    # Create queues
    shop_queue: asyncio.Queue = asyncio.Queue()
    result_queue: asyncio.Queue = asyncio.Queue()

    # Fill the shop queue
    for shop_url in start_urls:
        await shop_queue.put(shop_url)

    logging.info(f"Queued {len(start_urls)} shops for processing")

    # Start the writer task
    writer = asyncio.create_task(writer_task(result_queue, csv_file_path))

    # Start worker pool
    workers = []
    for worker_id in range(SHOP_BATCH_SIZE):
        worker = asyncio.create_task(
            shop_worker(shop_queue, result_queue, existing_urls, worker_id)
        )
        workers.append(worker)

    logging.info(f"Started {len(workers)} workers")

    # Wait for all shops to be processed
    await shop_queue.join()
    logging.info("All shops processed by workers")

    # Stop workers
    for _ in range(SHOP_BATCH_SIZE):
        await shop_queue.put(None)

    # Wait for workers to finish
    await asyncio.gather(*workers)
    logging.info("All workers stopped")

    # Signal the writer that we are done
    await result_queue.put(None)

    # Wait for the writer to finish and collect counts
    product_count, non_product_count, failed_count = await writer

    logging.info("All crawl batches completed!")

    # Final summary
    total_processed = product_count + non_product_count + failed_count
    logging.info(f"\n{'=' * 80}\n📊 FINAL SUMMARY\n{'=' * 80}")
    logging.info(f"Total new URLs processed: {total_processed}")
    if total_processed > 0:
        percentage_products = (product_count * 100) / total_processed
        percentage_non_products = (non_product_count * 100) / total_processed
        percentage_failed = (failed_count * 100) / total_processed
        logging.info(
            f"🛍️  Products found:           {product_count} ({percentage_products:.2f}%)"
        )
        logging.info(
            f"❌ Non-products:             {non_product_count} ({percentage_non_products:.2f}%)"
        )
        logging.info(
            f"⚠️  Failed to crawl:          {failed_count} ({percentage_failed:.2f}%)"
        )
    logging.info(f"📁 Data saved to: {csv_file_path}")
    total_in_csv = len(existing_urls) + total_processed
    logging.info(f"📊 Total unique URLs in CSV: {total_in_csv}\n{'=' * 80}")


if __name__ == "__main__":
    asyncio.run(crawl_batch_parallel(load_antique_shop_urls()[20:]))
