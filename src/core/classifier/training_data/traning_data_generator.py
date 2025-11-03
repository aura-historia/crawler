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
from crawl4ai import (
    BrowserConfig,
    CrawlerRunConfig,
    AsyncWebCrawler,
)
from extruct import extract as extruct_extract
from w3lib.html import get_base_url

from src.core.utils.spider import crawl_urls
from src.core.utils.standards_extractor import extract_standard

# Setup logging
logging.basicConfig(
    level=logging.INFO, format="%(asctime)s - %(levelname)s - %(message)s"
)

# Batch sizes
SHOP_BATCH_SIZE = 20
URL_BATCH_SIZE = 100


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


async def crawl_and_process_url(
    crawler: AsyncWebCrawler, url_to_crawl: str, run_config: CrawlerRunConfig
) -> Dict:
    """Crawl a single URL and determine whether it contains product data.

    This function uses the provided crawler instance to fetch HTML, then
    extracts structured data (json-ld, microdata, rdfa, opengraph) and runs
    the repository's standard extractor.

    Args:
        crawler: An active AsyncWebCrawler instance.
        url_to_crawl: The target URL to crawl.
        run_config: CrawlerRunConfig controlling crawler behavior.

    Returns:
        A dict with keys: 'url', 'label' ('product', 'non-product', 'failed'),
        and 'status' (either 'success', '429', or an error message).
    """
    try:
        result = await crawler.arun(url=url_to_crawl, config=run_config)

        if result.success and "429 Too Many Requests" in (result.html or ""):
            logging.warning(f"RATE LIMIT: Server returned 429 for {url_to_crawl}")
            return {"url": url_to_crawl, "label": "failed", "status": "429"}

        if result.success:
            base_url = get_base_url(result.html, result.url)
            structured = extruct_extract(
                result.html,
                base_url=base_url,
                syntaxes=["json-ld", "microdata", "rdfa", "opengraph"],
            )

            extracted_data = await extract_standard(structured, result.url)
            if extracted_data:
                return {"url": url_to_crawl, "label": "product", "status": "success"}
            return {"url": url_to_crawl, "label": "non-product", "status": "success"}

        logging.error(
            f"FAILED to crawl {url_to_crawl}: {getattr(result, 'error_message', 'unknown')}"
        )
        return {
            "url": url_to_crawl,
            "label": "failed",
            "status": getattr(result, "error_message", "unknown"),
        }

    except asyncio.CancelledError:
        raise
    except Exception as exc:
        logging.error(f"Exception during crawling {url_to_crawl}: {exc}")
        return {"url": url_to_crawl, "label": "failed", "status": str(exc)}


async def crawl_batch(
    start_url: str,
    queue: asyncio.Queue,
    existing_urls: Set[str],
    run_config: CrawlerRunConfig,
):
    """Crawl URLs discovered from a single start URL and put results into a queue.

    This function extracts links from `start_url` using `crawl_urls`, filters
    out already-known URLs, and then crawls targets in bounded batches to
    limit concurrency.

    Args:
        start_url: The base page to scan for product links.
        queue: An asyncio.Queue where results dicts will be placed.
        existing_urls: A set of URLs already present in the CSV (to skip).
        run_config: CrawlerRunConfig to pass to the crawler when fetching pages.
    """
    logging.info(f"Starting crawl batch for: {start_url}")

    try:
        urls_list = await crawl_urls(start_url)
        logging.info(f"Found {len(urls_list)} URLs from {start_url}")
    except Exception as exc:
        logging.error(f"Could not extract URLs from {start_url}: {exc}")
        return

    urls_to_process = [url for url in urls_list if url not in existing_urls]
    duplicates_skipped = len(urls_list) - len(urls_to_process)
    if duplicates_skipped > 0:
        logging.info(f"Skipping {duplicates_skipped} duplicate URLs for {start_url}")

    if not urls_to_process:
        logging.info(f"No new URLs to process for {start_url}")
        return

    browser_config = BrowserConfig(headless=True, verbose=False)
    async with AsyncWebCrawler(config=browser_config) as crawler:
        # Process target URLs in bounded batches to avoid creating too many concurrent tasks
        for batch in _chunked(urls_to_process, URL_BATCH_SIZE):
            tasks = [crawl_and_process_url(crawler, url, run_config) for url in batch]
            results = await asyncio.gather(*tasks)
            for res in results:
                await queue.put(res)
            logging.info(f"Completed batch of {len(batch)} URLs for {start_url}")


async def writer_task(
    queue: asyncio.Queue, csv_file_path: Path, total_urls_to_process: int
):
    """Consume results from a queue and append them to a CSV file.

    Uses an asynchronous file API (aiofiles) to avoid blocking the event loop.

    Args:
        queue: An asyncio.Queue that receives result dicts from crawlers.
        csv_file_path: Destination CSV file path.
        total_urls_to_process: Expected total number of new URLs to process (for progress reporting).

    Returns:
        A tuple with counts: (product_count, non_product_count, failed_count).
    """
    fieldnames = ["url", "label"]
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

            csv_label = (
                "non-product"
                if result.get("label") == "failed"
                else result.get("label")
            )

            sio = io.StringIO()
            writer = csv.writer(sio)
            writer.writerow([result.get("url"), csv_label])
            await afp.write(sio.getvalue())

            processed_count += 1
            if result.get("label") == "product":
                product_count += 1
            elif result.get("label") == "non-product":
                non_product_count += 1
            else:  # failed
                failed_count += 1

            if processed_count % 20 == 0 or processed_count == total_urls_to_process:
                logging.info(
                    f"Progress: {processed_count}/{total_urls_to_process} URLs processed. "
                    f"Products: {product_count}, Non-Products: {non_product_count}, Failed: {failed_count}"
                )

            queue.task_done()

    logging.info("Writer task finished.")
    return product_count, non_product_count, failed_count


async def _extract_all_target_urls(
    start_urls: List[str], existing_urls: Set[str]
) -> Set[str]:
    """Extract target URLs from a list of start/shop URLs in SHOP_BATCH_SIZE batches.

    Args:
        start_urls: list of shop root pages to scan.
        existing_urls: set of URLs already present in CSV to avoid duplicates.

    Returns:
        A set of unique target URLs discovered across all start pages.
    """
    all_urls: Set[str] = set()
    for shop_batch in _chunked(start_urls, SHOP_BATCH_SIZE):
        tasks = [crawl_urls(url) for url in shop_batch]
        results = await asyncio.gather(*tasks, return_exceptions=True)
        for idx, extraction_result in enumerate(results):
            shop_url = shop_batch[idx]
            if isinstance(extraction_result, Exception):
                logging.error(
                    f"Failed to extract URLs from {shop_url}: {extraction_result}"
                )
                continue
            for url in extraction_result:
                if url not in existing_urls:
                    all_urls.add(url)
        logging.info(
            f"Processed a batch of {len(shop_batch)} shops; collected {len(all_urls)} unique target URLs so far."
        )
    return all_urls


async def _process_url_batches(
    unique_urls: List[str], queue: asyncio.Queue, run_config: CrawlerRunConfig
):
    """Crawl a list of URLs using AsyncWebCrawler in URL_BATCH_SIZE batches.

    Args:
        unique_urls: list of target URLs to crawl.
        queue: asyncio.Queue to put results into.
        run_config: CrawlerRunConfig for crawler behavior.
    """
    browser_config = BrowserConfig(headless=True, verbose=False)
    async with AsyncWebCrawler(config=browser_config) as crawler:
        for url_batch in _chunked(unique_urls, URL_BATCH_SIZE):
            tasks = [
                crawl_and_process_url(crawler, url, run_config) for url in url_batch
            ]
            crawled_results = await asyncio.gather(*tasks)
            for res in crawled_results:
                await queue.put(res)
            logging.info(f"Completed a URL batch of {len(url_batch)} targets.")


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


async def crawl_batch_parallel(start_urls: List[str]):
    """Orchestrate crawling for multiple start URLs with centralized writing.

    This function reads existing URLs from the CSV to avoid duplicates,
    extracts target URLs from the provided `start_urls` while processing the
    start URLs in batches of size `SHOP_BATCH_SIZE`, and crawls target URLs in
    bounded batches of size `URL_BATCH_SIZE` to limit concurrency.

    Args:
        start_urls: A list of base pages (shops) to scan for product pages.
    """
    logging.info(f"Starting PARALLEL crawl for {len(start_urls)} base URLs")

    csv_file_path = Path(__file__).parent / "training_data.csv"
    csv_file_path.parent.mkdir(parents=True, exist_ok=True)

    # Read existing URLs from CSV (asynchronously). Avoid pre-assigning to prevent linter unused-variable warning.
    existing_urls = await _read_existing_urls(csv_file_path)

    logging.info(f"Found {len(existing_urls)} existing URLs in CSV.")

    logging.info("Step 1: Extracting all URLs from start pages in batches...")
    all_urls_to_process = await _extract_all_target_urls(start_urls, existing_urls)

    total_urls_to_process = len(all_urls_to_process)
    if total_urls_to_process == 0:
        logging.info("No new URLs to process across all start URLs. Exiting.")
        return

    logging.info(f"Total of {total_urls_to_process} new unique URLs to be crawled.")

    run_config = CrawlerRunConfig(
        stream=True,
        check_robots_txt=True,
        verbose=False,
        delay_before_return_html=2.0,
        mean_delay=3.0,
        max_range=2.0,
    )

    queue: asyncio.Queue = asyncio.Queue()

    # Start the writer task
    writer = asyncio.create_task(
        writer_task(queue, csv_file_path, total_urls_to_process)
    )

    unique_urls_to_crawl = list(all_urls_to_process)
    logging.info(f"Creating crawl tasks for {len(unique_urls_to_crawl)} URLs.")

    await _process_url_batches(unique_urls_to_crawl, queue, run_config)

    # Signal the writer that we are done
    await queue.put(None)

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
    asyncio.run(crawl_batch_parallel(load_antique_shop_urls()))
