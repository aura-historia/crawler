import asyncio
import csv
from pathlib import Path
import logging
from typing import List, Dict, Set
import json

from crawl4ai import (
    BrowserConfig,
    CrawlerRunConfig,
    CacheMode,
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


async def crawl_and_process_url(
    crawler: AsyncWebCrawler, url_to_crawl: str, run_config: CrawlerRunConfig
) -> Dict:
    """Crawl a single URL and process its content."""
    try:
        result = await crawler.arun(url=url_to_crawl, config=run_config)

        if result.success and "429 Too Many Requests" in result.html:
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
            else:
                return {
                    "url": url_to_crawl,
                    "label": "non-product",
                    "status": "success",
                }
        else:
            logging.error(f"FAILED to crawl {url_to_crawl}: {result.error_message}")
            return {
                "url": url_to_crawl,
                "label": "failed",
                "status": result.error_message,
            }
    except Exception as e:
        logging.error(f"Exception during crawling {url_to_crawl}: {e}")
        return {"url": url_to_crawl, "label": "failed", "status": str(e)}


async def crawl_batch(
    start_url: str,
    queue: asyncio.Queue,
    existing_urls: Set[str],
    run_config: CrawlerRunConfig,
):
    """Crawl a batch of URLs from a starting URL and put results in a queue."""
    logging.info(f"Starting crawl batch for: {start_url}")

    try:
        urls_list = await crawl_urls(start_url)
        logging.info(f"Found {len(urls_list)} URLs from {start_url}")
    except Exception as e:
        logging.error(f"Could not extract URLs from {start_url}: {e}")
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
        tasks = [
            crawl_and_process_url(crawler, url, run_config) for url in urls_to_process
        ]
        results = await asyncio.gather(*tasks)
        for res in results:
            await queue.put(res)


async def writer_task(
    queue: asyncio.Queue, csv_file_path: Path, total_urls_to_process: int
):
    """A task that writes results from a queue to a CSV file."""
    fieldnames = ["url", "label"]
    file_exists = csv_file_path.exists()

    processed_count = 0
    product_count = 0
    non_product_count = 0
    failed_count = 0

    with open(csv_file_path, "a", newline="", encoding="utf-8") as csv_file:
        writer = csv.DictWriter(csv_file, fieldnames=fieldnames)
        if not file_exists:
            writer.writeheader()
            logging.info(f"Created new CSV file with headers at {csv_file_path}")

        while True:
            result = await queue.get()
            if result is None:  # Sentinel value to stop
                break

            # Write only url and label to CSV
            writer.writerow(
                {
                    "url": result["url"],
                    "label": "non-product"
                    if result["label"] == "failed"
                    else result["label"],
                }
            )
            processed_count += 1

            if result["label"] == "product":
                product_count += 1
            elif result["label"] == "non-product":
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


async def crawl_batch_parallel(start_urls: List[str]):
    """
    Processes multiple crawl_batch calls in parallel with centralized CSV writing.
    """
    logging.info(f"Starting PARALLEL crawl for {len(start_urls)} base URLs")

    csv_file_path = (
        Path(__file__).parent.parent.parent.parent / "data" / "training_data.csv"
    )
    csv_file_path.parent.mkdir(parents=True, exist_ok=True)

    existing_urls = set()
    if csv_file_path.exists():
        try:
            with open(csv_file_path, "r", encoding="utf-8") as f:
                reader = csv.DictReader(f)
                existing_urls = {row["url"] for row in reader if "url" in row}
            logging.info(f"Found {len(existing_urls)} existing URLs in CSV.")
        except Exception as e:
            logging.error(f"Could not read existing CSV: {e}")

    # First, gather all URLs to be processed to get a total count
    logging.info("Step 1: Extracting all URLs from all start pages...")
    all_urls_to_process = set()
    url_extraction_tasks = [crawl_urls(url) for url in start_urls]
    results = await asyncio.gather(*url_extraction_tasks, return_exceptions=True)

    for i, result in enumerate(results):
        if isinstance(result, Exception):
            logging.error(f"Failed to extract URLs from {start_urls[i]}: {result}")
        else:
            for url in result:
                if url not in existing_urls:
                    all_urls_to_process.add(url)

    total_urls_to_process = len(all_urls_to_process)
    if total_urls_to_process == 0:
        logging.info("No new URLs to process across all start URLs. Exiting.")
        return

    logging.info(f"Total of {total_urls_to_process} new unique URLs to be crawled.")

    run_config = CrawlerRunConfig(
        cache_mode=CacheMode.BYPASS,
        stream=True,
        check_robots_txt=True,
        verbose=False,
        delay_before_return_html=2.0,
        mean_delay=3.0,
        max_range=2.0,
    )

    queue = asyncio.Queue()

    # Start the writer task
    writer = asyncio.create_task(
        writer_task(queue, csv_file_path, total_urls_to_process)
    )

    unique_urls_to_crawl = list(all_urls_to_process)
    logging.info(f"Creating crawl tasks for {len(unique_urls_to_crawl)} URLs.")

    browser_config = BrowserConfig(headless=True, verbose=False)
    async with AsyncWebCrawler(config=browser_config) as crawler:
        # Split URLs into chunks to avoid creating too many tasks at once
        chunk_size = 100
        for i in range(0, len(unique_urls_to_crawl), chunk_size):
            chunk = unique_urls_to_crawl[i : i + chunk_size]
            tasks = [crawl_and_process_url(crawler, url, run_config) for url in chunk]
            crawled_results = await asyncio.gather(*tasks)
            for res in crawled_results:
                await queue.put(res)
            logging.info(f"Completed a chunk of {len(chunk)} URLs.")

    # Signal the writer that we are done
    await queue.put(None)

    # Wait for the writer to finish
    product_count, non_product_count, failed_count = await writer

    logging.info("All crawl batches completed!")

    # Final summary
    total_processed = product_count + non_product_count + failed_count
    logging.info(f"\n{'=' * 80}\n📊 FINAL SUMMARY\n{'=' * 80}")
    logging.info(f"Total new URLs processed: {total_processed}")
    if total_processed > 0:
        percentage_products = (
            (product_count * 100) / total_processed if total_processed > 0 else 0
        )
        percentage_non_products = (
            (non_product_count * 100) / total_processed if total_processed > 0 else 0
        )
        percentage_failed = (
            (failed_count * 100) / total_processed if total_processed > 0 else 0
        )
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
    # Load start URLs from scripts/data/antique_shops_urls_domains.json
    data_file = Path(__file__).parent / "data" / "antique_shops_urls_domains.json"
    start_urls: List[str] = []
    if data_file.exists():
        try:
            with open(data_file, "r", encoding="utf-8") as f:
                payload = json.load(f)
            entries = (
                payload.get("urls_and_domains", []) if isinstance(payload, dict) else []
            )
            for entry in entries:
                try:
                    if entry.get("is_antique_shop"):
                        url = entry.get("url")
                        if url:
                            start_urls.append(url)
                except Exception:
                    continue
            logging.info(
                f"Found {len(start_urls)} antique shop start URLs from {data_file}"
            )
        except Exception as e:
            logging.error(f"Failed to read {data_file}: {e}")

    if not start_urls:
        logging.warning(
            "No start URLs found in JSON, falling back to an empty string example."
        )
        start_urls = [""]

    # Run the parallel crawler with the extracted start URLs
    asyncio.run(crawl_batch_parallel(start_urls))
