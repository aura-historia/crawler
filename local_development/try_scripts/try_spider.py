"""
Test script for product_spider - crawls a website, classifies URLs, and saves to database.

This script simulates the product spider workflow:
1. Sends a test domain to the SQS queue
2. Starts the spider worker to crawl the website
3. Classifies discovered URLs (product vs non-product)
4. Saves results to DynamoDB

Usage:
    python try_product_spider.py
"""

import asyncio
import json
import logging
from dotenv import load_dotenv

from src.core.aws.sqs.queue_wrapper import create_queue, get_queue
from src.core.aws.sqs.message_wrapper import send_message
from src.core.worker.product_spider import main as product_spider_main

# Load environment variables from .env file
load_dotenv()

# Configure logging
logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s - %(name)s - %(levelname)s - %(message)s",
    handlers=[logging.StreamHandler()],
)
logger = logging.getLogger(__name__)


async def run_test():
    """
    Sends test messages to the queue and then starts the product spider worker
    to crawl the websites, classify URLs, and save results to the database.
    """
    logger.info("Starting product spider test run...")

    # --- Configuration ---
    test_domains = [
        "antik-und-stil.com",
    ]
    queue_name = "spider_product_queue"
    batch_size = 50

    # --- Initialization and Message Sending ---
    try:
        create_queue(queue_name, attributes={"VisibilityTimeout": "600"})
        queue = get_queue(queue_name)
        queue.delete()
        create_queue(queue_name, attributes={"VisibilityTimeout": "50"})

        print(
            f"Current Visibility Timeout: {queue.attributes.get('VisibilityTimeout')}"
        )

        logger.info("--- Queue Visualizer ---")
        logger.info(f"Queue '{queue_name}' is ready at URL: {queue.url}")
        logger.info("------------------------")

        # Purge the queue to ensure a clean run
        logger.info("Purging the queue to remove any old messages...")
        queue.delete()
        logger.info("Queue purged. Waiting a moment for the purge to complete...")
        await asyncio.sleep(2)

        create_queue(queue_name, attributes={"VisibilityTimeout": "600"})

        for domain in test_domains:
            message_body = json.dumps({"domain": domain})
            send_message(queue, message_body)
            logger.info(f"Sent message for domain: {domain}")
            logger.info(f"  Start URL will be: https://{domain}")

        # Visualize queue attributes
        queue.reload()
        message_count = queue.attributes.get("ApproximateNumberOfMessages", "N/A")
        logger.info("--- Queue Visualizer ---")
        logger.info(f"Approximate messages in queue: {message_count}")
        logger.info("------------------------")

    except Exception as e:
        logger.error(f"Failed to initialize or send message: {e}", exc_info=True)
        return

    # --- Execution ---
    try:
        logger.info("Starting product spider worker...")
        logger.info("This will:")
        logger.info(f"  1. Crawl {len(test_domains)} domains using BFS strategy:")
        for domain in test_domains:
            logger.info(f"     - {domain}")
        logger.info("  2. Classify each URL as product/non-product using BERT model")
        logger.info("  3. Save results to DynamoDB")
        logger.info(
            "  4. Process up to max_depth=1000 with exclusions for assets/media"
        )
        logger.info("")

        # Create tasks for the spider worker and the termination simulator
        worker_task = asyncio.create_task(
            product_spider_main(batch_size=batch_size, n_workers=5)
        )

        # Wait for either the worker to finish or the termination to be triggered
        _, pending = await asyncio.wait(
            [worker_task], return_when=asyncio.FIRST_COMPLETED
        )

        # Cancel any remaining tasks
        for task in pending:
            task.cancel()
            await task

        logger.info("Product spider worker finished.")

    except Exception as e:
        logger.error(f"An error occurred during the spider run: {e}", exc_info=True)
    finally:
        # --- Final Queue Inspection ---
        logger.info("--- Post-Shutdown Queue Visualizer ---")
        try:
            # Re-connect to the queue to get the latest state
            final_queue = get_queue(queue_name)
            final_queue.reload()
            visible_messages = final_queue.attributes.get(
                "ApproximateNumberOfMessages", "N/A"
            )
            invisible_messages = final_queue.attributes.get(
                "ApproximateNumberOfMessagesNotVisible", "N/A"
            )
            logger.info(f"Inspecting queue '{queue_name}' after shutdown...")
            logger.info(f"Messages available for processing: {visible_messages}")
            logger.info(
                f"Messages currently in flight (being processed): {invisible_messages}"
            )
            logger.info("------------------------------------")
        except Exception as qe:
            logger.error(f"Could not inspect queue after shutdown: {qe}")

        logger.info("")
        logger.info("=" * 80)
        logger.info("Product spider test run finished!")
        logger.info("=" * 80)
        logger.info("")
        logger.info("Next steps:")
        logger.info("  1. Check DynamoDB for crawled URLs and classifications")
        logger.info("  2. Query for domains:")
        for domain in test_domains:
            logger.info(f"     - {domain}")
        logger.info("  3. Analyze product vs non-product ratios per domain")
        logger.info("")


if __name__ == "__main__":
    asyncio.run(run_test())
