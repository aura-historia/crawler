import asyncio
import json
import logging
from dotenv import load_dotenv

from src.core.aws.sqs.queue_wrapper import create_queue, get_queue
from src.core.aws.sqs.message_wrapper import send_message
from src.core.worker.product_scraper import main as product_scraper_main

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
    Sends multiple test messages to the queue and then starts the main product scraper worker
    with parallel processing and a simulated spot termination.
    """
    logger.info("Starting scraper test run...")

    # --- Configuration ---
    test_domains = ["antik-shop.de"]
    queue_name = "product_scraper_queue"
    n_shops = 2
    backend_batch_size = 100
    vllm_batch_size = 5

    # --- Initialization and Message Sending ---
    try:
        create_queue(queue_name, attributes={"VisibilityTimeout": "600"})
        queue = get_queue(queue_name)
        queue.delete()
        # VisibilityTimeout: 600 seconds (10 minutes) to prevent message from becoming
        # visible again while still being processed
        create_queue(queue_name, attributes={"VisibilityTimeout": "600"})
        logger.info("--- Queue Visualizer ---")
        logger.info(f"Queue '{queue_name}' is ready at URL: {queue.url}")
        logger.info("------------------------")

        # Purge the queue to ensure a clean run
        logger.info("Purging the queue to remove any old messages...")
        queue.purge()
        logger.info("Queue purged. Waiting a moment for the purge to complete...")
        # Purging is an eventual consistency operation, a small delay helps ensure it's cleared.
        await asyncio.sleep(2)

        # Send test messages to the queue
        for domain in test_domains:
            message_body = json.dumps({"domain": domain})
            send_message(queue, message_body)
            logger.info(f"Sent message for domain: {domain}")

        # Visualize queue attributes
        # Note: This shows the state *after* sending. SQS attributes can have a slight delay in updating.
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
        logger.info(
            f"Starting product scraper main worker with {n_shops} parallel shops..."
        )

        # Create tasks for the scraper worker and the termination simulator
        worker_task = asyncio.create_task(
            product_scraper_main(
                n_workers=n_shops,
                backend_batch_size=backend_batch_size,
                vllm_batch_size=vllm_batch_size,
            )
        )

        # Wait for either the worker to finish or the termination to be triggered
        await asyncio.gather(worker_task)

        logger.info("Product scraper main worker finished.")

    except Exception as e:
        logger.error(f"An error occurred during the scraper run: {e}", exc_info=True)
    finally:
        await asyncio.sleep(1)  # Small delay before final inspection
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

        logger.info("Scraper test run finished.")


if __name__ == "__main__":
    asyncio.run(run_test())
