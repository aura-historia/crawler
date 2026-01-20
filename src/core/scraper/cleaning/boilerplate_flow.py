import asyncio
import logging
import os
from typing import List
from dotenv import load_dotenv
from src.core.scraper.qwen import extract
from src.core.aws.s3 import S3Operations
from src.core.aws.database.models import URLEntry
from src.core.aws.database.operations import DynamoDBOperations
from src.core.scraper.cleaning.boilerplate_discovery import BoilerplateDiscovery

# Set environment variables for local testing before imports
os.environ["DYNAMODB_TABLE_NAME"] = "aura-historia-data"
os.environ["S3_ENDPOINT_URL"] = "http://localhost:4566"
os.environ["DYNAMODB_ENDPOINT_URL"] = "http://localhost:8000"

load_dotenv()

logging.basicConfig(
    level=logging.INFO, format="%(asctime)s - %(levelname)s - %(message)s"
)
logger = logging.getLogger(__name__)


async def setup_mock_data(domain: str, urls: List[str]):
    """Populate local DynamoDB with some product URLs for testing."""
    db = DynamoDBOperations()
    entries = [URLEntry(domain=domain, url=url, type="product") for url in urls]
    logger.info(f"Populating DynamoDB with {len(entries)} URLs for {domain}")
    # Using thread for sync db operation
    await asyncio.to_thread(db.batch_write_url_entries, entries)


async def run_test():
    domain = "liveauctioneers.com"
    # Example product URLs from the same shop
    sample_urls = [
        "https://www.liveauctioneers.com/item/224036629_alfred-christensen-for-slagelse-m-185-loveseat-norwalk-ct",
        "https://www.liveauctioneers.com/item/224067926_mixed-lot-rock-stone-lapidary-specimen-thayer-mo",
        "https://www.liveauctioneers.com/item/223737749_1965-peru-gold-commemorative-lima-mint-towson-md",
        "https://www.liveauctioneers.com/item/224009795_catherine-abel-australian-b-1966-san-rafael-ca",
        "https://www.liveauctioneers.com/item/223771797_audemars-piguet-royal-oak-dania-beach-fl",
    ]

    # 1. Ensure S3 bucket exists
    s3 = S3Operations()
    try:
        await asyncio.to_thread(s3.ensure_bucket_exists)
    except Exception as e:
        logger.error(
            f"Failed to ensure S3 bucket exists. Make sure LocalStack is running: {e}"
        )
        return

    # 2. Setup mock data in DynamoDB
    await setup_mock_data(domain, sample_urls)

    # 3. Trigger Discovery
    discovery = BoilerplateDiscovery()
    logger.info(f"--- Starting Discovery for {domain} ---")
    blocks = await discovery.discover_and_save(domain)

    if not blocks:
        logger.error(
            "Discovery failed to find any blocks. Check logs for LLM validation issues."
        )
        return

    logger.info(f"Successfully discovered {len(blocks)} blocks!")

    # 4. Test Extraction with Cleaning
    test_url = sample_urls[0]

    from src.core.scraper.base import get_markdown

    logger.info(f"--- Fetching and Cleaning Markdown for {test_url} ---")
    raw_markdown = await get_markdown(test_url)

    # We can call extract directly which now includes the removal logic
    logger.info("Calling extract() with domain to trigger cleaning...")
    # Passing domain triggers the _apply_boilerplate_removal
    result = await extract(markdown=raw_markdown, domain=domain)

    if result:
        logger.info("Extraction successful!")
        # Result is ExtractedProduct object
        print("\nExtracted Data Sample:")
        import json

        print(json.dumps(result.model_dump(), indent=2, ensure_ascii=False))
    else:
        logger.error("Extraction failed or page was determined NOT_A_PRODUCT.")


if __name__ == "__main__":
    asyncio.run(run_test())
