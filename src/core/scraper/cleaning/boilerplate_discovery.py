import logging
import re
from typing import List
from datetime import datetime, timezone

from src.core.aws.database.operations import DynamoDBOperations
from src.core.aws.s3 import S3Operations
from src.core.scraper.base import chat_completion, get_markdown
from src.core.scraper.prompts.cleaner import CLEANER_PROMPT_TEMPLATE

logger = logging.getLogger(__name__)


class BoilerplateDiscovery:
    """Discovers boilerplate blocks for a specific shop by comparing multiple product pages."""

    def __init__(self):
        self.db = DynamoDBOperations()
        self.s3 = S3Operations()

    async def get_valid_product_markdowns(
        self, domain: str, target_count: int = 5
    ) -> List[str]:
        """Fetch and validate product markdowns until target_count is reached."""
        urls, _ = self.db.get_product_urls_by_domain(
            domain, max_urls=20
        )  # Get a few to try

        valid_markdowns = []
        current_time_iso = (
            datetime.now(timezone.utc)
            .replace(microsecond=0)
            .isoformat()
            .replace("+00:00", "Z")
        )

        for url in urls:
            if len(valid_markdowns) >= target_count:
                break

            try:
                logger.info(f"Fetching markdown for validation: {url}")
                markdown = await get_markdown(url)

                # Use Qwen to validate if it's a single product page
                prompt = CLEANER_PROMPT_TEMPLATE.format(
                    current_time=current_time_iso,
                    markdown=markdown[
                        :40000
                    ],  # Use first 10k for validation to save tokens
                )

                response = await chat_completion(prompt)

                if "NOT_A_PRODUCT" not in response:
                    logger.info(f"Valid product page found: {url}")
                    valid_markdowns.append(markdown)
                else:
                    logger.warning(
                        f"URL is not a single product page according to LLM: {url}"
                    )

            except Exception as e:
                logger.error(f"Error processing {url}: {e}")
                continue

        return valid_markdowns

    def normalize_text(self, text: str) -> str:
        """Normalize text by collapsing whitespace."""
        return re.sub(r"\s+", " ", text).strip()

    def find_common_blocks(
        self, markdowns: List[str], min_words: int = 5, min_frequency: int = 4
    ) -> List[str]:
        """Find common blocks across markdowns using SequenceMatcher."""
        # Split into lines for comparison
        lines_list = [m.splitlines() for m in markdowns]

        # We'll count occurrence of each line across all samples
        line_counts = {}
        for lines in lines_list:
            unique_lines = {self.normalize_text(line) for line in lines if line.strip()}
            for line in unique_lines:
                line_counts[line] = line_counts.get(line, 0) + 1

        # Filter blocks that appear in at least min_frequency samples and have enough words
        common_blocks = []
        for line, count in line_counts.items():
            if count >= min_frequency:
                # Check word count
                if len(line.split()) >= min_words:
                    common_blocks.append(line)

        # Sort by length descending to help with clean removal later
        common_blocks.sort(key=len, reverse=True)
        return common_blocks

    async def discover_and_save(self, domain: str) -> List[str]:
        """Orchestrate discovery and save to S3."""
        logger.info(f"Starting boilerplate discovery for {domain}")
        markdowns = await self.get_valid_product_markdowns(domain)

        if len(markdowns) < 4:
            logger.warning(
                f"Not enough valid product pages found for {domain} (found {len(markdowns)})"
            )
            # We still try if we have at least 2, but maybe it won't be good
            if len(markdowns) < 2:
                return []

        common_blocks = self.find_common_blocks(markdowns)

        if common_blocks:
            logger.info(f"Found {len(common_blocks)} common blocks for {domain}")
            self.s3.upload_json(
                f"boilerplate/{domain}.json",
                {
                    "domain": domain,
                    "blocks": common_blocks,
                    "updated_at": datetime.now(timezone.utc).isoformat(),
                },
            )
        else:
            logger.warning(f"No common blocks found for {domain}")

        return common_blocks
