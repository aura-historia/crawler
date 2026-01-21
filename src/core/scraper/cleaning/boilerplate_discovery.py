import logging
import re
from typing import List
from datetime import datetime, timezone

from src.core.aws.database.operations import DynamoDBOperations
from src.core.aws.s3 import S3Operations
from src.core.scraper.base import get_markdown

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
        for url in urls:
            if len(valid_markdowns) >= target_count:
                break

            try:
                logger.info(f"Fetching markdown for validation: {url}")
                markdown = await get_markdown(url)

                # Basic sanity check: skip very short markdowns (likely error pages)
                if len(markdown) < 500:
                    logger.warning(
                        f"Markdown too short ({len(markdown)} chars) for {url}, skipping."
                    )
                    continue

                # Use Qwen to validate if it's a single product page
                # We slice to 40k for verification efficiency, but keep full markdown for discovery
                from src.core.scraper.qwen import extract

                verification_markdown = markdown[:40000]
                product = await extract(markdown=verification_markdown)

                if product and getattr(product, "is_product", False):
                    logger.info(f"Valid product page found: {url}")
                    valid_markdowns.append(markdown)
                elif product:
                    logger.warning(f"URL validated but is NOT a product: {url}")
                else:
                    logger.warning(f"LLM failed to analyze markdown for: {url}")

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
        from difflib import SequenceMatcher

        if not markdowns:
            return []

        # We'll use the first markdown as a reference and find matching blocks in all others
        reference = markdowns[0]
        potential_blocks = []

        # For each subsequent markdown, find matching blocks with the reference
        for other in markdowns[1:]:
            matcher = SequenceMatcher(None, reference, other, autojunk=False)
            for match in matcher.get_matching_blocks():
                # A block must have a minimum length to be considered boilerplate
                if match.size < 200:  # Structural sections only
                    continue

                block_text = reference[match.a : match.a + match.size].strip()
                if len(block_text.split()) >= min_words:
                    potential_blocks.append(block_text)

        # Count occurrences of detected blocks
        block_counts = {}
        for block in potential_blocks:
            # Normalize to avoid duplicates with minor whitespace diffs
            norm_block = self.normalize_text(block)
            block_counts[norm_block] = block_counts.get(norm_block, 0) + 1

        # Return blocks that appear with high frequency
        common_blocks = []
        # Since we compare reference (1) against others (N-1),
        # a block in all N markdowns will have count N-1.
        required_matches = min_frequency - 1

        for block, count in block_counts.items():
            if count >= required_matches:
                common_blocks.append(block)

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
