import logging
import difflib
import re
from typing import List

from src.core.aws.database.operations import DynamoDBOperations
from src.core.aws.s3 import S3Operations
from src.core.scraper.base import get_markdown

logger = logging.getLogger(__name__)


class BoilerplateDiscovery:
    """Discovers boilerplate blocks for a specific shop using pairwise difflib comparison."""

    def __init__(self):
        self.db = DynamoDBOperations()
        self.s3 = S3Operations()
        # 1. Images: Identify Markdown images.
        #    We want to keep images in the final content, even if they match (e.g. logos might match,
        #    but it's safer to let the extractor handle them than to delete them here).
        self.img_pattern = re.compile(r"!\[.*\]\([^)]*\)")
        # 2. Critical Data Risk:
        #    If "Price: $175" appears in both docs (coincidence), we do NOT want to remove it.
        #    Regex detects: "Price/Inventory" keywords + numbers, OR currency symbols + numbers.
        self.critical_data_pattern = re.compile(
            r"(?i)(\b(price|inventory|sku|item|lot)\b.*\d|[\$€£]\s*\d|\d\s*[\$€£])"
        )

    async def get_valid_product_markdowns(
        self, domain: str, target_count: int = 5
    ) -> List[str]:
        """Fetch and validate product markdowns until target_count is reached."""
        urls, _ = self.db.get_product_urls_by_domain(domain, max_urls=15)

        valid_markdowns = []
        seen_content_hashes = set()

        for url in urls:
            if len(valid_markdowns) >= target_count:
                break

            try:
                markdown = await get_markdown(url)
                if not markdown or len(markdown) < 500:
                    continue

                content_hash = hash(markdown[:5000])
                if content_hash in seen_content_hashes:
                    continue
                seen_content_hashes.add(content_hash)

                from src.core.scraper.qwen import extract

                verification_markdown = markdown[:10000]
                product = await extract(markdown=verification_markdown)

                if product and getattr(product, "is_product", False):
                    valid_markdowns.append(markdown)
                    logger.info(f"Valid product found: {url}")
                else:
                    logger.warning(f"URL is not a product: {url}")

            except Exception as e:
                logger.error(f"Error processing {url}: {e}")
                continue

        return valid_markdowns

    def find_common_blocks_detailed(self, markdowns: List[str]) -> List[List[str]]:
        """
        Identifies common text blocks (boilerplate) between documents.
        Logic: If a block appears in both documents, it is considered boilerplate,
        UNLESS it contains critical data (Prices, Images) or headers.
        """
        if len(markdowns) < 2:
            return []

        for i in range(len(markdowns)):
            for j in range(i + 1, len(markdowns)):
                # Strip whitespace to ensure solid matching even with indentation differences
                lines_a = [line.strip() for line in markdowns[i].splitlines()]
                lines_b = [line.strip() for line in markdowns[j].splitlines()]

                match_blocks = self._find_match_blocks(lines_a, lines_b)

                if match_blocks:
                    return match_blocks

        return []

    def _find_match_blocks(
        self, lines_a: List[str], lines_b: List[str]
    ) -> List[List[str]]:
        """Find matching blocks between two lists of lines."""
        matcher = difflib.SequenceMatcher(None, lines_a, lines_b, autojunk=False)
        match_blocks = []

        # get_matching_blocks() finds sequences that are identical in both A and B
        for match in matcher.get_matching_blocks():
            if match.size > 0:
                block = lines_a[match.a : match.a + match.size]

                # Filter out unsafe lines
                safe_block = []
                for line in block:
                    # Skip images, critical data, and headers
                    if (
                        self.img_pattern.search(line)
                        or self.critical_data_pattern.search(line)
                        or line.startswith("#")
                    ):
                        continue
                    safe_block.append(line)

                # Only add if the block has content after filtering and more than 2 words
                if safe_block and sum(len(line.split()) for line in safe_block) > 2:
                    match_blocks.append(safe_block)

        return match_blocks
