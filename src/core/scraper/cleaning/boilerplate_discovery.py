import logging
import difflib
import re
import asyncio
from typing import List, Optional

from src.core.aws.database.operations import DynamoDBOperations
from src.core.aws.s3 import S3Operations
from src.core.scraper.base import get_markdown

logger = logging.getLogger(__name__)


class BoilerplateDiscovery:
    """Discovers boilerplate blocks for a specific shop using pairwise difflib comparison."""

    def __init__(self):
        self.db = DynamoDBOperations()
        self.s3 = S3Operations()
        self.critical_keywords_pattern_1 = re.compile(
            r"(?i)\b(price|inventory|sku|item|lot)\b"
        )
        self.critical_keywords_pattern_2 = re.compile(
            r"(?i)\b(verfügbarkeit|vorrätig|availability|stock|zustand)\b"
        )
        self.currency_pattern = re.compile(r"[\$€£]\s*\d|\d\s*[\$€£]")
        self._locks = {}  # Stores asyncio.Lock() per domain

    async def get_valid_product_markdowns(
        self, domain: str, target_count: int = 3
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

    async def discover_and_save(self, domain: str) -> List[List[str]]:
        """
        Full workflow: Check S3 -> Fetch 3 products -> Discover -> Save to S3.
        Returns the discovered blocks.
        Ensures only one discovery process runs per domain at a time (in-process).
        """
        # 0. Initialize lock for this domain if not present
        if domain not in self._locks:
            self._locks[domain] = asyncio.Lock()

        # 1. Check S3 first (fast path)
        # If data exists, we don't even need to wait for the lock.
        existing_blocks = await self._check_s3_for_blocks(domain)
        if existing_blocks is not None:
            return existing_blocks

        # 2. Acquire Lock
        async with self._locks[domain]:
            # 3. DOUBLE-CHECK: Check S3 again inside the lock
            # Another request might have finished while we were waiting.
            existing_blocks = await self._check_s3_for_blocks(domain)
            if existing_blocks is not None:
                return existing_blocks

            # 4. Fetch valid products
            logger.info(f"Starting boilerplate discovery for {domain}...")
            markdowns = await self.get_valid_product_markdowns(domain, target_count=3)

            if len(markdowns) < 2:
                logger.warning(
                    f"Not enough valid markdowns found for {domain} to discover boilerplate. Saving empty state to prevent re-discovery."
                )
                blocks = []
            else:
                # 5. Discover blocks
                blocks = self.find_common_blocks_detailed(markdowns)

            if blocks:
                logger.info(f"Discovered {len(blocks)} boilerplate blocks for {domain}")
            else:
                logger.info(
                    f"No common boilerplate blocks found (or not enough markdowns) for {domain}. Saving empty state."
                )

            # 6. Save to S3 (even if empty, to mark as "checked")
            await asyncio.to_thread(
                self.s3.upload_json, f"boilerplate/{domain}.json", {"blocks": blocks}
            )

            return blocks

    async def _check_s3_for_blocks(self, domain: str) -> Optional[List[List[str]]]:
        """Helper to check S3 for existing boilerplate blocks."""
        try:
            existing_data = await asyncio.to_thread(
                self.s3.download_json, f"boilerplate/{domain}.json"
            )
            if existing_data and "blocks" in existing_data:
                logger.info(
                    f"Boilerplate blocks already exist for {domain} (S3 found), skipping discovery."
                )
                return existing_data["blocks"]
        except Exception:
            # Ignore S3 errors (doesn't exist etc) and proceed
            pass
        return None

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

    def _is_safe_line(self, line: str) -> bool:
        """Check if a line is safe to include in boilerplate (no images, prices, or headers)."""
        if self.critical_keywords_pattern_1.search(line):
            return False
        if self.critical_keywords_pattern_2.search(line):
            return False
        if self.currency_pattern.search(line):
            return False
        if line.startswith("#"):
            return False
        return True

    def _is_valid_block(self, block: List[str]) -> bool:
        """Check if a block has enough content to be considered valid boilerplate."""
        if not block:
            return False
        total_words = sum(len(line.split()) for line in block)
        return total_words > 3

    def _find_match_blocks(
        self, lines_a: List[str], lines_b: List[str]
    ) -> List[List[str]]:
        """Find matching blocks between two lists of lines."""
        matcher = difflib.SequenceMatcher(None, lines_a, lines_b, autojunk=False)
        match_blocks = []
        seen_blocks = set()

        # get_matching_blocks() finds sequences that are identical in both A and B
        for match in matcher.get_matching_blocks():
            if match.size == 0:
                continue

            block = lines_a[match.a : match.a + match.size]
            safe_block = [line for line in block if self._is_safe_line(line)]

            if not self._is_valid_block(safe_block):
                continue

            block_tuple = tuple(safe_block)
            if block_tuple in seen_blocks:
                continue

            seen_blocks.add(block_tuple)
            match_blocks.append(safe_block)

        return match_blocks
