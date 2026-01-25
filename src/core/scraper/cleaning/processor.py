import logging
import os
import re
import time
import asyncio
from typing import List, Optional, Dict
from datetime import datetime, timezone


from src.core.aws.s3 import S3Operations
from src.core.scraper.cleaning.boilerplate_discovery import BoilerplateDiscovery

logger = logging.getLogger(__name__)


class BoilerplateRemover:
    """Removes boilerplate blocks from markdown and detects structural changes."""

    def __init__(self, cache_ttl: int = 3600):
        self.s3 = S3Operations()
        self.cache: Dict[str, dict] = {}
        self.cache_ttl = cache_ttl

    def _get_from_cache(self, domain: str) -> Optional[List[List[str]]]:
        """Get blocks from cache if not expired."""
        if domain in self.cache:
            entry = self.cache[domain]
            if time.time() - entry["timestamp"] < self.cache_ttl:
                return entry["blocks"]
        return None

    def _save_to_cache(self, domain: str, blocks: List[List[str]]):
        """Save blocks to cache."""
        self.cache[domain] = {"blocks": blocks, "timestamp": time.time()}

    async def load_for_shop(
        self, domain: str, force_refresh: bool = False
    ) -> List[List[str]]:
        """Load boilerplate blocks from S3 with local caching."""
        if not force_refresh:
            cached = self._get_from_cache(domain)
            if cached is not None:
                return cached

        data = await asyncio.to_thread(
            self.s3.download_json, f"boilerplate/{domain}.json"
        )
        if data and "blocks" in data:
            self._save_to_cache(domain, data["blocks"])
            return data["blocks"]

        return []

    def clean(
        self, markdown: str, blocks: List[List[str]], remove_related: bool = True
    ) -> str:
        """
        Remove boilerplate blocks from markdown using block-based matching.
        Optionally remove related product sections first.
        Returns cleaned markdown.
        """
        cleaned_markdown = markdown

        if remove_related:
            cleaned_markdown = self.remove_related_sections_strict(
                markdown_text=markdown
            )

        if not blocks or not cleaned_markdown:
            return cleaned_markdown

        # Split markdown into lines
        md_lines = cleaned_markdown.splitlines()
        stripped_lines = [line.strip() for line in md_lines]

        # Collect positions to remove, in reverse order
        positions = []
        for block in blocks:
            if not block:
                continue
            start = 0
            while True:
                idx = self.find_subsequence(stripped_lines[start:], block)
                if idx == -1:
                    break
                actual_start = start + idx
                positions.append((actual_start, len(block)))
                start = actual_start + 1  # Move past this occurrence

        # Sort positions by start index descending
        positions.sort(reverse=True)

        # Remove the blocks
        for start, length in positions:
            del md_lines[start : start + length]

        # Rejoin cleaned lines
        cleaned_markdown = "\n".join(md_lines)

        return cleaned_markdown

    def find_subsequence(self, lines: List[str], sub: List[str]) -> int:
        """Find the start index of sub in lines, or -1 if not found."""
        if len(sub) == 0:
            return -1
        for i in range(len(lines) - len(sub) + 1):
            if lines[i : i + len(sub)] == sub:
                return i
        return -1

    def remove_related_sections_strict(self, markdown_text: str) -> str:
        lines = markdown_text.splitlines()
        clean_lines = []
        skip = False
        current_skip_level = 0
        trigger_keywords = [
            "related products",
            "ähnliche produkte",
            "lieferung anderer artikel in unserem shop",
            "unsere bundesweiten lieferkosten",
            "other items",
            "search",
            "similar products",
            "recommended products",
            "empfohlene produkte",
        ]

        for line in lines:
            stripped = line.strip()
            header_match = re.match(r"^(#{1,6})\s*(.*)", stripped)

            if header_match:
                level = len(header_match.group(1))
                header_text = header_match.group(2).lower().strip()

                logger.info(header_match)

                # If this is a trigger header, start skipping
                if any(t in header_text for t in trigger_keywords):
                    skip = True
                    current_skip_level = level
                    continue

                # Stop skipping when a new header of same or higher level appears
                if skip and level <= current_skip_level:
                    skip = False

            if not skip:
                clean_lines.append(line)

        return "\n".join(clean_lines)

    async def should_rediscover(self, domain: str, hit_rate: float) -> bool:
        """Decide if we should re-trigger discovery based on hit rate or staleness."""
        data = await asyncio.to_thread(
            self.s3.download_json, f"boilerplate/{domain}.json"
        )
        if not data:
            return True

        # Check staleness (30 days)
        # Handle both old structure (top-level updated_at) and new structure (metadata/updated_at)
        updated_at_str = data.get("metadata", {}).get("updated_at") or data.get(
            "updated_at"
        )

        if updated_at_str:
            updated_at = datetime.fromisoformat(updated_at_str)
            if (datetime.now(timezone.utc) - updated_at).days > 30:
                logger.info(f"Boilerplate for {domain} is stale (> 30 days).")
                return True
        else:
            logger.warning(
                f"No timestamp found for {domain} boilerplate, triggering rediscovery."
            )
            return True

        # Check structural shift
        if hit_rate < 0.5 and len(data.get("blocks", [])) > 5:
            logger.warning(
                f"Low hit rate ({hit_rate:.2f}) for {domain}. Site redesign likely."
            )
            return True

        return False


if __name__ == "__main__":
    with open(
        os.path.join(os.path.dirname(__file__), "../../../../data/out4.md"),
        "r",
        encoding="utf-8",
    ) as f:
        markdown1 = f.read()
    with open(
        os.path.join(os.path.dirname(__file__), "../../../../data/out3.md"),
        "r",
        encoding="utf-8",
    ) as f:
        markdown2 = f.read()
    with open(
        os.path.join(os.path.dirname(__file__), "../../../../data/out2.md"),
        "r",
        encoding="utf-8",
    ) as f:
        markdown3 = f.read()
    dis = BoilerplateDiscovery()
    blocks = dis.find_common_blocks_detailed(markdowns=[markdown1, markdown2])

    print(len(blocks))

    remover = BoilerplateRemover()

    cleaned = remover.clean(markdown3, blocks)
    print("Original Markdown length:", len(markdown3))
    print("Cleaned Markdown length:", len(cleaned))
    print("\nCleaned ends with:")
    print(repr(cleaned))  # Last 500 chars

    # Write cleaned markdown to out1.md
    with open(
        os.path.join(os.path.dirname(__file__), "../../../../data/out1.md"),
        "w",
        encoding="utf-8",
    ) as f:
        f.write(cleaned)
    print("Cleaned markdown written to data/out1.md")
