import logging
import time
import asyncio
from typing import List, Optional, Dict
from datetime import datetime, timezone

from src.core.aws.s3 import S3Operations

logger = logging.getLogger(__name__)


class BoilerplateRemover:
    """Removes boilerplate blocks from markdown and detects structural changes."""

    def __init__(self, cache_ttl: int = 3600):
        self.s3 = S3Operations()
        self.cache: Dict[str, dict] = {}
        self.cache_ttl = cache_ttl

    def _get_from_cache(self, domain: str) -> Optional[List[str]]:
        """Get blocks from cache if not expired."""
        if domain in self.cache:
            entry = self.cache[domain]
            if time.time() - entry["timestamp"] < self.cache_ttl:
                return entry["blocks"]
        return None

    def _save_to_cache(self, domain: str, blocks: List[str]):
        """Save blocks to cache."""
        self.cache[domain] = {"blocks": blocks, "timestamp": time.time()}

    async def load_for_shop(
        self, domain: str, force_refresh: bool = False
    ) -> List[str]:
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

    def clean(self, markdown: str, blocks: List[str]) -> tuple[str, float]:
        """
        Remove boilerplate lines from markdown using line-based matching.
        Returns cleaned markdown and the 'hit rate' (percentage of boilerplate lines found).
        """
        if not blocks or not markdown:
            return markdown, 1.0

        # Split markdown into lines
        md_lines = markdown.splitlines()
        cleaned_lines = []
        matches = 0

        # Strip boilerplate blocks for comparison
        stripped_boilerplate = {line.strip() for line in blocks if line.strip()}

        for line in md_lines:
            stripped_line = line.strip()
            if stripped_line and stripped_line in stripped_boilerplate:
                matches += 1
            else:
                cleaned_lines.append(line)

        # Rejoin cleaned lines
        cleaned_markdown = "\n".join(cleaned_lines)

        hit_rate = matches / len(blocks) if blocks else 1.0
        return cleaned_markdown, hit_rate

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
