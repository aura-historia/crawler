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
        Remove blocks from markdown.
        Returns cleaned markdown and the 'hit rate' (percentage of blocks found).
        """
        if not blocks or not markdown:
            return markdown, 1.0

        matches = 0
        cleaned_markdown = markdown

        for block in blocks:
            # We use a simple but somewhat robust replacement
            # Normalizing both slightly might help
            if block in cleaned_markdown:
                cleaned_markdown = cleaned_markdown.replace(block, "")
                matches += 1
            else:
                # Try a slightly more relaxed match (ignoring extra whitespace)
                # This could be expensive, so we only do it if direct match fails
                pass

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
        updated_at = datetime.fromisoformat(data["updated_at"])
        if (datetime.now(timezone.utc) - updated_at).days > 30:
            logger.info(f"Boilerplate for {domain} is stale (> 30 days).")
            return True

        # Check structural shift
        if hit_rate < 0.5 and len(data.get("blocks", [])) > 5:
            logger.warning(
                f"Low hit rate ({hit_rate:.2f}) for {domain}. Site redesign likely."
            )
            return True

        return False
