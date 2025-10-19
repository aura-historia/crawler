from typing import List, Set, Dict, Tuple, Optional
import logging
from crawl4ai import BFSDeepCrawlStrategy, AsyncWebCrawler, CrawlerRunConfig
from crawl4ai.utils import normalize_url_for_deep_crawl


class BFSNoCycleDeepCrawlStrategy(BFSDeepCrawlStrategy):
    """
    BFS-based deep crawl strategy that avoids cycles and returns the full set
    of discovered links for the website.

    Differences from `BFSDeepCrawlStrategy`:
    - Ensures each normalized URL is visited at most once (no cycles).
    - Returns a flat list of discovered URLs when running in batch mode.
    - In streaming mode yields discovered URLs as they are found.
    """

    def __init__(
        self,
        max_depth: int,
        include_external: bool = False,
        max_pages: int = float("inf"),
        logger: Optional[logging.Logger] = None,
    ):
        # Reuse parent init for common fields, provide defaults for filters/scorers
        super().__init__(
            max_depth=max_depth,
            include_external=include_external,
            max_pages=max_pages,
            logger=logger,
        )

    async def _arun_batch(
        self,
        start_url: str,
        crawler: AsyncWebCrawler,
        config: CrawlerRunConfig,
    ) -> List[str]:
        """
        Batch mode: performs a BFS and returns a list of discovered URLs (strings)
        without cycles.
        """
        visited: Set[str] = set()
        discovered: Set[str] = set()

        # Normalize start URL and seed
        start_norm = normalize_url_for_deep_crawl(start_url, start_url)
        current_level: List[Tuple[str, Optional[str]]] = [(start_norm, None)]
        depths: Dict[str, int] = {start_norm: 0}
        visited.add(start_norm)
        discovered.add(start_norm)

        while current_level and not self._cancel_event.is_set():
            # Stop if we've crawled enough pages
            if self._pages_crawled >= self.max_pages:
                break

            next_level: List[Tuple[str, Optional[str]]] = []
            urls = [url for url, _ in current_level]

            # Clone config to avoid recursive deep crawling; use batch mode.
            batch_config = config.clone(deep_crawl_strategy=None, stream=False)
            batch_results = await crawler.arun_many(urls=urls, config=batch_config)

            # Count successful crawls
            successful_results = [r for r in batch_results if r.success]
            self._pages_crawled += len(successful_results)

            for result in batch_results:
                source = result.url
                depth = depths.get(source, 0)

                # Only discover links from successful crawls
                if not result.success:
                    continue

                # Collect internal links and optionally external ones
                links = result.links.get("internal", [])[:]
                if self.include_external:
                    links += result.links.get("external", [])

                next_depth = depth + 1
                if next_depth > self.max_depth:
                    continue

                for link in links:
                    raw = link.get("href")
                    if not raw:
                        continue
                    normalized = normalize_url_for_deep_crawl(raw, source)

                    # Skip already visited to avoid cycles
                    if normalized in visited:
                        continue

                    # Validate URL using parent method (which checks scheme/domain/etc.)
                    if not await self.can_process_url(normalized, next_depth):
                        self.stats.urls_skipped += 1
                        continue

                    visited.add(normalized)
                    discovered.add(normalized)
                    depths[normalized] = next_depth
                    next_level.append((normalized, source))

            current_level = next_level

        # Return the discovered links as a list
        return list(discovered)

    async def shutdown(self) -> None:
        self._cancel_event.set()
        self.stats.end_time = None
