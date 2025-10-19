from typing import List, Set, Dict, Tuple, Optional, Iterable
import logging
from urllib.parse import urlparse
import fnmatch
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

    Added features:
    - exclude_extensions: iterable of file extensions (e.g. ['jpg','pdf']) to skip
    - exclude_patterns: iterable of wildcard URL patterns to skip (fnmatch-style)
    """

    def __init__(
        self,
        max_depth: int,
        include_external: bool = False,
        max_pages: int = float("inf"),
        logger: Optional[logging.Logger] = None,
        exclude_extensions: Optional[Iterable[str]] = None,
        exclude_patterns: Optional[Iterable[str]] = None,
    ):
        # Reuse parent init for common fields, provide defaults for filters/scorers
        super().__init__(
            max_depth=max_depth,
            include_external=include_external,
            max_pages=max_pages,
            logger=logger,
        )

        if exclude_extensions is None:
            exclude_extensions = []
        self._exclude_extensions: Set[str] = {
            ext.lower().lstrip(".") for ext in exclude_extensions
        }

        self._exclude_patterns = list(exclude_patterns) if exclude_patterns else []

    async def can_process_url(self, url: str, depth: int) -> bool:
        """Checks URL against parent filters and the additional extension/pattern filters."""
        # Delegate to parent for base checks
        if not await super().can_process_url(url, depth):
            return False

        # Extension and pattern checks delegated to helpers to reduce complexity
        if self._is_excluded_by_extension(url):
            return False

        if self._is_excluded_by_pattern(url):
            return False

        return True

    def _is_excluded_by_extension(self, url: str) -> bool:
        """Return True if url should be excluded due to its file extension.

        Non-async helper to keep `can_process_url` simple for the linter.
        """
        if not self._exclude_extensions:
            return False

        try:
            path = urlparse(url).path or ""
            if "." in path:
                ext = path.rsplit(".", 1)[-1].lower()
                return ext in self._exclude_extensions
        except Exception:
            # On parse error, don't exclude; parent checks handle validity
            return False

        return False

    def _is_excluded_by_pattern(self, url: str) -> bool:
        """Return True if url matches any of the exclusion patterns."""
        if not self._exclude_patterns:
            return False

        for pattern in self._exclude_patterns:
            if self._matches_pattern(url, pattern):
                return True
        return False

    @staticmethod
    def _matches_pattern(url: str, pattern: str) -> bool:
        """Simple wildcard matcher for URL patterns (case-insensitive)."""
        return fnmatch.fnmatch(url.lower(), pattern.lower())

    async def _process_result_links(
        self,
        result,
        depths: Dict[str, int],
        visited: Set[str],
        discovered: Set[str],
        next_level: List[Tuple[str, Optional[str]]],
    ) -> None:
        """Process links from a single crawl result and append valid ones to next_level.

        This helper extracts links, normalizes them, checks duplication and filters,
        and updates visited/discovered/depths accordingly.
        """
        source = result.url
        depth = depths.get(source, 0)

        # Only process successful results
        if not result.success:
            return

        links = result.links.get("internal", [])[:]
        if self.include_external:
            links += result.links.get("external", [])

        next_depth = depth + 1
        if next_depth > self.max_depth:
            return

        for link in links:
            raw = link.get("href")
            if not raw:
                continue
            normalized = normalize_url_for_deep_crawl(raw, source)

            # Skip already visited to avoid cycles
            if normalized in visited:
                continue

            # Validate URL using parent method and additional filters
            if not await self.can_process_url(normalized, next_depth):
                self.stats.urls_skipped += 1
                continue

            visited.add(normalized)
            discovered.add(normalized)
            depths[normalized] = next_depth
            next_level.append((normalized, source))

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

            # Process each result via helper to keep complexity low
            for result in batch_results:
                await self._process_result_links(result, depths, visited, discovered, next_level)

            current_level = next_level

        # Return the discovered links as a list
        return list(discovered)

    async def shutdown(self) -> None:
        self._cancel_event.set()
        self.stats.end_time = None
