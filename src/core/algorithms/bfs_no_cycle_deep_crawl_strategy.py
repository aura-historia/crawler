from typing import List, Set, Dict, Tuple, Optional, Iterable, AsyncGenerator
import logging
from urllib.parse import urlparse
import fnmatch
from crawl4ai import BFSDeepCrawlStrategy, AsyncWebCrawler, CrawlerRunConfig
from crawl4ai.types import CrawlResult
from crawl4ai.utils import normalize_url_for_deep_crawl


class BFSNoCycleDeepCrawlStrategy(BFSDeepCrawlStrategy):
    """
    BFS-based deep crawl strategy that ensures no cycles occur.

    Differences from `BFSDeepCrawlStrategy`:
    - Ensures each normalized URL is visited at most once (no cycles).
    - Uses a visited set that is checked before enqueueing any URL.

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

    async def link_discovery(
        self,
        result: CrawlResult,
        source_url: str,
        current_depth: int,
        visited: Set[str],
        next_level: List[Tuple[str, Optional[str]]],
        depths: Dict[str, int],
    ) -> None:
        """
        Extracts links from the crawl result, validates and filters them, and
        prepares the next level of URLs. Ensures no cycles by checking visited set.
        Each valid URL is appended to next_level as a tuple (url, parent_url)
        and its depth is tracked.
        """
        next_depth = current_depth + 1
        if next_depth > self.max_depth:
            return

        # Check if we've reached the max pages limit
        remaining_capacity = self.max_pages - self._pages_crawled
        if remaining_capacity <= 0:
            self.logger.info(
                f"Max pages limit ({self.max_pages}) reached, stopping link discovery"
            )
            return

        # Get internal links and, if enabled, external links
        links = result.links.get("internal", [])
        if self.include_external:
            links += result.links.get("external", [])

        for link in links:
            raw = link.get("href")
            if not raw:
                continue

            # Quick filter: skip non-HTTP(S) schemes early (tel:, mailto:, sms:, etc.)
            try:
                parsed = urlparse(raw)
                if parsed.scheme and parsed.scheme not in ("http", "https", ""):
                    continue
            except Exception:
                continue

            normalized = normalize_url_for_deep_crawl(raw, source_url)

            # Skip already visited to avoid cycles
            if normalized in visited:
                continue

            # Validate URL using can_process_url (includes extension/pattern filters)
            if not await self.can_process_url(normalized, next_depth):
                self.stats.urls_skipped += 1
                continue

            # Mark as visited immediately to prevent cycles
            visited.add(normalized)
            depths[normalized] = next_depth
            next_level.append((normalized, source_url))

    async def _arun_batch(
        self,
        start_url: str,
        crawler: AsyncWebCrawler,
        config: CrawlerRunConfig,
    ) -> List[CrawlResult]:
        """
        Batch (non-streaming) mode:
        Processes one BFS level at a time, then returns all the results.
        Ensures no cycles by maintaining a visited set.
        """
        visited: Set[str] = set()
        # current_level holds tuples: (url, parent_url)
        current_level: List[Tuple[str, Optional[str]]] = [(start_url, None)]
        depths: Dict[str, int] = {start_url: 0}

        results: List[CrawlResult] = []

        while current_level and not self._cancel_event.is_set():
            # Check if we've already reached max_pages before starting a new level
            if self._pages_crawled >= self.max_pages:
                self.logger.info(
                    f"Max pages limit ({self.max_pages}) reached, stopping crawl"
                )
                break

            next_level: List[Tuple[str, Optional[str]]] = []
            urls = [url for url, _ in current_level]

            # Clone the config to disable deep crawling recursion and enforce batch mode
            batch_config = config.clone(deep_crawl_strategy=None, stream=False)
            batch_results = await crawler.arun_many(urls=urls, config=batch_config)

            # Update pages crawled counter - count only successful crawls
            successful_results = [r for r in batch_results if r.success]
            self._pages_crawled += len(successful_results)

            for result in batch_results:
                url = result.url
                depth = depths.get(url, 0)
                result.metadata = result.metadata or {}
                result.metadata["depth"] = depth
                parent_url = next(
                    (parent for (u, parent) in current_level if u == url), None
                )
                result.metadata["parent_url"] = parent_url
                results.append(result)

                # Only discover links from successful crawls
                if result.success:
                    # Link discovery will handle the max pages limit internally
                    await self.link_discovery(
                        result, url, depth, visited, next_level, depths
                    )

            current_level = next_level

        return results

    async def _arun_stream(
        self,
        start_url: str,
        crawler: AsyncWebCrawler,
        config: CrawlerRunConfig,
    ) -> AsyncGenerator[CrawlResult, None]:
        """
        Streaming mode:
        Processes one BFS level at a time and yields results immediately as they arrive.
        Ensures no cycles by maintaining a visited set.
        """
        visited: Set[str] = set()
        current_level: List[Tuple[str, Optional[str]]] = [(start_url, None)]
        depths: Dict[str, int] = {start_url: 0}

        while current_level and not self._cancel_event.is_set():
            next_level: List[Tuple[str, Optional[str]]] = []
            urls = [url for url, _ in current_level]
            visited.update(urls)

            stream_config = config.clone(deep_crawl_strategy=None, stream=True)
            stream_gen = await crawler.arun_many(urls=urls, config=stream_config)

            # Keep track of processed results for this batch
            results_count = 0
            async for result in stream_gen:
                url = result.url
                depth = depths.get(url, 0)
                result.metadata = result.metadata or {}
                result.metadata["depth"] = depth
                parent_url = next(
                    (parent for (u, parent) in current_level if u == url), None
                )
                result.metadata["parent_url"] = parent_url

                # Count only successful crawls
                if result.success:
                    self._pages_crawled += 1
                    # Check if we've reached the limit during batch processing
                    if self._pages_crawled >= self.max_pages:
                        self.logger.info(
                            f"Max pages limit ({self.max_pages}) reached during batch, stopping crawl"
                        )
                        yield result
                        return  # Exit the generator

                results_count += 1
                yield result

                # Only discover links from successful crawls
                if result.success:
                    # Link discovery will handle the max pages limit internally
                    await self.link_discovery(
                        result, url, depth, visited, next_level, depths
                    )

            # If we didn't get results back (e.g. due to errors), avoid getting stuck in an infinite loop
            # by considering these URLs as visited but not counting them toward the max_pages limit
            if results_count == 0 and urls:
                self.logger.warning(
                    f"No results returned for {len(urls)} URLs, marking as visited"
                )

            current_level = next_level

    async def shutdown(self) -> None:
        """
        Clean up resources and signal cancellation of the crawl.
        """
        self._cancel_event.set()
        if self.stats:
            from datetime import datetime

            self.stats.end_time = datetime.now()
