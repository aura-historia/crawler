from typing import Set, Iterable
from urllib.parse import urlparse
from src.core.algorithms.bfs_no_cycle_deep_crawl_strategy import (
    BFSNoCycleDeepCrawlStrategy,
)


class ExtensionExcludeBFSStrategy(BFSNoCycleDeepCrawlStrategy):
    """
    Deep crawl strategy that excludes URLs with specific file extensions.

    Example:
        strategy = ExtensionExcludeBFSStrategy(
            exclude_extensions=['jpg', 'png', 'gif', 'pdf'],
            max_depth=5,
            include_external=False
        )
    """

    def __init__(self, exclude_extensions: Iterable[str] = None, **kwargs):
        """
        Args:
            exclude_extensions: List of file extensions to exclude (e.g. ['jpg', 'png', 'pdf'])
            **kwargs: Additional parameters for BFSNoCycleDeepCrawlStrategy
        """
        super().__init__(**kwargs)
        if exclude_extensions is None:
            exclude_extensions = []
        # Normalize extensions: lowercase and remove leading dots
        self._exclude_extensions: Set[str] = {
            ext.lower().lstrip(".") for ext in exclude_extensions
        }

    async def can_process_url(self, url: str, depth: int) -> bool:
        """
        Checks if a URL should be processed.
        URLs with excluded file extensions will be skipped.
        """
        # First perform the base check
        if not await super().can_process_url(url, depth):
            return False

        # Then check extension
        if self._exclude_extensions:
            try:
                path = urlparse(url).path or ""
                if "." in path:
                    # Extract the file extension
                    ext = path.rsplit(".", 1)[-1].lower()
                    if ext in self._exclude_extensions:
                        return False
            except Exception:
                # On parse errors: don't exclude the URL
                pass

        return True


class MultipleExtensionExcludeBFSStrategy(BFSNoCycleDeepCrawlStrategy):
    """
    Advanced strategy with additional filtering options.

    Example:
        strategy = MultipleExtensionExcludeBFSStrategy(
            exclude_extensions=['jpg', 'png', 'gif', 'pdf', 'zip'],
            exclude_patterns=['*/download/*', '*/media/*'],
            max_depth=5,
            include_external=False
        )
    """

    def __init__(
        self,
        exclude_extensions: Iterable[str] = None,
        exclude_patterns: Iterable[str] = None,
        **kwargs,
    ):
        """
        Args:
            exclude_extensions: List of file extensions to exclude
            exclude_patterns: List of URL patterns to exclude (wildcards supported)
            **kwargs: Additional parameters for BFSNoCycleDeepCrawlStrategy
        """
        super().__init__(**kwargs)

        if exclude_extensions is None:
            exclude_extensions = []
        self._exclude_extensions: Set[str] = {
            ext.lower().lstrip(".") for ext in exclude_extensions
        }

        self._exclude_patterns = list(exclude_patterns) if exclude_patterns else []

    async def can_process_url(self, url: str, depth: int) -> bool:
        """Checks URL against all configured filters."""
        if not await super().can_process_url(url, depth):
            return False

        # Extension filter
        if self._exclude_extensions:
            try:
                path = urlparse(url).path or ""
                if "." in path:
                    ext = path.rsplit(".", 1)[-1].lower()
                    if ext in self._exclude_extensions:
                        return False
            except Exception:
                pass

        # Pattern filter (simple wildcard support)
        if self._exclude_patterns:
            for pattern in self._exclude_patterns:
                if self._matches_pattern(url, pattern):
                    return False

        return True

    @staticmethod
    def _matches_pattern(url: str, pattern: str) -> bool:
        """Simple wildcard matcher for URL patterns."""
        import fnmatch

        return fnmatch.fnmatch(url.lower(), pattern.lower())
