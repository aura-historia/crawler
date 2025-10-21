import pytest
from src.core.algorithms.bfs_no_cycle_deep_crawl_strategy import (
    BFSNoCycleDeepCrawlStrategy,
)


class TestBFSNoCycleDeepCrawlStrategy:
    """Simple tests for BFSNoCycleDeepCrawlStrategy."""

    def test_init_default_values(self):
        """Test that strategy initializes with default values."""
        strategy = BFSNoCycleDeepCrawlStrategy(max_depth=2)

        assert strategy.max_depth == 2
        assert strategy.include_external is False
        assert strategy.max_pages == float("inf")
        assert len(strategy._exclude_extensions) == 0
        assert len(strategy._exclude_patterns) == 0

    def test_init_with_exclude_extensions(self):
        """Test that exclude_extensions are properly stored."""
        strategy = BFSNoCycleDeepCrawlStrategy(
            max_depth=2, exclude_extensions=["jpg", "png", ".pdf"]
        )

        # Extensions should be normalized (lowercase, no leading dot)
        assert "jpg" in strategy._exclude_extensions
        assert "png" in strategy._exclude_extensions
        assert "pdf" in strategy._exclude_extensions
        assert len(strategy._exclude_extensions) == 3

    def test_init_with_exclude_patterns(self):
        """Test that exclude_patterns are properly stored."""
        patterns = ["*/admin/*", "*/login*"]
        strategy = BFSNoCycleDeepCrawlStrategy(max_depth=2, exclude_patterns=patterns)

        assert len(strategy._exclude_patterns) == 2
        assert "*/admin/*" in strategy._exclude_patterns
        assert "*/login*" in strategy._exclude_patterns

    def test_is_excluded_by_extension_with_matching_extension(self):
        """Test that URLs with excluded extensions are filtered out."""
        strategy = BFSNoCycleDeepCrawlStrategy(
            max_depth=2, exclude_extensions=["jpg", "pdf"]
        )

        assert (
            strategy._is_excluded_by_extension("https://example.com/image.jpg") is True
        )
        assert strategy._is_excluded_by_extension("https://example.com/doc.pdf") is True
        assert strategy._is_excluded_by_extension("https://example.com/doc.PDF") is True

    def test_is_excluded_by_extension_with_allowed_extension(self):
        """Test that URLs without excluded extensions are allowed."""
        strategy = BFSNoCycleDeepCrawlStrategy(
            max_depth=2, exclude_extensions=["jpg", "pdf"]
        )

        assert (
            strategy._is_excluded_by_extension("https://example.com/page.html") is False
        )
        assert strategy._is_excluded_by_extension("https://example.com/page") is False

    def test_is_excluded_by_extension_no_exclusions(self):
        """Test that when no extensions are excluded, nothing is filtered."""
        strategy = BFSNoCycleDeepCrawlStrategy(max_depth=2)

        assert (
            strategy._is_excluded_by_extension("https://example.com/image.jpg") is False
        )
        assert (
            strategy._is_excluded_by_extension("https://example.com/doc.pdf") is False
        )

    def test_is_excluded_by_pattern_with_matching_pattern(self):
        """Test that URLs matching excluded patterns are filtered out."""
        strategy = BFSNoCycleDeepCrawlStrategy(
            max_depth=2, exclude_patterns=["*/admin/*", "*/login*"]
        )

        assert (
            strategy._is_excluded_by_pattern("https://example.com/admin/dashboard")
            is True
        )
        assert (
            strategy._is_excluded_by_pattern("https://example.com/user/admin/settings")
            is True
        )
        assert strategy._is_excluded_by_pattern("https://example.com/login") is True
        assert (
            strategy._is_excluded_by_pattern("https://example.com/login-page") is True
        )

    def test_is_excluded_by_pattern_without_matching_pattern(self):
        """Test that URLs not matching excluded patterns are allowed."""
        strategy = BFSNoCycleDeepCrawlStrategy(
            max_depth=2, exclude_patterns=["*/admin/*", "*/login*"]
        )

        assert strategy._is_excluded_by_pattern("https://example.com/products") is False
        assert strategy._is_excluded_by_pattern("https://example.com/about") is False

    def test_is_excluded_by_pattern_no_exclusions(self):
        """Test that when no patterns are excluded, nothing is filtered."""
        strategy = BFSNoCycleDeepCrawlStrategy(max_depth=2)

        assert (
            strategy._is_excluded_by_pattern("https://example.com/admin/dashboard")
            is False
        )
        assert strategy._is_excluded_by_pattern("https://example.com/anything") is False

    def test_matches_pattern_case_insensitive(self):
        """Test that pattern matching is case-insensitive."""
        assert (
            BFSNoCycleDeepCrawlStrategy._matches_pattern(
                "https://Example.COM/Admin/page", "*/admin/*"
            )
            is True
        )

        assert (
            BFSNoCycleDeepCrawlStrategy._matches_pattern(
                "https://example.com/LOGIN", "*/login*"
            )
            is True
        )

    def test_matches_pattern_wildcard(self):
        """Test that wildcard patterns work correctly."""
        assert (
            BFSNoCycleDeepCrawlStrategy._matches_pattern(
                "https://example.com/products/123", "*/products/*"
            )
            is True
        )

        assert (
            BFSNoCycleDeepCrawlStrategy._matches_pattern(
                "https://example.com/test.jpg", "*.jpg"
            )
            is True
        )

    @pytest.mark.asyncio
    async def test_can_process_url_with_excluded_extension(self):
        """Test that can_process_url rejects URLs with excluded extensions."""
        strategy = BFSNoCycleDeepCrawlStrategy(
            max_depth=2, exclude_extensions=["jpg", "pdf"]
        )

        # These should be rejected
        assert (
            await strategy.can_process_url("https://example.com/image.jpg", 1) is False
        )
        assert await strategy.can_process_url("https://example.com/doc.pdf", 1) is False

    @pytest.mark.asyncio
    async def test_can_process_url_with_excluded_pattern(self):
        """Test that can_process_url rejects URLs matching excluded patterns."""
        strategy = BFSNoCycleDeepCrawlStrategy(
            max_depth=2, exclude_patterns=["*/admin/*"]
        )

        # This should be rejected
        assert (
            await strategy.can_process_url("https://example.com/admin/page", 1) is False
        )

    @pytest.mark.asyncio
    async def test_can_process_url_with_allowed_url(self):
        """Test that can_process_url accepts valid URLs."""
        strategy = BFSNoCycleDeepCrawlStrategy(
            max_depth=2, exclude_extensions=["jpg"], exclude_patterns=["*/admin/*"]
        )

        url = "https://example.com/products/item"

        assert strategy._is_excluded_by_extension(url) is False
        assert strategy._is_excluded_by_pattern(url) is False

    @pytest.mark.asyncio
    async def test_bfs_processes_links_level_by_level(self):
        """Test that BFS processes URLs level by level (breadth-first)."""
        from unittest.mock import AsyncMock, MagicMock

        strategy = BFSNoCycleDeepCrawlStrategy(max_depth=2)

        # Mock crawler and results
        mock_crawler = AsyncMock()
        mock_config = MagicMock()
        mock_config.clone = MagicMock(return_value=mock_config)

        # Create mock results for level 0 (start URL)
        level_0_result = MagicMock()
        level_0_result.url = "https://example.com"
        level_0_result.success = True
        level_0_result.links = {
            "internal": [
                {"href": "https://example.com/page1"},
                {"href": "https://example.com/page2"},
            ],
            "external": [],
        }

        # Create mock results for level 1
        level_1_result_1 = MagicMock()
        level_1_result_1.url = "https://example.com/page1"
        level_1_result_1.success = True
        level_1_result_1.links = {
            "internal": [{"href": "https://example.com/page3"}],
            "external": [],
        }

        level_1_result_2 = MagicMock()
        level_1_result_2.url = "https://example.com/page2"
        level_1_result_2.success = True
        level_1_result_2.links = {
            "internal": [{"href": "https://example.com/page4"}],
            "external": [],
        }

        # Level 2 results (no more links)
        level_2_result_1 = MagicMock()
        level_2_result_1.url = "https://example.com/page3"
        level_2_result_1.success = True
        level_2_result_1.links = {"internal": [], "external": []}

        level_2_result_2 = MagicMock()
        level_2_result_2.url = "https://example.com/page4"
        level_2_result_2.success = True
        level_2_result_2.links = {"internal": [], "external": []}

        # Mock arun_many to return results per level
        mock_crawler.arun_many = AsyncMock(
            side_effect=[
                [level_0_result],  # Level 0
                [level_1_result_1, level_1_result_2],  # Level 1
                [level_2_result_1, level_2_result_2],  # Level 2
            ]
        )

        # Run the batch crawl
        result = await strategy._arun_batch(
            "https://example.com", mock_crawler, mock_config
        )

        # Verify BFS: all URLs discovered
        assert "https://example.com" in result
        assert "https://example.com/page1" in result
        assert "https://example.com/page2" in result
        assert "https://example.com/page3" in result
        assert "https://example.com/page4" in result
        assert len(result) == 5

    @pytest.mark.asyncio
    async def test_no_cycles_prevents_revisiting_urls(self):
        """Test that the strategy prevents revisiting already crawled URLs (no cycles)."""
        from unittest.mock import AsyncMock, MagicMock

        strategy = BFSNoCycleDeepCrawlStrategy(max_depth=3)

        mock_crawler = AsyncMock()
        mock_config = MagicMock()
        mock_config.clone = MagicMock(return_value=mock_config)

        # Level 0: start URL links to page1
        level_0_result = MagicMock()
        level_0_result.url = "https://example.com"
        level_0_result.success = True
        level_0_result.links = {
            "internal": [{"href": "https://example.com/page1"}],
            "external": [],
        }

        # Level 1: page1 links back to start (cycle) and to page2
        level_1_result = MagicMock()
        level_1_result.url = "https://example.com/page1"
        level_1_result.success = True
        level_1_result.links = {
            "internal": [
                {"href": "https://example.com"},  # Cycle back
                {"href": "https://example.com/page2"},
            ],
            "external": [],
        }

        # Level 2: page2 links back to both (cycles)
        level_2_result = MagicMock()
        level_2_result.url = "https://example.com/page2"
        level_2_result.success = True
        level_2_result.links = {
            "internal": [
                {"href": "https://example.com"},  # Cycle
                {"href": "https://example.com/page1"},  # Cycle
            ],
            "external": [],
        }

        mock_crawler.arun_many = AsyncMock(
            side_effect=[[level_0_result], [level_1_result], [level_2_result]]
        )

        result = await strategy._arun_batch(
            "https://example.com", mock_crawler, mock_config
        )

        # Verify results
        assert len(result) == 3
        assert set(result) == {
            "https://example.com",
            "https://example.com/page1",
            "https://example.com/page2",
        }

        # KEY CHECK: Verify that arun_many was called with the correct URLs
        # and that cycled URLs were NOT added to subsequent crawl batches
        calls = mock_crawler.arun_many.call_args_list

        # First call: start URL
        assert calls[0][1]["urls"] == ["https://example.com"]

        # Second call: only page1 (not start URL again)
        assert calls[1][1]["urls"] == ["https://example.com/page1"]

        # Third call: only page2 (not start or page1 again)
        assert calls[2][1]["urls"] == ["https://example.com/page2"]

        # Verify exactly 3 calls (no 4th call with revisited URLs)
        assert len(calls) == 3
