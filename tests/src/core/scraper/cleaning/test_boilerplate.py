import pytest
from unittest.mock import patch, MagicMock
from src.core.scraper.cleaning.boilerplate_discovery import BoilerplateDiscovery
from src.core.scraper.cleaning.boilerplate_remover import BoilerplateRemover


@patch("src.core.scraper.cleaning.boilerplate_discovery.DynamoDBOperations")
@patch("src.core.scraper.cleaning.boilerplate_discovery.S3Operations")
class TestBoilerplateDiscovery:
    @pytest.mark.asyncio
    @patch("src.core.scraper.cleaning.boilerplate_discovery.get_markdown")
    @patch("src.core.scraper.qwen.extract")
    async def test_get_valid_product_markdowns_integrated(
        self, mock_extract, mock_markdown, mock_s3, mock_db
    ):
        """Solid test verifying the filtering and validation flow for product markdowns."""
        discovery = BoilerplateDiscovery()
        discovery.db.get_product_urls_by_domain = MagicMock(
            return_value=(["http://a.com", "http://b.com", "http://c.com"], None)
        )

        # 1. Too short, 2. Valid Product, 3. Valid but Not a Product
        mock_markdown.side_effect = [
            "short",
            "md_valid_prod" * 100,
            "md_not_prod" * 100,
        ]

        product_mock = MagicMock()
        product_mock.is_product = True
        mock_extract.side_effect = [product_mock, MagicMock(is_product=False)]

        valid = await discovery.get_valid_product_markdowns("test.com", target_count=2)

        # Should only contain 'md_valid_prod'
        assert len(valid) == 1
        assert valid[0] == "md_valid_prod" * 100
        assert mock_extract.call_count == 2

    @pytest.mark.parametrize(
        "line,expected_safe",
        [
            ("Normal text line", True),
            ("Contact us for info", True),
            ("Price: $100", False),
            ("SKU: ABC123", False),
            ("Verfügbarkeit: auf Lager", False),
            ("Total: €99.99", False),
            ("# Title Header", False),
            ("## Sub Header", False),
        ],
    )
    def test_is_safe_line_parametrizised(self, mock_s3, mock_db, line, expected_safe):
        discovery = BoilerplateDiscovery()
        assert discovery._is_safe_line(line) == expected_safe

    @pytest.mark.parametrize(
        "block,expected_valid",
        [
            ([], False),
            (["one"], False),
            (["one two"], False),
            (["one two three"], False),
            (["one two three four"], True),
            (["one two", "three four"], True),
        ],
    )
    def test_is_valid_block_parametrized(self, mock_s3, mock_db, block, expected_valid):
        discovery = BoilerplateDiscovery()
        assert discovery._is_valid_block(block) == expected_valid

    def test_find_match_blocks_exact_content(self, mock_s3, mock_db):
        """Verify that discovery finds exact matching blocks while filtering unsafe lines."""
        discovery = BoilerplateDiscovery()

        lines_a = [
            "This is a common header information block",
            "Price: $100",
            "This is a common footer information block",
        ]
        lines_b = [
            "This is a common header information block",
            "Price: $200",
            "This is a common footer information block",
        ]

        blocks = discovery._find_match_blocks(lines_a, lines_b)

        # Verify both blocks are found
        assert ["This is a common header information block"] in blocks
        assert ["This is a common footer information block"] in blocks
        assert len(blocks) == 2

    def test_find_common_blocks_detailed_full_flow(self, mock_s3, mock_db):
        """Verify full discovery logic with multiple documents and specific output."""
        discovery = BoilerplateDiscovery()

        markdowns = [
            "Common Header Block Text\nUnique A\nCommon Footer Block Text",
            "Common Header Block Text\nUnique B\nCommon Footer Block Text",
        ]

        blocks = discovery.find_common_blocks_detailed(markdowns)

        assert ["Common Header Block Text"] in blocks
        assert ["Common Footer Block Text"] in blocks


@patch("src.core.scraper.cleaning.boilerplate_remover.S3Operations")
class TestBoilerplateRemover:
    @pytest.mark.asyncio
    async def test_load_for_shop_with_caching(self, mock_s3):
        """Solid test for S3 loading vs Cache hits."""
        remover = BoilerplateRemover(cache_ttl=60)
        domain = "test.com"
        mock_data = {"blocks": [["s3_block"]]}

        with patch(
            "src.core.scraper.cleaning.boilerplate_remover.asyncio.to_thread",
            return_value=mock_data,
        ) as mock_thread:
            # 1. First call -> S3
            blocks1 = await remover.load_for_shop(domain)
            assert blocks1 == [["s3_block"]]
            assert mock_thread.call_count == 1

            # 2. Second call -> Cache hit
            blocks2 = await remover.load_for_shop(domain)
            assert blocks2 == [["s3_block"]]
            assert mock_thread.call_count == 1  # No extra call

            # 3. Third call -> Force refresh
            blocks3 = await remover.load_for_shop(domain, force_refresh=True)
            assert blocks3 == [["s3_block"]]
            assert mock_thread.call_count == 2

    @pytest.mark.asyncio
    async def test_load_for_shop_missing_data(self, mock_s3):
        """Verify load_for_shop returns None when S3 data is missing."""
        remover = BoilerplateRemover()

        with patch(
            "src.core.scraper.cleaning.boilerplate_remover.asyncio.to_thread",
            return_value=None,
        ):
            blocks = await remover.load_for_shop("missing.com")
            assert blocks is None

    def test_clean_exact_output(self, mock_s3):
        """Verify boilerplate removal preserves content integrity and removes exact blocks."""
        remover = BoilerplateRemover()
        markdown = (
            "KEEP THIS LINE\n"
            "DISCARD BLOCK LINE 1\n"
            "DISCARD BLOCK LINE 2\n"
            "MIDDLE CONTENT\n"
            "DISCARD BLOCK LINE 1\n"
            "DISCARD BLOCK LINE 2\n"
            "END CONTENT"
        )
        blocks = [["DISCARD BLOCK LINE 1", "DISCARD BLOCK LINE 2"]]

        cleaned = remover.clean(markdown, blocks, remove_noise=False)

        expected = "KEEP THIS LINE\nMIDDLE CONTENT\nEND CONTENT"
        assert cleaned == expected

    @pytest.mark.parametrize(
        "markdown,expected_contains,expected_not_contains",
        [
            # Basic noise removal
            (
                "## Description\nReal info\n## Related Products\nBad info\n## Reviews\nGood info",
                ["Description", "Real info", "Reviews", "Good info"],
                ["Related Products", "Bad info"],
            ),
            # German keywords
            (
                "## Produktinfo\nInhalt\n## Ähnliche Produkte\nIgnorieren\n## Versand\nLöschen\n## Mehr",
                ["Produktinfo", "Inhalt", "Mehr"],
                ["Ähnliche Produkte", "Ignorieren", "Versand", "Löschen"],
            ),
            # Nested headers stop condition
            (
                "## Main\nInfo\n### Related Products\nNested junk\n### Social\nMore junk\n## Next Section\nSafe info",
                ["Main", "Info", "Next Section", "Safe info"],
                ["Related Products", "Nested junk", "Social", "More junk"],
            ),
            # Content safety: don't remove if keyword is just part of a sentence
            (
                "We offer free delivery for related products when you buy more.",
                ["We offer free delivery for related products when you buy more."],
                [],
            ),
        ],
    )
    def test_remove_noise_sections_robust(
        self, mock_s3, markdown, expected_contains, expected_not_contains
    ):
        remover = BoilerplateRemover()
        cleaned = remover.remove_noise_sections(markdown)

        for item in expected_contains:
            assert item in cleaned
        for item in expected_not_contains:
            assert item not in cleaned

    def test_remove_noise_sections_stops_at_higher_level_header(self, mock_s3):
        """Verify that skipping starts at header level and stops at same or higher level."""
        remover = BoilerplateRemover()
        markdown = (
            "### Section 1\n"
            "Keep 1\n"
            "## Related Products\n"
            "Remove this\n"
            "### Still part of Related Products\n"
            "Remove this too\n"
            "## Section 2\n"
            "Keep 2"
        )
        cleaned = remover.remove_noise_sections(markdown)

        assert "Section 1" in cleaned
        assert "Keep 1" in cleaned
        assert "Section 2" in cleaned
        assert "Keep 2" in cleaned
        assert "Related Products" not in cleaned
        assert "Remove this" not in cleaned
        assert "Still part of Related Products" not in cleaned
