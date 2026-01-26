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
    async def test_get_valid_product_markdowns(
        self, mock_extract, mock_markdown, mock_s3, mock_db
    ):
        discovery = BoilerplateDiscovery()
        discovery.db.get_product_urls_by_domain = MagicMock(
            return_value=(["http://a.com", "http://b.com"], None)
        )

        # Mock markdown returns
        mock_markdown.side_effect = ["md1" * 200, "md2" * 200]

        # Mock extract returns - first is a product, second is not
        product_mock = MagicMock()
        product_mock.is_product = True
        mock_extract.side_effect = [product_mock, None]

        valid = await discovery.get_valid_product_markdowns("test.com", target_count=1)

        assert len(valid) == 1
        assert "md1" in valid[0]

    @pytest.mark.asyncio
    @patch("src.core.scraper.cleaning.boilerplate_discovery.get_markdown")
    @patch("src.core.scraper.qwen.extract")
    async def test_get_valid_product_markdowns_skips_short_content(
        self, mock_extract, mock_markdown, mock_s3, mock_db
    ):
        discovery = BoilerplateDiscovery()
        discovery.db.get_product_urls_by_domain = MagicMock(
            return_value=(["http://a.com", "http://b.com"], None)
        )

        # First markdown too short, second is valid
        mock_markdown.side_effect = ["short", "valid" * 200]

        product_mock = MagicMock()
        product_mock.is_product = True
        mock_extract.return_value = product_mock

        valid = await discovery.get_valid_product_markdowns("test.com", target_count=1)

        assert len(valid) == 1
        assert "valid" in valid[0]

    @pytest.mark.asyncio
    @patch("src.core.scraper.cleaning.boilerplate_discovery.get_markdown")
    @patch("src.core.scraper.qwen.extract")
    async def test_get_valid_product_markdowns_deduplicates(
        self, mock_extract, mock_markdown, mock_s3, mock_db
    ):
        discovery = BoilerplateDiscovery()
        discovery.db.get_product_urls_by_domain = MagicMock(
            return_value=(["http://a.com", "http://b.com"], None)
        )

        # Return same content twice
        same_content = "duplicate" * 200
        mock_markdown.side_effect = [same_content, same_content]

        product_mock = MagicMock()
        product_mock.is_product = True
        mock_extract.return_value = product_mock

        valid = await discovery.get_valid_product_markdowns("test.com", target_count=2)

        # Should only have 1 result despite 2 URLs due to deduplication
        assert len(valid) == 1

    def test_is_safe_line_filters_critical_keywords(self, mock_s3, mock_db):
        discovery = BoilerplateDiscovery()

        # Should filter lines with critical keywords
        assert not discovery._is_safe_line("Price: $100")
        assert not discovery._is_safe_line("Item number: 12345")
        assert not discovery._is_safe_line("SKU: ABC123")
        assert not discovery._is_safe_line("Verfügbarkeit: auf Lager")
        assert not discovery._is_safe_line("Stock: 5 items")

        # Should also filter currency symbols
        assert not discovery._is_safe_line("Total: €99.99")
        assert not discovery._is_safe_line("Only £50")

        # Should filter headers
        assert not discovery._is_safe_line("# Main Title")
        assert not discovery._is_safe_line("## Subtitle")

        # These should be safe
        assert discovery._is_safe_line("This is a normal line of text")
        assert discovery._is_safe_line("Contact us for more information")

    def test_is_valid_block_checks_word_count(self, mock_s3, mock_db):
        discovery = BoilerplateDiscovery()

        # Not enough words
        assert not discovery._is_valid_block([])
        assert not discovery._is_valid_block(["one"])
        assert not discovery._is_valid_block(["one", "two"])

        # Enough words
        assert discovery._is_valid_block(["one two three four"])
        assert discovery._is_valid_block(["one two", "three four"])

    def test_find_match_blocks(self, mock_s3, mock_db):
        discovery = BoilerplateDiscovery()

        lines_a = [
            "Header text here",
            "Unique content for A",
            "Common footer line text",
            "Another common line text",
        ]

        lines_b = [
            "Header text here",
            "Unique content for B",
            "Common footer line text",
            "Another common line text",
        ]

        blocks = discovery._find_match_blocks(lines_a, lines_b)

        # Should find the matching blocks
        assert len(blocks) > 0

    def test_find_match_blocks_filters_unsafe_lines(self, mock_s3, mock_db):
        discovery = BoilerplateDiscovery()

        lines_a = [
            "Common header text information",
            "Price: $100 matches",
            "Safe footer text here information",
        ]

        lines_b = [
            "Common header text information",
            "Price: $200 different",  # Same position but different price
            "Safe footer text here information",
        ]

        blocks = discovery._find_match_blocks(lines_a, lines_b)

        # Should not include price lines
        for block in blocks:
            for line in block:
                assert "Price:" not in line

    def test_find_common_blocks_detailed_requires_two_docs(self, mock_s3, mock_db):
        discovery = BoilerplateDiscovery()

        # Should return empty for single document
        assert discovery.find_common_blocks_detailed(["single doc"]) == []
        assert discovery.find_common_blocks_detailed([]) == []

    def test_find_common_blocks_detailed_finds_matches(self, mock_s3, mock_db):
        discovery = BoilerplateDiscovery()

        markdowns = [
            "Header line text\nProduct specific A\nFooter with text information\nContact us today",
            "Header line text\nProduct specific B\nFooter with text information\nContact us today",
        ]

        blocks = discovery.find_common_blocks_detailed(markdowns)

        # Should find some common blocks
        assert len(blocks) > 0


@patch("src.core.scraper.cleaning.boilerplate_remover.S3Operations")
class TestBoilerplateRemover:
    @pytest.mark.asyncio
    @patch("src.core.scraper.cleaning.boilerplate_remover.asyncio.to_thread")
    async def test_load_for_shop_cache(self, mock_thread, mock_s3):
        remover = BoilerplateRemover(cache_ttl=10)
        remover._save_to_cache("test.com", [["block1"]])

        blocks = await remover.load_for_shop("test.com")
        assert blocks == [["block1"]]
        mock_thread.assert_not_called()

    @pytest.mark.asyncio
    @patch("src.core.scraper.cleaning.boilerplate_remover.asyncio.to_thread")
    async def test_load_for_shop_from_s3(self, mock_thread, mock_s3):
        remover = BoilerplateRemover(cache_ttl=10)

        mock_thread.return_value = {"blocks": [["s3block"]]}

        blocks = await remover.load_for_shop("test.com")

        assert blocks == [["s3block"]]
        assert mock_thread.called

    @pytest.mark.asyncio
    @patch("src.core.scraper.cleaning.boilerplate_remover.asyncio.to_thread")
    async def test_load_for_shop_force_refresh(self, mock_thread, mock_s3):
        remover = BoilerplateRemover(cache_ttl=10)
        remover._save_to_cache("test.com", [["cached"]])

        mock_thread.return_value = {"blocks": [["fresh"]]}

        blocks = await remover.load_for_shop("test.com", force_refresh=True)

        assert blocks == [["fresh"]]
        assert mock_thread.called

    def test_get_from_cache_returns_none_when_expired(self, mock_s3):
        import time

        remover = BoilerplateRemover(cache_ttl=0.1)

        remover._save_to_cache("test.com", [["block"]])
        time.sleep(0.2)

        cached = remover._get_from_cache("test.com")
        assert cached is None

    def test_get_from_cache_returns_data_when_valid(self, mock_s3):
        remover = BoilerplateRemover(cache_ttl=10)

        remover._save_to_cache("test.com", [["block"]])
        cached = remover._get_from_cache("test.com")

        assert cached == [["block"]]

    def test_find_subsequence_finds_match(self, mock_s3):
        remover = BoilerplateRemover()

        lines = ["line1", "line2", "line3", "line4", "line5"]
        sub = ["line2", "line3"]

        idx = remover.find_subsequence(lines, sub)
        assert idx == 1

    def test_find_subsequence_returns_minus_one_when_not_found(self, mock_s3):
        remover = BoilerplateRemover()

        lines = ["line1", "line2", "line3"]
        sub = ["line4", "line5"]

        idx = remover.find_subsequence(lines, sub)
        assert idx == -1

    def test_find_subsequence_empty_sub(self, mock_s3):
        remover = BoilerplateRemover()

        lines = ["line1", "line2"]
        sub = []

        idx = remover.find_subsequence(lines, sub)
        assert idx == -1

    def test_clean_removes_blocks(self, mock_s3):
        remover = BoilerplateRemover()

        markdown = "line1\nblock1\nblock2\nline2\nline3"
        blocks = [["block1", "block2"]]

        cleaned = remover.clean(markdown, blocks, remove_related=False)

        assert "block1" not in cleaned
        assert "block2" not in cleaned
        assert "line1" in cleaned
        assert "line2" in cleaned

    def test_clean_handles_multiple_occurrences(self, mock_s3):
        remover = BoilerplateRemover()

        markdown = "start\nfooter\nend\nmiddle\nfooter\nlast"
        blocks = [["footer"]]

        cleaned = remover.clean(markdown, blocks, remove_related=False)

        # Both occurrences should be removed
        assert cleaned.count("footer") == 0
        assert "start" in cleaned
        assert "middle" in cleaned

    def test_clean_empty_blocks(self, mock_s3):
        remover = BoilerplateRemover()

        markdown = "line1\nline2"
        blocks = []

        cleaned = remover.clean(markdown, blocks, remove_related=False)

        assert cleaned == markdown

    def test_remove_noise_sections_removes_related_products(self, mock_s3):
        remover = BoilerplateRemover()

        markdown = """# Main Content
            Some product info
            ## Related Products
            Product 1
            Product 2
            ## More Info
            Additional content"""

        cleaned = remover.remove_noise_sections(markdown)

        assert "Related Products" not in cleaned
        assert "Product 1" not in cleaned
        assert "Main Content" in cleaned
        assert "More Info" in cleaned

    def test_remove_noise_sections_handles_german_keywords(self, mock_s3):
        remover = BoilerplateRemover()

        markdown = """# Hauptinhalt
            Produktinfo
            ## Ähnliche Produkte
            Produkt 1
            ## Versand
            Versandinfo
            ## Weitere Infos
            Mehr Inhalt"""

        cleaned = remover.remove_noise_sections(markdown)

        assert "Ähnliche Produkte" not in cleaned
        assert "Versand" not in cleaned
        assert "Hauptinhalt" in cleaned
        assert "Weitere Infos" in cleaned

    def test_remove_noise_sections_stops_at_same_level_header(self, mock_s3):
        remover = BoilerplateRemover()

        markdown = """## Section 1
            Content 1
            ## Related Products
            Should be removed
            ## Section 2
            Should be kept"""

        cleaned = remover.remove_noise_sections(markdown)

        assert "Related Products" not in cleaned
        assert "Should be removed" not in cleaned
        assert "Section 2" in cleaned
        assert "Should be kept" in cleaned

    def test_remove_noise_sections_social_media(self, mock_s3):
        remover = BoilerplateRemover()

        markdown = """# Product
            Great product
            ## Follow us
            ### Facebook
            Link here
            ### Instagram
            Link here
            ## Description
            Product description"""

        cleaned = remover.remove_noise_sections(markdown)

        assert "Follow us" not in cleaned
        assert "Facebook" not in cleaned
        assert "Instagram" not in cleaned
        assert "Description" in cleaned
        assert "Product description" in cleaned
