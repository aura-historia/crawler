import pytest
from unittest.mock import patch, MagicMock
from src.core.scraper.cleaning.boilerplate_discovery import BoilerplateDiscovery
from src.core.scraper.cleaning.processor import BoilerplateRemover


class TestBoilerplateDiscovery:
    @pytest.mark.asyncio
    @patch("src.core.scraper.cleaning.boilerplate_discovery.get_markdown")
    @patch("src.core.scraper.cleaning.boilerplate_discovery.chat_completion")
    async def test_get_valid_product_markdowns(self, mock_chat, mock_markdown):
        discovery = BoilerplateDiscovery()
        discovery.db.get_product_urls_by_domain = MagicMock(
            return_value=(["http://a.com", "http://b.com"], None)
        )

        mock_markdown.side_effect = ["md1", "md2"]
        mock_chat.side_effect = ["PROD INFO", "NOT_A_PRODUCT"]

        valid = await discovery.get_valid_product_markdowns("test.com", target_count=1)

        assert len(valid) == 1
        assert valid[0] == "md1"
        assert mock_chat.call_count == 1

    def test_find_common_blocks(self):
        discovery = BoilerplateDiscovery()
        markdowns = [
            "Header\nProduct 1\nFooter\nSocial\nContact",
            "Header\nProduct 2\nFooter\nSocial\nContact",
            "Header\nProduct 3\nFooter\nSocial\nContact",
            "Header\nProduct 4\nFooter\nSocial\nContact",
            "Header\nProduct 5\nFooter\nSocial\nContact",
        ]

        # "Header" is 1 word (< 5)
        # "Footer" is 1 word (< 5)
        # "Social Link One Two Three Four Five" (simulated)
        markdowns = [
            m.replace("Social", "Social Link One Two Three Four Five")
            for m in markdowns
        ]

        common = discovery.find_common_blocks(markdowns, min_words=5, min_frequency=4)

        assert "Social Link One Two Three Four Five" in common
        assert "Header" not in common  # Too short


class TestBoilerplateRemover:
    @pytest.mark.asyncio
    @patch("src.core.scraper.cleaning.processor.asyncio.to_thread")
    async def test_load_for_shop_cache(self, mock_thread):
        remover = BoilerplateRemover(cache_ttl=10)
        remover._save_to_cache("test.com", ["block1"])

        blocks = await remover.load_for_shop("test.com")
        assert blocks == ["block1"]
        mock_thread.assert_not_called()

    def test_clean(self):
        remover = BoilerplateRemover()
        markdown = "Some text block1 more text block2 end"
        blocks = ["block1", "block2"]

        cleaned = remover.clean(markdown, blocks)

        assert "block1" not in cleaned
        assert "block2" not in cleaned

    @pytest.mark.asyncio
    @patch("src.core.scraper.cleaning.processor.asyncio.to_thread")
    async def test_should_rediscover_staleness(self, mock_thread):
        remover = BoilerplateRemover()
        from datetime import datetime, timezone, timedelta

        stale_date = (datetime.now(timezone.utc) - timedelta(days=31)).isoformat()

        mock_thread.return_value = {"blocks": ["a"], "updated_at": stale_date}

        should = await remover.should_rediscover("test.com", 1.0)
        assert should is True
