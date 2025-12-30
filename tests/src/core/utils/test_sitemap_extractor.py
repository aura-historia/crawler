import pytest
from unittest.mock import AsyncMock, patch

import src.core.utils.sitemap_extractor as sitemap_extractor_mod


@pytest.mark.asyncio
async def test_sitemap_extractor_returns_expected_dict():
    """Test that sitemap_extractor returns the correct domain->url-list mapping."""
    domains = ["example.com", "shop.de"]
    fake_results = {
        "example.com": [
            {"url": "https://example.com/a"},
            {"url": "https://example.com/b"},
        ],
        "shop.de": [
            {"url": "https://shop.de/1"},
        ],
    }
    # Patch AsyncUrlSeeder and SeedingConfig
    with (
        patch.object(sitemap_extractor_mod, "AsyncUrlSeeder") as SeederMock,
        patch.object(sitemap_extractor_mod, "SeedingConfig") as ConfigMock,
    ):
        seeder_instance = SeederMock.return_value.__aenter__.return_value
        seeder_instance.many_urls = AsyncMock(return_value=fake_results)
        ConfigMock.return_value = object()  # config is not used in logic

        result = await sitemap_extractor_mod.sitemap_extractor(domains)
        assert result == {
            "example.com": ["https://example.com/a", "https://example.com/b"],
            "shop.de": ["https://shop.de/1"],
        }


@pytest.mark.asyncio
async def test_sitemap_extractor_empty():
    """Test that sitemap_extractor handles empty input and empty results."""
    domains = []
    fake_results = {}
    with (
        patch.object(sitemap_extractor_mod, "AsyncUrlSeeder") as SeederMock,
        patch.object(sitemap_extractor_mod, "SeedingConfig") as ConfigMock,
    ):
        seeder_instance = SeederMock.return_value.__aenter__.return_value
        seeder_instance.many_urls = AsyncMock(return_value=fake_results)
        ConfigMock.return_value = object()
        result = await sitemap_extractor_mod.sitemap_extractor(domains)
        assert result == {}


@pytest.mark.asyncio
async def test_sitemap_extractor_handles_missing_urls_key():
    """Test that sitemap_extractor handles domains with empty url lists."""
    domains = ["empty.com"]
    fake_results = {"empty.com": []}
    with (
        patch.object(sitemap_extractor_mod, "AsyncUrlSeeder") as SeederMock,
        patch.object(sitemap_extractor_mod, "SeedingConfig") as ConfigMock,
    ):
        seeder_instance = SeederMock.return_value.__aenter__.return_value
        seeder_instance.many_urls = AsyncMock(return_value=fake_results)
        ConfigMock.return_value = object()
        result = await sitemap_extractor_mod.sitemap_extractor(domains)
        assert result == {"empty.com": []}
