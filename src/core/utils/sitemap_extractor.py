from crawl4ai import AsyncUrlSeeder, SeedingConfig


async def sitemap_extractor(domains: list[str]) -> dict[str, list[str]]:
    """Seed URLs using sitemap + Common Crawl."""
    async with AsyncUrlSeeder() as seeder:
        config = SeedingConfig(
            source="sitemap+cc", concurrency=50, force=True, max_urls=1000
        )
        results = await seeder.many_urls(domains, config)
        print(
            {domain: [item["url"] for item in urls] for domain, urls in results.items()}
        )
        return {
            domain: [item["url"] for item in urls] for domain, urls in results.items()
        }
