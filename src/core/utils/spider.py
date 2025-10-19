import asyncio
import json
from pathlib import Path
from crawl4ai import AsyncWebCrawler, CrawlerRunConfig
from crawl4ai.content_scraping_strategy import LXMLWebScrapingStrategy
from src.core.utils.url_filters import ExtensionExcludeBFSStrategy


async def main(url: str):
    strategy = ExtensionExcludeBFSStrategy(
        max_depth=1000,
        include_external=False,
        exclude_extensions=[
            "jpg",
            "jpeg",
            "png",
            "gif",
            "webp",
            "svg",  # Images
            "mp4",
            "avi",
            "mov",
            "wmv",
            "flv",  # Videos
            "pdf",
            "doc",
            "docx",
            "xls",
            "xlsx",  # Documents
            "zip",
            "rar",
            "tar",
            "gz",  # Archives
            "css",
            "js",
            "ico",
            "woff",
            "woff2",
            "ttf",  # Assets
        ],
    )

    config = CrawlerRunConfig(
        deep_crawl_strategy=strategy,
        scraping_strategy=LXMLWebScrapingStrategy(),
        verbose=True,
        stream=False,
        check_robots_txt=True,
    )

    async with AsyncWebCrawler() as crawler:
        print("Starting deep crawl with extension filtering...")
        print(f"   Excluded extensions: {', '.join(strategy._exclude_extensions)}")
        print()

        # Use strategy directly to get all discovered URLs
        discovered = await strategy.arun(start_url=url, crawler=crawler, config=config)

        print(f"\n Done! {len(discovered)} unique URLs found")
        print("   (Images, videos and other files were excluded)")

        script_dir = Path(__file__).resolve().parent
        project_root = script_dir.parent.parent.parent
        data_dir = project_root / "data"

        data_dir.mkdir(exist_ok=True)

        output_file = data_dir / "crawled_url_filtered.json"

        with open(output_file, "w") as f:
            json.dump(discovered, f, indent=4)

        print(f"\n URLs saved to: {output_file}")

        print("\n First 10 discovered URLs:")
        for i, url in enumerate(discovered[:10], 1):
            print(f"   {i}. {url}")

        if len(discovered) > 10:
            print(f"   ... and {len(discovered) - 10} more")


if __name__ == "__main__":
    asyncio.run(main())
