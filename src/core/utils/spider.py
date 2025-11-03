import asyncio
import json
from pathlib import Path
from typing import Any

import aiofiles
from crawl4ai import AsyncWebCrawler, CrawlerRunConfig, CacheMode
from crawl4ai.content_scraping_strategy import LXMLWebScrapingStrategy

from src.core.algorithms.bfs_no_cycle_deep_crawl_strategy import (
    BFSNoCycleDeepCrawlStrategy,
)
from src.core.llms.groq import client as groq_client


def evaluate_urls(urls: list[str]) -> dict[str, Any]:
    """
    Evaluate multiple URLs and return a structured response with confidence scores.
    Each item contains the URL and a confidence score (1–100).
    """

    # Construct a clear and strict instruction
    prompt_user = (
        "You will receive several links that may or may not point to e-commerce product detail pages "
        "in the antiques or collectibles domain.\n\n"
        "For each provided link, output a JSON array where each element contains:\n"
        "- 'url': the original URL\n"
        "- 'confidence': an integer between 1 and 100 representing how likely it is a product detail page.\n\n"
        "Do NOT make up URLs. Only evaluate the ones given. "
        "Example format:\n"
        "{'evaluations': [{'url': 'https://example.com/item1', 'confidence': 92}]}.\n\n"
        f"Links: {urls}"
    )

    messages = [
        {
            "role": "system",
            "content": "You are a precise URL evaluator that outputs only valid JSON according to the schema.",
        },
        {"role": "user", "content": prompt_user},
    ]

    # JSON schema definition for structured output
    response_format = {
        "type": "json_schema",
        "json_schema": {
            "name": "url_confidence_evaluation",
            "schema": {
                "type": "object",
                "properties": {
                    "evaluations": {
                        "type": "array",
                        "items": {
                            "type": "object",
                            "properties": {
                                "url": {"type": "string"},
                                "confidence": {
                                    "type": "integer",
                                    "minimum": 1,
                                    "maximum": 100,
                                },
                            },
                            "required": ["url", "confidence"],
                            "additionalProperties": False,
                        },
                    }
                },
                "required": ["evaluations"],
                "additionalProperties": False,
            },
        },
    }

    client = groq_client.get_client()

    # Call the Groq Structured Output API
    chat_completion = client.chat.completions.create(
        model="moonshotai/kimi-k2-instruct-0905",
        messages=messages,
        response_format=response_format,
    )

    response_content = chat_completion.choices[0].message.content
    result = json.loads(response_content)

    return result


async def crawl_urls(url: str):
    """Crawl URLs starting from the given URL using BFSNoCycleDeepCrawlStrategy."""
    strategy = BFSNoCycleDeepCrawlStrategy(
        max_depth=1000,
        max_pages=10,
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
        exclude_patterns=["*wishlist*", "*cart*", "*login*", "*signup*"],
    )

    config = CrawlerRunConfig(
        cache_mode=CacheMode.BYPASS,
        deep_crawl_strategy=strategy,
        scraping_strategy=LXMLWebScrapingStrategy(),
        verbose=True,
        stream=False,
        check_robots_txt=True,
        delay_before_return_html=2.0,  # Wait 2 seconds for page to fully load
        mean_delay=3.0,  # Average 3 seconds delay between requests
        max_range=2.0,  # Random delay between 1-5 seconds
    )

    async with AsyncWebCrawler() as crawler:
        # Use strategy directly to get all discovered URLs
        discovered = await strategy.arun(start_url=url, crawler=crawler, config=config)

        return discovered


async def main(start_url: str):
    print(f"Starting crawl from: {start_url}")
    discovered = await crawl_urls(start_url)
    print(f"\n Done! {len(discovered)} unique URLs found")

    script_dir = Path(__file__).resolve().parent
    project_root = script_dir.parent.parent.parent
    data_dir = project_root / "data"

    data_dir.mkdir(exist_ok=True)

    output_file = data_dir / "crawled_url.json"

    async with aiofiles.open(output_file, "w") as f:
        await f.write(json.dumps(discovered, indent=4))

    batch_size = 20
    all_evaluations = []
    for i in range(0, len(discovered), batch_size):
        batch = discovered[i : i + batch_size]
        evaluation_result = evaluate_urls(batch)
        all_evaluations.extend(evaluation_result["evaluations"])

    print(f"Evaluated {len(all_evaluations)} URLs.")

    output_file = data_dir / "crawled_url_evaluated.json"

    # Save evaluations to a separate file
    async with aiofiles.open(output_file, "w") as f:
        await f.write(json.dumps(all_evaluations, indent=4))


if __name__ == "__main__":
    test_url = ""
    asyncio.run(main(test_url))
