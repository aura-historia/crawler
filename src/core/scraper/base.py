import logging
from typing import List
from openai import AsyncOpenAI
from crawl4ai import AsyncWebCrawler, CrawlerRunConfig, CacheMode, BrowserConfig
import asyncio

logger = logging.getLogger(__name__)

# Shared async client for OpenAI-compatible vLLM server
client = AsyncOpenAI(
    base_url="http://localhost:8003/v1",
    api_key="dummy",
)
MODEL_NAME = "unsloth/Qwen3-8B-bnb-4bit"


async def chat_completion(prompt: str) -> str:
    """Send an async chat completion request to the vLLM server."""
    try:
        response = await client.chat.completions.create(
            model=MODEL_NAME,
            messages=[{"role": "user", "content": prompt}],
            temperature=0,
            max_tokens=2048,
            extra_body={"chat_template_kwargs": {"enable_thinking": False}},
        )
        return response.choices[0].message.content or ""
    except Exception as e:
        logger.error(f"vLLM Error: {type(e).__name__}: {e}")
        return "{}"


async def get_markdown(url: str) -> str:
    """Fetch a single page and return its markdown."""
    results = await get_markdowns([url])
    return results[0] if results else ""


async def get_markdowns(urls: List[str]) -> List[str]:
    """Fetch multiple pages sequentially and efficiently."""
    if not urls:
        return []

    excluded_tags = [
        "nav",
        "footer",
        "header",
        "navbar",
        "navigation",
        "site-header",
        "site-footer",
        "aside",
        "form",
        "button",
        "input",
        "noscript",
        "script",
        "style",
        "canvas",
        "video",
        "audio",
        "advertisement",
        "ads",
        "cookie-banner",
        "popup",
        "subscribe-modal",
        "rcb-banner",
        "rcb-overlay",
    ]

    run_config = CrawlerRunConfig(
        cache_mode=CacheMode.BYPASS,
        check_robots_txt=True,
        page_timeout=60000,
        excluded_tags=excluded_tags,
    )
    browser_config = BrowserConfig(
        headless=True,
        verbose=False,
    )

    markdowns = []
    try:
        async with AsyncWebCrawler(config=browser_config) as crawler:
            results = await crawler.arun_many(urls=urls, config=run_config)
            for result in results:
                if result.success:
                    markdowns.append(result.markdown)
                else:
                    logger.error(
                        f"Failed to fetch {result.url}: {result.error_message}"
                    )
    except Exception as e:
        logger.error(f"Critical error in get_markdowns crawler session: {e}")

    return markdowns


async def main(url: str):
    """Fetch a URL and print its markdown; used for manual testing."""
    markdown = await get_markdown(url)
    print(markdown)


if __name__ == "__main__":
    asyncio.run(
        main(
            url="https://www.antiquitaeten-tuebingen.de/anhaenger-in-585-gelbgold-mit-aquamarin-und-brillanten-j289/"
        )
    )
