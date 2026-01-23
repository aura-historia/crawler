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


async def get_markdowns(urls: List[str], progress_callback=None) -> List[str]:
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
        remove_overlay_elements=False,  # Don't remove overlays for discovery
    )
    browser_config = BrowserConfig(
        headless=True,
        verbose=False,
        user_agent="Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/120.0.0.0 Safari/537.36",
    )

    markdowns = []
    try:
        async with AsyncWebCrawler(config=browser_config) as crawler:
            for i, url in enumerate(urls):
                try:
                    result = await crawler.arun(url=url, config=run_config)
                    if result.success:
                        markdowns.append(result.markdown)
                    else:
                        logger.error(f"Failed to fetch {url}: {result.error_message}")
                        markdowns.append("")
                except Exception as e:
                    logger.error(f"Error fetching {url}: {e}")
                    markdowns.append("")

                if progress_callback:
                    progress_callback(i + 1, len(urls))
    except Exception as e:
        logger.error(f"Critical error in get_markdowns crawler session: {e}")
        # Fill remaining if crashed early
        while len(markdowns) < len(urls):
            markdowns.append("")

    return markdowns


async def get_markdown_with_details(url: str) -> dict:
    """Fetch a single page and return result details."""
    from crawl4ai import AsyncWebCrawler, CrawlerRunConfig, CacheMode, BrowserConfig

    run_config = CrawlerRunConfig(
        cache_mode=CacheMode.BYPASS,
        check_robots_txt=True,
        page_timeout=60000,
        excluded_tags=[],  # Include everything for debugging
        process_iframes=True,
        remove_overlay_elements=True,
    )
    browser_config = BrowserConfig(
        headless=True,
        verbose=True,
        user_agent="Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/120.0.0.0 Safari/537.36",
    )

    try:
        async with AsyncWebCrawler(config=browser_config) as crawler:
            result = await crawler.arun(url=url, config=run_config)
            return {
                "success": result.success,
                "markdown": result.markdown if result.success else "",
                "error_message": result.error_message if not result.success else "",
                "status_code": getattr(result, "status_code", None),
            }
    except Exception as e:
        return {
            "success": False,
            "markdown": "",
            "error_message": str(e),
            "status_code": None,
        }


async def main(url: str):
    """Fetch a URL and print its markdown; used for manual testing."""
    markdown = await get_markdown(url)
    print(markdown)


if __name__ == "__main__":
    asyncio.run(
        main(
            url="https://www.liveauctioneers.com/item/223771797_audemars-piguet-royal-oak-dania-beach-fl"
        )
    )
