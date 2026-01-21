import logging
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
    """Fetch a page and return its markdown representation."""
    run_config = CrawlerRunConfig(
        cache_mode=CacheMode.BYPASS,
        check_robots_txt=True,
        excluded_tags=[
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
        ],
        process_iframes=True,
        remove_overlay_elements=True,
        simulate_user=True,
    )
    browser_config = BrowserConfig(headless=True, verbose=False, enable_stealth=True)
    async with AsyncWebCrawler(config=browser_config) as crawler:
        result = await crawler.arun(url=url, config=run_config)

        if result.success:
            return result.markdown
        else:
            raise result.exception


async def main(url: str):
    """Fetch a URL and print its markdown; used for manual testing."""
    markdown = await get_markdown(url)
    print(markdown[:10000])  # Print first 1000 characters for brevity


if __name__ == "__main__":
    asyncio.run(
        main(
            url="https://www.liveauctioneers.com/item/223771797_audemars-piguet-royal-oak-dania-beach-fl"
        )
    )
