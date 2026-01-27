import logging
from typing import List
from openai import AsyncOpenAI
from crawl4ai import AsyncWebCrawler

from core.utils.configs import build_product_scraper_components

logger = logging.getLogger(__name__)

# Shared async client for OpenAI-compatible vLLM server
client = AsyncOpenAI(
    base_url="http://localhost:8003/v1",
    api_key="dummy",
)
MODEL_NAME = "unsloth/Qwen2.5-7B-Instruct-bnb-4bit"


async def chat_completion(task: str, prompt: str) -> str:
    """Send an async chat completion request to the vLLM server."""
    try:
        response = await client.chat.completions.create(
            model=MODEL_NAME,
            messages=[
                {"role": "system", "content": task},
                {"role": "user", "content": prompt},
            ],
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

    browser_config, run_config = build_product_scraper_components()
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
