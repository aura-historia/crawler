import asyncio
from typing import Optional
import json
import logging
from openai import AsyncOpenAI
from crawl4ai import AsyncWebCrawler, CrawlerRunConfig, CacheMode, BrowserConfig
from datetime import datetime, timezone

from pydantic import ValidationError

from core.scraper.prompts.cleaner import CLEANER_PROMPT_TEMPLATE
from core.scraper.prompts.extractor import EXTRACTION_PROMPT_TEMPLATE
from core.scraper.schemas.extracted_product import ExtractedProduct
from core.scraper.schemas.put_products_collection_data_mapper import (
    map_extracted_product_to_schema,
)
from core.utils.send_items import send_items

logging.basicConfig(
    level=logging.INFO, format="%(asctime)s - %(levelname)s - %(message)s"
)
logger = logging.getLogger(__name__)

# Shared async client for OpenAI-compatible vLLM server
client = AsyncOpenAI(
    base_url="http://localhost:8003/v1",
    api_key="dummy",  # vLLM server doesn't require a real key
)
MODEL_NAME = "unsloth/Qwen3-8B-bnb-4bit"


async def chat_completion(prompt: str) -> str:
    """Send an async chat completion request to the vLLM server.

    Args:
        prompt: The user prompts to send.

    Returns:
        The completion text from the model.
    """
    try:
        response = await client.chat.completions.create(
            model=MODEL_NAME,
            messages=[{"role": "user", "content": prompt}],
            temperature=0,
            max_tokens=2048,
            extra_body={"chat_template_kwargs": {"enable_thinking": False}},
        )

        content = response.choices[0].message.content or ""

        return content
    except Exception as e:
        logger.error(f"vLLM Error: {type(e).__name__}: {e}")
        import traceback

        logger.error(traceback.format_exc())
        return "{}"


def _find_balanced_brace_object(text: str) -> Optional[str]:
    """Find the first JSON-like object in text by scanning for balanced braces.

    Args:
        text: The string to search.

    Returns:
        The substring containing the first balanced JSON object, or None if not found.
    """
    start = None
    depth = 0
    for i, ch in enumerate(text):
        if ch == "{":
            if start is None:
                start = i
            depth += 1
        elif ch == "}" and start is not None:
            depth -= 1
            if depth == 0:
                return text[start : i + 1]
    return None


def _parse_llm_response(response_text: str) -> str:
    """Parse LLM response to extract valid JSON.

    Tries multiple parsing strategies in order:
    1. Direct JSON parsing
    2. Extract balanced brace object
    3. Sanitize newlines and retry

    Args:
        response_text: Raw text response from the LLM

    Returns:
        JSON string (either parsed data or empty '{}')
    """
    if not response_text:
        return "{}"

    # Strategy 1: Try direct parsing
    try:
        parsed = json.loads(response_text)
        return json.dumps(parsed, ensure_ascii=False)
    except Exception:
        pass

    # Strategy 2: Try to find balanced JSON object
    candidate = _find_balanced_brace_object(response_text)
    if not candidate:
        return "{}"

    try:
        parsed = json.loads(candidate)
        return json.dumps(parsed, ensure_ascii=False)
    except Exception:
        pass

    # Strategy 3: Try sanitizing newlines
    try:
        safe = candidate.replace("\n", " ").strip()
        parsed = json.loads(safe)
        return json.dumps(parsed, ensure_ascii=False)
    except Exception:
        return "{}"


def validate_extracted_data(data: dict) -> tuple[dict, Optional[str]]:
    """Validate the extracted data using the Pydantic model.

    Args:
        data: The parsed JSON data as a dictionary.

    Returns:
        The validated data as a dictionary, or an empty dictionary if validation fails.
    """
    if not data:
        return {}, "No data to validate."

    try:
        validated_product = ExtractedProduct(**data)
        return validated_product.model_dump(), None
    except ValidationError as e:
        return {}, e.json()


async def extract(
    markdown: str, current_time: Optional[datetime] = None, max_retries: int = 3
) -> ExtractedProduct | None:
    """Extract product information as JSON string from markdown in two steps: validation & cleaning, then extraction.

    Pass CURRENT_TIME to the LLM so it can calculate auction dates.
    Args:
        markdown: Page content (Markdown or HTML) to analyze.
        current_time: Optional UTC datetime as reference for relative times.
        max_retries: Number of retries for extraction on validation failure.

    Returns:
        A JSON string with extracted fields or '{}' on errors.
    """
    if not isinstance(markdown, str):
        return None

    if current_time is None:
        current_time = datetime.now(timezone.utc)

    current_time_iso = (
        current_time.astimezone(timezone.utc)
        .replace(microsecond=0)
        .isoformat()
        .replace("+00:00", "Z")
    )
    logger.info(f"Current time: {current_time_iso}")

    cleaner_prompt = CLEANER_PROMPT_TEMPLATE.format(
        current_time=current_time_iso, markdown=markdown
    )
    raw_summary = await chat_completion(cleaner_prompt)
    if "NOT_A_PRODUCT" in raw_summary or not raw_summary.strip():
        logger.info("Step 1 determined this is not a product page.")
        return None
    logger.info(f"Step 1 extracted data: {raw_summary}")

    extraction_prompt_base = EXTRACTION_PROMPT_TEMPLATE.format(
        current_time=current_time_iso, clean_text=raw_summary
    )

    last_exception = None
    for attempt in range(max_retries):
        prompt = extraction_prompt_base
        if last_exception:
            prompt += f"\n\n# VALIDATION ERROR\nThe previous extraction failed validation with error:\n{last_exception}\nPlease fix the output."

        response_text = await chat_completion(prompt)
        print(response_text)
        parsed_data = json.loads(_parse_llm_response(response_text))
        validated_data, last_exception = validate_extracted_data(parsed_data)

        if validated_data:
            return ExtractedProduct(**validated_data)

        logger.warning(f"Retry {attempt + 1}/{max_retries} failed.")
    return None


async def get_markdown(url: str) -> str:
    """Fetch a page and return its markdown representation.

    Uses the crawl4ai AsyncWebCrawler to fetch and render a page and returns
    a truncated markdown string (max 20k characters) suitable for the LLM.

    Args:
        url: The URL to fetch.

    Returns:
        The page markdown as a string.

    Raises:
        The underlying exception from the crawler when fetch fails.
    """
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
        ],
        process_iframes=True,
        remove_overlay_elements=True,
    )
    browser_config = BrowserConfig(headless=True, verbose=False)
    async with AsyncWebCrawler(config=browser_config) as crawler:
        result = await crawler.arun(url=url, config=run_config)
        if result.success:
            markdown = result.markdown[:40000]
        else:
            raise result.exception
    return markdown


async def main(url: str):
    """Fetch a URL and run extraction on its markdown; used for manual testing."""
    markdown = await get_markdown(url)
    print(markdown)
    result = await extract(markdown)
    data = map_extracted_product_to_schema(result, url)
    print(json.dumps(data, indent=2, ensure_ascii=False))

    await send_items(data)


if __name__ == "__main__":
    asyncio.run(
        main(
            "https://www.antik-shop.de/produkt/louis-seize-stil-tisch-mahagoni-shabby-chic-um-1930/"
        )
    )
