import asyncio
from typing import Optional, List
import json
import logging
from openai import AsyncOpenAI
from crawl4ai import AsyncWebCrawler, CrawlerRunConfig, CacheMode, BrowserConfig

# Configure logging
logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(levelname)s - %(message)s'
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
        prompt: The user prompt to send.
    Returns:
        The completion text from the model.
    """
    try:
        response = await client.chat.completions.create(
            model=MODEL_NAME,
            messages=[{"role": "user", "content": prompt}],
            temperature=0,
            max_tokens=2048,
            extra_body={"chat_template_kwargs": {"enable_thinking": False}}
        )

        content = response.choices[0].message.content or ""

        if not content or content.strip() == "{}":
            logger.warning(f"Empty response from vLLM (prompt: {len(prompt)} chars)")

        return content
    except Exception as e:
        logger.error(f"vLLM Error: {type(e).__name__}: {e}")
        import traceback
        logger.error(traceback.format_exc())
        return "{}"
def _find_balanced_brace_object(text: str) -> Optional[str]:
    """Find the first JSON-like object in `text` by scanning for balanced braces.
    This method attempts to locate the first substring that represents a balanced
    JSON object (starting with '{' and ending with the matching '}'), which is
    more robust than a simple regex when nested braces or surrounding text exist.
    Args:
        text: The string to search.
    Returns:
        The substring containing the first balanced JSON object (including braces),
        or None if none is found.
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
async def extract_batch(markdowns: List[str]) -> List[str]:
    """Extract product information from multiple markdown texts concurrently.
    This function sends all requests concurrently to the vLLM server, which
    handles batching internally for efficient GPU utilization.
    Args:
        markdowns: List of markdown/HTML content strings to analyze.
    Returns:
        List of JSON strings containing extracted fields for each input.
    """
    if not markdowns:
        return []
    empty_json = {}
    # Prepare all prompts
    prompts = []
    for markdown in markdowns:
        if not isinstance(markdown, str):
            prompts.append("{}")
            continue
        prompt = f"""
            ### TASK
            Determine if the content is a SINGLE product page or a LIST/BLOG.
            ### STEP 1: STRUCTURE ANALYSIS (STRICT)
            Check for these 'Red Flags' of a List or Blog page:
            - Presence of multiple "WEITERLESEN" or "READ MORE" buttons.
            - Multiple different Art.Nr. (e.g., 1941, 2025, 3344) in the same text.
            - Home page indicators like "Startseite", "Home", "Willkommen".
            IF ANY RED FLAGS ARE FOUND:
            You MUST return ONLY {empty_json}. Do not extract any data.
            ### STEP 2: PRODUCT VERIFICATION
            Only proceed if there is EXACTLY ONE main product.
            - A real product page must have a "Warenkorb" (Cart) button or a clear "In den Warenkorb" text.
            - A real product page must have a clear Price with a Currency symbol (e.g. €, EUR) that is NOT inside an image link.
            ### STEP 3: EXTRACTION
            If Step 1 and 2 pass:
            - shop_item_id (string): The product ID, SKU, or article number (e.g., "Art.Nr", "SKU", "ID")
            - title (string): The product title or name
            - current_price (number, optional): The current selling price as a numeric value in cents (e.g., 1999 for €19.99)
            - currency (string, optional): The currency code (e.g., "EUR", "USD")
            - description (string): The longest and most detailed product description found
                - Extract ONLY the technical description of the primary item.
                - CRITICAL: Escape all double quotes with a backslash (e.g. \") or replace them with single quotes to ensure valid JSON.
            - state (string | UNKNOWN): Only exactly one of these: AVAILABLE, SOLD, LISTED, RESERVED, REMOVED or UNKNOWN. Definitions:
                - AVAILABLE: Product is available for purchase
                - SOLD: Product has been sold and can no longer be purchased
                - LISTED: Product has been listed
                - RESERVED: Product is reserved by a buyer
                - REMOVED: Product has been removed and can no longer be tracked
                - UNKNOWN: Product has an unknown state
            - images (array of strings, optional): Product images (must end with .jpeg .png) only if present in the markdown
            - language (string, optional): The language code of the product information (e.g., "de", "en")
            ### OUTPUT
            Return ONLY the JSON or {empty_json}. No words. No markdown.
            CONTENT:
            {markdown}
        """
        prompts.append(prompt)
    # Send all requests concurrently - vLLM server will batch them
    try:
        tasks = [chat_completion(prompt) for prompt in prompts]
        responses = await asyncio.gather(*tasks, return_exceptions=True)
    except Exception as e:
        logger.error(f"Error during concurrent LLM requests: {e}")
        return [json.dumps(empty_json, ensure_ascii=False)] * len(markdowns)
    # Process each response
    results = []
    for i, response in enumerate(responses):
        if isinstance(response, Exception):
            logger.error(f"Error in request {i}: {response}")
            results.append(json.dumps(empty_json, ensure_ascii=False))
            continue
        text = response if isinstance(response, str) else ""
        if not text:
            results.append(json.dumps(empty_json, ensure_ascii=False))
            continue
        try:
            parsed = json.loads(text)
            results.append(json.dumps(parsed, ensure_ascii=False))
            continue
        except Exception:
            pass
        candidate = _find_balanced_brace_object(text)
        if candidate:
            try:
                parsed = json.loads(candidate)
                results.append(json.dumps(parsed, ensure_ascii=False))
                continue
            except Exception:
                safe = candidate.replace("\n", " ").strip()
                try:
                    parsed = json.loads(safe)
                    results.append(json.dumps(parsed, ensure_ascii=False))
                    continue
                except Exception:
                    pass
        results.append(json.dumps(empty_json, ensure_ascii=False))
    return results

async def extract(markdown: str) -> str:
    """Extract product information as a JSON string from `markdown`.
    This function sends the request directly to the vLLM server.
    Multiple concurrent calls from different workers will all be sent
    to the server, which handles batching automatically.
    Args:
        markdown: The page content (markdown or HTML) to analyze.
    Returns:
        A JSON string containing the extracted fields, or '{}' if parsing fails.
    """
    results = await extract_batch([markdown])
    return results[0] if results else "{}"

async def get_markdown(url: str) -> str:
    """Fetch a page and return its markdown representation.
    Uses the crawl4ai AsyncWebCrawler to fetch and render a page and returns
    a truncated markdown string (max 40k characters) suitable for the LLM.
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
        excluded_tags=["nav", "footer", "header", "navbar", "navigation", "site-header", "site-footer", "aside"],
        process_iframes=True
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
    result=extract(markdown)
    print(result)

    print(f"{'='*60}\n")

if __name__ == "__main__":
    asyncio.run(main("https://www.antiquitaeten-tuebingen.de/eckhaengeschrank-nussbaum-intarsiert-2-haelfte-18-jh-art-nr-8190/"))
