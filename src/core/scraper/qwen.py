import asyncio
import os
from typing import Optional, Any
import json

import torch
from crawl4ai import AsyncWebCrawler, CrawlerRunConfig, CacheMode, BrowserConfig
from vllm import LLM, SamplingParams

os.environ["PYTORCH_ALLOC_CONF"] = "expandable_segments:True"


sampling_params = SamplingParams(
        temperature=0,
        max_tokens=2048
)
llm = LLM(model="unsloth/Qwen3-8B-bnb-4bit",
          dtype=torch.bfloat16,
          trust_remote_code=True,
          enforce_eager=True
          )


def _join_outputs(outputs) -> str:
    """Helper to collect response chunks and join them into one string.

    vLLM may return text in `out.outputs` as chunks or directly in `out.text`.
    This helper aggregates all available text fragments into a single string.

    Args:
        outputs: Iterable of vLLM response objects.

    Returns:
        Concatenated text from all chunks.
    """
    parts = []
    for out in outputs:
        if hasattr(out, "outputs") and out.outputs:
            for chunk in out.outputs:
                text = getattr(chunk, "text", None)
                if text:
                    parts.append(text)
        else:
            text = getattr(out, "text", None)
            if text:
                parts.append(text)
    return "".join(parts)


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


def extract(markdown: str) -> str:
    """Extract product information as a JSON string from `markdown`.

    Behavior:
    - Sends a structured prompt to the vLLM model.
    - Attempts to parse the model's response as JSON.
    - If the model emits additional explanatory text, attempts to heuristically
      extract the first balanced JSON object from the output.
    - Always returns a JSON string; returns '{}' as a safe fallback on failure.

    Args:
        markdown: The page content (markdown or HTML) to analyze.

    Returns:
        A JSON string containing the extracted fields, or '{}' if parsing fails.
    """
    if not isinstance(markdown, str):
        raise TypeError("markdown must be a str")

    empty_json = {}
    prompt = f"""
            ### TASK
            Determine if the content is a SINGLE product page or a LIST/BLOG.

            ### STEP 1: STRUCTURE ANALYSIS (STRICT)
            Check for these 'Red Flags' of a List or Blog page:
            - Presence of multiple "WEITERLESEN" or "READ MORE" buttons.
            - Multiple different Art.Nr. (e.g., 1941, 2025, 3344) in the same text.

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
            - current_price (number, optional): The current selling price as a numeric value
            - currency (string, optional): The currency code (e.g., "EUR", "USD")
            - description (string): The longest and most detailed product description found
                - Extract ONLY the technical description of the primary item.
                - CRITICAL: Escape all double quotes with a backslash (e.g. \") or replace them with single quotes to ensure valid JSON.
            - state (string | UNKNOWN): One of: LISTED, AVAILABLE, RESERVED, SOLD, or REMOVED
            - images (array of strings, optional): Product images (must end with .jpeg .png) only if present in the markdown
            - language (string, optional): The language code of the product information (e.g., "de", "en")


            ### OUTPUT
            Return ONLY the JSON or {empty_json}. No words. No markdown.

            CONTENT:
            {markdown}
        """

    messages = [{"role": "user", "content": prompt}]

    try:
        # vLLM expects a list of requests; each request is a list of messages -> [messages]
        requests: Any = [messages]
        outputs = llm.chat(
            requests,
            sampling_params,
            chat_template_kwargs={"enable_thinking": False},
        )
    except Exception:
        return json.dumps(empty_json, ensure_ascii=False)

    text = _join_outputs(outputs)
    if not text:
        return json.dumps(empty_json, ensure_ascii=False)

    try:
        parsed = json.loads(text)
        return json.dumps(parsed, ensure_ascii=False)
    except Exception:
        pass

    candidate = _find_balanced_brace_object(text)
    if candidate:
        try:
            parsed = json.loads(candidate)
            return json.dumps(parsed, ensure_ascii=False)
        except Exception:
            # Last attempt: clean simple issues (e.g. newlines) and parse
            safe = candidate.replace("\n", " ").strip()
            try:
                parsed = json.loads(safe)
                return json.dumps(parsed, ensure_ascii=False)
            except Exception:
                pass

    return json.dumps(empty_json, ensure_ascii=False)

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
        excluded_tags=["nav", "footer", "header"],
    )
    browser_config = BrowserConfig(headless=True, verbose=False)

    async with AsyncWebCrawler(config=browser_config) as crawler:
        result = await crawler.arun(url=url, config=run_config)
        if result.success:
            markdown = result.markdown[:40000]
            print(len(markdown))
        else:
            raise result.exception
    return markdown

async def main(url: str):
    """Fetch a URL and run extraction on its markdown; used for manual testing."""
    markdown = await get_markdown(url)
    result=extract(markdown)
    print(result)



if __name__ == "__main__":
    asyncio.run(main("https://www.lot-tissimo.com/de-de/auction-catalogues/east-bristol-auctions/catalogue-id-sreas11269/lot-56553fae-122f-4db7-820c-b3c400aa318b"))
