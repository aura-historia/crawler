import asyncio
import json
import logging
from typing import Optional
from datetime import datetime, timezone
from src.core.scraper.base import chat_completion, get_markdown

from pydantic import ValidationError

from src.core.scraper.prompts.cleaner import CLEANER_PROMPT_TEMPLATE
from src.core.scraper.prompts.extractor import EXTRACTION_PROMPT_TEMPLATE
from src.core.scraper.schemas.extracted_product import ExtractedProduct
from src.core.scraper.cleaning.processor import BoilerplateRemover
from src.core.scraper.cleaning.boilerplate_discovery import BoilerplateDiscovery

# Logger is already initialized in base or can be kept here
logger = logging.getLogger(__name__)

boilerplate_remover = BoilerplateRemover()
boilerplate_discovery = BoilerplateDiscovery()


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


async def _apply_boilerplate_removal(markdown: str, domain: str) -> str:
    """Helper to handle boilerplate removal logic."""
    try:
        blocks = await boilerplate_remover.load_for_shop(domain)

        # Check if we need to rediscover (missing, stale, or structural shift)
        if not blocks or await boilerplate_remover.should_rediscover(domain, 1.0):
            logger.info(f"Triggering boilerplate discovery for {domain}")
            blocks = await boilerplate_discovery.discover_and_save(domain)

        if blocks:
            original_len = len(markdown)
            markdown, hit_rate = boilerplate_remover.clean(markdown, blocks)
            logger.info(
                f"Boilerplate removed for {domain}. Hit rate: {hit_rate:.2f}. "
                f"Length reduced from {original_len} to {len(markdown)}"
            )

            # Check for structural shift AFTER cleaning
            if await boilerplate_remover.should_rediscover(domain, hit_rate):
                logger.info(
                    f"Structural shift detected for {domain}, re-triggering discovery in background"
                )
                _ = asyncio.create_task(boilerplate_discovery.discover_and_save(domain))
    except Exception as e:
        logger.error(f"Error during boilerplate removal for {domain}: {e}")

    return markdown


async def extract(
    markdown: str,
    domain: Optional[str] = None,
    current_time: Optional[datetime] = None,
    max_retries: int = 3,
) -> ExtractedProduct | None:
    """Extract product information as JSON string from markdown in two steps: validation & cleaning, then extraction.

    Pass CURRENT_TIME to the LLM so it can calculate auction dates.
    Args:
        markdown: Page content (Markdown or HTML) to analyze.
        domain: Optional shop domain for boilerplate removal.
        current_time: Optional UTC datetime as reference for relative times.
        max_retries: Number of retries for extraction on validation failure.

    Returns:
        A JSON string with extracted fields or '{}' on errors.
    """
    if not isinstance(markdown, str):
        return None

    # Apply Boilerplate Removal if domain is provided
    if domain:
        markdown = await _apply_boilerplate_removal(markdown, domain)

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


async def main(url: str):
    """Fetch a URL and run extraction on its markdown; used for manual testing."""
    markdown = await get_markdown(url)
    print(markdown)


if __name__ == "__main__":
    asyncio.run(
        main(
            "https://www.lot-tissimo.com/de-de/auction-catalogues/bieberle/catalogue-id-auktio37-10038/lot-e143db6d-3dc7-4e74-8dba-b35600ea7536"
        )
    )
