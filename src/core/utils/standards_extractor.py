import asyncio
from typing import Optional, Dict, Any, List
from langdetect import detect, LangDetectException
from src.strategies.registry import EXTRACTORS
import aiohttp
from extruct import extract as extruct_extract


def is_valid_product(product: Optional[Dict[str, Any]]) -> bool:
    """
    Check if a product dictionary contains valid data.

    A product is considered valid if it has a non-empty title text.

    Args:
        product: Product dictionary to validate

    Returns:
        True if product has valid title text, False otherwise
    """
    if not product or not isinstance(product, dict):
        return False
    return bool(product.get("title", {}).get("text"))


def merge_products(base: Dict[str, Any], new: Dict[str, Any]) -> Dict[str, Any]:
    """
    Merge two product dictionaries.

    Keeps existing values in base and fills in missing ones from new.
    Handles special merging logic for specific fields like price and images.

    Args:
        base: Base product dictionary to merge into
        new: New product dictionary with additional data

    Returns:
        Merged product dictionary with combined data from both inputs

    Note:
        - Prefers non-URL item IDs over URLs
        - Merges price amounts and currencies intelligently
        - Deduplicates images while preserving order
        - Fills in missing text fields and language codes
    """
    merged = dict(base)

    for key, value in new.items():
        if key == "shopsProductId":
            merged[key] = _merge_item_id(merged.get(key, ""), value)
        elif key == "price":
            merged[key] = _merge_price(merged.get(key, {}), value)
        elif key == "images":
            merged[key] = _merge_images(merged.get(key, []), value)
        elif key in ("title", "description"):
            merged[key] = _merge_text_field(merged.get(key, {}), value)
        elif _is_empty_value(merged.get(key)) and not _is_empty_value(value):
            merged[key] = value

    return merged


def _merge_item_id(existing: str, new: str) -> str:
    """
    Merge item IDs, preferring non-URL IDs.

    Args:
        existing: Current item ID
        new: New item ID to merge

    Returns:
        Preferred item ID (non-URL if available)
    """
    if existing.startswith("http") and new and not new.startswith("http"):
        return new
    return new if _is_empty_value(existing) else existing


def _merge_price(existing: Dict[str, Any], new: Dict[str, Any]) -> Dict[str, Any]:
    """
    Merge price dictionaries, preferring valid amounts and currencies.

    Args:
        existing: Current price dictionary
        new: New price dictionary to merge

    Returns:
        Merged price dictionary with the best available amount and currency
    """
    if not isinstance(new, dict):
        return existing

    merged = dict(existing)

    amount = new.get("amount")
    if isinstance(amount, (int, float)) and amount > 0:
        merged["amount"] = amount
    elif "amount" not in merged:
        merged["amount"] = amount or 0

    currency = new.get("currency")
    if currency and currency not in ("", "UNKNOWN"):
        merged["currency"] = currency
    elif "currency" not in merged:
        merged["currency"] = currency or "UNKNOWN"

    return merged


def _merge_images(existing: List[str], new: List[str]) -> List[str]:
    """
    Deduplicate and merge image lists.

    Args:
        existing: Current list of image URLs
        new: New list of image URLs to add

    Returns:
        Combined list with duplicates removed, preserving order
    """
    if not isinstance(new, list):
        new = [new] if new else []
    if not isinstance(existing, list):
        existing = [existing] if existing else []

    seen = set(existing)
    result = list(existing)

    for img in new:
        if img and img not in seen:
            result.append(img)
            seen.add(img)

    return result


def _merge_text_field(existing: Dict[str, Any], new: Dict[str, Any]) -> Dict[str, Any]:
    """
    Merge text fields (title/description), preferring valid text and languages.

    Args:
        existing: Current text field dictionary
        new: New text field dictionary to merge

    Returns:
        Merged text field with the best available text and language
    """
    if not isinstance(new, dict):
        return existing

    merged = dict(existing)

    text = new.get("text")
    if text and text != "UNKNOWN":
        if not merged.get("text") or merged.get("text") == "UNKNOWN":
            merged["text"] = text

    language = new.get("language")
    if language and language != "UNKNOWN":
        if not merged.get("language") or merged.get("language") == "UNKNOWN":
            merged["language"] = language

    return merged


def _is_empty_value(value: Any) -> bool:
    """
    Check if a value is considered empty.

    Args:
        value: Value to check

    Returns:
        True if value is empty (None, "", "UNKNOWN", 0, or empty dict), False otherwise
    """
    return value in ("", "UNKNOWN", 0, None) or (isinstance(value, dict) and not value)


def _detect_language(text: str) -> str:
    """
    Detect language of text, return UNKNOWN on failure.

    Args:
        text: Text string to analyze

    Returns:
        Two-letter language code (e.g., "en", "de") or "UNKNOWN"

    Note:
        Uses langdetect library for detection. Returns "UNKNOWN" if detection fails.
    """
    if not text or not isinstance(text, str) or text == "UNKNOWN":
        return "UNKNOWN"

    # Handle list-type text

    try:
        return detect(text)
    except (LangDetectException, Exception):
        return "UNKNOWN"


def _apply_language_detection(product: Dict[str, Any]) -> None:
    """
    Apply language detection to title and description if needed.

    Modifies the product dictionary in-place, updating language fields
    that are set to "UNKNOWN".

    Args:
        product: Product dictionary to update
    """
    for key in ("title", "description"):
        field = product.get(key, {})
        if isinstance(field, dict) and field.get("language") == "UNKNOWN":
            text = field.get("text", "")
            detected = _detect_language(text)
            if detected != "UNKNOWN":
                field["language"] = detected


async def extract_standard(
    data: Dict[str, Any], url: str, preferred: Optional[List[str]] = None
) -> Optional[Dict[str, Any]]:
    """
    Combine results from multiple extractors to create complete product data.

    Iterates through registered extractors (optionally in preferred order) and
    merges their results into a single comprehensive product dictionary.

    Args:
        data: Dictionary containing structured data from extruct
        url: The URL of the page being processed
        preferred: Optional list of extractor names in order of preference
                  (e.g., ["json-ld", "microdata", "rdfa", "opengraph"])

    Returns:
        Single product dictionary with merged data from all extractors,
        or None if no valid product found

    Note:
        - Extractors are tried in preferred order if specified
        - Results are merged intelligently, preserving the best available data
        - Language detection is applied as fallback for UNKNOWN languages
    """
    extractors = EXTRACTORS
    if preferred:
        extractors = sorted(
            EXTRACTORS,
            key=lambda e: preferred.index(e.name)
            if e.name in preferred
            else len(preferred),
        )

    combined: Optional[Dict[str, Any]] = None

    for extractor in extractors:
        result = await extractor.extract(data, url)
        if not is_valid_product(result):
            continue

        if combined is None:
            combined = result
        else:
            combined = merge_products(combined, result)

    if combined:
        _apply_language_detection(combined)

    return combined


async def single_url(url: str):
    """
    Fetch a product page and test extraction with all registered extractors.

    This function is primarily for testing and debugging purposes.

    Args:
        url: URL of the product page to extract

    Note:
        Prints the final combined product result to console.
    """
    async with aiohttp.ClientSession() as session:
        async with session.get(url) as resp:
            html = await resp.text()
            base_url = str(resp.url)

    data = extruct_extract(
        html,
        base_url=base_url,
        syntaxes=["json-ld", "microdata", "rdfa", "opengraph", "microformat"],
    )

    result = await extract_standard(
        data, url, preferred=["json-ld", "microdata", "rdfa", "opengraph"]
    )

    print("\n✅ Final combined product result:")
    print(result)


if __name__ == "__main__":
    test_url = ""
    asyncio.run(single_url(test_url))
