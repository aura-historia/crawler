import asyncio
from langdetect import detect, LangDetectException
from src.strategies.registry import EXTRACTORS
import aiohttp
from extruct import extract as extruct_extract


def is_valid_product(extracted) -> bool:
    """Accepts a single product or a list of products."""
    if not extracted:
        return False
    if isinstance(extracted, list):
        return any(is_valid_product(item) for item in extracted)
    # Single product
    if extracted.get("title", {}).get("text"):
        return True
    return False


def merge_products(base: dict, new: dict) -> dict:
    """
    Merge two product dictionaries.
    Keeps existing values in `base` and fills in missing ones from `new`.
    If both have nested dicts, merges them recursively.
    Also deduplicates images by URL.
    """
    merged = dict(base)
    for key, value in new.items():
        # Special case for shopsItemId: prefer value without URL if available
        if key == "shopsItemId":
            if (
                merged.get(key, "").startswith("http")
                and value
                and not value.startswith("http")
            ):
                merged[key] = value
            elif not merged.get(key) or merged.get(key) in ["", "UNKNOWN"]:
                merged[key] = value
        # Price merge
        elif key == "price" and isinstance(value, dict):
            merged_price = dict(merged.get("price", {}))

            new_amount = value.get("amount")
            if isinstance(new_amount, (int, float)) and new_amount > 0:
                merged_price["amount"] = new_amount
            elif "amount" not in merged_price:
                merged_price["amount"] = new_amount or 0

            new_currency = value.get("currency")
            if new_currency and new_currency not in ["", "UNKNOWN"]:
                merged_price["currency"] = new_currency
            elif "currency" not in merged_price:
                merged_price["currency"] = new_currency or "UNKNOWN"

            merged[key] = merged_price
        # Recursive merge for dicts
        elif isinstance(value, dict):
            merged[key] = merge_products(merged.get(key, {}), value)
        # Deduplicate images
        elif isinstance(value, list) and key == "images":
            existing_urls = set()
            new_images = []

            # Ensure merged[key] is a list, not a string like "UNKNOWN"
            existing_images = merged.get(key, [])
            if not isinstance(existing_images, list):
                existing_images = []
                merged[key] = []

            for img in existing_images:
                if isinstance(img, dict) and img.get("url"):
                    existing_urls.add(img["url"])
                elif isinstance(img, str):
                    existing_urls.add(img)
            for img in value:
                url = (
                    img["url"]
                    if isinstance(img, dict) and img.get("url")
                    else img
                    if isinstance(img, str)
                    else None
                )
                if url and url not in existing_urls:
                    new_images.append(img)
                    existing_urls.add(url)
            merged.setdefault(key, []).extend(new_images)
        # Replace "" or UNKNOWN with valid value
        elif value not in ["", "UNKNOWN", 0] and (
            merged.get(key) in ["", "UNKNOWN", 0] or not merged.get(key)
        ):
            merged[key] = value
    return merged


def are_products_equal(p1: dict, p2: dict) -> bool:
    """
    Check if two products are likely the same.
    Comparison is based on title text or product URL if available.
    """
    title1 = (p1.get("title") or {}).get("text", "").strip().lower()
    title2 = (p2.get("title") or {}).get("text", "").strip().lower()
    url1 = (p1.get("url") or "").strip().lower()
    url2 = (p2.get("url") or "").strip().lower()

    if url1 and url2:
        return url1 == url2
    if title1 and title2:
        return title1 == title2
    return False


def merge_product_lists(list1: list[dict], list2: list[dict]) -> list[dict]:
    """
    Merge two product lists.
    - Products that look identical are merged.
    - Unique ones are added.
    """
    merged = list(list1)

    for new_item in list2:
        found = False
        for i, existing_item in enumerate(merged):
            if are_products_equal(existing_item, new_item):
                merged[i] = merge_products(existing_item, new_item)
                found = True
                break
        if not found:
            merged.append(new_item)

    return merged


async def extract_standard(
    data: dict, url: str, preferred: list[str] | None = None
) -> dict | list[dict] | None:
    """
    Combines results from multiple extractors to create the most complete and deduplicated product data possible.
    """
    extractors = EXTRACTORS
    if preferred:
        extractors = sorted(
            EXTRACTORS,
            key=lambda e: preferred.index(e.name)
            if e.name in preferred
            else len(preferred),
        )

    combined_result = None

    for extractor in extractors:
        result = await extractor.extract(data, url)
        print(f"Extractor '{extractor.name}' result: {result}")
        if not is_valid_product(result):
            continue

        if combined_result is None:
            combined_result = result
        else:
            # Merge logic for different result types
            if isinstance(combined_result, list) and isinstance(result, list):
                combined_result = merge_product_lists(combined_result, result)
            elif isinstance(combined_result, dict) and isinstance(result, dict):
                combined_result = merge_products(combined_result, result)
            elif isinstance(combined_result, list) and isinstance(result, dict):
                combined_result = merge_product_lists(combined_result, [result])
            elif isinstance(combined_result, dict) and isinstance(result, list):
                combined_result = merge_product_lists([combined_result], result)

    # Fallback language detection
    if isinstance(combined_result, dict):
        for key in ["title", "description"]:
            text = combined_result.get(key, {}).get("text", "")
            if (
                combined_result.get(key, {}).get("language") == "UNKNOWN"
                and text
                and text != "UNKNOWN"
            ):
                try:
                    lang = detect(text)
                    combined_result[key]["language"] = lang
                except LangDetectException:
                    pass  # Ignore if language cannot be detected
    elif isinstance(combined_result, list):
        for product in combined_result:
            for key in ["title", "description"]:
                text = product.get(key, {}).get("text", "")
                if (
                    product.get(key, {}).get("language") == "UNKNOWN"
                    and text
                    and text != "UNKNOWN"
                ):
                    try:
                        lang = detect(text)
                        product[key]["language"] = lang
                    except LangDetectException:
                        pass  # Ignore if language cannot be detected

    return combined_result


async def single_url(url: str):
    """Fetch a product page and test extraction with all registered extractors."""
    async with aiohttp.ClientSession() as session:
        async with session.get(url) as resp:
            html = await resp.text()
            base_url = str(resp.url)

    # Run extruct to collect all structured data syntaxes
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
