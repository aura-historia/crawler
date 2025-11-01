import json
import os
from typing import Optional
from openai import OpenAI
from dotenv import load_dotenv

load_dotenv()

_client: Optional[OpenAI] = None


def get_client() -> OpenAI:
    """Get or create the DeepSeek client instance.

    DeepSeek API is OpenAI-compatible, so we use the OpenAI client
    with a custom base URL.

    Returns:
        OpenAI client instance configured for DeepSeek

    Raises:
        ValueError: If DEEPSEEK_API_KEY environment variable is not set
    """
    global _client
    if _client is None:
        api_key = os.environ.get("DEEPSEEK_API_KEY")
        if not api_key:
            raise ValueError("DEEPSEEK_API_KEY environment variable is not set")

        _client = OpenAI(api_key=api_key, base_url="https://api.deepseek.com")
    return _client


def analyze_text(prompt: str, text: str, model: str = "deepseek-chat") -> str:
    """
    Analyze text using DeepSeek.

    Args:
        prompt: The system prompt or instruction
        text: The text to analyze
        model: DeepSeek model to use (default: deepseek-chat)

    Returns:
        The model's response as a string

    Raises:
        Exception: If the API call fails
    """
    client = get_client()

    response = client.chat.completions.create(
        model=model,
        messages=[
            {"role": "system", "content": prompt},
            {"role": "user", "content": text},
        ],
        response_format={"type": "json_object"},
        temperature=0,
    )

    return response.choices[0].message.content


def analyze_antique_shops_batch(
    pages: list[dict], model: str = "deepseek-chat"
) -> list[dict]:
    """
    Batch-analyze multiple webpages to detect antique shops efficiently.

    Args:
        pages: List of dicts like [{"url": str, "content": str}]
        model: DeepSeek model name (default: deepseek-chat)

    Returns:
        List of dicts in the same structure as analyze_antique_shop()
        [
            {
                "url": str,
                "is_antique_shop": bool,
                "confidence": float,
            },
            ...
        ]
    """
    if not pages:
        return []

    # Truncate overly long content to save tokens
    for p in pages:
        if len(p["content"]) > 4000:
            p["content"] = p["content"][:4000] + "..."

    # Build combined prompt
    prompt = """You are an expert at analyzing multiple webpages to determine which are antique online shops.
        An antique online shop should:
        1. Sell antiques, collectibles, militaria, or vintage items.
        2. Not just be informational or historical content.
        
        For EACH of the following webpages, analyze and return a JSON array with one object per URL:
        [
          {
            "url": "string",
            "is_antique_shop": true or false,
            "confidence": 0.0-1.0,
          }
        ]
        
        Be concise but accurate. Respond ONLY with valid JSON.
        """

    # Combine all page contents into a single batch text
    combined_text = ""
    for i, page in enumerate(pages, 1):
        combined_text += (
            f"\n--- PAGE {i} ---\nURL: {page['url']}\nCONTENT:\n{page['content']}\n"
        )

    response = analyze_text(prompt, combined_text, model=model)
    print(response)

    return json.loads(response)


# For backwards compatibility
class _ClientProxy:
    """Proxy that provides lazy access to the DeepSeek client."""

    def __getattr__(self, name):
        return getattr(get_client(), name)


client = _ClientProxy()
