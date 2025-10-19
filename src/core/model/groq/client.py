from groq import Groq
from pydantic import BaseModel
from typing import Any, Dict, cast

client = Groq()


class UrlClassifier(BaseModel):
    confidence: int


def evaluate_url(url: str) -> Dict[str, Any]:
    """Evaluate a URL and return a validated dict with a single key 'confidence'."""
    prompt_user = (
        "I will give you a possible link to an e-commerce product in the antiques domain. "
        "You have to decide and give a confidence score whether this link points to an e-commerce product detail page. "
        "Do not search the web. Just analyze the link provided. Respond with a JSON object with exactly one field "
        '"confidence" (1-100) where 100 means the link points to an e-commerce product detail page. '
        '"For example: {"confidence": 95}\n\n"'
        f"Link: {url}"
    )

    messages = cast(
        Any,
        [
            {"role": "system", "content": "You are a helpful classifier."},
            {"role": "user", "content": prompt_user},
        ],
    )

    response_fmt = cast(
        Any,
        {
            "type": "json_schema",
            "json_schema": {
                "name": "confidence",
                "schema": UrlClassifier.model_json_schema(),
            },
        },
    )

    response = client.chat.completions.create(
        model="moonshotai/kimi-k2-instruct-0905",
        messages=messages,
        response_format=response_fmt,
    )

    review = UrlClassifier.model_validate(response)
    return review.model_dump()
