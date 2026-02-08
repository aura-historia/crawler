from typing import List, Optional, Literal, Annotated
from pydantic import BaseModel, Field, HttpUrl

# Define the allowed states
AllowedStates = Literal["AVAILABLE", "SOLD", "LISTED", "RESERVED", "REMOVED", "UNKNOWN"]

# Backend-supported languages (ISO 639-1)
AllowedLanguages = Literal["de", "en", "fr", "es"]

# Backend-supported currencies (ISO 4217)
AllowedCurrencies = Literal["EUR", "GBP", "USD", "AUD", "CAD", "NZD"]


class LocalizedText(BaseModel):
    text: str = Field(..., description="The full text content.")
    language: AllowedLanguages = Field(
        ...,
        description="Return the ISO 639-1 language code of the content. Use only valid two-letter codes like 'en', 'de', 'fr'.",
    )


class MonetaryValue(BaseModel):
    amount: Annotated[int, Field(ge=0)] = Field(
        ...,
        description="The amount converted to integer CENTS (e.g., $10.50 -> 1050).",
    )
    currency: AllowedCurrencies = Field(
        ...,
        description="ISO 4217 currency code (e.g., 'USD', 'EUR', 'GBP').",
    )


class ExtractedProduct(BaseModel):
    # Flag used by tests and extraction flow to mark whether LLM determined product
    is_product: bool = Field(
        False,
        description="True if the extracted object is a product. You can detect a product by seeing if it has a detailed product description. Lists, categories, or vague descriptions likely indicate it's NOT a product. Be conservative and only set to True if you're confident this is a specific item for sale.",
    )

    shopsProductId: str = Field(
        ...,
        description="The product's identifier EXACTLY as shown on the page (e.g. Art.Nr., Lot-Nr., SKU). Do NOT invent.",
    )
    title: LocalizedText = Field(
        ..., description="The full title of the item. (NO MARKDOWN only text)"
    )
    description: Optional[LocalizedText] = Field(
        None,
        description="Only include details about the specific item (NO other information). FORMAT it as Markdown and highlight the main points with **crucial**",
    )
    price: Optional[MonetaryValue] = Field(
        None,
        description="The resolved price. Use Current Bid, if missing use Starting Bid, if missing use List Price.",
    )
    priceEstimateMin: Optional[MonetaryValue] = Field(
        None, description="Lower estimate if available."
    )
    priceEstimateMax: Optional[MonetaryValue] = Field(
        None, description="Upper estimate if available."
    )

    state: AllowedStates = Field(
        ...,
        description="EXACTLY ONE OF: AVAILABLE | SOLD | LISTED | RESERVED | REMOVED | UNKNOWN",
    )
    images: List[HttpUrl] = Field(
        default_factory=list,
        description="Return only real absolute image URLs ending in .jpg, .jpeg, or .png. Do not invent URLs.",
    )
    auctionStart: Optional[str] = Field(
        None,
        description="UTC ISO8601 timestamp (e.g., 2024-05-24T10:00:00Z). Calculate from relative text using CURRENT_TIME if needed.",
    )
    auctionEnd: Optional[str] = Field(
        None,
        description="UTC ISO8601 timestamp. Calculate from phrases like 'Closing: X days' using CURRENT_TIME + X days.",
    )

    model_config = {"extra": "forbid"}
