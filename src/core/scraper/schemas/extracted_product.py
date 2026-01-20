from typing import List, Optional, Literal
from pydantic import BaseModel, Field

# Define the allowed states
AllowedStates = Literal["AVAILABLE", "SOLD", "LISTED", "RESERVED", "REMOVED", "UNKNOWN"]


class LocalizedText(BaseModel):
    text: str = Field(
        ..., description="The full text content. Do NOT summarize or translate."
    )
    language: str = Field(..., description="ISO 639-1 code (e.g., 'en', 'de', 'fr').")


class MonetaryValue(BaseModel):
    amount: int = Field(
        ..., description="The amount converted to integer CENTS (e.g., $10.50 -> 1050)."
    )
    currency: str = Field(
        ..., description="ISO 4217 currency code (e.g., 'USD', 'EUR', 'GBP')."
    )


class ExtractedProduct(BaseModel):
    is_product: bool = Field(
        ...,
        description="True if the page describes a SINGLE specific item (Lot/Product). False if it is a list, category grid, or generic page.",
    )
    shopsProductId: Optional[str] = Field(
        None, description="The Shop or Lot ID exactly as shown (e.g., '970')."
    )
    title: Optional[LocalizedText] = Field(
        None, description="The full title of the item."
    )
    description: Optional[LocalizedText] = Field(
        None,
        description="The longest technical description available. Use newline characters (\\n) between sections.",
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
        "UNKNOWN",
        description="EXACTLY ONE OF: AVAILABLE | SOLD | LISTED | RESERVED | REMOVED | UNKNOWN",
    )
    images: List[str] = Field(
        default_factory=list,
        description="List of absolute URLs to images ending in .jpg, .jpeg, or .png.",
    )
    auctionStart: Optional[str] = Field(
        None,
        description="UTC ISO8601 timestamp (e.g., 2024-05-24T10:00:00Z). Calculate from relative text using CURRENT_TIME if needed.",
    )
    auctionEnd: Optional[str] = Field(
        None,
        description="UTC ISO8601 timestamp. Calculate from phrases like 'Closing: X days' using CURRENT_TIME + X days.",
    )
    url: Optional[str] = Field(None, description="The URL of the item page.")

    model_config = {"extra": "forbid"}
