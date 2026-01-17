from typing import List, Optional, Literal
from pydantic import BaseModel, Field

# Define the allowed states using Literal
AllowedStates = Literal["AVAILABLE", "SOLD", "LISTED", "RESERVED", "REMOVED", "UNKNOWN"]


class ExtractedProduct(BaseModel):
    shop_item_id: Optional[str] = None
    title: str

    priceAmount: Optional[int] = None
    priceCurrency: Optional[str] = None

    priceEstimateMinAmount: Optional[int] = None
    priceEstimateMinCurrency: Optional[str] = None
    priceEstimateMaxAmount: Optional[int] = None
    priceEstimateMaxCurrency: Optional[str] = None

    description: str
    state: AllowedStates = "UNKNOWN"
    images: Optional[List[str]] = Field(default_factory=list)
    language: str

    auctionStart: Optional[str] = None
    auctionEnd: Optional[str] = None

    model_config = {"extra": "forbid"}
