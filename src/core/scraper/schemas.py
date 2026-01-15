from typing import List, Optional, Literal
from pydantic import BaseModel, Field, ValidationError
import time

# Define the allowed states using Literal
AllowedStates = Literal["AVAILABLE", "SOLD", "LISTED", "RESERVED", "REMOVED", "UNKNOWN"]


class ExtractedProduct(BaseModel):
    """
    Pydantic model for validating the extracted product data from the LLM.
    """

    shop_item_id: Optional[str] = None
    title: str
    current_price: Optional[int] = None
    currency: Optional[str] = None
    description: str
    state: AllowedStates = "UNKNOWN"
    images: Optional[List[str]] = Field(default_factory=list)
    language: str
    auctionStart: Optional[str] = None
    auctionEnd: Optional[str] = None


def validate_and_retry(
    data: dict, max_retries: int = 3, delay: int = 2
) -> Optional[ExtractedProduct]:
    """
    Validates the structured data and retries if validation fails.

    Args:
        data (dict): The data to validate.
        max_retries (int): Maximum number of retries.
        delay (int): Delay between retries in seconds.

    Returns:
        Optional[ExtractedProduct]: The validated data, or None if validation fails.

    Raises:
        ValidationError: If validation fails after all retries.
    """
    last_exception = None
    for attempt in range(max_retries):
        try:
            # Validate the data using the ExtractedProduct model
            return ExtractedProduct(**data)
        except ValidationError as e:
            last_exception = e
            print(f"Validation failed on attempt {attempt + 1}/{max_retries}: {e}")
            if attempt < max_retries - 1:
                time.sleep(delay)  # Wait before retrying
            else:
                print("All retries failed. Raising the last exception.")
                raise last_exception  # Raise the last exception explicitly
    return None
