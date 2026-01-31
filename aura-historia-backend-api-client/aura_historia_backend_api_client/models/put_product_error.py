from enum import Enum


class PutProductError(str, Enum):
    MONETARY_AMOUNT_OVERFLOW = "MONETARY_AMOUNT_OVERFLOW"
    NO_DOMAIN = "NO_DOMAIN"
    PRODUCT_ENRICHMENT_FAILED = "PRODUCT_ENRICHMENT_FAILED"
    SHOP_NOT_FOUND = "SHOP_NOT_FOUND"

    def __str__(self) -> str:
        return str(self.value)
