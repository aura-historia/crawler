from enum import Enum


class ShopTypeData(str, Enum):
    AUCTION_HOUSE = "AUCTION_HOUSE"
    AUCTION_PLATFORM = "AUCTION_PLATFORM"
    COMMERCIAL_DEALER = "COMMERCIAL_DEALER"
    MARKETPLACE = "MARKETPLACE"

    def __str__(self) -> str:
        return str(self.value)
