from enum import Enum


class ProductStateData(str, Enum):
    AVAILABLE = "AVAILABLE"
    LISTED = "LISTED"
    REMOVED = "REMOVED"
    RESERVED = "RESERVED"
    SOLD = "SOLD"
    UNKNOWN = "UNKNOWN"

    def __str__(self) -> str:
        return str(self.value)
