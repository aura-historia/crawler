from enum import Enum


class SortProductFieldData(str, Enum):
    CREATED = "created"
    ORIGINYEAR = "originYear"
    PRICE = "price"
    SCORE = "score"
    UPDATED = "updated"

    def __str__(self) -> str:
        return str(self.value)
