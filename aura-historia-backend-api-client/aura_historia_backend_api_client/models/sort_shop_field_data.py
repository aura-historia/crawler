from enum import Enum


class SortShopFieldData(str, Enum):
    CREATED = "created"
    NAME = "name"
    SCORE = "score"
    UPDATED = "updated"

    def __str__(self) -> str:
        return str(self.value)
