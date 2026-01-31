from enum import Enum


class ProhibitedContentData(str, Enum):
    NAZI_GERMANY = "NAZI_GERMANY"
    NONE = "NONE"
    UNKNOWN = "UNKNOWN"

    def __str__(self) -> str:
        return str(self.value)
