from enum import Enum


class AuthenticityData(str, Enum):
    LATER_COPY = "LATER_COPY"
    ORIGINAL = "ORIGINAL"
    QUESTIONABLE = "QUESTIONABLE"
    REPRODUCTION = "REPRODUCTION"
    UNKNOWN = "UNKNOWN"

    def __str__(self) -> str:
        return str(self.value)
