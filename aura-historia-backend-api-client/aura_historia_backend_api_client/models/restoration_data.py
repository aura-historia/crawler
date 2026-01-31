from enum import Enum


class RestorationData(str, Enum):
    MAJOR = "MAJOR"
    MINOR = "MINOR"
    NONE = "NONE"
    UNKNOWN = "UNKNOWN"

    def __str__(self) -> str:
        return str(self.value)
