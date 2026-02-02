from enum import Enum


class ConditionData(str, Enum):
    EXCELLENT = "EXCELLENT"
    FAIR = "FAIR"
    GOOD = "GOOD"
    GREAT = "GREAT"
    POOR = "POOR"
    UNKNOWN = "UNKNOWN"

    def __str__(self) -> str:
        return str(self.value)
