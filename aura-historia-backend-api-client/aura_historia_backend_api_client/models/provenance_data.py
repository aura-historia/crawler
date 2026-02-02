from enum import Enum


class ProvenanceData(str, Enum):
    CLAIMED = "CLAIMED"
    COMPLETE = "COMPLETE"
    NONE = "NONE"
    PARTIAL = "PARTIAL"
    UNKNOWN = "UNKNOWN"

    def __str__(self) -> str:
        return str(self.value)
