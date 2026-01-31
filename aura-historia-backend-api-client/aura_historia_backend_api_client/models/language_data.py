from enum import Enum


class LanguageData(str, Enum):
    DE = "de"
    EN = "en"
    ES = "es"
    FR = "fr"

    def __str__(self) -> str:
        return str(self.value)
