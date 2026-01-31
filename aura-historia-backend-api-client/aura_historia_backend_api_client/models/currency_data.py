from enum import Enum


class CurrencyData(str, Enum):
    AUD = "AUD"
    CAD = "CAD"
    EUR = "EUR"
    GBP = "GBP"
    NZD = "NZD"
    USD = "USD"

    def __str__(self) -> str:
        return str(self.value)
