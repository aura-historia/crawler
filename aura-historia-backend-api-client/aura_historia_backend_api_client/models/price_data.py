from __future__ import annotations

from collections.abc import Mapping
from typing import Any, TypeVar

from attrs import define as _attrs_define
from attrs import field as _attrs_field

from ..models.currency_data import CurrencyData

T = TypeVar("T", bound="PriceData")


@_attrs_define
class PriceData:
    """Price information with currency

    Attributes:
        currency (CurrencyData): Supported currencies (ISO 4217 codes):
            - EUR: Euro
            - GBP: British Pound
            - USD: US Dollar
            - AUD: Australian Dollar
            - CAD: Canadian Dollar
            - NZD: New Zealand Dollar
             Example: EUR.
        amount (int): Price amount in minor currency units (e.g., cents for EUR/USD) Example: 2999.
    """

    currency: CurrencyData
    amount: int
    additional_properties: dict[str, Any] = _attrs_field(init=False, factory=dict)

    def to_dict(self) -> dict[str, Any]:
        currency = self.currency.value

        amount = self.amount

        field_dict: dict[str, Any] = {}
        field_dict.update(self.additional_properties)
        field_dict.update(
            {
                "currency": currency,
                "amount": amount,
            }
        )

        return field_dict

    @classmethod
    def from_dict(cls: type[T], src_dict: Mapping[str, Any]) -> T:
        d = dict(src_dict)
        currency = CurrencyData(d.pop("currency"))

        amount = d.pop("amount")

        price_data = cls(
            currency=currency,
            amount=amount,
        )

        price_data.additional_properties = d
        return price_data

    @property
    def additional_keys(self) -> list[str]:
        return list(self.additional_properties.keys())

    def __getitem__(self, key: str) -> Any:
        return self.additional_properties[key]

    def __setitem__(self, key: str, value: Any) -> None:
        self.additional_properties[key] = value

    def __delitem__(self, key: str) -> None:
        del self.additional_properties[key]

    def __contains__(self, key: str) -> bool:
        return key in self.additional_properties
