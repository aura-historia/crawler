from __future__ import annotations

from collections.abc import Mapping
from typing import TYPE_CHECKING, Any, TypeVar

from attrs import define as _attrs_define
from attrs import field as _attrs_field

if TYPE_CHECKING:
    from ..models.price_data import PriceData


T = TypeVar("T", bound="ProductEventPriceDiscoveredPayloadData")


@_attrs_define
class ProductEventPriceDiscoveredPayloadData:
    """Payload for price discovered events when a product's price is first detected

    Attributes:
        new_price (PriceData): Price information with currency
    """

    new_price: PriceData
    additional_properties: dict[str, Any] = _attrs_field(init=False, factory=dict)

    def to_dict(self) -> dict[str, Any]:
        new_price = self.new_price.to_dict()

        field_dict: dict[str, Any] = {}
        field_dict.update(self.additional_properties)
        field_dict.update(
            {
                "newPrice": new_price,
            }
        )

        return field_dict

    @classmethod
    def from_dict(cls: type[T], src_dict: Mapping[str, Any]) -> T:
        from ..models.price_data import PriceData

        d = dict(src_dict)
        new_price = PriceData.from_dict(d.pop("newPrice"))

        product_event_price_discovered_payload_data = cls(
            new_price=new_price,
        )

        product_event_price_discovered_payload_data.additional_properties = d
        return product_event_price_discovered_payload_data

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
