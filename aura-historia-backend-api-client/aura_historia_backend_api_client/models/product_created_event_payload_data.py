from __future__ import annotations

from collections.abc import Mapping
from typing import TYPE_CHECKING, Any, TypeVar

from attrs import define as _attrs_define
from attrs import field as _attrs_field

from ..models.product_state_data import ProductStateData
from ..types import UNSET, Unset

if TYPE_CHECKING:
    from ..models.price_data import PriceData


T = TypeVar("T", bound="ProductCreatedEventPayloadData")


@_attrs_define
class ProductCreatedEventPayloadData:
    """Minimal information about an product when it was created

    Attributes:
        state (ProductStateData): Current state of the product:
            - LISTED: Product has been listed
            - AVAILABLE: Product is available for purchase
            - RESERVED: Product is reserved by a buyer
            - SOLD: Product has been sold
            - REMOVED: Product has been removed and can no longer be tracked
            - UNKNOWN: Product has an unknown state
             Example: AVAILABLE.
        price (PriceData | Unset): Price information with currency
        price_estimate_min (PriceData | Unset): Price information with currency
        price_estimate_max (PriceData | Unset): Price information with currency
    """

    state: ProductStateData
    price: PriceData | Unset = UNSET
    price_estimate_min: PriceData | Unset = UNSET
    price_estimate_max: PriceData | Unset = UNSET
    additional_properties: dict[str, Any] = _attrs_field(init=False, factory=dict)

    def to_dict(self) -> dict[str, Any]:
        state = self.state.value

        price: dict[str, Any] | Unset = UNSET
        if not isinstance(self.price, Unset):
            price = self.price.to_dict()

        price_estimate_min: dict[str, Any] | Unset = UNSET
        if not isinstance(self.price_estimate_min, Unset):
            price_estimate_min = self.price_estimate_min.to_dict()

        price_estimate_max: dict[str, Any] | Unset = UNSET
        if not isinstance(self.price_estimate_max, Unset):
            price_estimate_max = self.price_estimate_max.to_dict()

        field_dict: dict[str, Any] = {}
        field_dict.update(self.additional_properties)
        field_dict.update(
            {
                "state": state,
            }
        )
        if price is not UNSET:
            field_dict["price"] = price
        if price_estimate_min is not UNSET:
            field_dict["priceEstimateMin"] = price_estimate_min
        if price_estimate_max is not UNSET:
            field_dict["priceEstimateMax"] = price_estimate_max

        return field_dict

    @classmethod
    def from_dict(cls: type[T], src_dict: Mapping[str, Any]) -> T:
        from ..models.price_data import PriceData

        d = dict(src_dict)
        state = ProductStateData(d.pop("state"))

        _price = d.pop("price", UNSET)
        price: PriceData | Unset
        if isinstance(_price, Unset):
            price = UNSET
        else:
            price = PriceData.from_dict(_price)

        _price_estimate_min = d.pop("priceEstimateMin", UNSET)
        price_estimate_min: PriceData | Unset
        if isinstance(_price_estimate_min, Unset):
            price_estimate_min = UNSET
        else:
            price_estimate_min = PriceData.from_dict(_price_estimate_min)

        _price_estimate_max = d.pop("priceEstimateMax", UNSET)
        price_estimate_max: PriceData | Unset
        if isinstance(_price_estimate_max, Unset):
            price_estimate_max = UNSET
        else:
            price_estimate_max = PriceData.from_dict(_price_estimate_max)

        product_created_event_payload_data = cls(
            state=state,
            price=price,
            price_estimate_min=price_estimate_min,
            price_estimate_max=price_estimate_max,
        )

        product_created_event_payload_data.additional_properties = d
        return product_created_event_payload_data

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
