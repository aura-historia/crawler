from __future__ import annotations

from collections.abc import Mapping
from typing import Any, TypeVar

from attrs import define as _attrs_define
from attrs import field as _attrs_field

from ..models.product_state_data import ProductStateData

T = TypeVar("T", bound="ProductEventStateChangedPayloadData")


@_attrs_define
class ProductEventStateChangedPayloadData:
    """Payload for state change events, containing both old and new state

    Attributes:
        old_state (ProductStateData): Current state of the product:
            - LISTED: Product has been listed
            - AVAILABLE: Product is available for purchase
            - RESERVED: Product is reserved by a buyer
            - SOLD: Product has been sold
            - REMOVED: Product has been removed and can no longer be tracked
            - UNKNOWN: Product has an unknown state
             Example: AVAILABLE.
        new_state (ProductStateData): Current state of the product:
            - LISTED: Product has been listed
            - AVAILABLE: Product is available for purchase
            - RESERVED: Product is reserved by a buyer
            - SOLD: Product has been sold
            - REMOVED: Product has been removed and can no longer be tracked
            - UNKNOWN: Product has an unknown state
             Example: AVAILABLE.
    """

    old_state: ProductStateData
    new_state: ProductStateData
    additional_properties: dict[str, Any] = _attrs_field(init=False, factory=dict)

    def to_dict(self) -> dict[str, Any]:
        old_state = self.old_state.value

        new_state = self.new_state.value

        field_dict: dict[str, Any] = {}
        field_dict.update(self.additional_properties)
        field_dict.update(
            {
                "oldState": old_state,
                "newState": new_state,
            }
        )

        return field_dict

    @classmethod
    def from_dict(cls: type[T], src_dict: Mapping[str, Any]) -> T:
        d = dict(src_dict)
        old_state = ProductStateData(d.pop("oldState"))

        new_state = ProductStateData(d.pop("newState"))

        product_event_state_changed_payload_data = cls(
            old_state=old_state,
            new_state=new_state,
        )

        product_event_state_changed_payload_data.additional_properties = d
        return product_event_state_changed_payload_data

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
