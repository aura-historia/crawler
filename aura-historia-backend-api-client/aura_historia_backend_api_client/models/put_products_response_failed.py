from __future__ import annotations

from collections.abc import Mapping
from typing import Any, TypeVar

from attrs import define as _attrs_define
from attrs import field as _attrs_field

from ..models.put_product_error import PutProductError

T = TypeVar("T", bound="PutProductsResponseFailed")


@_attrs_define
class PutProductsResponseFailed:
    """Map of product URLs to error codes for products that failed processing.
    The key is the product URL, and the value is the error code explaining why it failed.

        Example:
            {'https://unknown-shop.com/item': 'SHOP_NOT_FOUND', 'https://tech-store.com/expensive-item':
                'MONETARY_AMOUNT_OVERFLOW', 'https://localhost:8080/item': 'NO_DOMAIN'}

    """

    additional_properties: dict[str, PutProductError] = _attrs_field(init=False, factory=dict)

    def to_dict(self) -> dict[str, Any]:
        field_dict: dict[str, Any] = {}
        for prop_name, prop in self.additional_properties.items():
            field_dict[prop_name] = prop.value

        return field_dict

    @classmethod
    def from_dict(cls: type[T], src_dict: Mapping[str, Any]) -> T:
        d = dict(src_dict)
        put_products_response_failed = cls()

        additional_properties = {}
        for prop_name, prop_dict in d.items():
            additional_property = PutProductError(prop_dict)

            additional_properties[prop_name] = additional_property

        put_products_response_failed.additional_properties = additional_properties
        return put_products_response_failed

    @property
    def additional_keys(self) -> list[str]:
        return list(self.additional_properties.keys())

    def __getitem__(self, key: str) -> PutProductError:
        return self.additional_properties[key]

    def __setitem__(self, key: str, value: PutProductError) -> None:
        self.additional_properties[key] = value

    def __delitem__(self, key: str) -> None:
        del self.additional_properties[key]

    def __contains__(self, key: str) -> bool:
        return key in self.additional_properties
