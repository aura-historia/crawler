from __future__ import annotations

from collections.abc import Mapping
from typing import Any, TypeVar
from uuid import UUID

from attrs import define as _attrs_define
from attrs import field as _attrs_field

T = TypeVar("T", bound="ProductKeyData")


@_attrs_define
class ProductKeyData:
    """Identifier for a product using shop ID and shop's product ID

    Attributes:
        shop_id (UUID): Unique identifier of the shop Example: 550e8400-e29b-41d4-a716-446655440000.
        shops_product_id (str): Shop's unique identifier for the product. Can be any arbitrary string. Example:
            6ba7b810.
    """

    shop_id: UUID
    shops_product_id: str
    additional_properties: dict[str, Any] = _attrs_field(init=False, factory=dict)

    def to_dict(self) -> dict[str, Any]:
        shop_id = str(self.shop_id)

        shops_product_id = self.shops_product_id

        field_dict: dict[str, Any] = {}
        field_dict.update(self.additional_properties)
        field_dict.update(
            {
                "shopId": shop_id,
                "shopsProductId": shops_product_id,
            }
        )

        return field_dict

    @classmethod
    def from_dict(cls: type[T], src_dict: Mapping[str, Any]) -> T:
        d = dict(src_dict)
        shop_id = UUID(d.pop("shopId"))

        shops_product_id = d.pop("shopsProductId")

        product_key_data = cls(
            shop_id=shop_id,
            shops_product_id=shops_product_id,
        )

        product_key_data.additional_properties = d
        return product_key_data

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
