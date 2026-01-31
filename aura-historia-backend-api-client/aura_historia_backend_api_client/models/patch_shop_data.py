from __future__ import annotations

from collections.abc import Mapping
from typing import Any, TypeVar, cast

from attrs import define as _attrs_define
from attrs import field as _attrs_field

from ..models.shop_type_data import ShopTypeData
from ..types import UNSET, Unset

T = TypeVar("T", bound="PatchShopData")


@_attrs_define
class PatchShopData:
    """Partial update data for a shop.
    All fields are optional - only provided fields will be updated.
    If the request body is empty or all fields are null, the shop is returned unchanged.

    **Note**: The shop name cannot be updated after creation as it determines the shop slug identifier.

        Attributes:
            shop_type (None | ShopTypeData | Unset): New shop type classification
            domains (list[str] | None | Unset): Complete new set of domains for the shop.
                Can be provided as full URLs (will be normalized) or as domain strings.
                When updating domains, the complete new set must be provided (not a diff).
                Domains are normalized to lowercase without scheme, www prefix, or path/query/fragment.
                Minimum 1 domain, maximum 100 domains.
                 Example: ['tech-store-premium.com', 'tech-store-premium.eu'].
            image (None | str | Unset): New URL to the shop's logo or image Example: https://tech-store-premium.com/new-
                logo.svg.
    """

    shop_type: None | ShopTypeData | Unset = UNSET
    domains: list[str] | None | Unset = UNSET
    image: None | str | Unset = UNSET
    additional_properties: dict[str, Any] = _attrs_field(init=False, factory=dict)

    def to_dict(self) -> dict[str, Any]:
        shop_type: None | str | Unset
        if isinstance(self.shop_type, Unset):
            shop_type = UNSET
        elif isinstance(self.shop_type, ShopTypeData):
            shop_type = self.shop_type.value
        else:
            shop_type = self.shop_type

        domains: list[str] | None | Unset
        if isinstance(self.domains, Unset):
            domains = UNSET
        elif isinstance(self.domains, list):
            domains = self.domains

        else:
            domains = self.domains

        image: None | str | Unset
        if isinstance(self.image, Unset):
            image = UNSET
        else:
            image = self.image

        field_dict: dict[str, Any] = {}
        field_dict.update(self.additional_properties)
        field_dict.update({})
        if shop_type is not UNSET:
            field_dict["shopType"] = shop_type
        if domains is not UNSET:
            field_dict["domains"] = domains
        if image is not UNSET:
            field_dict["image"] = image

        return field_dict

    @classmethod
    def from_dict(cls: type[T], src_dict: Mapping[str, Any]) -> T:
        d = dict(src_dict)

        def _parse_shop_type(data: object) -> None | ShopTypeData | Unset:
            if data is None:
                return data
            if isinstance(data, Unset):
                return data
            try:
                if not isinstance(data, str):
                    raise TypeError()
                shop_type_type_1 = ShopTypeData(data)

                return shop_type_type_1
            except (TypeError, ValueError, AttributeError, KeyError):
                pass
            return cast(None | ShopTypeData | Unset, data)

        shop_type = _parse_shop_type(d.pop("shopType", UNSET))

        def _parse_domains(data: object) -> list[str] | None | Unset:
            if data is None:
                return data
            if isinstance(data, Unset):
                return data
            try:
                if not isinstance(data, list):
                    raise TypeError()
                domains_type_0 = cast(list[str], data)

                return domains_type_0
            except (TypeError, ValueError, AttributeError, KeyError):
                pass
            return cast(list[str] | None | Unset, data)

        domains = _parse_domains(d.pop("domains", UNSET))

        def _parse_image(data: object) -> None | str | Unset:
            if data is None:
                return data
            if isinstance(data, Unset):
                return data
            return cast(None | str | Unset, data)

        image = _parse_image(d.pop("image", UNSET))

        patch_shop_data = cls(
            shop_type=shop_type,
            domains=domains,
            image=image,
        )

        patch_shop_data.additional_properties = d
        return patch_shop_data

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
