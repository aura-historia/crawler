from __future__ import annotations

from collections.abc import Mapping
from typing import Any, TypeVar, cast

from attrs import define as _attrs_define
from attrs import field as _attrs_field

from ..models.shop_type_data import ShopTypeData
from ..types import UNSET, Unset

T = TypeVar("T", bound="PostShopData")


@_attrs_define
class PostShopData:
    """Data required to create a new shop

    Attributes:
        name (str): Display name of the shop Example: Tech Store Premium.
        shop_type (ShopTypeData): Type of vendor or shop:
            - AUCTION_HOUSE: Auction house selling items through auctions
            - AUCTION_PLATFORM: Auction platform hosting auctions for auction-houses
            - COMMERCIAL_DEALER: Commercial dealer or shop selling items directly
            - MARKETPLACE: Marketplace platform connecting buyers and sellers
             Example: COMMERCIAL_DEALER.
        domains (list[str]): All domains associated with the shop.
            Can be provided as full URLs (will be normalized) or as domain strings.
            Domains are normalized to lowercase without scheme, www prefix, or path/query/fragment.
            At least one domain is required, maximum 100 domains allowed.
             Example: ['tech-store-premium.com', 'tech-store-premium.de'].
        image (None | str | Unset): Optional URL to the shop's logo or image Example: https://tech-store-
            premium.com/logo.svg.
    """

    name: str
    shop_type: ShopTypeData
    domains: list[str]
    image: None | str | Unset = UNSET
    additional_properties: dict[str, Any] = _attrs_field(init=False, factory=dict)

    def to_dict(self) -> dict[str, Any]:
        name = self.name

        shop_type = self.shop_type.value

        domains = self.domains

        image: None | str | Unset
        if isinstance(self.image, Unset):
            image = UNSET
        else:
            image = self.image

        field_dict: dict[str, Any] = {}
        field_dict.update(self.additional_properties)
        field_dict.update(
            {
                "name": name,
                "shopType": shop_type,
                "domains": domains,
            }
        )
        if image is not UNSET:
            field_dict["image"] = image

        return field_dict

    @classmethod
    def from_dict(cls: type[T], src_dict: Mapping[str, Any]) -> T:
        d = dict(src_dict)
        name = d.pop("name")

        shop_type = ShopTypeData(d.pop("shopType"))

        domains = cast(list[str], d.pop("domains"))

        def _parse_image(data: object) -> None | str | Unset:
            if data is None:
                return data
            if isinstance(data, Unset):
                return data
            return cast(None | str | Unset, data)

        image = _parse_image(d.pop("image", UNSET))

        post_shop_data = cls(
            name=name,
            shop_type=shop_type,
            domains=domains,
            image=image,
        )

        post_shop_data.additional_properties = d
        return post_shop_data

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
