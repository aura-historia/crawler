from __future__ import annotations

import datetime
from collections.abc import Mapping
from typing import Any, TypeVar, cast
from uuid import UUID

from attrs import define as _attrs_define
from attrs import field as _attrs_field
from dateutil.parser import isoparse

from ..models.shop_type_data import ShopTypeData
from ..types import UNSET, Unset

T = TypeVar("T", bound="GetShopData")


@_attrs_define
class GetShopData:
    """Complete shop information including metadata

    Attributes:
        shop_id (UUID): Unique identifier of the shop Example: 550e8400-e29b-41d4-a716-446655440000.
        shop_slug_id (str): Human-readable slug identifier of the shop (kebab-case, derived from shop name).
            Example: "tech-store-premium" or "christies"
             Example: tech-store-premium.
        name (str): Display name of the shop Example: Tech Store Premium.
        shop_type (ShopTypeData): Type of vendor or shop:
            - AUCTION_HOUSE: Auction house selling items through auctions
            - AUCTION_PLATFORM: Auction platform hosting auctions for auction-houses
            - COMMERCIAL_DEALER: Commercial dealer or shop selling items directly
            - MARKETPLACE: Marketplace platform connecting buyers and sellers
             Example: COMMERCIAL_DEALER.
        domains (list[str]): All known domains associated with the shop.
            Domains are normalized (lowercase, no scheme, no www prefix, no path/query/fragment).
             Example: ['tech-store-premium.com', 'tech-store-premium.de', 'apple.tech-store-premium.com'].
        created (datetime.datetime): When the shop was first created (RFC3339 format) Example: 2024-01-01T10:00:00Z.
        updated (datetime.datetime): When the shop was last updated (RFC3339 format) Example: 2024-01-01T12:00:00Z.
        image (None | str | Unset): Optional URL to the shop's logo or image Example: https://tech-store-
            premium.com/logo.svg.
    """

    shop_id: UUID
    shop_slug_id: str
    name: str
    shop_type: ShopTypeData
    domains: list[str]
    created: datetime.datetime
    updated: datetime.datetime
    image: None | str | Unset = UNSET
    additional_properties: dict[str, Any] = _attrs_field(init=False, factory=dict)

    def to_dict(self) -> dict[str, Any]:
        shop_id = str(self.shop_id)

        shop_slug_id = self.shop_slug_id

        name = self.name

        shop_type = self.shop_type.value

        domains = self.domains

        created = self.created.isoformat()

        updated = self.updated.isoformat()

        image: None | str | Unset
        if isinstance(self.image, Unset):
            image = UNSET
        else:
            image = self.image

        field_dict: dict[str, Any] = {}
        field_dict.update(self.additional_properties)
        field_dict.update(
            {
                "shopId": shop_id,
                "shopSlugId": shop_slug_id,
                "name": name,
                "shopType": shop_type,
                "domains": domains,
                "created": created,
                "updated": updated,
            }
        )
        if image is not UNSET:
            field_dict["image"] = image

        return field_dict

    @classmethod
    def from_dict(cls: type[T], src_dict: Mapping[str, Any]) -> T:
        d = dict(src_dict)
        shop_id = UUID(d.pop("shopId"))

        shop_slug_id = d.pop("shopSlugId")

        name = d.pop("name")

        shop_type = ShopTypeData(d.pop("shopType"))

        domains = cast(list[str], d.pop("domains"))

        created = isoparse(d.pop("created"))

        updated = isoparse(d.pop("updated"))

        def _parse_image(data: object) -> None | str | Unset:
            if data is None:
                return data
            if isinstance(data, Unset):
                return data
            return cast(None | str | Unset, data)

        image = _parse_image(d.pop("image", UNSET))

        get_shop_data = cls(
            shop_id=shop_id,
            shop_slug_id=shop_slug_id,
            name=name,
            shop_type=shop_type,
            domains=domains,
            created=created,
            updated=updated,
            image=image,
        )

        get_shop_data.additional_properties = d
        return get_shop_data

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
