from __future__ import annotations

import datetime
from collections.abc import Mapping
from typing import Any, TypeVar
from uuid import UUID

from attrs import define as _attrs_define
from attrs import field as _attrs_field
from dateutil.parser import isoparse

T = TypeVar("T", bound="WatchlistProductPatchResponse")


@_attrs_define
class WatchlistProductPatchResponse:
    """Response after patching a watchlist product, containing core product identifiers and notification settings

    Attributes:
        shop_id (UUID): Unique identifier of the shop Example: 550e8400-e29b-41d4-a716-446655440000.
        shops_product_id (str): Shop's unique identifier for the product Example: 6ba7b810-9dad-11d1-80b4-00c04fd430c8.
        product_id (UUID): Internal product identifier Example: 7c9e6679-7425-40de-944b-e07fc1f90ae7.
        notifications (bool): Current notification setting for this watchlist product Example: True.
        created (datetime.datetime): When the product was added to the watchlist (RFC3339 format) Example:
            2024-01-15T08:00:00Z.
        updated (datetime.datetime): When the watchlist product was last updated (RFC3339 format) Example:
            2024-01-15T08:30:00Z.
    """

    shop_id: UUID
    shops_product_id: str
    product_id: UUID
    notifications: bool
    created: datetime.datetime
    updated: datetime.datetime
    additional_properties: dict[str, Any] = _attrs_field(init=False, factory=dict)

    def to_dict(self) -> dict[str, Any]:
        shop_id = str(self.shop_id)

        shops_product_id = self.shops_product_id

        product_id = str(self.product_id)

        notifications = self.notifications

        created = self.created.isoformat()

        updated = self.updated.isoformat()

        field_dict: dict[str, Any] = {}
        field_dict.update(self.additional_properties)
        field_dict.update(
            {
                "shopId": shop_id,
                "shopsProductId": shops_product_id,
                "productId": product_id,
                "notifications": notifications,
                "created": created,
                "updated": updated,
            }
        )

        return field_dict

    @classmethod
    def from_dict(cls: type[T], src_dict: Mapping[str, Any]) -> T:
        d = dict(src_dict)
        shop_id = UUID(d.pop("shopId"))

        shops_product_id = d.pop("shopsProductId")

        product_id = UUID(d.pop("productId"))

        notifications = d.pop("notifications")

        created = isoparse(d.pop("created"))

        updated = isoparse(d.pop("updated"))

        watchlist_product_patch_response = cls(
            shop_id=shop_id,
            shops_product_id=shops_product_id,
            product_id=product_id,
            notifications=notifications,
            created=created,
            updated=updated,
        )

        watchlist_product_patch_response.additional_properties = d
        return watchlist_product_patch_response

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
