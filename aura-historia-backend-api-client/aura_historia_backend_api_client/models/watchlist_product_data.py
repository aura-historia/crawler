from __future__ import annotations

import datetime
from collections.abc import Mapping
from typing import TYPE_CHECKING, Any, TypeVar

from attrs import define as _attrs_define
from attrs import field as _attrs_field
from dateutil.parser import isoparse

if TYPE_CHECKING:
    from ..models.get_product_data import GetProductData


T = TypeVar("T", bound="WatchlistProductData")


@_attrs_define
class WatchlistProductData:
    """Watchlist product containing the product data and when it was added to the watchlist

    Attributes:
        product (GetProductData): Complete product information including metadata and localized content
        notifications (bool): Whether notifications are enabled for this watchlist product
        created (datetime.datetime): When the product was added to the watchlist (RFC3339 format) Example:
            2024-01-15T08:00:00Z.
        updated (datetime.datetime): When the watchlist product was last updated (RFC3339 format) Example:
            2024-01-15T08:00:00Z.
    """

    product: GetProductData
    notifications: bool
    created: datetime.datetime
    updated: datetime.datetime
    additional_properties: dict[str, Any] = _attrs_field(init=False, factory=dict)

    def to_dict(self) -> dict[str, Any]:
        product = self.product.to_dict()

        notifications = self.notifications

        created = self.created.isoformat()

        updated = self.updated.isoformat()

        field_dict: dict[str, Any] = {}
        field_dict.update(self.additional_properties)
        field_dict.update(
            {
                "product": product,
                "notifications": notifications,
                "created": created,
                "updated": updated,
            }
        )

        return field_dict

    @classmethod
    def from_dict(cls: type[T], src_dict: Mapping[str, Any]) -> T:
        from ..models.get_product_data import GetProductData

        d = dict(src_dict)
        product = GetProductData.from_dict(d.pop("product"))

        notifications = d.pop("notifications")

        created = isoparse(d.pop("created"))

        updated = isoparse(d.pop("updated"))

        watchlist_product_data = cls(
            product=product,
            notifications=notifications,
            created=created,
            updated=updated,
        )

        watchlist_product_data.additional_properties = d
        return watchlist_product_data

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
