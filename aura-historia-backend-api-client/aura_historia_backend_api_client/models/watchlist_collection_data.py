from __future__ import annotations

import datetime
from collections.abc import Mapping
from typing import TYPE_CHECKING, Any, TypeVar, cast

from attrs import define as _attrs_define
from attrs import field as _attrs_field
from dateutil.parser import isoparse

from ..types import UNSET, Unset

if TYPE_CHECKING:
    from ..models.watchlist_product_data import WatchlistProductData


T = TypeVar("T", bound="WatchlistCollectionData")


@_attrs_define
class WatchlistCollectionData:
    """Paginated collection of watchlist products using cursor-based pagination

    Attributes:
        items (list[WatchlistProductData]): Array of watchlist products in the current page
        size (int): Number of products in the current page Example: 21.
        search_after (datetime.datetime | None | Unset): Cursor for the next page (RFC3339 timestamp). Present when
            there are more results. Example: 2024-01-15T08:00:00Z.
        total (int | None | Unset): Total number of products (optional, may not be available for cursor-based
            pagination) Example: 127.
    """

    items: list[WatchlistProductData]
    size: int
    search_after: datetime.datetime | None | Unset = UNSET
    total: int | None | Unset = UNSET
    additional_properties: dict[str, Any] = _attrs_field(init=False, factory=dict)

    def to_dict(self) -> dict[str, Any]:
        items = []
        for items_item_data in self.items:
            items_item = items_item_data.to_dict()
            items.append(items_item)

        size = self.size

        search_after: None | str | Unset
        if isinstance(self.search_after, Unset):
            search_after = UNSET
        elif isinstance(self.search_after, datetime.datetime):
            search_after = self.search_after.isoformat()
        else:
            search_after = self.search_after

        total: int | None | Unset
        if isinstance(self.total, Unset):
            total = UNSET
        else:
            total = self.total

        field_dict: dict[str, Any] = {}
        field_dict.update(self.additional_properties)
        field_dict.update(
            {
                "items": items,
                "size": size,
            }
        )
        if search_after is not UNSET:
            field_dict["searchAfter"] = search_after
        if total is not UNSET:
            field_dict["total"] = total

        return field_dict

    @classmethod
    def from_dict(cls: type[T], src_dict: Mapping[str, Any]) -> T:
        from ..models.watchlist_product_data import WatchlistProductData

        d = dict(src_dict)
        items = []
        _items = d.pop("items")
        for items_item_data in _items:
            items_item = WatchlistProductData.from_dict(items_item_data)

            items.append(items_item)

        size = d.pop("size")

        def _parse_search_after(data: object) -> datetime.datetime | None | Unset:
            if data is None:
                return data
            if isinstance(data, Unset):
                return data
            try:
                if not isinstance(data, str):
                    raise TypeError()
                search_after_type_0 = isoparse(data)

                return search_after_type_0
            except (TypeError, ValueError, AttributeError, KeyError):
                pass
            return cast(datetime.datetime | None | Unset, data)

        search_after = _parse_search_after(d.pop("searchAfter", UNSET))

        def _parse_total(data: object) -> int | None | Unset:
            if data is None:
                return data
            if isinstance(data, Unset):
                return data
            return cast(int | None | Unset, data)

        total = _parse_total(d.pop("total", UNSET))

        watchlist_collection_data = cls(
            items=items,
            size=size,
            search_after=search_after,
            total=total,
        )

        watchlist_collection_data.additional_properties = d
        return watchlist_collection_data

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
