from __future__ import annotations

from collections.abc import Mapping
from typing import TYPE_CHECKING, Any, TypeVar, cast

from attrs import define as _attrs_define
from attrs import field as _attrs_field

from ..types import UNSET, Unset

if TYPE_CHECKING:
    from ..models.user_search_filter_data import UserSearchFilterData


T = TypeVar("T", bound="UserSearchFilterCollectionData")


@_attrs_define
class UserSearchFilterCollectionData:
    """Paginated collection of user search filters with flattened pagination

    Attributes:
        items (list[UserSearchFilterData]): Array of search filters in the current page
        from_ (int): Number of products skipped (offset)
        size (int): Number of products in the current page Example: 21.
        total (int | None | Unset): Total number of products matching the query Example: 127.
    """

    items: list[UserSearchFilterData]
    from_: int
    size: int
    total: int | None | Unset = UNSET
    additional_properties: dict[str, Any] = _attrs_field(init=False, factory=dict)

    def to_dict(self) -> dict[str, Any]:
        items = []
        for items_item_data in self.items:
            items_item = items_item_data.to_dict()
            items.append(items_item)

        from_ = self.from_

        size = self.size

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
                "from": from_,
                "size": size,
            }
        )
        if total is not UNSET:
            field_dict["total"] = total

        return field_dict

    @classmethod
    def from_dict(cls: type[T], src_dict: Mapping[str, Any]) -> T:
        from ..models.user_search_filter_data import UserSearchFilterData

        d = dict(src_dict)
        items = []
        _items = d.pop("items")
        for items_item_data in _items:
            items_item = UserSearchFilterData.from_dict(items_item_data)

            items.append(items_item)

        from_ = d.pop("from")

        size = d.pop("size")

        def _parse_total(data: object) -> int | None | Unset:
            if data is None:
                return data
            if isinstance(data, Unset):
                return data
            return cast(int | None | Unset, data)

        total = _parse_total(d.pop("total", UNSET))

        user_search_filter_collection_data = cls(
            items=items,
            from_=from_,
            size=size,
            total=total,
        )

        user_search_filter_collection_data.additional_properties = d
        return user_search_filter_collection_data

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
