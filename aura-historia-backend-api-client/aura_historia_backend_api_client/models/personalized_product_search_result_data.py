from __future__ import annotations

from collections.abc import Mapping
from typing import TYPE_CHECKING, Any, TypeVar, cast

from attrs import define as _attrs_define
from attrs import field as _attrs_field

from ..types import UNSET, Unset

if TYPE_CHECKING:
    from ..models.personalized_get_product_summary_data import PersonalizedGetProductSummaryData


T = TypeVar("T", bound="PersonalizedProductSearchResultData")


@_attrs_define
class PersonalizedProductSearchResultData:
    """Paginated collection of personalized products using cursor-based pagination (search-after pattern).
    Each product may include user-specific state when the request is authenticated.

        Attributes:
            items (list[PersonalizedGetProductSummaryData]): Array of personalized product summaries in the current page
            size (int): Number of products returned in the current page Example: 21.
            total (int | None | Unset): Total number of products matching the query (optional, may not always be available)
                Example: 127.
            search_after (list[Any] | None | Unset): Cursor for the next page (JSON value). Present when there are more
                results.
                Pass this value as the `searchAfter` query parameter to get the next page.
                This can be ANY heterogeneous array.
                 Example: [2999, "550e8400-e29b-41d4-a716-446655440000"].
    """

    items: list[PersonalizedGetProductSummaryData]
    size: int
    total: int | None | Unset = UNSET
    search_after: list[Any] | None | Unset = UNSET
    additional_properties: dict[str, Any] = _attrs_field(init=False, factory=dict)

    def to_dict(self) -> dict[str, Any]:
        items = []
        for items_item_data in self.items:
            items_item = items_item_data.to_dict()
            items.append(items_item)

        size = self.size

        total: int | None | Unset
        if isinstance(self.total, Unset):
            total = UNSET
        else:
            total = self.total

        search_after: list[Any] | None | Unset
        if isinstance(self.search_after, Unset):
            search_after = UNSET
        elif isinstance(self.search_after, list):
            search_after = self.search_after

        else:
            search_after = self.search_after

        field_dict: dict[str, Any] = {}
        field_dict.update(self.additional_properties)
        field_dict.update(
            {
                "items": items,
                "size": size,
            }
        )
        if total is not UNSET:
            field_dict["total"] = total
        if search_after is not UNSET:
            field_dict["searchAfter"] = search_after

        return field_dict

    @classmethod
    def from_dict(cls: type[T], src_dict: Mapping[str, Any]) -> T:
        from ..models.personalized_get_product_summary_data import PersonalizedGetProductSummaryData

        d = dict(src_dict)
        items = []
        _items = d.pop("items")
        for items_item_data in _items:
            items_item = PersonalizedGetProductSummaryData.from_dict(items_item_data)

            items.append(items_item)

        size = d.pop("size")

        def _parse_total(data: object) -> int | None | Unset:
            if data is None:
                return data
            if isinstance(data, Unset):
                return data
            return cast(int | None | Unset, data)

        total = _parse_total(d.pop("total", UNSET))

        def _parse_search_after(data: object) -> list[Any] | None | Unset:
            if data is None:
                return data
            if isinstance(data, Unset):
                return data
            try:
                if not isinstance(data, list):
                    raise TypeError()
                search_after_type_0 = cast(list[Any], data)

                return search_after_type_0
            except (TypeError, ValueError, AttributeError, KeyError):
                pass
            return cast(list[Any] | None | Unset, data)

        search_after = _parse_search_after(d.pop("searchAfter", UNSET))

        personalized_product_search_result_data = cls(
            items=items,
            size=size,
            total=total,
            search_after=search_after,
        )

        personalized_product_search_result_data.additional_properties = d
        return personalized_product_search_result_data

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
