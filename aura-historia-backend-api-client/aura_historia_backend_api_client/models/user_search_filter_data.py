from __future__ import annotations

import datetime
from collections.abc import Mapping
from typing import TYPE_CHECKING, Any, TypeVar
from uuid import UUID

from attrs import define as _attrs_define
from attrs import field as _attrs_field
from dateutil.parser import isoparse

if TYPE_CHECKING:
    from ..models.product_search_data import ProductSearchData


T = TypeVar("T", bound="UserSearchFilterData")


@_attrs_define
class UserSearchFilterData:
    """Complete user search filter with metadata

    Attributes:
        user_id (UUID): Unique identifier of the user who owns this search filter Example:
            550e8400-e29b-41d4-a716-446655440000.
        user_search_filter_id (UUID): Unique identifier for this search filter Example:
            6ba7b810-9dad-11d1-80b4-00c04fd430c8.
        name (str): User-defined name for the search filter Example: My Tech Store Search.
        product_search (ProductSearchData): Product search configuration with query parameters and filtering options
        created (datetime.datetime): When the search filter was created (RFC3339 format) Example: 2024-01-01T10:00:00Z.
        updated (datetime.datetime): When the search filter was last updated (RFC3339 format) Example:
            2024-01-01T12:00:00Z.
    """

    user_id: UUID
    user_search_filter_id: UUID
    name: str
    product_search: ProductSearchData
    created: datetime.datetime
    updated: datetime.datetime
    additional_properties: dict[str, Any] = _attrs_field(init=False, factory=dict)

    def to_dict(self) -> dict[str, Any]:
        user_id = str(self.user_id)

        user_search_filter_id = str(self.user_search_filter_id)

        name = self.name

        product_search = self.product_search.to_dict()

        created = self.created.isoformat()

        updated = self.updated.isoformat()

        field_dict: dict[str, Any] = {}
        field_dict.update(self.additional_properties)
        field_dict.update(
            {
                "userId": user_id,
                "userSearchFilterId": user_search_filter_id,
                "name": name,
                "productSearch": product_search,
                "created": created,
                "updated": updated,
            }
        )

        return field_dict

    @classmethod
    def from_dict(cls: type[T], src_dict: Mapping[str, Any]) -> T:
        from ..models.product_search_data import ProductSearchData

        d = dict(src_dict)
        user_id = UUID(d.pop("userId"))

        user_search_filter_id = UUID(d.pop("userSearchFilterId"))

        name = d.pop("name")

        product_search = ProductSearchData.from_dict(d.pop("productSearch"))

        created = isoparse(d.pop("created"))

        updated = isoparse(d.pop("updated"))

        user_search_filter_data = cls(
            user_id=user_id,
            user_search_filter_id=user_search_filter_id,
            name=name,
            product_search=product_search,
            created=created,
            updated=updated,
        )

        user_search_filter_data.additional_properties = d
        return user_search_filter_data

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
