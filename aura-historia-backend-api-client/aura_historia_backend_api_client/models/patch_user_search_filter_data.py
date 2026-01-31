from __future__ import annotations

from collections.abc import Mapping
from typing import TYPE_CHECKING, Any, TypeVar, cast

from attrs import define as _attrs_define
from attrs import field as _attrs_field

from ..types import UNSET, Unset

if TYPE_CHECKING:
    from ..models.patch_product_search_data import PatchProductSearchData


T = TypeVar("T", bound="PatchUserSearchFilterData")


@_attrs_define
class PatchUserSearchFilterData:
    """Partial search filter update data.
    All fields are optional and only provided fields will be updated.
    Can update the search filter name and/or the search filter criteria.

        Attributes:
            name (None | str | Unset): User-defined name for the search filter (max 255 characters, will be truncated if
                longer) Example: Updated Filter Name.
            product_search (None | PatchProductSearchData | Unset): Partial search filter criteria to update
    """

    name: None | str | Unset = UNSET
    product_search: None | PatchProductSearchData | Unset = UNSET
    additional_properties: dict[str, Any] = _attrs_field(init=False, factory=dict)

    def to_dict(self) -> dict[str, Any]:
        from ..models.patch_product_search_data import PatchProductSearchData

        name: None | str | Unset
        if isinstance(self.name, Unset):
            name = UNSET
        else:
            name = self.name

        product_search: dict[str, Any] | None | Unset
        if isinstance(self.product_search, Unset):
            product_search = UNSET
        elif isinstance(self.product_search, PatchProductSearchData):
            product_search = self.product_search.to_dict()
        else:
            product_search = self.product_search

        field_dict: dict[str, Any] = {}
        field_dict.update(self.additional_properties)
        field_dict.update({})
        if name is not UNSET:
            field_dict["name"] = name
        if product_search is not UNSET:
            field_dict["productSearch"] = product_search

        return field_dict

    @classmethod
    def from_dict(cls: type[T], src_dict: Mapping[str, Any]) -> T:
        from ..models.patch_product_search_data import PatchProductSearchData

        d = dict(src_dict)

        def _parse_name(data: object) -> None | str | Unset:
            if data is None:
                return data
            if isinstance(data, Unset):
                return data
            return cast(None | str | Unset, data)

        name = _parse_name(d.pop("name", UNSET))

        def _parse_product_search(data: object) -> None | PatchProductSearchData | Unset:
            if data is None:
                return data
            if isinstance(data, Unset):
                return data
            try:
                if not isinstance(data, dict):
                    raise TypeError()
                product_search_type_1 = PatchProductSearchData.from_dict(data)

                return product_search_type_1
            except (TypeError, ValueError, AttributeError, KeyError):
                pass
            return cast(None | PatchProductSearchData | Unset, data)

        product_search = _parse_product_search(d.pop("productSearch", UNSET))

        patch_user_search_filter_data = cls(
            name=name,
            product_search=product_search,
        )

        patch_user_search_filter_data.additional_properties = d
        return patch_user_search_filter_data

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
