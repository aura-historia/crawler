from __future__ import annotations

from collections.abc import Mapping
from typing import TYPE_CHECKING, Any, TypeVar, cast

from attrs import define as _attrs_define
from attrs import field as _attrs_field

from ..models.shop_type_data import ShopTypeData
from ..types import UNSET, Unset

if TYPE_CHECKING:
    from ..models.range_query_date_time import RangeQueryDateTime


T = TypeVar("T", bound="ShopSearchData")


@_attrs_define
class ShopSearchData:
    """Search filter configuration for shops with query parameters and filtering options

    Attributes:
        shop_name_query (str | Unset): Optional text query for searching shops by name Example: tech store.
        shop_type (list[ShopTypeData] | None | Unset): Optional filter by shop types Example: ['AUCTION_HOUSE'].
        created (None | RangeQueryDateTime | Unset): Optional filter by shop creation date range
        updated (None | RangeQueryDateTime | Unset): Optional filter by shop last updated date range
    """

    shop_name_query: str | Unset = UNSET
    shop_type: list[ShopTypeData] | None | Unset = UNSET
    created: None | RangeQueryDateTime | Unset = UNSET
    updated: None | RangeQueryDateTime | Unset = UNSET
    additional_properties: dict[str, Any] = _attrs_field(init=False, factory=dict)

    def to_dict(self) -> dict[str, Any]:
        from ..models.range_query_date_time import RangeQueryDateTime

        shop_name_query = self.shop_name_query

        shop_type: list[str] | None | Unset
        if isinstance(self.shop_type, Unset):
            shop_type = UNSET
        elif isinstance(self.shop_type, list):
            shop_type = []
            for shop_type_type_0_item_data in self.shop_type:
                shop_type_type_0_item = shop_type_type_0_item_data.value
                shop_type.append(shop_type_type_0_item)

        else:
            shop_type = self.shop_type

        created: dict[str, Any] | None | Unset
        if isinstance(self.created, Unset):
            created = UNSET
        elif isinstance(self.created, RangeQueryDateTime):
            created = self.created.to_dict()
        else:
            created = self.created

        updated: dict[str, Any] | None | Unset
        if isinstance(self.updated, Unset):
            updated = UNSET
        elif isinstance(self.updated, RangeQueryDateTime):
            updated = self.updated.to_dict()
        else:
            updated = self.updated

        field_dict: dict[str, Any] = {}
        field_dict.update(self.additional_properties)
        field_dict.update({})
        if shop_name_query is not UNSET:
            field_dict["shopNameQuery"] = shop_name_query
        if shop_type is not UNSET:
            field_dict["shopType"] = shop_type
        if created is not UNSET:
            field_dict["created"] = created
        if updated is not UNSET:
            field_dict["updated"] = updated

        return field_dict

    @classmethod
    def from_dict(cls: type[T], src_dict: Mapping[str, Any]) -> T:
        from ..models.range_query_date_time import RangeQueryDateTime

        d = dict(src_dict)
        shop_name_query = d.pop("shopNameQuery", UNSET)

        def _parse_shop_type(data: object) -> list[ShopTypeData] | None | Unset:
            if data is None:
                return data
            if isinstance(data, Unset):
                return data
            try:
                if not isinstance(data, list):
                    raise TypeError()
                shop_type_type_0 = []
                _shop_type_type_0 = data
                for shop_type_type_0_item_data in _shop_type_type_0:
                    shop_type_type_0_item = ShopTypeData(shop_type_type_0_item_data)

                    shop_type_type_0.append(shop_type_type_0_item)

                return shop_type_type_0
            except (TypeError, ValueError, AttributeError, KeyError):
                pass
            return cast(list[ShopTypeData] | None | Unset, data)

        shop_type = _parse_shop_type(d.pop("shopType", UNSET))

        def _parse_created(data: object) -> None | RangeQueryDateTime | Unset:
            if data is None:
                return data
            if isinstance(data, Unset):
                return data
            try:
                if not isinstance(data, dict):
                    raise TypeError()
                created_type_1 = RangeQueryDateTime.from_dict(data)

                return created_type_1
            except (TypeError, ValueError, AttributeError, KeyError):
                pass
            return cast(None | RangeQueryDateTime | Unset, data)

        created = _parse_created(d.pop("created", UNSET))

        def _parse_updated(data: object) -> None | RangeQueryDateTime | Unset:
            if data is None:
                return data
            if isinstance(data, Unset):
                return data
            try:
                if not isinstance(data, dict):
                    raise TypeError()
                updated_type_1 = RangeQueryDateTime.from_dict(data)

                return updated_type_1
            except (TypeError, ValueError, AttributeError, KeyError):
                pass
            return cast(None | RangeQueryDateTime | Unset, data)

        updated = _parse_updated(d.pop("updated", UNSET))

        shop_search_data = cls(
            shop_name_query=shop_name_query,
            shop_type=shop_type,
            created=created,
            updated=updated,
        )

        shop_search_data.additional_properties = d
        return shop_search_data

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
