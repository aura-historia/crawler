from __future__ import annotations

from collections.abc import Mapping
from typing import TYPE_CHECKING, Any, TypeVar

from attrs import define as _attrs_define
from attrs import field as _attrs_field

if TYPE_CHECKING:
    from ..models.watchlist_user_state_data import WatchlistUserStateData


T = TypeVar("T", bound="ProductUserStateData")


@_attrs_define
class ProductUserStateData:
    """User-specific state information for a product

    Attributes:
        watchlist (WatchlistUserStateData): Watchlist-specific user state for a product
    """

    watchlist: WatchlistUserStateData
    additional_properties: dict[str, Any] = _attrs_field(init=False, factory=dict)

    def to_dict(self) -> dict[str, Any]:
        watchlist = self.watchlist.to_dict()

        field_dict: dict[str, Any] = {}
        field_dict.update(self.additional_properties)
        field_dict.update(
            {
                "watchlist": watchlist,
            }
        )

        return field_dict

    @classmethod
    def from_dict(cls: type[T], src_dict: Mapping[str, Any]) -> T:
        from ..models.watchlist_user_state_data import WatchlistUserStateData

        d = dict(src_dict)
        watchlist = WatchlistUserStateData.from_dict(d.pop("watchlist"))

        product_user_state_data = cls(
            watchlist=watchlist,
        )

        product_user_state_data.additional_properties = d
        return product_user_state_data

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
