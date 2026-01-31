from __future__ import annotations

from collections.abc import Mapping
from typing import Any, TypeVar

from attrs import define as _attrs_define
from attrs import field as _attrs_field

T = TypeVar("T", bound="WatchlistUserStateData")


@_attrs_define
class WatchlistUserStateData:
    """Watchlist-specific user state for a product

    Attributes:
        watching (bool): Whether the product is on the user's watchlist Example: True.
        notifications (bool): Whether notifications are enabled for this watchlist product
    """

    watching: bool
    notifications: bool
    additional_properties: dict[str, Any] = _attrs_field(init=False, factory=dict)

    def to_dict(self) -> dict[str, Any]:
        watching = self.watching

        notifications = self.notifications

        field_dict: dict[str, Any] = {}
        field_dict.update(self.additional_properties)
        field_dict.update(
            {
                "watching": watching,
                "notifications": notifications,
            }
        )

        return field_dict

    @classmethod
    def from_dict(cls: type[T], src_dict: Mapping[str, Any]) -> T:
        d = dict(src_dict)
        watching = d.pop("watching")

        notifications = d.pop("notifications")

        watchlist_user_state_data = cls(
            watching=watching,
            notifications=notifications,
        )

        watchlist_user_state_data.additional_properties = d
        return watchlist_user_state_data

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
