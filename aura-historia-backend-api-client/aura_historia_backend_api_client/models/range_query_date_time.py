from __future__ import annotations

import datetime
from collections.abc import Mapping
from typing import Any, TypeVar

from attrs import define as _attrs_define
from attrs import field as _attrs_field
from dateutil.parser import isoparse

from ..types import UNSET, Unset

T = TypeVar("T", bound="RangeQueryDateTime")


@_attrs_define
class RangeQueryDateTime:
    """Range query for date and time values

    Attributes:
        min_ (datetime.datetime | Unset): Minimum date and time (inclusive, RFC3339 format) Example:
            2024-01-01T00:00:00Z.
        max_ (datetime.datetime | Unset): Maximum date and time (inclusive, RFC3339 format) Example:
            2024-12-31T23:59:59Z.
    """

    min_: datetime.datetime | Unset = UNSET
    max_: datetime.datetime | Unset = UNSET
    additional_properties: dict[str, Any] = _attrs_field(init=False, factory=dict)

    def to_dict(self) -> dict[str, Any]:
        min_: str | Unset = UNSET
        if not isinstance(self.min_, Unset):
            min_ = self.min_.isoformat()

        max_: str | Unset = UNSET
        if not isinstance(self.max_, Unset):
            max_ = self.max_.isoformat()

        field_dict: dict[str, Any] = {}
        field_dict.update(self.additional_properties)
        field_dict.update({})
        if min_ is not UNSET:
            field_dict["min"] = min_
        if max_ is not UNSET:
            field_dict["max"] = max_

        return field_dict

    @classmethod
    def from_dict(cls: type[T], src_dict: Mapping[str, Any]) -> T:
        d = dict(src_dict)
        _min_ = d.pop("min", UNSET)
        min_: datetime.datetime | Unset
        if isinstance(_min_, Unset):
            min_ = UNSET
        else:
            min_ = isoparse(_min_)

        _max_ = d.pop("max", UNSET)
        max_: datetime.datetime | Unset
        if isinstance(_max_, Unset):
            max_ = UNSET
        else:
            max_ = isoparse(_max_)

        range_query_date_time = cls(
            min_=min_,
            max_=max_,
        )

        range_query_date_time.additional_properties = d
        return range_query_date_time

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
