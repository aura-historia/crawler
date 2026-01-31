from __future__ import annotations

from collections.abc import Mapping
from typing import Any, TypeVar

from attrs import define as _attrs_define
from attrs import field as _attrs_field

from ..models.api_error_source_source_type import ApiErrorSourceSourceType

T = TypeVar("T", bound="ApiErrorSource")


@_attrs_define
class ApiErrorSource:
    """Information about the source of the error

    Attributes:
        field (str): Name of the field that caused the error Example: shopId.
        source_type (ApiErrorSourceSourceType): Type of parameter that caused the error Example: path.
    """

    field: str
    source_type: ApiErrorSourceSourceType
    additional_properties: dict[str, Any] = _attrs_field(init=False, factory=dict)

    def to_dict(self) -> dict[str, Any]:
        field = self.field

        source_type = self.source_type.value

        field_dict: dict[str, Any] = {}
        field_dict.update(self.additional_properties)
        field_dict.update(
            {
                "field": field,
                "sourceType": source_type,
            }
        )

        return field_dict

    @classmethod
    def from_dict(cls: type[T], src_dict: Mapping[str, Any]) -> T:
        d = dict(src_dict)
        field = d.pop("field")

        source_type = ApiErrorSourceSourceType(d.pop("sourceType"))

        api_error_source = cls(
            field=field,
            source_type=source_type,
        )

        api_error_source.additional_properties = d
        return api_error_source

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
