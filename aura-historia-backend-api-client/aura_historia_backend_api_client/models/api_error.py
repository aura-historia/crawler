from __future__ import annotations

from collections.abc import Mapping
from typing import TYPE_CHECKING, Any, TypeVar

from attrs import define as _attrs_define
from attrs import field as _attrs_field

from ..types import UNSET, Unset

if TYPE_CHECKING:
    from ..models.api_error_source import ApiErrorSource


T = TypeVar("T", bound="ApiError")


@_attrs_define
class ApiError:
    """Standard error response format (RFC 9457)

    Attributes:
        status (int): HTTP status code Example: 400.
        title (str): HTTP status code in human-readable form Example: Bad Request.
        error (str): Error code identifier Example: BAD_PARAMETER.
        source (ApiErrorSource | Unset): Information about the source of the error
        detail (str | Unset): Human-readable error message Example: Expected any of: 'true' or 'false'. Got: 'invalid'.
    """

    status: int
    title: str
    error: str
    source: ApiErrorSource | Unset = UNSET
    detail: str | Unset = UNSET
    additional_properties: dict[str, Any] = _attrs_field(init=False, factory=dict)

    def to_dict(self) -> dict[str, Any]:
        status = self.status

        title = self.title

        error = self.error

        source: dict[str, Any] | Unset = UNSET
        if not isinstance(self.source, Unset):
            source = self.source.to_dict()

        detail = self.detail

        field_dict: dict[str, Any] = {}
        field_dict.update(self.additional_properties)
        field_dict.update(
            {
                "status": status,
                "title": title,
                "error": error,
            }
        )
        if source is not UNSET:
            field_dict["source"] = source
        if detail is not UNSET:
            field_dict["detail"] = detail

        return field_dict

    @classmethod
    def from_dict(cls: type[T], src_dict: Mapping[str, Any]) -> T:
        from ..models.api_error_source import ApiErrorSource

        d = dict(src_dict)
        status = d.pop("status")

        title = d.pop("title")

        error = d.pop("error")

        _source = d.pop("source", UNSET)
        source: ApiErrorSource | Unset
        if isinstance(_source, Unset):
            source = UNSET
        else:
            source = ApiErrorSource.from_dict(_source)

        detail = d.pop("detail", UNSET)

        api_error = cls(
            status=status,
            title=title,
            error=error,
            source=source,
            detail=detail,
        )

        api_error.additional_properties = d
        return api_error

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
