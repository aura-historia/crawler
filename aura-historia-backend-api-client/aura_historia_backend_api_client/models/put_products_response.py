from __future__ import annotations

from collections.abc import Mapping
from typing import TYPE_CHECKING, Any, TypeVar, cast

from attrs import define as _attrs_define
from attrs import field as _attrs_field

from ..types import UNSET, Unset

if TYPE_CHECKING:
    from ..models.put_products_response_failed import PutProductsResponseFailed


T = TypeVar("T", bound="PutProductsResponse")


@_attrs_define
class PutProductsResponse:
    """Response from bulk product creation/update operation with enrichment

    Attributes:
        skipped (int): Number of products that were skipped during processing because they had no changes Example: 2.
        unprocessed (list[str] | Unset): Product URLs that could not be processed due to temporary issues.
            These products may succeed if retried.
             Example: ['https://tech-store.com/products/temporary-issue'].
        failed (PutProductsResponseFailed | Unset): Map of product URLs to error codes for products that failed
            processing.
            The key is the product URL, and the value is the error code explaining why it failed.
             Example: {'https://unknown-shop.com/item': 'SHOP_NOT_FOUND', 'https://tech-store.com/expensive-item':
            'MONETARY_AMOUNT_OVERFLOW', 'https://localhost:8080/item': 'NO_DOMAIN'}.
    """

    skipped: int
    unprocessed: list[str] | Unset = UNSET
    failed: PutProductsResponseFailed | Unset = UNSET
    additional_properties: dict[str, Any] = _attrs_field(init=False, factory=dict)

    def to_dict(self) -> dict[str, Any]:
        skipped = self.skipped

        unprocessed: list[str] | Unset = UNSET
        if not isinstance(self.unprocessed, Unset):
            unprocessed = self.unprocessed

        failed: dict[str, Any] | Unset = UNSET
        if not isinstance(self.failed, Unset):
            failed = self.failed.to_dict()

        field_dict: dict[str, Any] = {}
        field_dict.update(self.additional_properties)
        field_dict.update(
            {
                "skipped": skipped,
            }
        )
        if unprocessed is not UNSET:
            field_dict["unprocessed"] = unprocessed
        if failed is not UNSET:
            field_dict["failed"] = failed

        return field_dict

    @classmethod
    def from_dict(cls: type[T], src_dict: Mapping[str, Any]) -> T:
        from ..models.put_products_response_failed import PutProductsResponseFailed

        d = dict(src_dict)
        skipped = d.pop("skipped")

        unprocessed = cast(list[str], d.pop("unprocessed", UNSET))

        _failed = d.pop("failed", UNSET)
        failed: PutProductsResponseFailed | Unset
        if isinstance(_failed, Unset):
            failed = UNSET
        else:
            failed = PutProductsResponseFailed.from_dict(_failed)

        put_products_response = cls(
            skipped=skipped,
            unprocessed=unprocessed,
            failed=failed,
        )

        put_products_response.additional_properties = d
        return put_products_response

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
