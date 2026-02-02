from __future__ import annotations

from collections.abc import Mapping
from typing import Any, TypeVar

from attrs import define as _attrs_define
from attrs import field as _attrs_field

from ..models.prohibited_content_data import ProhibitedContentData

T = TypeVar("T", bound="ProductImageData")


@_attrs_define
class ProductImageData:
    """Product image with prohibited content classification

    Attributes:
        url (str): URL to the product image Example: https://my-shop.com/images/product-1.jpg.
        prohibited_content (ProhibitedContentData): Classification of prohibited or sensitive content that may be
            present in product images:
            - UNKNOWN: Content classification has not been determined
            - NONE: No prohibited content detected in the image
            - NAZI_GERMANY: Image contains Nazi Germany symbols, insignia, or related content
             Example: NONE.
    """

    url: str
    prohibited_content: ProhibitedContentData
    additional_properties: dict[str, Any] = _attrs_field(init=False, factory=dict)

    def to_dict(self) -> dict[str, Any]:
        url = self.url

        prohibited_content = self.prohibited_content.value

        field_dict: dict[str, Any] = {}
        field_dict.update(self.additional_properties)
        field_dict.update(
            {
                "url": url,
                "prohibitedContent": prohibited_content,
            }
        )

        return field_dict

    @classmethod
    def from_dict(cls: type[T], src_dict: Mapping[str, Any]) -> T:
        d = dict(src_dict)
        url = d.pop("url")

        prohibited_content = ProhibitedContentData(d.pop("prohibitedContent"))

        product_image_data = cls(
            url=url,
            prohibited_content=prohibited_content,
        )

        product_image_data.additional_properties = d
        return product_image_data

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
