from __future__ import annotations

from collections.abc import Mapping
from typing import TYPE_CHECKING, Any, TypeVar, cast

from attrs import define as _attrs_define
from attrs import field as _attrs_field

from ..types import UNSET, Unset

if TYPE_CHECKING:
    from ..models.get_product_data import GetProductData
    from ..models.product_user_state_data import ProductUserStateData


T = TypeVar("T", bound="PersonalizedGetProductData")


@_attrs_define
class PersonalizedGetProductData:
    """Wrapper for product data with optional user-specific state.
    When user is authenticated, includes personalized information such as watchlist status.
    When user is anonymous, only the product data is present.

        Attributes:
            item (GetProductData): Complete product information including metadata and localized content
            user_state (None | ProductUserStateData | Unset): Optional user-specific state for this product (only present
                when authenticated)
    """

    item: GetProductData
    user_state: None | ProductUserStateData | Unset = UNSET
    additional_properties: dict[str, Any] = _attrs_field(init=False, factory=dict)

    def to_dict(self) -> dict[str, Any]:
        from ..models.product_user_state_data import ProductUserStateData

        item = self.item.to_dict()

        user_state: dict[str, Any] | None | Unset
        if isinstance(self.user_state, Unset):
            user_state = UNSET
        elif isinstance(self.user_state, ProductUserStateData):
            user_state = self.user_state.to_dict()
        else:
            user_state = self.user_state

        field_dict: dict[str, Any] = {}
        field_dict.update(self.additional_properties)
        field_dict.update(
            {
                "item": item,
            }
        )
        if user_state is not UNSET:
            field_dict["userState"] = user_state

        return field_dict

    @classmethod
    def from_dict(cls: type[T], src_dict: Mapping[str, Any]) -> T:
        from ..models.get_product_data import GetProductData
        from ..models.product_user_state_data import ProductUserStateData

        d = dict(src_dict)
        item = GetProductData.from_dict(d.pop("item"))

        def _parse_user_state(data: object) -> None | ProductUserStateData | Unset:
            if data is None:
                return data
            if isinstance(data, Unset):
                return data
            try:
                if not isinstance(data, dict):
                    raise TypeError()
                user_state_type_1 = ProductUserStateData.from_dict(data)

                return user_state_type_1
            except (TypeError, ValueError, AttributeError, KeyError):
                pass
            return cast(None | ProductUserStateData | Unset, data)

        user_state = _parse_user_state(d.pop("userState", UNSET))

        personalized_get_product_data = cls(
            item=item,
            user_state=user_state,
        )

        personalized_get_product_data.additional_properties = d
        return personalized_get_product_data

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
