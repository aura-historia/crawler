from __future__ import annotations

import datetime
from collections.abc import Mapping
from typing import TYPE_CHECKING, Any, TypeVar, cast

from attrs import define as _attrs_define
from attrs import field as _attrs_field
from dateutil.parser import isoparse

from ..models.product_state_data import ProductStateData
from ..types import UNSET, Unset

if TYPE_CHECKING:
    from ..models.localized_text_data import LocalizedTextData
    from ..models.price_data import PriceData


T = TypeVar("T", bound="PutProductData")


@_attrs_define
class PutProductData:
    """Data required to create or update a product.
    Shop information (shopId and shopName) is automatically enriched based on the product's URL.

        Attributes:
            shops_product_id (str): Shop's unique identifier for the product. Can be any arbitrary string. Example:
                6ba7b810-9dad-11d1-80b4-00c04fd430c8.
            title (LocalizedTextData): Text content with language information
            state (ProductStateData): Current state of the product:
                - LISTED: Product has been listed
                - AVAILABLE: Product is available for purchase
                - RESERVED: Product is reserved by a buyer
                - SOLD: Product has been sold
                - REMOVED: Product has been removed and can no longer be tracked
                - UNKNOWN: Product has an unknown state
                 Example: AVAILABLE.
            url (str): URL to the product on the shop's website.
                The shop will be automatically identified and enriched based on the domain extracted from this URL.
                 Example: https://tech-store-premium.com/products/smartphone-case.
            description (LocalizedTextData | None | Unset): Optional product description
            price (None | PriceData | Unset): Optional product price
            price_estimate_min (None | PriceData | Unset): Optional minimum estimated price for the product
            price_estimate_max (None | PriceData | Unset): Optional maximum estimated price for the product
            images (list[str] | Unset): Array of image URLs for the product Example: ['https://tech-store-
                premium.com/images/case-1.jpg', 'https://tech-store-premium.com/images/case-2.jpg'].
            auction_start (datetime.datetime | None | Unset): Start datetime of the auction window for this product (RFC3339
                format).
                Only applicable for products from auction houses with scheduled auction times.
                Used to indicate when bidding begins or when the item will be auctioned.
                 Example: 2026-02-15T10:00:00Z.
            auction_end (datetime.datetime | None | Unset): End datetime of the auction window for this product (RFC3339
                format).
                Only applicable for products from auction houses with scheduled auction times.
                Used to indicate when bidding ends or when the auction session concludes.
                 Example: 2026-02-15T14:00:00Z.
    """

    shops_product_id: str
    title: LocalizedTextData
    state: ProductStateData
    url: str
    description: LocalizedTextData | None | Unset = UNSET
    price: None | PriceData | Unset = UNSET
    price_estimate_min: None | PriceData | Unset = UNSET
    price_estimate_max: None | PriceData | Unset = UNSET
    images: list[str] | Unset = UNSET
    auction_start: datetime.datetime | None | Unset = UNSET
    auction_end: datetime.datetime | None | Unset = UNSET
    additional_properties: dict[str, Any] = _attrs_field(init=False, factory=dict)

    def to_dict(self) -> dict[str, Any]:
        from ..models.localized_text_data import LocalizedTextData
        from ..models.price_data import PriceData

        shops_product_id = self.shops_product_id

        title = self.title.to_dict()

        state = self.state.value

        url = self.url

        description: dict[str, Any] | None | Unset
        if isinstance(self.description, Unset):
            description = UNSET
        elif isinstance(self.description, LocalizedTextData):
            description = self.description.to_dict()
        else:
            description = self.description

        price: dict[str, Any] | None | Unset
        if isinstance(self.price, Unset):
            price = UNSET
        elif isinstance(self.price, PriceData):
            price = self.price.to_dict()
        else:
            price = self.price

        price_estimate_min: dict[str, Any] | None | Unset
        if isinstance(self.price_estimate_min, Unset):
            price_estimate_min = UNSET
        elif isinstance(self.price_estimate_min, PriceData):
            price_estimate_min = self.price_estimate_min.to_dict()
        else:
            price_estimate_min = self.price_estimate_min

        price_estimate_max: dict[str, Any] | None | Unset
        if isinstance(self.price_estimate_max, Unset):
            price_estimate_max = UNSET
        elif isinstance(self.price_estimate_max, PriceData):
            price_estimate_max = self.price_estimate_max.to_dict()
        else:
            price_estimate_max = self.price_estimate_max

        images: list[str] | Unset = UNSET
        if not isinstance(self.images, Unset):
            images = self.images

        auction_start: None | str | Unset
        if isinstance(self.auction_start, Unset):
            auction_start = UNSET
        elif isinstance(self.auction_start, datetime.datetime):
            auction_start = self.auction_start.isoformat()
        else:
            auction_start = self.auction_start

        auction_end: None | str | Unset
        if isinstance(self.auction_end, Unset):
            auction_end = UNSET
        elif isinstance(self.auction_end, datetime.datetime):
            auction_end = self.auction_end.isoformat()
        else:
            auction_end = self.auction_end

        field_dict: dict[str, Any] = {}
        field_dict.update(self.additional_properties)
        field_dict.update(
            {
                "shopsProductId": shops_product_id,
                "title": title,
                "state": state,
                "url": url,
            }
        )
        if description is not UNSET:
            field_dict["description"] = description
        if price is not UNSET:
            field_dict["price"] = price
        if price_estimate_min is not UNSET:
            field_dict["priceEstimateMin"] = price_estimate_min
        if price_estimate_max is not UNSET:
            field_dict["priceEstimateMax"] = price_estimate_max
        if images is not UNSET:
            field_dict["images"] = images
        if auction_start is not UNSET:
            field_dict["auctionStart"] = auction_start
        if auction_end is not UNSET:
            field_dict["auctionEnd"] = auction_end

        return field_dict

    @classmethod
    def from_dict(cls: type[T], src_dict: Mapping[str, Any]) -> T:
        from ..models.localized_text_data import LocalizedTextData
        from ..models.price_data import PriceData

        d = dict(src_dict)
        shops_product_id = d.pop("shopsProductId")

        title = LocalizedTextData.from_dict(d.pop("title"))

        state = ProductStateData(d.pop("state"))

        url = d.pop("url")

        def _parse_description(data: object) -> LocalizedTextData | None | Unset:
            if data is None:
                return data
            if isinstance(data, Unset):
                return data
            try:
                if not isinstance(data, dict):
                    raise TypeError()
                description_type_1 = LocalizedTextData.from_dict(data)

                return description_type_1
            except (TypeError, ValueError, AttributeError, KeyError):
                pass
            return cast(LocalizedTextData | None | Unset, data)

        description = _parse_description(d.pop("description", UNSET))

        def _parse_price(data: object) -> None | PriceData | Unset:
            if data is None:
                return data
            if isinstance(data, Unset):
                return data
            try:
                if not isinstance(data, dict):
                    raise TypeError()
                price_type_1 = PriceData.from_dict(data)

                return price_type_1
            except (TypeError, ValueError, AttributeError, KeyError):
                pass
            return cast(None | PriceData | Unset, data)

        price = _parse_price(d.pop("price", UNSET))

        def _parse_price_estimate_min(data: object) -> None | PriceData | Unset:
            if data is None:
                return data
            if isinstance(data, Unset):
                return data
            try:
                if not isinstance(data, dict):
                    raise TypeError()
                price_estimate_min_type_1 = PriceData.from_dict(data)

                return price_estimate_min_type_1
            except (TypeError, ValueError, AttributeError, KeyError):
                pass
            return cast(None | PriceData | Unset, data)

        price_estimate_min = _parse_price_estimate_min(d.pop("priceEstimateMin", UNSET))

        def _parse_price_estimate_max(data: object) -> None | PriceData | Unset:
            if data is None:
                return data
            if isinstance(data, Unset):
                return data
            try:
                if not isinstance(data, dict):
                    raise TypeError()
                price_estimate_max_type_1 = PriceData.from_dict(data)

                return price_estimate_max_type_1
            except (TypeError, ValueError, AttributeError, KeyError):
                pass
            return cast(None | PriceData | Unset, data)

        price_estimate_max = _parse_price_estimate_max(d.pop("priceEstimateMax", UNSET))

        images = cast(list[str], d.pop("images", UNSET))

        def _parse_auction_start(data: object) -> datetime.datetime | None | Unset:
            if data is None:
                return data
            if isinstance(data, Unset):
                return data
            try:
                if not isinstance(data, str):
                    raise TypeError()
                auction_start_type_0 = isoparse(data)

                return auction_start_type_0
            except (TypeError, ValueError, AttributeError, KeyError):
                pass
            return cast(datetime.datetime | None | Unset, data)

        auction_start = _parse_auction_start(d.pop("auctionStart", UNSET))

        def _parse_auction_end(data: object) -> datetime.datetime | None | Unset:
            if data is None:
                return data
            if isinstance(data, Unset):
                return data
            try:
                if not isinstance(data, str):
                    raise TypeError()
                auction_end_type_0 = isoparse(data)

                return auction_end_type_0
            except (TypeError, ValueError, AttributeError, KeyError):
                pass
            return cast(datetime.datetime | None | Unset, data)

        auction_end = _parse_auction_end(d.pop("auctionEnd", UNSET))

        put_product_data = cls(
            shops_product_id=shops_product_id,
            title=title,
            state=state,
            url=url,
            description=description,
            price=price,
            price_estimate_min=price_estimate_min,
            price_estimate_max=price_estimate_max,
            images=images,
            auction_start=auction_start,
            auction_end=auction_end,
        )

        put_product_data.additional_properties = d
        return put_product_data

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
