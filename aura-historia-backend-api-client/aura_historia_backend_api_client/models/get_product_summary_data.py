from __future__ import annotations

import datetime
from collections.abc import Mapping
from typing import TYPE_CHECKING, Any, TypeVar, cast
from uuid import UUID

from attrs import define as _attrs_define
from attrs import field as _attrs_field
from dateutil.parser import isoparse

from ..models.product_state_data import ProductStateData
from ..models.shop_type_data import ShopTypeData
from ..types import UNSET, Unset

if TYPE_CHECKING:
    from ..models.localized_text_data import LocalizedTextData
    from ..models.price_data import PriceData
    from ..models.product_image_data import ProductImageData


T = TypeVar("T", bound="GetProductSummaryData")


@_attrs_define
class GetProductSummaryData:
    """Lightweight product summary information for use in search results and similar products listings.
    Contains essential product details without extended metadata fields like description, estimates,
    origin year details, authenticity, condition, provenance, restoration, auction times, or history.

        Attributes:
            product_id (UUID): Unique internal identifier for the product
            product_slug_id (str): Human-readable slug identifier for the product (kebab-case with 6-character hex suffix).
                Format: {product-title}-{6-char-hex} where the title is derived from the product name.
                Example: "amazing-product-fa87c4"
            shop_slug_id (str): Human-readable slug identifier of the shop (kebab-case, derived from shop name).
                Example: "tech-store-premium" or "christies"
            event_id (UUID): Unique identifier for the current state/version of the product
            shop_id (UUID): Unique identifier of the shop
            shops_product_id (str): Shop's unique identifier for the product. Can be any arbitrary string.
            shop_name (str): Display name of the shop
            shop_type (ShopTypeData): Type of vendor or shop:
                - AUCTION_HOUSE: Auction house selling items through auctions
                - AUCTION_PLATFORM: Auction platform hosting auctions for auction-houses
                - COMMERCIAL_DEALER: Commercial dealer or shop selling items directly
                - MARKETPLACE: Marketplace platform connecting buyers and sellers
                 Example: COMMERCIAL_DEALER.
            title (LocalizedTextData): Text content with language information
            state (ProductStateData): Current state of the product:
                - LISTED: Product has been listed
                - AVAILABLE: Product is available for purchase
                - RESERVED: Product is reserved by a buyer
                - SOLD: Product has been sold
                - REMOVED: Product has been removed and can no longer be tracked
                - UNKNOWN: Product has an unknown state
                 Example: AVAILABLE.
            url (str): URL to the product on the shop's website
            images (list[ProductImageData]): Array of product images with prohibited content classification
            created (datetime.datetime): When the product was first created (RFC3339 format)
            updated (datetime.datetime): When the product was last updated (RFC3339 format)
            price (None | PriceData | Unset): Optional product price
    """

    product_id: UUID
    product_slug_id: str
    shop_slug_id: str
    event_id: UUID
    shop_id: UUID
    shops_product_id: str
    shop_name: str
    shop_type: ShopTypeData
    title: LocalizedTextData
    state: ProductStateData
    url: str
    images: list[ProductImageData]
    created: datetime.datetime
    updated: datetime.datetime
    price: None | PriceData | Unset = UNSET
    additional_properties: dict[str, Any] = _attrs_field(init=False, factory=dict)

    def to_dict(self) -> dict[str, Any]:
        from ..models.price_data import PriceData

        product_id = str(self.product_id)

        product_slug_id = self.product_slug_id

        shop_slug_id = self.shop_slug_id

        event_id = str(self.event_id)

        shop_id = str(self.shop_id)

        shops_product_id = self.shops_product_id

        shop_name = self.shop_name

        shop_type = self.shop_type.value

        title = self.title.to_dict()

        state = self.state.value

        url = self.url

        images = []
        for images_item_data in self.images:
            images_item = images_item_data.to_dict()
            images.append(images_item)

        created = self.created.isoformat()

        updated = self.updated.isoformat()

        price: dict[str, Any] | None | Unset
        if isinstance(self.price, Unset):
            price = UNSET
        elif isinstance(self.price, PriceData):
            price = self.price.to_dict()
        else:
            price = self.price

        field_dict: dict[str, Any] = {}
        field_dict.update(self.additional_properties)
        field_dict.update(
            {
                "productId": product_id,
                "productSlugId": product_slug_id,
                "shopSlugId": shop_slug_id,
                "eventId": event_id,
                "shopId": shop_id,
                "shopsProductId": shops_product_id,
                "shopName": shop_name,
                "shopType": shop_type,
                "title": title,
                "state": state,
                "url": url,
                "images": images,
                "created": created,
                "updated": updated,
            }
        )
        if price is not UNSET:
            field_dict["price"] = price

        return field_dict

    @classmethod
    def from_dict(cls: type[T], src_dict: Mapping[str, Any]) -> T:
        from ..models.localized_text_data import LocalizedTextData
        from ..models.price_data import PriceData
        from ..models.product_image_data import ProductImageData

        d = dict(src_dict)
        product_id = UUID(d.pop("productId"))

        product_slug_id = d.pop("productSlugId")

        shop_slug_id = d.pop("shopSlugId")

        event_id = UUID(d.pop("eventId"))

        shop_id = UUID(d.pop("shopId"))

        shops_product_id = d.pop("shopsProductId")

        shop_name = d.pop("shopName")

        shop_type = ShopTypeData(d.pop("shopType"))

        title = LocalizedTextData.from_dict(d.pop("title"))

        state = ProductStateData(d.pop("state"))

        url = d.pop("url")

        images = []
        _images = d.pop("images")
        for images_item_data in _images:
            images_item = ProductImageData.from_dict(images_item_data)

            images.append(images_item)

        created = isoparse(d.pop("created"))

        updated = isoparse(d.pop("updated"))

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

        get_product_summary_data = cls(
            product_id=product_id,
            product_slug_id=product_slug_id,
            shop_slug_id=shop_slug_id,
            event_id=event_id,
            shop_id=shop_id,
            shops_product_id=shops_product_id,
            shop_name=shop_name,
            shop_type=shop_type,
            title=title,
            state=state,
            url=url,
            images=images,
            created=created,
            updated=updated,
            price=price,
        )

        get_product_summary_data.additional_properties = d
        return get_product_summary_data

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
