from __future__ import annotations

import datetime
from collections.abc import Mapping
from typing import TYPE_CHECKING, Any, TypeVar, cast
from uuid import UUID

from attrs import define as _attrs_define
from attrs import field as _attrs_field
from dateutil.parser import isoparse

from ..models.authenticity_data import AuthenticityData
from ..models.condition_data import ConditionData
from ..models.product_state_data import ProductStateData
from ..models.provenance_data import ProvenanceData
from ..models.restoration_data import RestorationData
from ..models.shop_type_data import ShopTypeData
from ..types import UNSET, Unset

if TYPE_CHECKING:
    from ..models.localized_text_data import LocalizedTextData
    from ..models.price_data import PriceData
    from ..models.product_image_data import ProductImageData


T = TypeVar("T", bound="GetProductData")


@_attrs_define
class GetProductData:
    """Complete product information including metadata and localized content

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
        description (LocalizedTextData | None | Unset): Optional product description
        price (None | PriceData | Unset): Optional product price
        price_estimate_min (None | PriceData | Unset): Optional minimum estimated price for the product
        price_estimate_max (None | PriceData | Unset): Optional maximum estimated price for the product
        origin_year_min (int | None | Unset): Lower end of the year range when the antique is estimated to have
            originated.
            Only present when the origin year is expressed as a range (originYear will be null).
            Can be present alone (without originYearMax) to indicate "after this year".
             Example: 1900.
        origin_year (int | None | Unset): Exact year the antique is estimated to have originated.
            When this value is present, both originYearMin and originYearMax will be null.
             Example: 1837.
        origin_year_max (int | None | Unset): Upper end of the year range when the antique is estimated to have
            originated.
            Only present when the origin year is expressed as a range (originYear will be null).
            Can be present alone (without originYearMin) to indicate "before this year".
             Example: 1950.
        authenticity (AuthenticityData | None | Unset): Authenticity classification of the antique product
        condition (ConditionData | None | Unset): Physical condition assessment of the antique product
        provenance (None | ProvenanceData | Unset): Documentation trail and ownership history of the antique product
        restoration (None | RestorationData | Unset): Level of restoration work performed on the antique product
        auction_start (datetime.datetime | None | Unset): Start datetime of the auction window for this product (RFC3339
            format).
            Only present for products from auction houses with scheduled auction times.
            Used to indicate when bidding begins or when the item will be auctioned.
             Example: 2026-02-15T10:00:00Z.
        auction_end (datetime.datetime | None | Unset): End datetime of the auction window for this product (RFC3339
            format).
            Only present for products from auction houses with scheduled auction times.
            Used to indicate when bidding ends or when the auction session concludes.
             Example: 2026-02-15T14:00:00Z.
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
    description: LocalizedTextData | None | Unset = UNSET
    price: None | PriceData | Unset = UNSET
    price_estimate_min: None | PriceData | Unset = UNSET
    price_estimate_max: None | PriceData | Unset = UNSET
    origin_year_min: int | None | Unset = UNSET
    origin_year: int | None | Unset = UNSET
    origin_year_max: int | None | Unset = UNSET
    authenticity: AuthenticityData | None | Unset = UNSET
    condition: ConditionData | None | Unset = UNSET
    provenance: None | ProvenanceData | Unset = UNSET
    restoration: None | RestorationData | Unset = UNSET
    auction_start: datetime.datetime | None | Unset = UNSET
    auction_end: datetime.datetime | None | Unset = UNSET
    additional_properties: dict[str, Any] = _attrs_field(init=False, factory=dict)

    def to_dict(self) -> dict[str, Any]:
        from ..models.localized_text_data import LocalizedTextData
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

        origin_year_min: int | None | Unset
        if isinstance(self.origin_year_min, Unset):
            origin_year_min = UNSET
        else:
            origin_year_min = self.origin_year_min

        origin_year: int | None | Unset
        if isinstance(self.origin_year, Unset):
            origin_year = UNSET
        else:
            origin_year = self.origin_year

        origin_year_max: int | None | Unset
        if isinstance(self.origin_year_max, Unset):
            origin_year_max = UNSET
        else:
            origin_year_max = self.origin_year_max

        authenticity: None | str | Unset
        if isinstance(self.authenticity, Unset):
            authenticity = UNSET
        elif isinstance(self.authenticity, AuthenticityData):
            authenticity = self.authenticity.value
        else:
            authenticity = self.authenticity

        condition: None | str | Unset
        if isinstance(self.condition, Unset):
            condition = UNSET
        elif isinstance(self.condition, ConditionData):
            condition = self.condition.value
        else:
            condition = self.condition

        provenance: None | str | Unset
        if isinstance(self.provenance, Unset):
            provenance = UNSET
        elif isinstance(self.provenance, ProvenanceData):
            provenance = self.provenance.value
        else:
            provenance = self.provenance

        restoration: None | str | Unset
        if isinstance(self.restoration, Unset):
            restoration = UNSET
        elif isinstance(self.restoration, RestorationData):
            restoration = self.restoration.value
        else:
            restoration = self.restoration

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
        if description is not UNSET:
            field_dict["description"] = description
        if price is not UNSET:
            field_dict["price"] = price
        if price_estimate_min is not UNSET:
            field_dict["priceEstimateMin"] = price_estimate_min
        if price_estimate_max is not UNSET:
            field_dict["priceEstimateMax"] = price_estimate_max
        if origin_year_min is not UNSET:
            field_dict["originYearMin"] = origin_year_min
        if origin_year is not UNSET:
            field_dict["originYear"] = origin_year
        if origin_year_max is not UNSET:
            field_dict["originYearMax"] = origin_year_max
        if authenticity is not UNSET:
            field_dict["authenticity"] = authenticity
        if condition is not UNSET:
            field_dict["condition"] = condition
        if provenance is not UNSET:
            field_dict["provenance"] = provenance
        if restoration is not UNSET:
            field_dict["restoration"] = restoration
        if auction_start is not UNSET:
            field_dict["auctionStart"] = auction_start
        if auction_end is not UNSET:
            field_dict["auctionEnd"] = auction_end

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

        def _parse_origin_year_min(data: object) -> int | None | Unset:
            if data is None:
                return data
            if isinstance(data, Unset):
                return data
            return cast(int | None | Unset, data)

        origin_year_min = _parse_origin_year_min(d.pop("originYearMin", UNSET))

        def _parse_origin_year(data: object) -> int | None | Unset:
            if data is None:
                return data
            if isinstance(data, Unset):
                return data
            return cast(int | None | Unset, data)

        origin_year = _parse_origin_year(d.pop("originYear", UNSET))

        def _parse_origin_year_max(data: object) -> int | None | Unset:
            if data is None:
                return data
            if isinstance(data, Unset):
                return data
            return cast(int | None | Unset, data)

        origin_year_max = _parse_origin_year_max(d.pop("originYearMax", UNSET))

        def _parse_authenticity(data: object) -> AuthenticityData | None | Unset:
            if data is None:
                return data
            if isinstance(data, Unset):
                return data
            try:
                if not isinstance(data, str):
                    raise TypeError()
                authenticity_type_1 = AuthenticityData(data)

                return authenticity_type_1
            except (TypeError, ValueError, AttributeError, KeyError):
                pass
            return cast(AuthenticityData | None | Unset, data)

        authenticity = _parse_authenticity(d.pop("authenticity", UNSET))

        def _parse_condition(data: object) -> ConditionData | None | Unset:
            if data is None:
                return data
            if isinstance(data, Unset):
                return data
            try:
                if not isinstance(data, str):
                    raise TypeError()
                condition_type_1 = ConditionData(data)

                return condition_type_1
            except (TypeError, ValueError, AttributeError, KeyError):
                pass
            return cast(ConditionData | None | Unset, data)

        condition = _parse_condition(d.pop("condition", UNSET))

        def _parse_provenance(data: object) -> None | ProvenanceData | Unset:
            if data is None:
                return data
            if isinstance(data, Unset):
                return data
            try:
                if not isinstance(data, str):
                    raise TypeError()
                provenance_type_1 = ProvenanceData(data)

                return provenance_type_1
            except (TypeError, ValueError, AttributeError, KeyError):
                pass
            return cast(None | ProvenanceData | Unset, data)

        provenance = _parse_provenance(d.pop("provenance", UNSET))

        def _parse_restoration(data: object) -> None | RestorationData | Unset:
            if data is None:
                return data
            if isinstance(data, Unset):
                return data
            try:
                if not isinstance(data, str):
                    raise TypeError()
                restoration_type_1 = RestorationData(data)

                return restoration_type_1
            except (TypeError, ValueError, AttributeError, KeyError):
                pass
            return cast(None | RestorationData | Unset, data)

        restoration = _parse_restoration(d.pop("restoration", UNSET))

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

        get_product_data = cls(
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
            description=description,
            price=price,
            price_estimate_min=price_estimate_min,
            price_estimate_max=price_estimate_max,
            origin_year_min=origin_year_min,
            origin_year=origin_year,
            origin_year_max=origin_year_max,
            authenticity=authenticity,
            condition=condition,
            provenance=provenance,
            restoration=restoration,
            auction_start=auction_start,
            auction_end=auction_end,
        )

        get_product_data.additional_properties = d
        return get_product_data

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
