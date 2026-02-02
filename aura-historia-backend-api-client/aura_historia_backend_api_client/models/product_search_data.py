from __future__ import annotations

from collections.abc import Mapping
from typing import TYPE_CHECKING, Any, TypeVar, cast

from attrs import define as _attrs_define
from attrs import field as _attrs_field

from ..models.authenticity_data import AuthenticityData
from ..models.condition_data import ConditionData
from ..models.currency_data import CurrencyData
from ..models.language_data import LanguageData
from ..models.product_state_data import ProductStateData
from ..models.provenance_data import ProvenanceData
from ..models.restoration_data import RestorationData
from ..models.shop_type_data import ShopTypeData
from ..types import UNSET, Unset

if TYPE_CHECKING:
    from ..models.range_query_date_time import RangeQueryDateTime
    from ..models.range_query_int_32 import RangeQueryInt32
    from ..models.range_query_u_int_64 import RangeQueryUInt64


T = TypeVar("T", bound="ProductSearchData")


@_attrs_define
class ProductSearchData:
    """Product search configuration with query parameters and filtering options

    Attributes:
        language (LanguageData): Supported languages (ISO 639-1 codes):
            - de: German (includes de-DE, de-AT, de-CH, de-LU, de-LI)
            - en: English (includes en-US, en-GB, en-AU, en-CA, en-NZ, en-IE)
            - fr: French (includes fr-FR, fr-CA, fr-BE, fr-CH, fr-LU)
            - es: Spanish (includes es-ES, es-MX, es-AR, es-CO, es-CL, es-PE, es-VE)
             Example: de.
        currency (CurrencyData): Supported currencies (ISO 4217 codes):
            - EUR: Euro
            - GBP: British Pound
            - USD: US Dollar
            - AUD: Australian Dollar
            - CAD: Canadian Dollar
            - NZD: New Zealand Dollar
             Example: EUR.
        product_query (str): Text query for searching products (minimum 3 characters) Example: smartphone case.
        shop_name (list[str] | Unset): Optional filter by exact shop names (keyword matching).
            Filters products to only those from shops with names exactly matching one of the provided values.
            This is an exact match filter, not a fuzzy text search.
             Example: ["Sotheby's", "Christie's"].
        exclude_shop_name (list[str] | Unset): Optional filter to exclude products from specific shop names (keyword
            matching).
            Products from shops with names exactly matching one of the provided values will be excluded from results.
            This is an exact match filter, not a fuzzy text search.
            Empty array means no shops are excluded.
             Example: ['Heritage Auctions', "Christie's"].
        shop_type (list[ShopTypeData] | None | Unset): Optional filter by shop types Example: ['COMMERCIAL_DEALER'].
        price (None | RangeQueryUInt64 | Unset): Optional price range filter in minor currency units
        state (list[ProductStateData] | None | Unset): Optional filter by product states Example: ['AVAILABLE'].
        origin_year (None | RangeQueryInt32 | Unset): Optional filter by product origin year range
        authenticity (list[AuthenticityData] | None | Unset): Optional filter by authenticity classifications Example:
            ['ORIGINAL'].
        condition (list[ConditionData] | None | Unset): Optional filter by product condition assessments Example:
            ['EXCELLENT'].
        provenance (list[ProvenanceData] | None | Unset): Optional filter by provenance documentation levels Example:
            ['PARTIAL'].
        restoration (list[RestorationData] | None | Unset): Optional filter by restoration work levels Example:
            ['UNKNOWN'].
        created (None | RangeQueryDateTime | Unset): Optional filter by product creation date range
        updated (None | RangeQueryDateTime | Unset): Optional filter by product last updated date range
        auction_start (None | RangeQueryDateTime | Unset): Optional filter by auction start datetime range.
            Filters products by when their auction windows begin.
            Only matches products that have auction start times set.
        auction_end (None | RangeQueryDateTime | Unset): Optional filter by auction end datetime range.
            Filters products by when their auction windows end.
            Only matches products that have auction end times set.
    """

    language: LanguageData
    currency: CurrencyData
    product_query: str
    shop_name: list[str] | Unset = UNSET
    exclude_shop_name: list[str] | Unset = UNSET
    shop_type: list[ShopTypeData] | None | Unset = UNSET
    price: None | RangeQueryUInt64 | Unset = UNSET
    state: list[ProductStateData] | None | Unset = UNSET
    origin_year: None | RangeQueryInt32 | Unset = UNSET
    authenticity: list[AuthenticityData] | None | Unset = UNSET
    condition: list[ConditionData] | None | Unset = UNSET
    provenance: list[ProvenanceData] | None | Unset = UNSET
    restoration: list[RestorationData] | None | Unset = UNSET
    created: None | RangeQueryDateTime | Unset = UNSET
    updated: None | RangeQueryDateTime | Unset = UNSET
    auction_start: None | RangeQueryDateTime | Unset = UNSET
    auction_end: None | RangeQueryDateTime | Unset = UNSET
    additional_properties: dict[str, Any] = _attrs_field(init=False, factory=dict)

    def to_dict(self) -> dict[str, Any]:
        from ..models.range_query_date_time import RangeQueryDateTime
        from ..models.range_query_int_32 import RangeQueryInt32
        from ..models.range_query_u_int_64 import RangeQueryUInt64

        language = self.language.value

        currency = self.currency.value

        product_query = self.product_query

        shop_name: list[str] | Unset = UNSET
        if not isinstance(self.shop_name, Unset):
            shop_name = self.shop_name

        exclude_shop_name: list[str] | Unset = UNSET
        if not isinstance(self.exclude_shop_name, Unset):
            exclude_shop_name = self.exclude_shop_name

        shop_type: list[str] | None | Unset
        if isinstance(self.shop_type, Unset):
            shop_type = UNSET
        elif isinstance(self.shop_type, list):
            shop_type = []
            for shop_type_type_0_item_data in self.shop_type:
                shop_type_type_0_item = shop_type_type_0_item_data.value
                shop_type.append(shop_type_type_0_item)

        else:
            shop_type = self.shop_type

        price: dict[str, Any] | None | Unset
        if isinstance(self.price, Unset):
            price = UNSET
        elif isinstance(self.price, RangeQueryUInt64):
            price = self.price.to_dict()
        else:
            price = self.price

        state: list[str] | None | Unset
        if isinstance(self.state, Unset):
            state = UNSET
        elif isinstance(self.state, list):
            state = []
            for state_type_0_item_data in self.state:
                state_type_0_item = state_type_0_item_data.value
                state.append(state_type_0_item)

        else:
            state = self.state

        origin_year: dict[str, Any] | None | Unset
        if isinstance(self.origin_year, Unset):
            origin_year = UNSET
        elif isinstance(self.origin_year, RangeQueryInt32):
            origin_year = self.origin_year.to_dict()
        else:
            origin_year = self.origin_year

        authenticity: list[str] | None | Unset
        if isinstance(self.authenticity, Unset):
            authenticity = UNSET
        elif isinstance(self.authenticity, list):
            authenticity = []
            for authenticity_type_0_item_data in self.authenticity:
                authenticity_type_0_item = authenticity_type_0_item_data.value
                authenticity.append(authenticity_type_0_item)

        else:
            authenticity = self.authenticity

        condition: list[str] | None | Unset
        if isinstance(self.condition, Unset):
            condition = UNSET
        elif isinstance(self.condition, list):
            condition = []
            for condition_type_0_item_data in self.condition:
                condition_type_0_item = condition_type_0_item_data.value
                condition.append(condition_type_0_item)

        else:
            condition = self.condition

        provenance: list[str] | None | Unset
        if isinstance(self.provenance, Unset):
            provenance = UNSET
        elif isinstance(self.provenance, list):
            provenance = []
            for provenance_type_0_item_data in self.provenance:
                provenance_type_0_item = provenance_type_0_item_data.value
                provenance.append(provenance_type_0_item)

        else:
            provenance = self.provenance

        restoration: list[str] | None | Unset
        if isinstance(self.restoration, Unset):
            restoration = UNSET
        elif isinstance(self.restoration, list):
            restoration = []
            for restoration_type_0_item_data in self.restoration:
                restoration_type_0_item = restoration_type_0_item_data.value
                restoration.append(restoration_type_0_item)

        else:
            restoration = self.restoration

        created: dict[str, Any] | None | Unset
        if isinstance(self.created, Unset):
            created = UNSET
        elif isinstance(self.created, RangeQueryDateTime):
            created = self.created.to_dict()
        else:
            created = self.created

        updated: dict[str, Any] | None | Unset
        if isinstance(self.updated, Unset):
            updated = UNSET
        elif isinstance(self.updated, RangeQueryDateTime):
            updated = self.updated.to_dict()
        else:
            updated = self.updated

        auction_start: dict[str, Any] | None | Unset
        if isinstance(self.auction_start, Unset):
            auction_start = UNSET
        elif isinstance(self.auction_start, RangeQueryDateTime):
            auction_start = self.auction_start.to_dict()
        else:
            auction_start = self.auction_start

        auction_end: dict[str, Any] | None | Unset
        if isinstance(self.auction_end, Unset):
            auction_end = UNSET
        elif isinstance(self.auction_end, RangeQueryDateTime):
            auction_end = self.auction_end.to_dict()
        else:
            auction_end = self.auction_end

        field_dict: dict[str, Any] = {}
        field_dict.update(self.additional_properties)
        field_dict.update(
            {
                "language": language,
                "currency": currency,
                "productQuery": product_query,
            }
        )
        if shop_name is not UNSET:
            field_dict["shopName"] = shop_name
        if exclude_shop_name is not UNSET:
            field_dict["excludeShopName"] = exclude_shop_name
        if shop_type is not UNSET:
            field_dict["shopType"] = shop_type
        if price is not UNSET:
            field_dict["price"] = price
        if state is not UNSET:
            field_dict["state"] = state
        if origin_year is not UNSET:
            field_dict["originYear"] = origin_year
        if authenticity is not UNSET:
            field_dict["authenticity"] = authenticity
        if condition is not UNSET:
            field_dict["condition"] = condition
        if provenance is not UNSET:
            field_dict["provenance"] = provenance
        if restoration is not UNSET:
            field_dict["restoration"] = restoration
        if created is not UNSET:
            field_dict["created"] = created
        if updated is not UNSET:
            field_dict["updated"] = updated
        if auction_start is not UNSET:
            field_dict["auctionStart"] = auction_start
        if auction_end is not UNSET:
            field_dict["auctionEnd"] = auction_end

        return field_dict

    @classmethod
    def from_dict(cls: type[T], src_dict: Mapping[str, Any]) -> T:
        from ..models.range_query_date_time import RangeQueryDateTime
        from ..models.range_query_int_32 import RangeQueryInt32
        from ..models.range_query_u_int_64 import RangeQueryUInt64

        d = dict(src_dict)
        language = LanguageData(d.pop("language"))

        currency = CurrencyData(d.pop("currency"))

        product_query = d.pop("productQuery")

        shop_name = cast(list[str], d.pop("shopName", UNSET))

        exclude_shop_name = cast(list[str], d.pop("excludeShopName", UNSET))

        def _parse_shop_type(data: object) -> list[ShopTypeData] | None | Unset:
            if data is None:
                return data
            if isinstance(data, Unset):
                return data
            try:
                if not isinstance(data, list):
                    raise TypeError()
                shop_type_type_0 = []
                _shop_type_type_0 = data
                for shop_type_type_0_item_data in _shop_type_type_0:
                    shop_type_type_0_item = ShopTypeData(shop_type_type_0_item_data)

                    shop_type_type_0.append(shop_type_type_0_item)

                return shop_type_type_0
            except (TypeError, ValueError, AttributeError, KeyError):
                pass
            return cast(list[ShopTypeData] | None | Unset, data)

        shop_type = _parse_shop_type(d.pop("shopType", UNSET))

        def _parse_price(data: object) -> None | RangeQueryUInt64 | Unset:
            if data is None:
                return data
            if isinstance(data, Unset):
                return data
            try:
                if not isinstance(data, dict):
                    raise TypeError()
                price_type_1 = RangeQueryUInt64.from_dict(data)

                return price_type_1
            except (TypeError, ValueError, AttributeError, KeyError):
                pass
            return cast(None | RangeQueryUInt64 | Unset, data)

        price = _parse_price(d.pop("price", UNSET))

        def _parse_state(data: object) -> list[ProductStateData] | None | Unset:
            if data is None:
                return data
            if isinstance(data, Unset):
                return data
            try:
                if not isinstance(data, list):
                    raise TypeError()
                state_type_0 = []
                _state_type_0 = data
                for state_type_0_item_data in _state_type_0:
                    state_type_0_item = ProductStateData(state_type_0_item_data)

                    state_type_0.append(state_type_0_item)

                return state_type_0
            except (TypeError, ValueError, AttributeError, KeyError):
                pass
            return cast(list[ProductStateData] | None | Unset, data)

        state = _parse_state(d.pop("state", UNSET))

        def _parse_origin_year(data: object) -> None | RangeQueryInt32 | Unset:
            if data is None:
                return data
            if isinstance(data, Unset):
                return data
            try:
                if not isinstance(data, dict):
                    raise TypeError()
                origin_year_type_1 = RangeQueryInt32.from_dict(data)

                return origin_year_type_1
            except (TypeError, ValueError, AttributeError, KeyError):
                pass
            return cast(None | RangeQueryInt32 | Unset, data)

        origin_year = _parse_origin_year(d.pop("originYear", UNSET))

        def _parse_authenticity(data: object) -> list[AuthenticityData] | None | Unset:
            if data is None:
                return data
            if isinstance(data, Unset):
                return data
            try:
                if not isinstance(data, list):
                    raise TypeError()
                authenticity_type_0 = []
                _authenticity_type_0 = data
                for authenticity_type_0_item_data in _authenticity_type_0:
                    authenticity_type_0_item = AuthenticityData(authenticity_type_0_item_data)

                    authenticity_type_0.append(authenticity_type_0_item)

                return authenticity_type_0
            except (TypeError, ValueError, AttributeError, KeyError):
                pass
            return cast(list[AuthenticityData] | None | Unset, data)

        authenticity = _parse_authenticity(d.pop("authenticity", UNSET))

        def _parse_condition(data: object) -> list[ConditionData] | None | Unset:
            if data is None:
                return data
            if isinstance(data, Unset):
                return data
            try:
                if not isinstance(data, list):
                    raise TypeError()
                condition_type_0 = []
                _condition_type_0 = data
                for condition_type_0_item_data in _condition_type_0:
                    condition_type_0_item = ConditionData(condition_type_0_item_data)

                    condition_type_0.append(condition_type_0_item)

                return condition_type_0
            except (TypeError, ValueError, AttributeError, KeyError):
                pass
            return cast(list[ConditionData] | None | Unset, data)

        condition = _parse_condition(d.pop("condition", UNSET))

        def _parse_provenance(data: object) -> list[ProvenanceData] | None | Unset:
            if data is None:
                return data
            if isinstance(data, Unset):
                return data
            try:
                if not isinstance(data, list):
                    raise TypeError()
                provenance_type_0 = []
                _provenance_type_0 = data
                for provenance_type_0_item_data in _provenance_type_0:
                    provenance_type_0_item = ProvenanceData(provenance_type_0_item_data)

                    provenance_type_0.append(provenance_type_0_item)

                return provenance_type_0
            except (TypeError, ValueError, AttributeError, KeyError):
                pass
            return cast(list[ProvenanceData] | None | Unset, data)

        provenance = _parse_provenance(d.pop("provenance", UNSET))

        def _parse_restoration(data: object) -> list[RestorationData] | None | Unset:
            if data is None:
                return data
            if isinstance(data, Unset):
                return data
            try:
                if not isinstance(data, list):
                    raise TypeError()
                restoration_type_0 = []
                _restoration_type_0 = data
                for restoration_type_0_item_data in _restoration_type_0:
                    restoration_type_0_item = RestorationData(restoration_type_0_item_data)

                    restoration_type_0.append(restoration_type_0_item)

                return restoration_type_0
            except (TypeError, ValueError, AttributeError, KeyError):
                pass
            return cast(list[RestorationData] | None | Unset, data)

        restoration = _parse_restoration(d.pop("restoration", UNSET))

        def _parse_created(data: object) -> None | RangeQueryDateTime | Unset:
            if data is None:
                return data
            if isinstance(data, Unset):
                return data
            try:
                if not isinstance(data, dict):
                    raise TypeError()
                created_type_1 = RangeQueryDateTime.from_dict(data)

                return created_type_1
            except (TypeError, ValueError, AttributeError, KeyError):
                pass
            return cast(None | RangeQueryDateTime | Unset, data)

        created = _parse_created(d.pop("created", UNSET))

        def _parse_updated(data: object) -> None | RangeQueryDateTime | Unset:
            if data is None:
                return data
            if isinstance(data, Unset):
                return data
            try:
                if not isinstance(data, dict):
                    raise TypeError()
                updated_type_1 = RangeQueryDateTime.from_dict(data)

                return updated_type_1
            except (TypeError, ValueError, AttributeError, KeyError):
                pass
            return cast(None | RangeQueryDateTime | Unset, data)

        updated = _parse_updated(d.pop("updated", UNSET))

        def _parse_auction_start(data: object) -> None | RangeQueryDateTime | Unset:
            if data is None:
                return data
            if isinstance(data, Unset):
                return data
            try:
                if not isinstance(data, dict):
                    raise TypeError()
                auction_start_type_1 = RangeQueryDateTime.from_dict(data)

                return auction_start_type_1
            except (TypeError, ValueError, AttributeError, KeyError):
                pass
            return cast(None | RangeQueryDateTime | Unset, data)

        auction_start = _parse_auction_start(d.pop("auctionStart", UNSET))

        def _parse_auction_end(data: object) -> None | RangeQueryDateTime | Unset:
            if data is None:
                return data
            if isinstance(data, Unset):
                return data
            try:
                if not isinstance(data, dict):
                    raise TypeError()
                auction_end_type_1 = RangeQueryDateTime.from_dict(data)

                return auction_end_type_1
            except (TypeError, ValueError, AttributeError, KeyError):
                pass
            return cast(None | RangeQueryDateTime | Unset, data)

        auction_end = _parse_auction_end(d.pop("auctionEnd", UNSET))

        product_search_data = cls(
            language=language,
            currency=currency,
            product_query=product_query,
            shop_name=shop_name,
            exclude_shop_name=exclude_shop_name,
            shop_type=shop_type,
            price=price,
            state=state,
            origin_year=origin_year,
            authenticity=authenticity,
            condition=condition,
            provenance=provenance,
            restoration=restoration,
            created=created,
            updated=updated,
            auction_start=auction_start,
            auction_end=auction_end,
        )

        product_search_data.additional_properties = d
        return product_search_data

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
