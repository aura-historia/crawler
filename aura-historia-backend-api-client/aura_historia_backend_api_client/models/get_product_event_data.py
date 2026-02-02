from __future__ import annotations

import datetime
from collections.abc import Mapping
from typing import TYPE_CHECKING, Any, TypeVar
from uuid import UUID

from attrs import define as _attrs_define
from attrs import field as _attrs_field
from dateutil.parser import isoparse

from ..models.product_event_type_data import ProductEventTypeData

if TYPE_CHECKING:
    from ..models.product_created_event_payload_data import ProductCreatedEventPayloadData
    from ..models.product_event_price_changed_payload_data import ProductEventPriceChangedPayloadData
    from ..models.product_event_price_discovered_payload_data import ProductEventPriceDiscoveredPayloadData
    from ..models.product_event_price_removed_payload_data import ProductEventPriceRemovedPayloadData
    from ..models.product_event_state_changed_payload_data import ProductEventStateChangedPayloadData


T = TypeVar("T", bound="GetProductEventData")


@_attrs_define
class GetProductEventData:
    """Historical event for a product

    Attributes:
        event_type (ProductEventTypeData): Types of events that can occur for a product Example: STATE_AVAILABLE.
        product_id (UUID): Unique internal identifier for the product
        event_id (UUID): Unique identifier for this event
        shop_id (UUID): Unique identifier of the shop
        shops_product_id (str): Shop's unique identifier for the product. Can be any arbitrary string.
        payload (ProductCreatedEventPayloadData | ProductEventPriceChangedPayloadData |
            ProductEventPriceDiscoveredPayloadData | ProductEventPriceRemovedPayloadData |
            ProductEventStateChangedPayloadData): Event-specific payload data. The structure varies depending on the event
            type:
            - CREATED: ProductCreatedEventPayloadData (initial product state and optional price)
            - STATE_LISTED, STATE_AVAILABLE, STATE_RESERVED, STATE_SOLD, STATE_REMOVED, STATE_UNKNOWN:
            ProductEventStateChangedPayloadData (old and new state)
            - PRICE_DISCOVERED: ProductEventPriceDiscoveredPayloadData (new price only, when price is first detected)
            - PRICE_DROPPED, PRICE_INCREASED: ProductEventPriceChangedPayloadData (old and new price)
            - PRICE_REMOVED: ProductEventPriceRemovedPayloadData (old price only, when price is removed)
        timestamp (datetime.datetime): When the event occurred (RFC3339 format)
    """

    event_type: ProductEventTypeData
    product_id: UUID
    event_id: UUID
    shop_id: UUID
    shops_product_id: str
    payload: (
        ProductCreatedEventPayloadData
        | ProductEventPriceChangedPayloadData
        | ProductEventPriceDiscoveredPayloadData
        | ProductEventPriceRemovedPayloadData
        | ProductEventStateChangedPayloadData
    )
    timestamp: datetime.datetime
    additional_properties: dict[str, Any] = _attrs_field(init=False, factory=dict)

    def to_dict(self) -> dict[str, Any]:
        from ..models.product_created_event_payload_data import ProductCreatedEventPayloadData
        from ..models.product_event_price_changed_payload_data import ProductEventPriceChangedPayloadData
        from ..models.product_event_price_discovered_payload_data import ProductEventPriceDiscoveredPayloadData
        from ..models.product_event_state_changed_payload_data import ProductEventStateChangedPayloadData

        event_type = self.event_type.value

        product_id = str(self.product_id)

        event_id = str(self.event_id)

        shop_id = str(self.shop_id)

        shops_product_id = self.shops_product_id

        payload: dict[str, Any]
        if isinstance(self.payload, ProductCreatedEventPayloadData):
            payload = self.payload.to_dict()
        elif isinstance(self.payload, ProductEventStateChangedPayloadData):
            payload = self.payload.to_dict()
        elif isinstance(self.payload, ProductEventPriceDiscoveredPayloadData):
            payload = self.payload.to_dict()
        elif isinstance(self.payload, ProductEventPriceChangedPayloadData):
            payload = self.payload.to_dict()
        else:
            payload = self.payload.to_dict()

        timestamp = self.timestamp.isoformat()

        field_dict: dict[str, Any] = {}
        field_dict.update(self.additional_properties)
        field_dict.update(
            {
                "eventType": event_type,
                "productId": product_id,
                "eventId": event_id,
                "shopId": shop_id,
                "shopsProductId": shops_product_id,
                "payload": payload,
                "timestamp": timestamp,
            }
        )

        return field_dict

    @classmethod
    def from_dict(cls: type[T], src_dict: Mapping[str, Any]) -> T:
        from ..models.product_created_event_payload_data import ProductCreatedEventPayloadData
        from ..models.product_event_price_changed_payload_data import ProductEventPriceChangedPayloadData
        from ..models.product_event_price_discovered_payload_data import ProductEventPriceDiscoveredPayloadData
        from ..models.product_event_price_removed_payload_data import ProductEventPriceRemovedPayloadData
        from ..models.product_event_state_changed_payload_data import ProductEventStateChangedPayloadData

        d = dict(src_dict)
        event_type = ProductEventTypeData(d.pop("eventType"))

        product_id = UUID(d.pop("productId"))

        event_id = UUID(d.pop("eventId"))

        shop_id = UUID(d.pop("shopId"))

        shops_product_id = d.pop("shopsProductId")

        def _parse_payload(
            data: object,
        ) -> (
            ProductCreatedEventPayloadData
            | ProductEventPriceChangedPayloadData
            | ProductEventPriceDiscoveredPayloadData
            | ProductEventPriceRemovedPayloadData
            | ProductEventStateChangedPayloadData
        ):
            try:
                if not isinstance(data, dict):
                    raise TypeError()
                componentsschemas_product_event_payload_data_type_0 = ProductCreatedEventPayloadData.from_dict(data)

                return componentsschemas_product_event_payload_data_type_0
            except (TypeError, ValueError, AttributeError, KeyError):
                pass
            try:
                if not isinstance(data, dict):
                    raise TypeError()
                componentsschemas_product_event_payload_data_type_1 = ProductEventStateChangedPayloadData.from_dict(
                    data
                )

                return componentsschemas_product_event_payload_data_type_1
            except (TypeError, ValueError, AttributeError, KeyError):
                pass
            try:
                if not isinstance(data, dict):
                    raise TypeError()
                componentsschemas_product_event_payload_data_type_2 = ProductEventPriceDiscoveredPayloadData.from_dict(
                    data
                )

                return componentsschemas_product_event_payload_data_type_2
            except (TypeError, ValueError, AttributeError, KeyError):
                pass
            try:
                if not isinstance(data, dict):
                    raise TypeError()
                componentsschemas_product_event_payload_data_type_3 = ProductEventPriceChangedPayloadData.from_dict(
                    data
                )

                return componentsschemas_product_event_payload_data_type_3
            except (TypeError, ValueError, AttributeError, KeyError):
                pass
            if not isinstance(data, dict):
                raise TypeError()
            componentsschemas_product_event_payload_data_type_4 = ProductEventPriceRemovedPayloadData.from_dict(data)

            return componentsschemas_product_event_payload_data_type_4

        payload = _parse_payload(d.pop("payload"))

        timestamp = isoparse(d.pop("timestamp"))

        get_product_event_data = cls(
            event_type=event_type,
            product_id=product_id,
            event_id=event_id,
            shop_id=shop_id,
            shops_product_id=shops_product_id,
            payload=payload,
            timestamp=timestamp,
        )

        get_product_event_data.additional_properties = d
        return get_product_event_data

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
