from __future__ import annotations

import datetime
from collections.abc import Mapping
from typing import Any, TypeVar, cast
from uuid import UUID

from attrs import define as _attrs_define
from attrs import field as _attrs_field
from dateutil.parser import isoparse

from ..models.currency_data import CurrencyData
from ..models.language_data import LanguageData
from ..types import UNSET, Unset

T = TypeVar("T", bound="GetUserAccountData")


@_attrs_define
class GetUserAccountData:
    """Complete user account information

    Attributes:
        user_id (UUID): Unique identifier for the user Example: 550e8400-e29b-41d4-a716-446655440000.
        email (str): User's email address Example: user@example.com.
        created (datetime.datetime): When the user account was created (RFC3339 format) Example: 2024-01-01T10:00:00Z.
        updated (datetime.datetime): When the user account was last updated (RFC3339 format) Example:
            2024-01-01T12:00:00Z.
        first_name (None | str | Unset): User's first name (optional, max 64 characters) Example: John.
        last_name (None | str | Unset): User's last name (optional, max 64 characters) Example: Doe.
        language (LanguageData | None | Unset): User's preferred language (optional)
        currency (CurrencyData | None | Unset): User's preferred currency (optional)
    """

    user_id: UUID
    email: str
    created: datetime.datetime
    updated: datetime.datetime
    first_name: None | str | Unset = UNSET
    last_name: None | str | Unset = UNSET
    language: LanguageData | None | Unset = UNSET
    currency: CurrencyData | None | Unset = UNSET
    additional_properties: dict[str, Any] = _attrs_field(init=False, factory=dict)

    def to_dict(self) -> dict[str, Any]:
        user_id = str(self.user_id)

        email = self.email

        created = self.created.isoformat()

        updated = self.updated.isoformat()

        first_name: None | str | Unset
        if isinstance(self.first_name, Unset):
            first_name = UNSET
        else:
            first_name = self.first_name

        last_name: None | str | Unset
        if isinstance(self.last_name, Unset):
            last_name = UNSET
        else:
            last_name = self.last_name

        language: None | str | Unset
        if isinstance(self.language, Unset):
            language = UNSET
        elif isinstance(self.language, LanguageData):
            language = self.language.value
        else:
            language = self.language

        currency: None | str | Unset
        if isinstance(self.currency, Unset):
            currency = UNSET
        elif isinstance(self.currency, CurrencyData):
            currency = self.currency.value
        else:
            currency = self.currency

        field_dict: dict[str, Any] = {}
        field_dict.update(self.additional_properties)
        field_dict.update(
            {
                "userId": user_id,
                "email": email,
                "created": created,
                "updated": updated,
            }
        )
        if first_name is not UNSET:
            field_dict["firstName"] = first_name
        if last_name is not UNSET:
            field_dict["lastName"] = last_name
        if language is not UNSET:
            field_dict["language"] = language
        if currency is not UNSET:
            field_dict["currency"] = currency

        return field_dict

    @classmethod
    def from_dict(cls: type[T], src_dict: Mapping[str, Any]) -> T:
        d = dict(src_dict)
        user_id = UUID(d.pop("userId"))

        email = d.pop("email")

        created = isoparse(d.pop("created"))

        updated = isoparse(d.pop("updated"))

        def _parse_first_name(data: object) -> None | str | Unset:
            if data is None:
                return data
            if isinstance(data, Unset):
                return data
            return cast(None | str | Unset, data)

        first_name = _parse_first_name(d.pop("firstName", UNSET))

        def _parse_last_name(data: object) -> None | str | Unset:
            if data is None:
                return data
            if isinstance(data, Unset):
                return data
            return cast(None | str | Unset, data)

        last_name = _parse_last_name(d.pop("lastName", UNSET))

        def _parse_language(data: object) -> LanguageData | None | Unset:
            if data is None:
                return data
            if isinstance(data, Unset):
                return data
            try:
                if not isinstance(data, str):
                    raise TypeError()
                language_type_1 = LanguageData(data)

                return language_type_1
            except (TypeError, ValueError, AttributeError, KeyError):
                pass
            return cast(LanguageData | None | Unset, data)

        language = _parse_language(d.pop("language", UNSET))

        def _parse_currency(data: object) -> CurrencyData | None | Unset:
            if data is None:
                return data
            if isinstance(data, Unset):
                return data
            try:
                if not isinstance(data, str):
                    raise TypeError()
                currency_type_1 = CurrencyData(data)

                return currency_type_1
            except (TypeError, ValueError, AttributeError, KeyError):
                pass
            return cast(CurrencyData | None | Unset, data)

        currency = _parse_currency(d.pop("currency", UNSET))

        get_user_account_data = cls(
            user_id=user_id,
            email=email,
            created=created,
            updated=updated,
            first_name=first_name,
            last_name=last_name,
            language=language,
            currency=currency,
        )

        get_user_account_data.additional_properties = d
        return get_user_account_data

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
