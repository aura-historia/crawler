from __future__ import annotations

from collections.abc import Mapping
from typing import Any, TypeVar

from attrs import define as _attrs_define
from attrs import field as _attrs_field

from ..models.language_data import LanguageData

T = TypeVar("T", bound="LocalizedTextData")


@_attrs_define
class LocalizedTextData:
    """Text content with language information

    Attributes:
        text (str): The text content
        language (LanguageData): Supported languages (ISO 639-1 codes):
            - de: German (includes de-DE, de-AT, de-CH, de-LU, de-LI)
            - en: English (includes en-US, en-GB, en-AU, en-CA, en-NZ, en-IE)
            - fr: French (includes fr-FR, fr-CA, fr-BE, fr-CH, fr-LU)
            - es: Spanish (includes es-ES, es-MX, es-AR, es-CO, es-CL, es-PE, es-VE)
             Example: de.
    """

    text: str
    language: LanguageData
    additional_properties: dict[str, Any] = _attrs_field(init=False, factory=dict)

    def to_dict(self) -> dict[str, Any]:
        text = self.text

        language = self.language.value

        field_dict: dict[str, Any] = {}
        field_dict.update(self.additional_properties)
        field_dict.update(
            {
                "text": text,
                "language": language,
            }
        )

        return field_dict

    @classmethod
    def from_dict(cls: type[T], src_dict: Mapping[str, Any]) -> T:
        d = dict(src_dict)
        text = d.pop("text")

        language = LanguageData(d.pop("language"))

        localized_text_data = cls(
            text=text,
            language=language,
        )

        localized_text_data.additional_properties = d
        return localized_text_data

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
