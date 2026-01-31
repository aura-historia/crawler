import re
from typing import Optional, Dict, Any
from dataclasses import dataclass, field
import hashlib
import os

import boto3
import tldextract

from src.core.aws.database.constants import STATE_NEVER, STATE_PROGRESS, STATE_DONE

extract_with_cache = tldextract.TLDExtract(cache_dir="/tmp/.tld_cache")

# Regex to validate state prefixes:
# - NEVER# (standalone, no timestamp)
# - PROGRESS# followed by an ISO timestamp
# - DONE# followed by an ISO timestamp
STATE_REGEX = re.compile(
    rf"^({re.escape(STATE_NEVER)}|{re.escape(STATE_PROGRESS)}.+|{re.escape(STATE_DONE)}.+)$"
)


def _validate_state(value: Optional[str]) -> Optional[str]:
    if value is None:
        return None
    if not STATE_REGEX.match(value):
        raise ValueError(f"Invalid state value: {value}")
    return value


def _get_dynamodb_config() -> Dict[str, Any]:
    """Build DynamoDB configuration from environment variables."""
    config = {
        "region_name": os.getenv("AWS_REGION", "eu-central-1"),  # Fallback if needed
    }

    # Add endpoint URL ONLY if specified (usually for local development)
    endpoint_url = os.getenv("DYNAMODB_ENDPOINT_URL")
    if endpoint_url:
        config["endpoint_url"] = endpoint_url

    return config


def get_dynamodb_client():
    """Get boto3 DynamoDB client with configuration from environment."""
    return boto3.client("dynamodb", **_get_dynamodb_config())


def get_dynamodb_resource():
    """Get boto3 DynamoDB resource with configuration from environment."""
    return boto3.resource("dynamodb", **_get_dynamodb_config())


METADATA_SK = "META#"


@dataclass
class ShopMetadata:
    """
    Shop metadata entry.
    SK = METADATA_SK
    """

    domain: str
    shop_country: Optional[str] = field(default=None)
    shop_name: Optional[str] = field(default=None)
    pk: Optional[str] = field(default=None)
    sk: str = field(default=METADATA_SK)
    last_crawled_start: Optional[str] = field(default=None)
    last_crawled_end: Optional[str] = field(default=STATE_NEVER)
    last_scraped_start: Optional[str] = field(default=None)
    last_scraped_end: Optional[str] = field(default=STATE_NEVER)
    core_domain_name: Optional[str] = field(default=None)

    def __post_init__(self):
        """Set pk to domain if not provided and normalize fields."""
        if self.pk is None:
            self.pk = f"SHOP#{self.domain}"

        self.core_domain_name = extract_with_cache(self.domain).domain

        if self.shop_country and not self.shop_country.startswith("COUNTRY#"):
            self.shop_country = f"COUNTRY#{self.shop_country}"

        self.last_crawled_end = _validate_state(self.last_crawled_end)
        self.last_scraped_end = _validate_state(self.last_scraped_end)

    def _add_optional_field(
        self, item: Dict[str, Any], field_name: str, value: Optional[str]
    ) -> None:
        """Add optional string field to DynamoDB item if value is not None."""
        if value is not None and value != "":
            item[field_name] = {"S": value}

    def _add_gsi2_keys(self, item: Dict[str, Any]) -> None:
        """Add GSI2 keys for crawl orchestration (country + last_crawled_end)."""
        if not self.shop_country:
            return

        item["gsi2_pk"] = {"S": self.shop_country}
        # Use actual crawled end date or epoch marker for never-crawled shops
        gsi2_sk_value = self.last_crawled_end or STATE_NEVER
        item["gsi2_sk"] = {"S": gsi2_sk_value}

    def _add_gsi3_keys(self, item: Dict[str, Any]) -> None:
        """Add GSI3 keys for scrape orchestration (country + last_scraped_end)."""
        if not self.shop_country:
            return

        item["gsi3_pk"] = {"S": self.shop_country}
        # Use actual scraped end date or epoch marker for never-scraped shops
        gsi3_sk_value = self.last_scraped_end or STATE_NEVER
        item["gsi3_sk"] = {"S": gsi3_sk_value}

    def _add_gsi4_keys(self, item: Dict[str, Any]) -> None:
        """Add GSI4 keys for core domain discovery."""
        if self.core_domain_name:
            item["gsi4_pk"] = {"S": self.core_domain_name}
            item["gsi4_sk"] = {"S": self.domain}

    def to_dynamodb_item(self) -> Dict[str, Any]:
        """Convert to DynamoDB item format."""
        item = {
            "pk": {"S": self.pk},
            "sk": {"S": self.sk},
            "domain": {"S": self.domain},
            "core_domain_name": {"S": self.core_domain_name},
        }

        # Add optional base fields
        self._add_optional_field(item, "shop_name", self.shop_name)
        self._add_optional_field(item, "shop_country", self.shop_country)
        self._add_optional_field(item, "last_crawled_start", self.last_crawled_start)
        self._add_optional_field(item, "last_crawled_end", self.last_crawled_end)
        self._add_optional_field(item, "last_scraped_start", self.last_scraped_start)
        self._add_optional_field(item, "last_scraped_end", self.last_scraped_end)

        # Add GSI keys
        self._add_gsi2_keys(item)
        self._add_gsi3_keys(item)
        self._add_gsi4_keys(item)

        return item

    @classmethod
    def from_dynamodb_item(cls, item: Dict[str, Any]) -> "ShopMetadata":
        """Create instance from DynamoDB item."""
        return cls(
            pk=item["pk"]["S"],
            sk=item["sk"]["S"],
            domain=item.get("domain", {}).get("S"),
            shop_country=item.get("shop_country", {}).get("S"),
            last_crawled_start=item.get("last_crawled_start", {}).get("S"),
            last_crawled_end=item.get("last_crawled_end", {}).get("S")
            or item.get("gsi2_sk", {}).get("S"),
            last_scraped_start=item.get("last_scraped_start", {}).get("S"),
            last_scraped_end=item.get("last_scraped_end", {}).get("S")
            or item.get("gsi3_sk", {}).get("S"),
            core_domain_name=item.get("core_domain_name", {}).get("S"),
        )


@dataclass
class URLEntry:
    """Represents a URL entry in the database."""

    domain: str
    url: str
    type: Optional[str] = field(default=None)
    hash: Optional[str] = field(default=None)
    pk: Optional[str] = field(default=None)
    sk: Optional[str] = field(default=None)

    def __post_init__(self):
        """Set pk and sk if not provided."""
        if self.pk is None:
            self.pk = f"SHOP#{self.domain}"
        if self.sk is None:
            self.sk = f"URL#{self.url}"

    def to_dynamodb_item(self) -> Dict[str, Any]:
        """Convert to DynamoDB item format."""
        item = {
            "pk": {"S": self.pk},
            "sk": {"S": self.sk},
            "url": {"S": self.url},
        }

        if self.type is not None:
            item["type"] = {"S": self.type}
            # GSI1: Product type index
            item["gsi1_pk"] = {"S": self.pk}
            item["gsi1_sk"] = {"S": self.type}

        if self.hash is not None:
            item["hash"] = {"S": self.hash}

        return item

    @classmethod
    def from_dynamodb_item(cls, item: Dict[str, Any]) -> "URLEntry":
        """Create instance from DynamoDB item."""
        pk = item["pk"]["S"]
        # Extract domain from pk (remove SHOP# prefix if present)
        domain = pk.replace("SHOP#", "", 1) if pk.startswith("SHOP#") else pk
        return cls(
            pk=pk,
            sk=item["sk"]["S"],
            domain=domain,
            url=item["url"]["S"],
            type=item.get("type", {}).get("S"),
            hash=item.get("hash", {}).get("S"),
        )

    @staticmethod
    def calculate_hash(markdown: str) -> str:
        """
        Calculate hash from status and price to detect changes.

        Args:
            markdown: Markdown content string

        Returns:
            SHA256 hash string
        """
        return hashlib.sha256(markdown.encode()).hexdigest()
