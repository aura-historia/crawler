from typing import Optional, Dict, Any
from dataclasses import dataclass, field
import hashlib
import os

import boto3
import tldextract

extract_with_cache = tldextract.TLDExtract(cache_dir="/tmp/.tld_cache")


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
    standards_used: bool = field(default=True)
    shop_country: Optional[str] = field(default=None)
    shop_name: Optional[str] = field(default=None)
    pk: Optional[str] = field(default=None)
    sk: str = field(default=METADATA_SK)
    last_crawled_start: Optional[str] = field(default=None)
    last_crawled_end: Optional[str] = field(default=None)
    last_scraped_start: Optional[str] = field(default=None)
    last_scraped_end: Optional[str] = field(default=None)
    core_domain_name: Optional[str] = field(default=None)

    def __post_init__(self):
        """Set pk to domain if not provided and normalize fields."""
        if self.pk is None:
            self.pk = f"SHOP#{self.domain}"
        # Extract and store the core domain name (e.g., 'example' from 'example.com')
        self.core_domain_name = extract_with_cache(self.domain).domain
        # Format shop_country with COUNTRY# prefix if not already formatted
        if self.shop_country and not self.shop_country.startswith("COUNTRY#"):
            self.shop_country = f"COUNTRY#{self.shop_country}"

    def to_dynamodb_item(self) -> Dict[str, Any]:
        """Convert to DynamoDB item format."""
        item = {
            "pk": {"S": self.pk},
            "sk": {"S": self.sk},
            "domain": {"S": self.domain},
            "standards_used": {"BOOL": self.standards_used},
            "core_domain_name": {"S": self.core_domain_name},
        }
        if self.shop_name:
            item["shop_name"] = {"S": self.shop_name}
        if self.shop_country:
            item["shop_country"] = {"S": self.shop_country}
            # GSI2/GSI3: Country-based indexes
            item["gsi2_pk"] = {"S": self.shop_country}
            item["gsi3_pk"] = {"S": self.shop_country}
        if self.last_crawled_start:
            item["last_crawled_start"] = {"S": self.last_crawled_start}
        if self.last_crawled_end:
            item["last_crawled_end"] = {"S": self.last_crawled_end}
            # GSI2: Country + crawled end date
            if self.shop_country:
                item["gsi2_sk"] = {"S": self.last_crawled_end}
        if self.last_scraped_start:
            item["last_scraped_start"] = {"S": self.last_scraped_start}
        if self.last_scraped_end:
            item["last_scraped_end"] = {"S": self.last_scraped_end}
            # GSI3: Country + scraped end date
            if self.shop_country:
                item["gsi3_sk"] = {"S": self.last_scraped_end}
        # GSI4: Core domain index
        if self.core_domain_name:
            item["gsi4_pk"] = {"S": self.core_domain_name}
            item["gsi4_sk"] = {"S": self.domain}
        return item

    @classmethod
    def from_dynamodb_item(cls, item: Dict[str, Any]) -> "ShopMetadata":
        """Create instance from DynamoDB item."""
        return cls(
            pk=item["pk"]["S"],
            sk=item["sk"]["S"],
            domain=item["domain"]["S"],
            standards_used=item.get("standards_used", {}).get("BOOL"),
            shop_country=item.get("shop_country", {}).get("S"),
            shop_name=item.get("shop_name", {}).get("S"),
            last_crawled_start=item.get("last_crawled_start", {}).get("S"),
            last_crawled_end=item.get("last_crawled_end", {}).get("S"),
            last_scraped_start=item.get("last_scraped_start", {}).get("S"),
            last_scraped_end=item.get("last_scraped_end", {}).get("S"),
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
