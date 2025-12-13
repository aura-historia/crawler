from typing import Optional, List, Dict, Any
from dataclasses import dataclass, field
import hashlib
import os

import boto3


def _get_dynamodb_config() -> Dict[str, Any]:
    """Build DynamoDB configuration from environment variables."""
    config = {
        "region_name": os.getenv("AWS_REGION"),
    }

    # Add endpoint URL if specified (for local DynamoDB)
    endpoint_url = os.getenv("DYNAMODB_ENDPOINT_URL")
    if endpoint_url:
        config["endpoint_url"] = endpoint_url

    # Add credentials if specified
    access_key = os.getenv("AWS_ACCESS_KEY_ID")
    secret_key = os.getenv("AWS_SECRET_ACCESS_KEY")
    if access_key and secret_key:
        config["aws_access_key_id"] = access_key
        config["aws_secret_access_key"] = secret_key

    return config


def get_dynamodb_client():
    """Get boto3 DynamoDB client with configuration from environment."""
    return boto3.client("dynamodb", **_get_dynamodb_config())


def get_dynamodb_resource():
    """Get boto3 DynamoDB resource with configuration from environment."""
    return boto3.resource("dynamodb", **_get_dynamodb_config())


@dataclass
class ShopMetadata:
    """
    Shop metadata entry.
    SK = 'META#'
    """

    domain: str
    standards_used: List[str] = field(default_factory=list)
    pk: Optional[str] = field(default=None)
    sk: str = field(default="META#")

    def __post_init__(self):
        """Set pk to domain if not provided."""
        if self.pk is None:
            self.pk = f"SHOP#{self.domain}"

    def to_dynamodb_item(self) -> Dict[str, Any]:
        """Convert to DynamoDB item format."""
        return {
            "PK": {"S": self.pk},
            "SK": {"S": self.sk},
            "domain": {"S": self.domain},
            "standards_used": {"L": [{"S": s} for s in self.standards_used]},
        }

    @classmethod
    def from_dynamodb_item(cls, item: Dict[str, Any]) -> "ShopMetadata":
        """Create instance from DynamoDB item."""
        return cls(
            pk=item["PK"]["S"],
            sk=item["SK"]["S"],
            domain=item["domain"]["S"],
            standards_used=[
                s["S"] for s in item.get("standards_used", {}).get("L", [])
            ],
        )


@dataclass
class URLEntry:
    """Represents a URL entry in the database."""

    domain: str
    url: str
    standards_used: List[str] = field(default_factory=list)
    type: Optional[str] = field(default=None)
    is_product: bool = field(default=False)
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
            "PK": {"S": self.pk},
            "SK": {"S": self.sk},
            "url": {"S": self.url},
            "standards_used": {"L": [{"S": s} for s in self.standards_used]},
            "is_product": {"BOOL": self.is_product},
        }

        if self.type is not None:
            item["type"] = {"S": self.type}

        if self.hash is not None:
            item["hash"] = {"S": self.hash}

        return item

    @classmethod
    def from_dynamodb_item(cls, item: Dict[str, Any]) -> "URLEntry":
        """Create instance from DynamoDB item."""
        pk = item["PK"]["S"]
        # Extract domain from PK (remove SHOP# prefix if present)
        domain = pk.replace("SHOP#", "", 1) if pk.startswith("SHOP#") else pk
        return cls(
            pk=pk,
            sk=item["SK"]["S"],
            domain=domain,
            url=item["url"]["S"],
            standards_used=[
                s["S"] for s in item.get("standards_used", {}).get("L", [])
            ],
            type=item.get("type", {}).get("S"),
            is_product=item.get("is_product", {}).get("BOOL", False),
            hash=item.get("hash", {}).get("S"),
        )

    @staticmethod
    def calculate_hash(status: Optional[str], price: Optional[float]) -> str:
        """
        Calculate hash from status and price to detect changes.

        Args:
            status: Product status (e.g., 'in_stock', 'out_of_stock')
            price: Product price

        Returns:
            SHA256 hash string
        """
        price_str = "" if price is None else str(price)
        hash_input = f"{status}|{price_str}"
        return hashlib.sha256(hash_input.encode()).hexdigest()
