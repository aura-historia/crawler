import logging
import os
import sys
from typing import Any, Dict, List, Optional, Tuple

import tldextract
from pythonjsonlogger import json as pythonjson

from aura_historia_backend_api_client.client import Client
from aura_historia_backend_api_client.api.shops import (
    create_shop,
    update_shop_by_domain,
)
from aura_historia_backend_api_client.models import PostShopData, PatchShopData
from aura_historia_backend_api_client.models.shop_type_data import ShopTypeData
from aura_historia_backend_api_client.models.api_error import ApiError

from src.core.aws.database.models import METADATA_SK, ShopMetadata
from src.core.aws.database.operations import db_operations

# Configure JSON logging for CloudWatch
logger = logging.getLogger(__name__)
if not logger.handlers:
    handler = logging.StreamHandler(sys.stdout)
    formatter = pythonjson.JsonFormatter(
        "%(asctime)s %(levelname)s %(name)s %(message)s",
        rename_fields={"levelname": "level", "asctime": "timestamp"},
    )
    handler.setFormatter(formatter)
    logger.addHandler(handler)
    logger.setLevel(logging.INFO)

API_BASE_URL = os.getenv("BACKEND_API_URL")


class ShopRegistrationError(Exception):
    """Exception raised when shop registration or update fails."""

    pass


extract_with_cache = tldextract.TLDExtract(cache_dir="/tmp/.tld_cache")

# Reusable client to benefit from connection pooling across warm Lambda
_GLOBAL_CLIENT: Optional[Client] = None


def _get_client() -> Client | None:
    """Return a module-level API client for making shop API calls.

    This reuses connections between Lambda invocations when the
    execution environment is reused (warm starts), improving latency.
    """
    global _GLOBAL_CLIENT
    if _GLOBAL_CLIENT is None:
        if not API_BASE_URL:
            raise ValueError("API_BASE_URL environment variable is not set.")
        _GLOBAL_CLIENT = Client(base_url=API_BASE_URL)
    return _GLOBAL_CLIENT


def _shop_type_from_string(shop_type_str: str) -> ShopTypeData:
    """Convert shop type string to ShopTypeData enum.

    Args:
        shop_type_str: String representation of shop type

    Returns:
        ShopTypeData enum value
    """
    shop_type_map = {
        "AUCTION_HOUSE": ShopTypeData.AUCTION_HOUSE,
        "AUCTION_PLATFORM": ShopTypeData.AUCTION_PLATFORM,
        "COMMERCIAL_DEALER": ShopTypeData.COMMERCIAL_DEALER,
        "MARKETPLACE": ShopTypeData.MARKETPLACE,
    }
    return shop_type_map.get(shop_type_str, ShopTypeData.COMMERCIAL_DEALER)


def find_existing_shop(
    new_domain: str, core_domain_name: Optional[str] = None
) -> Optional[Tuple[str, List[str]]]:
    """
    Finds an existing shop by matching the core domain name using GSI4.

    Args:
        new_domain: The new domain to check.
        core_domain_name: Optional pre-extracted core domain name to avoid re-extraction.
    Returns:
        A tuple of (shopIdentifier, all_existing_domains) if an existing shop
    """

    # If caller didn't provide a core domain name, derive it
    if not core_domain_name:
        # Extract core domain name from new_domain (e.g., 'example' from 'shop.example.co.uk')
        core_domain_name = extract_with_cache(new_domain).domain

    logger.info(
        "Searching for existing shop",
        extra={"new_domain": new_domain, "core_domain_name": core_domain_name},
    )

    # Get ALL shops with the same core domain name
    all_shops = db_operations.find_all_domains_by_core_domain_name(core_domain_name)

    # Filter out the new domain at application level
    existing_domains = [shop.domain for shop in all_shops if shop.domain != new_domain]

    if not existing_domains:
        logger.info(
            "No other shops found with same core domain name",
            extra={"core_domain_name": core_domain_name, "new_domain": new_domain},
        )
        return None

    logger.info(
        "Found existing shop with multiple domains",
        extra={
            "core_domain_name": core_domain_name,
            "existing_domains": existing_domains,
            "shop_identifier": existing_domains[0],
            "total_existing_domains": len(existing_domains),
        },
    )

    # Use the first existing domain as the shopIdentifier for the PATCH request
    return existing_domains[0], existing_domains


def register_or_update_shop(shop: ShopMetadata, client: Client) -> None:
    """
    Registers a new shop or updates an existing one with a new domain using
    the OpenAPI client.

    Args:
        shop: The shop data from the DynamoDB stream (ShopMetadata must contain
            shop_name if present in the stream).
        client: OpenAPI Client to perform API calls.

    Raises:
        ValueError: If API_BASE_URL is not set.
        Exception: If API calls fail.
    """
    logger.info(
        "Starting shop registration/update process",
        extra={
            "domain": shop.domain,
            "shop_name": shop.shop_name,
            "shop_type": shop.shop_type,
            "core_domain_name": shop.core_domain_name,
        },
    )

    existing_shop_info = find_existing_shop(
        shop.domain, core_domain_name=shop.core_domain_name
    )

    if existing_shop_info:
        # Add domain to existing shop
        shop_identifier, all_existing_domains = existing_shop_info

        # all_existing_domains already excludes the new domain (filtered by GSI4 query)
        # Add the new domain to the complete list for the backend
        all_domains_for_backend = all_existing_domains + [shop.domain]

        patch_data = PatchShopData(
            domains=all_domains_for_backend,
        )

        logger.info(
            "Adding domain to existing shop",
            extra={
                "operation": "PATCH",
                "new_domain": shop.domain,
                "shop_identifier": shop_identifier,
                "all_domains": all_domains_for_backend,
                "total_domains_count": len(all_domains_for_backend),
            },
        )

        response = update_shop_by_domain.sync(
            shop_domain=shop_identifier,
            client=client,
            body=patch_data,
        )

        if isinstance(response, ApiError):
            logger.error(
                "Failed to update shop",
                extra={
                    "domain": shop.domain,
                    "shop_identifier": shop_identifier,
                    "error_title": response.title,
                    "error_detail": response.detail,
                },
            )
            raise ShopRegistrationError(f"Failed to update shop: {response.detail}")

        logger.info(
            "Successfully added domain to existing shop",
            extra={"domain": shop.domain, "shop_identifier": shop_identifier},
        )
    else:
        shop_name = shop.shop_name if shop.shop_name else None
        if not shop_name:
            shop_name = shop.core_domain_name if shop.core_domain_name else None
        if not shop_name:
            shop_name = tldextract.extract(shop.domain).domain

        shop_type_enum = _shop_type_from_string(shop.shop_type)

        post_data = PostShopData(
            name=shop_name,
            shop_type=shop_type_enum,
            domains=[shop.domain],
            image=shop.shop_image,
        )

        logger.info(
            "Creating new shop",
            extra={
                "operation": "POST",
                "domain": shop.domain,
                "shop_name": shop_name,
                "shop_type": shop.shop_type,
            },
        )

        response = create_shop.sync(
            client=client,
            body=post_data,
        )

        if isinstance(response, ApiError):
            logger.error(
                "Failed to create shop",
                extra={
                    "domain": shop.domain,
                    "error_title": response.title,
                    "error_detail": response.detail,
                },
            )
            raise ShopRegistrationError(f"Failed to create shop: {response.detail}")

        logger.info(
            "Successfully created new shop",
            extra={"domain": shop.domain, "shop_name": shop_name},
        )


def _extract_minimal_shop_from_image(
    new_image: Dict[str, Any],
) -> Optional[ShopMetadata]:
    """Extract only the minimal fields we need from a DynamoDB Stream NewImage.

    Returns a ShopMetadata with domain and optional shop_name, or None if domain
    cannot be determined.
    """
    if not new_image:
        return None

    # Prefer explicit 'domain' attribute. Fallback to parsing 'pk' (SHOP#...)
    domain = None
    if new_image.get("domain") and new_image.get("domain").get("S"):
        domain = new_image.get("domain").get("S")
    else:
        pk_val = new_image.get("pk", {}).get("S") if new_image.get("pk") else None
        if pk_val and pk_val.startswith("SHOP#"):
            domain = pk_val.replace("SHOP#", "", 1)

    if not domain:
        return None

    shop_name = (
        new_image.get("shop_name", {}).get("S") if new_image.get("shop_name") else None
    )
    shop_image = (
        new_image.get("shop_image", {}).get("S")
        if new_image.get("shop_image")
        else None
    )
    shop_type = (
        new_image.get("shop_type", {}).get("S")
        if new_image.get("shop_type")
        else "COMMERCIAL_DEALER"
    )
    core_domain_name = (
        new_image.get("core_domain_name", {}).get("S")
        if new_image.get("core_domain_name")
        else None
    )

    shop = ShopMetadata(
        domain=domain,
        shop_name=shop_name,
        shop_image=shop_image,
        shop_type=shop_type,
        core_domain_name=core_domain_name,
    )

    return shop


def _process_record(record: Dict[str, Any], client: Client) -> Optional[str]:
    """Process a single DynamoDB stream record.

    Returns a SequenceNumber string when processing failed (to be used as
    batchItemFailure identifier), or None on success.
    """
    event_name = record.get("eventName")
    seq = record.get("dynamodb", {}).get("SequenceNumber", "UNKNOWN")

    logger.info(
        "Processing DynamoDB stream record",
        extra={"event_name": event_name, "sequence_number": seq},
    )

    if event_name != "INSERT":
        logger.debug(
            "Skipping non-INSERT event",
            extra={"event_name": event_name, "sequence_number": seq},
        )
        return None

    dynamodb_data = record.get("dynamodb", {})
    new_image = dynamodb_data.get("NewImage")

    if not new_image or new_image.get("sk", {}).get("S") != METADATA_SK:
        logger.debug(
            "Skipping record: not a METADATA record",
            extra={
                "has_new_image": bool(new_image),
                "sk": new_image.get("sk", {}).get("S") if new_image else None,
                "sequence_number": seq,
            },
        )
        return None

    shop = _extract_minimal_shop_from_image(new_image)
    if not shop:
        logger.error(
            "Skipping record: domain missing in NewImage",
            extra={"sequence_number": seq, "new_image_keys": list(new_image.keys())},
        )
        return seq

    logger.info(
        "Extracted shop data from stream record",
        extra={
            "domain": shop.domain,
            "shop_name": shop.shop_name,
            "shop_type": shop.shop_type,
            "core_domain_name": shop.core_domain_name,
            "sequence_number": seq,
        },
    )

    try:
        register_or_update_shop(shop, client)
        logger.info(
            "Successfully processed record",
            extra={"domain": shop.domain, "sequence_number": seq},
        )
        return None
    except Exception as exc:
        logger.error(
            "Failed to register/update shop",
            extra={
                "domain": shop.domain,
                "error": str(exc),
                "error_type": type(exc).__name__,
                "sequence_number": seq,
            },
        )
        return seq


def handler(event: Dict[str, Any], context: object) -> Dict[str, Any]:
    """
    AWS Lambda handler for processing DynamoDB Stream events.

    Processes only INSERT events and builds a minimal ShopMetadata from the
    provided NewImage (domain + optional shop_name) to avoid extra DB reads.
    """
    records = event.get("Records", []) or []

    logger.info(
        "Lambda invoked with DynamoDB stream batch",
        extra={
            "record_count": len(records),
            "event_names": [r.get("eventName") for r in records][:10],
        },
    )

    batch_item_failures: List[Dict[str, str]] = []

    client = _get_client()

    for idx, record in enumerate(records, 1):
        logger.info(f"Processing record {idx}/{len(records)}")
        seq = _process_record(record, client)
        if seq:
            batch_item_failures.append({"itemIdentifier": seq})

    logger.info(
        "Finished processing DynamoDB stream batch",
        extra={
            "total_records": len(records),
            "successful_records": len(records) - len(batch_item_failures),
            "failed_records": len(batch_item_failures),
        },
    )

    return {"batchItemFailures": batch_item_failures}
