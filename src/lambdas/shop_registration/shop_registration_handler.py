import logging
import os
import sys
from typing import Any, Dict, List, Optional, Tuple
import requests
from requests import Session
from requests.adapters import HTTPAdapter
from urllib3.util.retry import Retry
import tldextract
from pythonjsonlogger import json as pythonjson

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

BACKEND_API_URL = os.getenv("BACKEND_API_URL")

extract_with_cache = tldextract.TLDExtract(cache_dir="/tmp/.tld_cache")

RETRY_STATUSES = {429, 500, 502, 503, 504}

# Reusable session to benefit from connection pooling across warm Lambda
_GLOBAL_SESSION: Optional[Session] = None


def _build_retry_session(
    retry_attempts: int = 3, backoff_factor: float = 1.0
) -> Session:
    """Create a requests.Session configured with urllib3 Retry for resilient calls.

    Args:
        retry_attempts (int): Number of retry attempts.
        backoff_factor (float): Backoff factor for exponential backoff.

    Returns:
        requests.Session: Configured session instance.
    """
    session = requests.Session()
    retry = Retry(
        total=retry_attempts,
        backoff_factor=backoff_factor,
        status_forcelist=list(RETRY_STATUSES),
        allowed_methods={"GET", "POST", "PUT", "PATCH"},
        raise_on_status=False,
    )
    adapter = HTTPAdapter(max_retries=retry)
    session.mount("https://", adapter)
    session.headers.update({"User-Agent": "aura-shop-registration/1.0"})
    return session


def _get_session() -> Session:
    """Return a module-level requests.Session configured for retries.

    This reuses TCP connections between Lambda invocations when the
    execution environment is reused (warm starts), improving latency.
    """
    global _GLOBAL_SESSION
    if _GLOBAL_SESSION is None:
        _GLOBAL_SESSION = _build_retry_session()
    return _GLOBAL_SESSION


def resilient_http_request_sync(
    url: str,
    session: Session,
    method: str = "GET",
    headers: Optional[dict] = None,
    params: Optional[dict] = None,
    json_data: Optional[dict] = None,
    data: Any = None,
    timeout_seconds: float = 30.0,
    return_json: bool = False,
) -> Any:
    """
    Perform a synchronous HTTP request with retries using requests + urllib3 Retry.

    Raises on non-2xx responses after retries.
    """
    logger.info(
        "Making HTTP request",
        extra={
            "method": method.upper(),
            "url": url,
            "has_json_payload": json_data is not None,
            "payload": json_data,
        },
    )

    try:
        # Merge headers with session defaults so callers can override/extend
        request_headers = (
            dict(session.headers) if getattr(session, "headers", None) else {}
        )
        if headers:
            request_headers.update(headers)

        response = session.request(
            method=method.upper(),
            url=url,
            headers=request_headers,
            params=params or {},
            json=json_data,
            data=data,
            timeout=timeout_seconds,
        )

        logger.info(
            "HTTP request completed",
            extra={
                "method": method.upper(),
                "url": url,
                "status": response.status_code,
                "response_preview": response.text[:200] if response.text else None,
            },
        )

        response.raise_for_status()

        if return_json:
            try:
                return response.json()
            except ValueError:
                logger.error(f"Invalid JSON from {url}")
                raise
        return response.text

    except requests.RequestException as exc:
        status = getattr(exc.response, "status_code", None)
        body = getattr(exc.response, "text", None)
        logger.error(
            "HTTP request failed",
            extra={
                "method": method.upper(),
                "url": url,
                "status": status,
                "body": body,
            },
        )
        raise
    except Exception as exc:
        logger.error(
            "Unexpected error during HTTP request",
            extra={"method": method.upper(), "url": url, "error": str(exc)},
        )
        raise


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

    # Get ALL shops with the same core domain name, excluding the new domain
    all_shops = db_operations.find_all_domains_by_core_domain_name(
        core_domain_name, domain_to_exclude=new_domain
    )

    if not all_shops:
        logger.info(
            "No other shops found with same core domain name",
            extra={"core_domain_name": core_domain_name, "new_domain": new_domain},
        )
        return None

    # Extract all domains from the shop metadata objects
    all_domains = [shop.domain for shop in all_shops]

    logger.info(
        "Found existing shop with multiple domains",
        extra={
            "core_domain_name": core_domain_name,
            "existing_domains": all_domains,
            "shop_identifier": all_domains[0],
            "total_existing_domains": len(all_domains),
        },
    )

    # Use the first existing domain as the shopIdentifier for the PATCH request
    return all_domains[0], all_domains


def register_or_update_shop(shop: ShopMetadata, session: Session) -> None:
    """
    Registers a new shop or updates an existing one with a new domain using
    a synchronous resilient HTTP client.

    Args:
        shop: The shop data from the DynamoDB stream (ShopMetadata must contain
            shop_name if present in the stream).
        session: requests.Session to perform HTTP calls (will be configured for
            retries by the caller).

    Raises:
        ValueError: If BACKEND_API_URL is not set.
        requests.RequestException: If HTTP calls fail after retries.
    """
    logger.info(
        "Starting shop registration/update process",
        extra={
            "domain": shop.domain,
            "shop_name": shop.shop_name,
            "core_domain_name": shop.core_domain_name,
        },
    )

    if not BACKEND_API_URL:
        logger.error("BACKEND_API_URL environment variable is not set.")
        raise ValueError("Backend API URL is not configured.")

    shops_endpoint = f"{BACKEND_API_URL.rstrip('/')}/shops"
    headers = {"Content-Type": "application/json"}

    existing_shop_info = find_existing_shop(
        shop.domain, core_domain_name=shop.core_domain_name
    )

    if existing_shop_info:
        # Add domain to existing shop
        shop_identifier, all_existing_domains = existing_shop_info

        # all_existing_domains already excludes the new domain (filtered by GSI4 query)
        # Add the new domain to the complete list for the backend
        all_domains_for_backend = all_existing_domains + [shop.domain]

        payload = {"domains": all_domains_for_backend}
        patch_url = f"{shops_endpoint}/{shop_identifier}"

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

        # Backend PATCH endpoint is idempotent:
        # sending the full domains list replaces state, so retries are safe.
        resilient_http_request_sync(
            patch_url,
            session,
            method="PATCH",
            json_data=payload,
            headers=headers,
            timeout_seconds=10,
        )
        logger.info(
            "Successfully added domain to existing shop",
            extra={"domain": shop.domain, "shop_identifier": shop_identifier},
        )
    else:
        shop_name = shop.shop_name if shop.shop_name else None
        if not shop_name:
            shop_name = shop.core_domain_name if shop.core_domain_name else None
        if not shop_name:
            import tldextract

            shop_name = tldextract.extract(shop.domain).domain

        payload = {
            "name": shop_name,
            "domains": [shop.domain],
        }

        logger.info(
            "Creating new shop",
            extra={
                "operation": "POST",
                "domain": shop.domain,
                "shop_name": shop_name,
                "payload": payload,
            },
        )
        resilient_http_request_sync(
            shops_endpoint,
            session,
            method="POST",
            json_data=payload,
            headers=headers,
            timeout_seconds=10,
        )
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
    core_domain_name = (
        new_image.get("core_domain_name", {}).get("S")
        if new_image.get("core_domain_name")
        else None
    )

    shop = ShopMetadata(
        domain=domain, shop_name=shop_name, core_domain_name=core_domain_name
    )

    return shop


def _process_record(record: Dict[str, Any], session: Session) -> Optional[str]:
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
            "core_domain_name": shop.core_domain_name,
            "sequence_number": seq,
        },
    )

    try:
        register_or_update_shop(shop, session)
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

    session = _get_session()

    for idx, record in enumerate(records, 1):
        logger.info(f"Processing record {idx}/{len(records)}")
        seq = _process_record(record, session)
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
