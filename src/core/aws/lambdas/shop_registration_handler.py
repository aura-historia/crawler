import os
from typing import Any, Dict, List, Optional, Tuple

import requests
from requests import Session

from src.core.aws.database.models import METADATA_SK, ShopMetadata
from src.core.aws.database.operations import db_operations
from src.core.utils.logger import logger

import tldextract

BACKEND_API_URL = os.getenv("BACKEND_API_URL")

http_session = requests.Session()


def get_core_domain_name(domain: str) -> str:
    """
    Extracts the core domain name from a given domain string using tldextract.
    e.g., 'sub.example.co.uk' -> 'example'
    e.g., 'example.com' -> 'example'

    Args:
        domain (str): The domain name.

    Returns:
        str: The core domain name.
    """
    extracted = tldextract.extract(domain)
    return extracted.domain


def find_existing_shop(new_domain: str) -> Optional[Tuple[str, List[str]]]:
    """
    Finds an existing shop by matching the core domain name using GSI4.

    Args:
        new_domain: The new domain to check (e.g., 'shop.fr').

    Returns:
        Optional[Tuple[str, List[str]]]: Tuple of (domain_to_use_as_identifier, all_domains)
        where domain_to_use_as_identifier is one of the existing domains (not the new one),
        and all_domains is the complete list of all domains with the same core name.
        Returns None if no existing shop found.
    """
    core_domain_name = get_core_domain_name(new_domain)
    logger.info(
        f"Searching for existing shop with core_domain_name: '{core_domain_name}'"
    )

    # Get ALL shops with the same core domain name, excluding the new domain
    all_shops = db_operations.find_all_domains_by_core_domain_name(
        core_domain_name, domain_to_exclude=new_domain
    )

    if not all_shops:
        logger.info("No other shops found with same core domain name.")
        return None

    # Extract all domains from the shop metadata objects
    all_domains = [shop.domain for shop in all_shops]

    logger.info(f"Found {len(all_domains)} existing domain(s): {all_domains}")

    # Use the first existing domain as the shopIdentifier for the PATCH request
    return all_domains[0], all_domains


def update_shop_domain(
    old_shop: ShopMetadata, new_shop: ShopMetadata, session: Session
) -> None:
    """
    Updates shop domain in the backend when a domain change is detected.

    Args:
        old_shop: The shop data before the modification.
        new_shop: The shop data after the modification.
        session: The requests Session object for making HTTP calls.

    Raises:
        requests.exceptions.RequestException: If the API call fails.
        ValueError: If BACKEND_API_URL is not set.
    """
    if not BACKEND_API_URL:
        logger.error("BACKEND_API_URL environment variable is not set.")
        raise ValueError("Backend API URL is not configured.")

    # Check if domain changed
    if old_shop.domain == new_shop.domain:
        logger.info(
            f"No domain change detected for '{new_shop.domain}', skipping update."
        )
        return

    logger.info(f"Domain changed: '{old_shop.domain}' -> '{new_shop.domain}'")

    shops_endpoint = f"{BACKEND_API_URL.rstrip('/')}/shops"
    headers = {"Content-Type": "application/json"}

    # Use the OLD domain as the identifier for the PATCH request
    patch_url = f"{shops_endpoint}/{old_shop.domain}"

    # Get all domains for this shop from GSI4 using the new domain's core name
    core_domain_name = get_core_domain_name(new_shop.domain)
    all_shops = db_operations.find_all_domains_by_core_domain_name(core_domain_name)

    if all_shops:
        # Extract all domains from shop metadata objects
        all_domains = [s.domain for s in all_shops]
        payload = {"domains": all_domains}

        logger.info(
            f"Updating shop domain via old domain '{old_shop.domain}'. New domains: {all_domains}"
        )
        response = session.patch(patch_url, json=payload, headers=headers, timeout=10)
        response.raise_for_status()
        logger.info(
            f"Successfully updated shop domain for '{old_shop.domain}' -> '{new_shop.domain}'"
        )
    else:
        logger.warning(f"No domains found for core_domain_name '{core_domain_name}'")


def register_or_update_shop(shop: ShopMetadata, session: Session) -> None:
    """
    Registers a new shop or updates an existing one with a new domain.

    Args:
        shop: The shop data from the DynamoDB stream.
        session: The requests Session object for making HTTP calls.

    Raises:
        requests.exceptions.RequestException: If the API call fails.
        ValueError: If BACKEND_API_URL is not set.
    """
    if not BACKEND_API_URL:
        logger.error("BACKEND_API_URL environment variable is not set.")
        raise ValueError("Backend API URL is not configured.")

    shops_endpoint = f"{BACKEND_API_URL.rstrip('/')}/shops"
    headers = {"Content-Type": "application/json"}

    existing_shop_info = find_existing_shop(shop.domain)

    if existing_shop_info:
        # Add domain to existing shop
        shop_identifier, all_existing_domains = existing_shop_info

        # all_existing_domains already excludes the new domain (filtered by GSI4 query)
        # Add the new domain to the complete list for the backend
        all_domains_for_backend = all_existing_domains + [shop.domain]

        payload = {"domains": all_domains_for_backend}
        patch_url = f"{shops_endpoint}/{shop_identifier}"

        logger.info(
            f"Adding domain '{shop.domain}' to shop via '{shop_identifier}'. "
            f"Total domains: {len(all_domains_for_backend)}"
        )

        response = session.patch(patch_url, json=payload, headers=headers, timeout=10)
        response.raise_for_status()
        logger.info(f"Successfully added domain '{shop.domain}'")
    else:
        shop_name = get_core_domain_name(shop.domain).capitalize()
        payload = {
            "name": shop_name,
            "domains": [shop.domain],
        }

        logger.info(f"Creating new shop for domain '{shop.domain}'")
        response = session.post(
            shops_endpoint, json=payload, headers=headers, timeout=10
        )
        response.raise_for_status()
        logger.info(f"Successfully created shop. Status: {response.status_code}")


def handler(event: Dict[str, Any], context: object) -> Dict[str, Any]:
    """
    AWS Lambda handler for processing DynamoDB Stream events.

    Processes:
    - INSERT events: Registers new shops or adds domains to existing shops
    - MODIFY events: Updates shop domain in backend if domain changed

    Args:
        event: The event payload from DynamoDB Streams.
        context: The runtime information of the Lambda function.

    Returns:
        Dict containing the status and number of processed records.
    """
    processed_records = 0

    for record in event.get("Records", []):
        event_name = record.get("eventName")

        if event_name not in ["INSERT", "MODIFY"]:
            continue

        dynamodb_data = record.get("dynamodb", {})
        new_image = dynamodb_data.get("NewImage")

        if not new_image or new_image.get("sk", {}).get("S") != METADATA_SK:
            continue

        try:
            new_shop = ShopMetadata.from_dynamodb_item(new_image)
            logger.info(f"Processing {event_name} event for shop: {new_shop.domain}")

            if event_name == "INSERT":
                register_or_update_shop(new_shop, http_session)
            elif event_name == "MODIFY":
                old_image = dynamodb_data.get("OldImage")
                if old_image:
                    old_shop = ShopMetadata.from_dynamodb_item(old_image)
                    update_shop_domain(old_shop, new_shop, http_session)
                else:
                    logger.warning("MODIFY event received but OldImage is missing")

            processed_records += 1

        except Exception as e:
            pk = new_image.get("pk", {}).get("S", "Unknown")
            logger.error(f"Error processing {event_name} event for shop pk '{pk}': {e}")

    logger.info(f"Successfully processed {processed_records} shop record(s).")
    return {"status": "success", "processed_records": processed_records}
