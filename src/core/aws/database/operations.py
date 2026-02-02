import logging
import os
from typing import List, Optional, Tuple
import socket
from iptocc import get_country_code

from botocore.exceptions import ClientError
from dotenv import load_dotenv

from src.core.aws.database.models import ShopMetadata, URLEntry, get_dynamodb_client
from src.core.aws.database.constants import STATE_NEVER, STATE_PROGRESS, STATE_DONE

load_dotenv()
logger = logging.getLogger(__name__)


def parse_gsi_sk(gsi_sk: str) -> tuple[str, Optional[str]]:
    """Parse GSI sort key into state and timestamp.

    Args:
        gsi_sk: Prefixed sort key.

    Returns:
        Tuple of (state, timestamp). Timestamp is None for NEVER state.

    Examples:
        > parse_gsi_sk("NEVER#")
        ('NEVER#', None)
        > parse_gsi_sk("DONE#2026-01-15T12:00:00Z")
        ('DONE#', '2026-01-15T12:00:00Z')
    """
    if gsi_sk.startswith(STATE_NEVER):
        return STATE_NEVER, None
    elif gsi_sk.startswith(STATE_PROGRESS):
        return STATE_PROGRESS, gsi_sk[len(STATE_PROGRESS) :]
    elif gsi_sk.startswith(STATE_DONE):
        return STATE_DONE, gsi_sk[len(STATE_DONE) :]
    raise ValueError(f"Invalid GSI SK: {gsi_sk}")


class DynamoDBOperations:
    """Operations for DynamoDB single-table design using boto3."""

    METADATA_SK = "META#"
    DOMAIN_ATTR = "#domain_attr"
    URL_ATTR = "#url_attr"
    COUNTRY_PREFIX = "COUNTRY#"
    COUNTRY_KEY_PLACEHOLDER = ":country"

    def __init__(self):
        self.client = get_dynamodb_client()
        self.table_name = os.getenv("DYNAMODB_TABLE_NAME")

    def get_product_urls_by_domain(
        self,
        domain: str,
        max_urls: int = 1000,
        last_evaluated_key: Optional[dict] = None,
    ) -> Tuple[List[str], Optional[dict]]:
        """
        Get product URLs for a domain with pagination support.

        This method limits the number of URLs fetched to prevent excessive RRU consumption
        and memory issues when dealing with domains that have tens of thousands of products.

        Args:
            domain: Shop domain
            max_urls: Maximum URLs to return in single call (default: 1000)
            last_evaluated_key: Pagination token from previous call

        Returns:
            Tuple of (urls, next_pagination_token). Token is None if no more results.
        """
        urls = []

        try:
            query_args = {
                "TableName": self.table_name,
                "IndexName": "GSI1",
                "KeyConditionExpression": "gsi1_pk = :pk AND gsi1_sk = :type",
                "ExpressionAttributeValues": {
                    ":pk": {"S": f"SHOP#{domain}"},
                    ":type": {"S": "product"},
                },
                "Limit": max_urls,
                "ProjectionExpression": self.URL_ATTR,
                "ExpressionAttributeNames": {self.URL_ATTR: "url"},
            }

            if last_evaluated_key:
                query_args["ExclusiveStartKey"] = last_evaluated_key

            response = self.client.query(**query_args)
            urls = [item["url"]["S"] for item in response.get("Items", [])]

            return urls, response.get("LastEvaluatedKey")
        except ClientError as e:
            logger.error(f"Error querying product URLs for {domain}: {e}")
            raise
        except Exception as e:
            logger.error(
                f"An unexpected error occurred while querying product URLs for {domain}: {e}"
            )
            raise

    def get_all_product_urls_by_domain(self, domain: str) -> List[str]:
        """
        Get ALL product URLs for a domain by automatically paginating through all results.

        Warning: This method will fetch ALL URLs regardless of count, which can:
        - Consume significant DynamoDB RCUs for domains with many products
        - Use substantial memory for large result sets (10k+ URLs)
        - Take considerable time to complete

        For large domains, consider using get_product_urls_by_domain() with manual
        pagination for better control and cost management.

        Args:
            domain: Shop domain

        Returns:
            List of all product URLs for the domain
        """
        all_urls = []
        last_evaluated_key = None
        page_count = 0

        try:
            while True:
                page_count += 1
                urls, last_evaluated_key = self.get_product_urls_by_domain(
                    domain=domain, max_urls=1000, last_evaluated_key=last_evaluated_key
                )

                all_urls.extend(urls)

                logger.debug(
                    f"Fetched page {page_count} for {domain}: {len(urls)} URLs "
                    f"(total: {len(all_urls)})"
                )

                if not last_evaluated_key:
                    break

            logger.info(
                f"Retrieved ALL {len(all_urls)} product URLs for {domain} "
                f"in {page_count} page(s)"
            )
            return all_urls

        except Exception as e:
            logger.error(
                f"Error fetching all product URLs for {domain} "
                f"(retrieved {len(all_urls)} before error): {e}"
            )
            raise

    def _build_core_domain_query_args(
        self,
        core_domain_name: str,
        last_evaluated_key: Optional[dict],
    ) -> dict:
        """
        Build query arguments for core domain search.

        Args:
            core_domain_name: Core domain name to search for
            last_evaluated_key: Pagination token

        Returns:
            Query arguments dict for DynamoDB
        """
        query_args = {
            "TableName": self.table_name,
            "IndexName": "GSI4",
            "KeyConditionExpression": "gsi4_pk = :cdn",
            "ExpressionAttributeValues": {
                ":cdn": {"S": core_domain_name},
            },
        }

        if last_evaluated_key:
            query_args["ExclusiveStartKey"] = last_evaluated_key

        return query_args

    def _handle_core_domain_error(
        self, error: ClientError, core_domain_name: str
    ) -> None:
        """
        Handle ClientError from core domain search.

        Args:
            error: ClientError exception
            core_domain_name: Core domain being searched
        """
        error_message = error.response.get("Error", {}).get("Message", "")
        if "does not have the specified index" in error_message:
            logger.warning("GSI 'GSI4' not found. Cannot search by core domain name.")
        else:
            logger.error(
                f"Error finding shops by core domain name '{core_domain_name}': {error}"
            )

    def find_all_domains_by_core_domain_name(
        self, core_domain_name: str
    ) -> List[ShopMetadata]:
        """
        Finds ALL shops by their core domain name using GSI4.

        Args:
            core_domain_name: The core domain name (e.g., 'example').

        Returns:
            List of ShopMetadata objects. Empty list if no matches found.
        """
        shops = []
        last_evaluated_key = None

        try:
            while True:
                query_args = self._build_core_domain_query_args(
                    core_domain_name, last_evaluated_key
                )
                response = self.client.query(**query_args)

                for item in response.get("Items", []):
                    shops.append(ShopMetadata.from_dynamodb_item(item))

                last_evaluated_key = response.get("LastEvaluatedKey")
                if not last_evaluated_key:
                    break

            logger.info(
                f"Found {len(shops)} shop(s) with core domain name '{core_domain_name}'"
            )
            return shops
        except ClientError as e:
            self._handle_core_domain_error(e, core_domain_name)
            raise
        except Exception as e:
            logger.error(
                f"Unexpected error querying shops by core domain name '{core_domain_name}': {e}"
            )
            raise

    def _batch_write_items(
        self, items: list, item_type: str, max_items: int = 1000
    ) -> dict:
        """
        Generic batch write operation for DynamoDB items.

        Args:
            items: List of item dicts (already in DynamoDB format)
            item_type: Description of item type for logging
            max_items: Maximum items to write in one call (default: 1000, prevents memory spikes)

        Returns:
            Response dict with UnprocessedItems
        """
        if not items:
            return {"UnprocessedItems": {}}

        # Enforce maximum items limit to prevent memory issues and excessive WCU costs
        if len(items) > max_items:
            logger.warning(
                f"Batch write requested {len(items)} {item_type}, limiting to {max_items}. "
                f"Consider pagination or multiple batch calls."
            )
            items = items[:max_items]

        unique_items = list(
            {
                (item["pk"]["S"], item.get("sk", {}).get("S", "")): item
                for item in items
            }.values()
        )

        if len(unique_items) < len(items):
            logger.info(
                f"Filtered out {len(items) - len(unique_items)} duplicate items from batch."
            )

        try:
            # DynamoDB batch_write_item supports max 25 items per request
            unprocessed_items = []

            for i in range(0, len(unique_items), 25):
                batch = unique_items[i : i + 25]
                request_items = {
                    self.table_name: [{"PutRequest": {"Item": item}} for item in batch]
                }

                response = self.client.batch_write_item(RequestItems=request_items)

                # Handle unprocessed items
                if response.get("UnprocessedItems"):
                    unprocessed_items.extend(
                        response["UnprocessedItems"].get(self.table_name, [])
                    )

            logger.info(f"Batch wrote {len(items)} {item_type}")

            result = {"UnprocessedItems": {}}
            if unprocessed_items:
                result["UnprocessedItems"] = {self.table_name: unprocessed_items}

            return result

        except ClientError as e:
            logger.error(f"Error in batch write {item_type}: {e}")
            raise
        except Exception as e:
            logger.error(f"Error in batch write {item_type}: {e}")
            raise

    def batch_write_url_entries(self, url_entries: List[URLEntry]) -> dict:
        """
        Batch write URL entries.

        Args:
            url_entries: List of URLEntry objects

        Returns:
            Response dict with UnprocessedItems
        """
        items = [entry.to_dynamodb_item() for entry in url_entries]
        return self._batch_write_items(items, "URL entries")

    def _add_timestamp_update(
        self,
        update_parts: list,
        attr_values: dict,
        field_name: str,
        value: Optional[str],
        placeholder: str,
        gsi_key: Optional[str] = None,
    ) -> None:
        """
        Add timestamp field update to expression parts.

        Args:
            update_parts: List to append update expressions to
            attr_values: Dict to add attribute values to
            field_name: Name of the field to update
            value: Timestamp value (None to set NULL, ... skipped in caller)
            placeholder: Placeholder name for expression attribute value
            gsi_key: Optional GSI key to also update
        """
        if value is not None:
            update_parts.append(f"{field_name} = :{placeholder}")
            attr_values[f":{placeholder}"] = {"S": value}
            if gsi_key:
                update_parts.append(f"{gsi_key} = :{placeholder}")
        else:
            update_parts.append(f"{field_name} = :{placeholder}_null")
            attr_values[f":{placeholder}_null"] = {"NULL": True}
            if gsi_key:
                update_parts.append(f"{gsi_key} = :{placeholder}_null")

    def update_shop_metadata(
        self,
        domain: str,
        last_crawled_start: Optional[str] = None,
        last_crawled_end: Optional[str] = None,
        last_scraped_start: Optional[str] = None,
        last_scraped_end: Optional[str] = None,
        shop_country: Optional[str] = None,
    ) -> dict:
        """
        Update shop metadata with new timestamp values.

        Args:
            domain: Shop domain
            last_crawled_start: ISO 8601 timestamp, None to skip update
            last_crawled_end: ISO 8601 timestamp, None to skip update
            last_scraped_start: ISO 8601 timestamp, None to skip update
            last_scraped_end: ISO 8601 timestamp, None to skip update
            shop_country: Country code, optional. Used to update GSI PKs.

        Returns:
            UpdateItem response
        """

        # Fetch current metadata only if country is needed and not provided
        if shop_country is None:
            current_metadata = self.get_shop_metadata(domain)
            shop_country = current_metadata.shop_country if current_metadata else None

        update_expression_parts = []
        expression_attribute_values = {}

        if last_crawled_start is not None:
            self._add_timestamp_update(
                update_expression_parts,
                expression_attribute_values,
                "last_crawled_start",
                last_crawled_start,
                "crawled_start",
            )

        if last_crawled_end is not None:
            self._add_timestamp_update(
                update_expression_parts,
                expression_attribute_values,
                "last_crawled_end",
                last_crawled_end,
                "crawled_end",
                "gsi2_sk",
            )
            if shop_country:
                update_expression_parts.append("gsi2_pk = :gsi2_pk")
                expression_attribute_values[":gsi2_pk"] = {"S": shop_country}

        if last_scraped_start is not None:
            self._add_timestamp_update(
                update_expression_parts,
                expression_attribute_values,
                "last_scraped_start",
                last_scraped_start,
                "scraped_start",
            )

        if last_scraped_end is not None:
            self._add_timestamp_update(
                update_expression_parts,
                expression_attribute_values,
                "last_scraped_end",
                last_scraped_end,
                "scraped_end",
                "gsi3_sk",
            )
            if shop_country:
                update_expression_parts.append("gsi3_pk = :gsi3_pk")
                expression_attribute_values[":gsi3_pk"] = {"S": shop_country}

        if not update_expression_parts:
            logger.warning("No fields to update for shop metadata.")
            return {}

        update_expression = "SET " + ", ".join(update_expression_parts)

        response = self.client.update_item(
            TableName=self.table_name,
            Key={"pk": {"S": f"SHOP#{domain}"}, "sk": {"S": self.METADATA_SK}},
            UpdateExpression=update_expression,
            ExpressionAttributeValues=expression_attribute_values,
            ReturnValues="UPDATED_NEW",
        )

        logger.info(f"Updated shop metadata for {domain}")
        return response["Attributes"]

    def update_url_hash(self, domain: str, url: str, new_hash: str) -> dict:
        """
        Update only the hash field for a given URL entry.

        Args:
            domain: Shop domain
            url: Product URL
            new_hash: New hash value to set

        Returns:
            UpdateItem response
        """
        try:
            response = self.client.update_item(
                TableName=self.table_name,
                Key={
                    "pk": {"S": f"SHOP#{domain}"},
                    "sk": {"S": f"URL#{url}"},
                },
                UpdateExpression="SET #h = :new_hash",
                ExpressionAttributeNames={"#h": "hash"},
                ExpressionAttributeValues={":new_hash": {"S": new_hash}},
                ReturnValues="UPDATED_NEW",
            )
            return response["Attributes"]
        except ClientError as e:
            logger.error(
                "Couldn't update hash for %s in %s. Here's why: %s: %s",
                url,
                domain,
                e.response["Error"]["Code"],
                e.response["Error"]["Message"],
            )
            raise

    def _upsert_item(self, item: dict, context: str) -> None:
        """
        Generic upsert operation for DynamoDB items.

        Args:
            item: DynamoDB item dict
            context: Contextual description for logging
        """
        try:
            self.client.put_item(TableName=self.table_name, Item=item)
            logger.info(f"Upserted {context}")
        except Exception as e:
            logger.error(f"Error upserting {context}: {e}")
            raise

    def upsert_shop_metadata(self, metadata: ShopMetadata) -> None:
        """
        Insert or update shop metadata.

        If the country is not provided, it will be determined from the domain's IP address.

        Args:
            metadata: ShopMetadata object
        """
        if metadata.shop_country is None:
            try:
                ip_address = socket.gethostbyname(metadata.domain)
                country_code = get_country_code(ip_address)
                metadata.shop_country = country_code
                logger.info(
                    f"Determined country for {metadata.domain} as {country_code}"
                )
            except socket.gaierror:
                logger.warning(
                    f"Could not resolve IP for domain: {metadata.domain}. Country not set."
                )
            except Exception as e:
                logger.error(
                    f"An error occurred during country lookup for {metadata.domain}: {e}"
                )

        self._upsert_item(
            metadata.to_dynamodb_item(), f"shop metadata for {metadata.domain}"
        )

    def get_shop_metadata(self, domain: str) -> Optional[ShopMetadata]:
        """
        Retrieve shop metadata for a domain.

        Args:
            domain: Shop domain

        Returns:
            ShopMetadata object if found, else None
        """
        try:
            response = self.client.get_item(
                TableName=self.table_name,
                Key={
                    "pk": {"S": f"SHOP#{domain}"},
                    "sk": {"S": self.METADATA_SK},
                },
            )
            item = response.get("Item")
            if item:
                return ShopMetadata.from_dynamodb_item(item)
            return None
        except Exception as e:
            logger.error(f"Error fetching shop metadata for {domain}: {e}")
            return None

    def get_url_entry(self, domain: str, url: str) -> Optional[URLEntry]:
        """
        Retrieve a single URL entry by domain and url.

        Args:
            domain: Shop domain
            url: Product URL

        Returns:
            URLEntry object if found, else None
        """
        try:
            response = self.client.get_item(
                TableName=self.table_name,
                Key={
                    "pk": {"S": f"SHOP#{domain}"},
                    "sk": {"S": f"URL#{url}"},
                },
            )
            item = response.get("Item")
            if item:
                return URLEntry.from_dynamodb_item(item)
            return None
        except Exception as e:
            logger.error(f"Error fetching URL entry for {url} in {domain}: {e}")
            return None

    def _get_orchestration_index_params(
        self, operation_type: str
    ) -> Tuple[str, str, str]:
        """Determine GSI params based on operation type."""
        if operation_type == "crawl":
            return "GSI2", "gsi2_pk", "gsi2_sk"
        elif operation_type == "scrape":
            return "GSI3", "gsi3_pk", "gsi3_sk"
        else:
            raise ValueError(
                f"operation_type must be 'crawl' or 'scrape', got: {operation_type}"
            )

    def _get_target_countries(self, country: Optional[str]) -> List[str]:
        """Resolve list of countries to query."""
        if country:
            return [
                country
                if country.startswith(self.COUNTRY_PREFIX)
                else f"COUNTRY#{country}"
            ]

        countries = ["COUNTRY#DE"]
        logger.info("No country specified, querying default countries: %s", countries)
        return countries

    def _query_paginated_shops(self, query_args: dict) -> List[ShopMetadata]:
        """Execute paginated query and return shops."""
        shops = []
        last_evaluated_key = None

        while True:
            if last_evaluated_key:
                query_args["ExclusiveStartKey"] = last_evaluated_key

            response = self.client.query(**query_args)
            for item in response.get("Items", []):
                shops.append(ShopMetadata.from_dynamodb_item(item))

            last_evaluated_key = response.get("LastEvaluatedKey")
            if not last_evaluated_key:
                break

        return shops

    def _fetch_eligible_shops_for_country(
        self,
        country_key: str,
        index_name: str,
        pk_attr: str,
        sk_attr: str,
        cutoff_date: str,
    ) -> List[ShopMetadata]:
        """Fetch eligible shops (NEVER or DONE < cutoff) for a country."""
        shops = []

        # 1. Query for STATE_NEVER
        query_args_never = {
            "TableName": self.table_name,
            "IndexName": index_name,
            "KeyConditionExpression": f"{pk_attr} = {self.COUNTRY_KEY_PLACEHOLDER} AND {sk_attr} = :never",
            "ExpressionAttributeValues": {
                self.COUNTRY_KEY_PLACEHOLDER: {"S": country_key},
                ":never": {"S": STATE_NEVER},
            },
        }
        shops.extend(self._query_paginated_shops(query_args_never))

        # 2. Query for STATE_DONE with timestamp <= cutoff
        query_args_done = {
            "TableName": self.table_name,
            "IndexName": index_name,
            "KeyConditionExpression": f"{pk_attr} = {self.COUNTRY_KEY_PLACEHOLDER} AND {sk_attr} BETWEEN :done_start AND :done_end",
            "ExpressionAttributeValues": {
                self.COUNTRY_KEY_PLACEHOLDER: {"S": country_key},
                ":done_start": {"S": STATE_DONE},
                ":done_end": {"S": f"{STATE_DONE}{cutoff_date}"},
            },
        }
        shops.extend(self._query_paginated_shops(query_args_done))

        return shops

    def get_shops_for_orchestration(
        self,
        operation_type: str,
        cutoff_date: str,
        country: Optional[str] = None,
    ) -> List[ShopMetadata]:
        """Get shops that need crawling or scraping based on operation type.

        Returns shops where the relevant timestamp starts with NEVER#
        or starts with DONE# and is older than cutoff_date.

        Args:
            operation_type: Either "crawl" or "scrape"
            cutoff_date: ISO 8601 date string (e.g., '2026-01-02T00:00:00Z')
            country: Optional country filter (without COUNTRY# prefix)

        Returns:
            List of ShopMetadata objects that need processing

        Raises:
            ValueError: If operation_type is not "crawl" or "scrape"
        """
        try:
            index_name, pk_attr, sk_attr = self._get_orchestration_index_params(
                operation_type
            )
            countries = self._get_target_countries(country)

            all_shops = []
            for country_key in countries:
                shops = self._fetch_eligible_shops_for_country(
                    country_key, index_name, pk_attr, sk_attr, cutoff_date
                )
                all_shops.extend(shops)

            logger.info(
                f"Found {len(all_shops)} shops for {operation_type} (cutoff: {cutoff_date})"
            )
            return all_shops

        except ClientError as e:
            logger.error(
                f"Error querying shops for {operation_type} orchestration: {e}"
            )
            raise
        except Exception as e:
            logger.error(
                f"Unexpected error in get_shops_for_orchestration ({operation_type}): {e}"
            )
            raise


db_operations = DynamoDBOperations()
