import logging
import os
from typing import List, Optional, Tuple
import socket
from iptocc import get_country_code

from botocore.exceptions import ClientError
from dotenv import load_dotenv

from src.core.aws.database.models import ShopMetadata, URLEntry, get_dynamodb_client

load_dotenv()
logger = logging.getLogger(__name__)


class DynamoDBOperations:
    """Operations for DynamoDB single-table design using boto3."""

    METADATA_SK = "META#"
    DOMAIN_ATTR = "#domain_attr"
    URL_ATTR = "#url_attr"

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
        domain_to_exclude: Optional[str],
        last_evaluated_key: Optional[dict],
    ) -> dict:
        """
        Build query arguments for core domain search.

        Args:
            core_domain_name: Core domain name to search for
            domain_to_exclude: Optional domain to exclude
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

        if domain_to_exclude:
            query_args["FilterExpression"] = "#d <> :domain_to_exclude"
            query_args["ExpressionAttributeNames"] = {"#d": "domain"}
            query_args["ExpressionAttributeValues"][":domain_to_exclude"] = {
                "S": domain_to_exclude
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
        self, core_domain_name: str, domain_to_exclude: Optional[str] = None
    ) -> List[ShopMetadata]:
        """
        Finds ALL shops by their core domain name using GSI4.

        Args:
            core_domain_name: The core domain name (e.g., 'example').
            domain_to_exclude: Optional domain to exclude from the search results.

        Returns:
            List of ShopMetadata objects. Empty list if no matches found.
        """
        shops = []
        last_evaluated_key = None

        try:
            while True:
                query_args = self._build_core_domain_query_args(
                    core_domain_name, domain_to_exclude, last_evaluated_key
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
            return []
        except Exception as e:
            logger.error(
                f"Unexpected error querying shops by core domain name '{core_domain_name}': {e}"
            )
            return []

    def get_shops_by_country_and_crawled_date(
        self, country: str, start_date: str, end_date: str
    ) -> List[str]:
        """
        Query shops by country and crawled date range using GSI2.

        Args:
            country: The country to query (will be prefixed with COUNTRY# if not already).
            start_date: The start of the date range (ISO 8601).
            end_date: The end of the date range (ISO 8601).

        Returns:
            A list of domains.
        """
        if not country.startswith("COUNTRY#"):
            country = f"COUNTRY#{country}"

        return self._query_shops_by_country_and_date(
            index_name="GSI2",
            date_attribute_name="gsi2_sk",
            country=country,
            start_date=start_date,
            end_date=end_date,
        )

    def get_shops_by_country_and_scraped_date(
        self, country: str, start_date: str, end_date: str
    ) -> List[str]:
        """
        Query shops by country and scraped date range using GSI3.

        Args:
            country: The country to query (will be prefixed with COUNTRY# if not already).
            start_date: The start of the date range (ISO 8601).
            end_date: The end of the date range (ISO 8601).

        Returns:
            A list of domains.
        """
        # Ensure country has COUNTRY# prefix
        if not country.startswith("COUNTRY#"):
            country = f"COUNTRY#{country}"

        return self._query_shops_by_country_and_date(
            index_name="GSI3",
            date_attribute_name="gsi3_sk",
            country=country,
            start_date=start_date,
            end_date=end_date,
        )

    def _query_shops_by_country_and_date(
        self,
        index_name: str,
        date_attribute_name: str,
        country: str,
        start_date: str,
        end_date: str,
    ) -> List[str]:
        """
        Generic helper to query shops by country and a date range from a GSI.

        Args:
            index_name: The name of the GSI to query (GSI2 or GSI3).
            date_attribute_name: The name of the sort key attribute (gsi2_sk or gsi3_sk).
            country: The country to query (with COUNTRY# prefix).
            start_date: The start of the date range (ISO 8601).
            end_date: The end of the date range (ISO 8601).

        Returns:
            A list of domains.
        """
        domains = []
        last_evaluated_key = None

        # Determine the partition key attribute based on index
        pk_attribute = "gsi2_pk" if index_name == "GSI2" else "gsi3_pk"

        try:
            while True:
                query_args = {
                    "TableName": self.table_name,
                    "IndexName": index_name,
                    "KeyConditionExpression": f"{pk_attribute} = :country AND {date_attribute_name} BETWEEN :start AND :end",
                    "ExpressionAttributeValues": {
                        ":country": {"S": country},
                        ":start": {"S": start_date},
                        ":end": {"S": end_date},
                    },
                    "ProjectionExpression": self.DOMAIN_ATTR,
                    "ExpressionAttributeNames": {self.DOMAIN_ATTR: "domain"},
                }
                if last_evaluated_key:
                    query_args["ExclusiveStartKey"] = last_evaluated_key

                response = self.client.query(**query_args)
                domains.extend(
                    [item["domain"]["S"] for item in response.get("Items", [])]
                )

                last_evaluated_key = response.get("LastEvaluatedKey")
                if not last_evaluated_key:
                    break
            return domains
        except ClientError as e:
            logger.error(
                f"Error querying shops by {date_attribute_name} for {country}: {e}"
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

    def batch_write_shop_metadata(self, metadata_list: List[ShopMetadata]) -> dict:
        """
        Batch write shop metadata entries.

        Args:
            metadata_list: List of ShopMetadata objects

        Returns:
            Response dict with UnprocessedItems
        """
        items = [metadata.to_dynamodb_item() for metadata in metadata_list]
        return self._batch_write_items(items, "shop metadata entries")

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
            value: Timestamp value (None for NULL, ... skipped in caller)
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
        last_crawled_start: Optional[str] = ...,
        last_crawled_end: Optional[str] = ...,
        last_scraped_start: Optional[str] = ...,
        last_scraped_end: Optional[str] = ...,
    ) -> dict:
        """
        Update shop metadata with new timestamp values.

        Important: GSI keys (gsi2_sk, gsi3_sk) must be explicitly updated.
        DynamoDB does NOT automatically sync them when base attributes change.

        Args:
            domain: Shop domain
            last_crawled_start: ISO 8601 timestamp, None to set NULL, or ... to skip update
            last_crawled_end: ISO 8601 timestamp, None to set NULL, or ... to skip update
            last_scraped_start: ISO 8601 timestamp, None to set NULL, or ... to skip update
            last_scraped_end: ISO 8601 timestamp, None to set NULL, or ... to skip update

        Returns:
            UpdateItem response
        """
        update_expression_parts = []
        expression_attribute_values = {}

        if last_crawled_start is not ...:
            self._add_timestamp_update(
                update_expression_parts,
                expression_attribute_values,
                "last_crawled_start",
                last_crawled_start,
                "crawled_start",
            )

        if last_crawled_end is not ...:
            self._add_timestamp_update(
                update_expression_parts,
                expression_attribute_values,
                "last_crawled_end",
                last_crawled_end,
                "crawled_end",
                "gsi2_sk",
            )

        if last_scraped_start is not ...:
            self._add_timestamp_update(
                update_expression_parts,
                expression_attribute_values,
                "last_scraped_start",
                last_scraped_start,
                "scraped_start",
            )

        if last_scraped_end is not ...:
            self._add_timestamp_update(
                update_expression_parts,
                expression_attribute_values,
                "last_scraped_end",
                last_scraped_end,
                "scraped_end",
                "gsi3_sk",
            )

        if not update_expression_parts:
            logger.warning("No fields to update for shop metadata.")
            return {}

        update_expression = "SET " + ", ".join(update_expression_parts)

        try:
            response = self.client.update_item(
                TableName=self.table_name,
                Key={"pk": {"S": f"SHOP#{domain}"}, "sk": {"S": self.METADATA_SK}},
                UpdateExpression=update_expression,
                ExpressionAttributeValues=expression_attribute_values,
                ReturnValues="UPDATED_NEW",
            )
            logger.info(f"Updated shop metadata for {domain}")
            return response["Attributes"]
        except ClientError as e:
            logger.error(
                "Couldn't update shop metadata for %s. Here's why: %s: %s",
                domain,
                e.response["Error"]["Code"],
                e.response["Error"]["Message"],
            )
            raise

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

    def upsert_url_entry(self, entry: URLEntry) -> None:
        """
        Insert or update URL entry.

        Args:
            entry: URLEntry object
        """
        self._upsert_item(entry.to_dynamodb_item(), f"URL entry for {entry.url}")

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


# Global operations instance
db_operations = DynamoDBOperations()
