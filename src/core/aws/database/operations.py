import logging
import os
from typing import List, Optional
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

    def get_product_urls_by_domain(self, domain: str) -> List[str]:
        """
        Get all product URLs for a given domain using GSI1.

        Args:
            domain: Shop domain

        Returns:
            List of product URLs
        """
        urls = []
        last_evaluated_key = None

        try:
            while True:
                query_args = {
                    "TableName": self.table_name,
                    "IndexName": "GSI1",
                    "KeyConditionExpression": "gsi1_pk = :pk AND gsi1_sk = :type",
                    "ExpressionAttributeValues": {
                        ":pk": {"S": f"SHOP#{domain}"},
                        ":type": {"S": "product"},
                    },
                    "ProjectionExpression": self.URL_ATTR,
                    "ExpressionAttributeNames": {self.URL_ATTR: "url"},
                }
                if last_evaluated_key:
                    query_args["ExclusiveStartKey"] = last_evaluated_key

                response = self.client.query(**query_args)
                urls.extend([item["url"]["S"] for item in response.get("Items", [])])

                last_evaluated_key = response.get("LastEvaluatedKey")
                if not last_evaluated_key:
                    break
            return urls
        except ClientError as e:
            logger.error(f"Error querying product URLs for {domain}: {e}")
            raise
        except Exception as e:
            logger.error(
                f"An unexpected error occurred while querying product URLs for {domain}: {e}"
            )
            raise

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
                query_args = {
                    "TableName": self.table_name,
                    "IndexName": "GSI4",
                    "KeyConditionExpression": "gsi4_pk = :cdn",
                    "ExpressionAttributeValues": {
                        ":cdn": {"S": core_domain_name},
                    },
                }

                # Add filter if domain_to_exclude is specified
                if domain_to_exclude:
                    query_args["FilterExpression"] = "#d <> :domain_to_exclude"
                    query_args["ExpressionAttributeNames"] = {"#d": "domain"}
                    query_args["ExpressionAttributeValues"][":domain_to_exclude"] = {
                        "S": domain_to_exclude
                    }

                if last_evaluated_key:
                    query_args["ExclusiveStartKey"] = last_evaluated_key

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
            if "does not have the specified index" in e.response.get("Error", {}).get(
                "Message", ""
            ):
                logger.warning(
                    "GSI 'GSI4' not found. Cannot search by core domain name."
                )
            else:
                logger.error(
                    f"Error finding shops by core domain name '{core_domain_name}': {e}"
                )
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

    def _batch_write_items(self, items: list, item_type: str) -> dict:
        """
        Generic batch write operation for DynamoDB items.

        Args:
            items: List of item dicts (already in DynamoDB format)
            item_type: Description of item type for logging

        Returns:
            Response dict with UnprocessedItems
        """
        if not items:
            return {"UnprocessedItems": {}}

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

            for i in range(0, len(items), 25):
                batch = items[i : i + 25]
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

    def update_shop_metadata(
        self,
        domain: str,
        last_crawled_start: Optional[str] = None,
        last_crawled_end: Optional[str] = None,
        last_scraped_start: Optional[str] = None,
        last_scraped_end: Optional[str] = None,
    ) -> dict:
        """
        Update shop metadata with new timestamp values.

        Important: GSI keys (gsi2_sk, gsi3_sk) must be explicitly updated.
        DynamoDB does NOT automatically sync them when base attributes change.

        Args:
            domain: Shop domain
            last_crawled_start: ISO 8601 timestamp for crawl start
            last_crawled_end: ISO 8601 timestamp for crawl end
            last_scraped_start: ISO 8601 timestamp for scrape start
            last_scraped_end: ISO 8601 timestamp for scrape end

        Returns:
            UpdateItem response
        """
        update_expression_parts = []
        expression_attribute_values = {}

        if last_crawled_start:
            update_expression_parts.append("last_crawled_start = :crawled_start")
            update_expression_parts.append("gsi2_sk = :crawled_start")
            expression_attribute_values[":crawled_start"] = {"S": last_crawled_start}

        if last_crawled_end:
            update_expression_parts.append("last_crawled_end = :crawled_end")
            expression_attribute_values[":crawled_end"] = {"S": last_crawled_end}

        if last_scraped_start:
            update_expression_parts.append("last_scraped_start = :scraped_start")
            update_expression_parts.append("gsi3_sk = :scraped_start")
            expression_attribute_values[":scraped_start"] = {"S": last_scraped_start}

        if last_scraped_end:
            update_expression_parts.append("last_scraped_end = :scraped_end")
            expression_attribute_values[":scraped_end"] = {"S": last_scraped_end}

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


# Global operations instance
db_operations = DynamoDBOperations()
