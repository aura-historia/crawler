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

    def __init__(self):
        self.client = get_dynamodb_client()
        self.table_name = os.getenv("DYNAMODB_TABLE_NAME")

    def get_product_urls_by_domain(self, domain: str) -> List[str]:
        """
        Get all product URLs for a given domain using the GSI.

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
                    "IndexName": "IsProductIndex",
                    "KeyConditionExpression": "PK = :pk AND is_product = :is_product",
                    "ExpressionAttributeValues": {
                        ":pk": {"S": f"SHOP#{domain}"},
                        ":is_product": {"N": "1"},
                    },
                    "ProjectionExpression": "#url_attr",
                    "ExpressionAttributeNames": {"#url_attr": "url"},
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

    def get_shops_by_country_and_crawled_date(
        self, country: str, start_date: str, end_date: str
    ) -> List[str]:
        """
        Query shops by country and crawled date range using GSI.

        Args:
            country: The country to query.
            start_date: The start of the date range (ISO 8601).
            end_date: The end of the date range (ISO 8601).

        Returns:
            A list of domains.
        """
        return self._query_shops_by_country_and_date(
            index_name="CountryLastCrawledIndex",
            date_attribute_name="last_crawled",
            country=country,
            start_date=start_date,
            end_date=end_date,
        )

    def get_shops_by_country_and_scraped_date(
        self, country: str, start_date: str, end_date: str
    ) -> List[str]:
        """
        Query shops by country and scraped date range using GSI.

        Args:
            country: The country to query.
            start_date: The start of the date range (ISO 8601).
            end_date: The end of the date range (ISO 8601).

        Returns:
            A list of domains.
        """
        return self._query_shops_by_country_and_date(
            index_name="CountryLastScrapedIndex",
            date_attribute_name="last_scraped",
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
            index_name: The name of the GSI to query.
            date_attribute_name: The name of the date attribute to filter on (e.g., 'last_crawled').
            country: The country to query.
            start_date: The start of the date range (ISO 8601).
            end_date: The end of the date range (ISO 8601).

        Returns:
            A list of domains.
        """
        domains = []
        last_evaluated_key = None
        try:
            while True:
                query_args = {
                    "TableName": self.table_name,
                    "IndexName": index_name,
                    "KeyConditionExpression": f"shop_country = :country AND {date_attribute_name} BETWEEN :start AND :end",
                    "ExpressionAttributeValues": {
                        ":country": {"S": country},
                        ":start": {"S": start_date},
                        ":end": {"S": end_date},
                    },
                    "ProjectionExpression": "#domain_attr",
                    "ExpressionAttributeNames": {"#domain_attr": "domain"},
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
        last_crawled: Optional[str] = None,
        last_scraped: Optional[str] = None,
    ) -> dict:
        """
        Update shop metadata with new values.

        Args:
            domain: Shop domain
            last_crawled: ISO 8601 timestamp for last crawl
            last_scraped: ISO 8601 timestamp for last scrape

        Returns:
            UpdateItem response
        """
        update_expression_parts = []
        expression_attribute_values = {}

        if last_crawled:
            update_expression_parts.append("last_crawled = :crawled")
            expression_attribute_values[":crawled"] = {"S": last_crawled}

        if last_scraped:
            update_expression_parts.append("last_scraped = :scraped")
            expression_attribute_values[":scraped"] = {"S": last_scraped}

        if not update_expression_parts:
            logger.warning("No fields to update for shop metadata.")
            return {}

        update_expression = "SET " + ", ".join(update_expression_parts)

        try:
            response = self.client.update_item(
                TableName=self.table_name,
                Key={"PK": {"S": f"SHOP#{domain}"}, "SK": {"S": self.METADATA_SK}},
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
