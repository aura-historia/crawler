import logging
import os
from typing import List, Optional

from botocore.exceptions import ClientError
from dotenv import load_dotenv

from src.core.aws.database.models import ShopMetadata, URLEntry, get_dynamodb_client

load_dotenv()
logger = logging.getLogger(__name__)


class DynamoDBOperations:
    """Operations for DynamoDB single-table design using boto3."""

    def __init__(self):
        self.client = get_dynamodb_client()
        self.table_name = os.getenv("DYNAMODB_TABLE_NAME")

    def get_shop_metadata(self, domain: str) -> Optional[ShopMetadata]:
        """
        Get shop metadata for a domain.

        Args:
            domain: Shop domain (e.g., 'example.com')

        Returns:
            ShopMetadata or None if not found
        """
        try:
            response = self.client.get_item(
                TableName=self.table_name,
                Key={"PK": {"S": f"SHOP#{domain}"}, "SK": {"S": "META#"}},
            )

            if "Item" not in response:
                logger.debug(f"Shop metadata not found for domain: {domain}")
                return None

            return ShopMetadata.from_dynamodb_item(response["Item"])

        except ClientError as e:
            logger.error(f"Error getting shop metadata for {domain}: {e}")
            raise
        except Exception as e:
            logger.error(f"Error getting shop metadata for {domain}: {e}")
            raise

    def get_url_entry(self, domain: str, url: str) -> Optional[URLEntry]:
        """
        Get URL entry for a specific URL.

        Args:
            domain: Shop domain
            url: Full URL

        Returns:
            URLEntry or None if not found
        """
        try:
            response = self.client.get_item(
                TableName=self.table_name,
                Key={"PK": {"S": f"SHOP#{domain}"}, "SK": {"S": f"URL#{url}"}},
            )

            if "Item" not in response:
                logger.debug(f"URL entry not found: {url}")
                return None

            return URLEntry.from_dynamodb_item(response["Item"])

        except ClientError as e:
            logger.error(f"Error getting URL entry for {url}: {e}")
            raise
        except Exception as e:
            logger.error(f"Error getting URL entry for {url}: {e}")
            raise

    def get_product_urls_by_domain(self, domain: str) -> List[str]:
        """
        Get all product URLs for a given domain.

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
                    "KeyConditionExpression": "PK = :pk AND begins_with(SK, :sk_prefix)",
                    "FilterExpression": "is_product = :is_product",
                    "ExpressionAttributeValues": {
                        ":pk": {"S": f"SHOP#{domain}"},
                        ":sk_prefix": {"S": "URL#"},
                        ":is_product": {"BOOL": True},
                    },
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
        last_crawled_date: Optional[str] = None,
        last_scraped_date: Optional[str] = None,
    ) -> dict:
        """
        Update shop metadata with new values.

        Args:
            domain: Shop domain
            last_crawled_date: ISO 8601 timestamp for last crawl
            last_scraped_date: ISO 8601 timestamp for last scrape

        Returns:
            UpdateItem response
        """
        update_expression_parts = []
        expression_attribute_values = {}

        if last_crawled_date:
            update_expression_parts.append("lastCrawledDate = :crawled")
            expression_attribute_values[":crawled"] = {"S": last_crawled_date}

        if last_scraped_date:
            update_expression_parts.append("lastScrapedDate = :scraped")
            expression_attribute_values[":scraped"] = {"S": last_scraped_date}

        if not update_expression_parts:
            logger.warning("No fields to update for shop metadata.")
            return {}

        update_expression = "SET " + ", ".join(update_expression_parts)

        try:
            response = self.client.update_item(
                TableName=self.table_name,
                Key={"PK": {"S": f"SHOP#{domain}"}, "SK": {"S": "META#"}},
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

        Args:
            metadata: ShopMetadata object
        """
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

    def query_all_urls_for_domain(self, domain: str) -> List[URLEntry]:
        """
        Query all URL entries for a domain (excluding META#).

        Args:
            domain: Shop domain

        Returns:
            List of URLEntry objects
        """
        try:
            results = []
            last_evaluated_key = None

            while True:
                query_params = {
                    "TableName": self.table_name,
                    "KeyConditionExpression": "PK = :pk AND begins_with(SK, :sk_prefix)",
                    "ExpressionAttributeValues": {
                        ":pk": {"S": f"SHOP#{domain}"},
                        ":sk_prefix": {"S": "URL#"},
                    },
                }

                if last_evaluated_key:
                    query_params["ExclusiveStartKey"] = last_evaluated_key

                response = self.client.query(**query_params)

                for item in response.get("Items", []):
                    results.append(URLEntry.from_dynamodb_item(item))

                last_evaluated_key = response.get("LastEvaluatedKey")
                if not last_evaluated_key:
                    break

            return results

        except ClientError as e:
            logger.error(f"Error querying URLs for domain {domain}: {e}")
            raise
        except Exception as e:
            logger.error(f"Error querying URLs for domain {domain}: {e}")
            raise


# Global operations instance
db_operations = DynamoDBOperations()
