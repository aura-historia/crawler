import pytest
from unittest.mock import Mock, patch
from botocore.exceptions import ClientError

from src.core.aws.database.models import ShopMetadata, URLEntry
from src.core.aws.database.operations import DynamoDBOperations, db_operations


class TestGetOperations:
    """Test GetItem operations."""

    @patch("src.core.aws.database.operations.get_dynamodb_client")
    def test_get_shop_metadata_success(self, mock_get_client):
        """Test successfully getting shop metadata."""
        # Arrange
        mock_client = Mock()
        mock_get_client.return_value = mock_client

        mock_client.get_item.return_value = {
            "Item": {
                "PK": {"S": "SHOP#example.com"},
                "SK": {"S": "META#"},
                "domain": {"S": "example.com"},
                "standards_used": {"L": [{"S": "json-ld"}]},
                "country": {"S": "US"},
            }
        }

        ops = DynamoDBOperations()

        # Act
        result = ops.get_shop_metadata("example.com")

        # Assert
        assert result is not None
        assert result.domain == "example.com"
        assert result.standards_used == ["json-ld"]
        assert result.country == "US"
        mock_client.get_item.assert_called_once()

    @patch("src.core.aws.database.operations.get_dynamodb_client")
    def test_get_shop_metadata_not_found(self, mock_get_client):
        """Test getting shop metadata when it doesn't exist."""
        # Arrange
        mock_client = Mock()
        mock_get_client.return_value = mock_client
        mock_client.get_item.return_value = {}  # No 'Item' key

        ops = DynamoDBOperations()

        # Act
        result = ops.get_shop_metadata("nonexistent.com")

        # Assert
        assert result is None

    @patch("src.core.aws.database.operations.get_dynamodb_client")
    def test_get_shop_metadata_error(self, mock_get_client):
        """Test error handling when getting shop metadata."""
        # Arrange
        mock_client = Mock()
        mock_get_client.return_value = mock_client
        mock_client.get_item.side_effect = ClientError(
            {"Error": {"Code": "InternalError", "Message": "Database error"}}, "GetItem"
        )

        ops = DynamoDBOperations()

        # Act & Assert
        with pytest.raises(ClientError):
            ops.get_shop_metadata("example.com")

    @patch("src.core.aws.database.operations.get_dynamodb_client")
    def test_get_url_entry_success(self, mock_get_client):
        """Test successfully getting URL entry."""
        # Arrange
        mock_client = Mock()
        mock_get_client.return_value = mock_client

        test_url = "https://example.com/product/123"
        mock_client.get_item.return_value = {
            "Item": {
                "PK": {"S": "SHOP#example.com"},
                "SK": {"S": f"URL#{test_url}"},
                "url": {"S": test_url},
                "standards_used": {"L": []},
                "is_product": {"BOOL": True},
            }
        }

        ops = DynamoDBOperations()

        # Act
        result = ops.get_url_entry("example.com", test_url)

        # Assert
        assert result is not None
        assert result.url == test_url
        assert result.is_product is True

    @patch("src.core.aws.database.operations.get_dynamodb_client")
    def test_get_url_entry_not_found(self, mock_get_client):
        """Test getting URL entry when it doesn't exist."""
        # Arrange
        mock_client = Mock()
        mock_get_client.return_value = mock_client
        mock_client.get_item.return_value = {}

        ops = DynamoDBOperations()

        # Act
        result = ops.get_url_entry("example.com", "https://example.com/missing")

        # Assert
        assert result is None


class TestBatchWriteOperations:
    """Test BatchWriteItem operations."""

    @patch("src.core.aws.database.operations.get_dynamodb_client")
    def test_batch_write_items_empty_list(self, mock_get_client):
        """Test batch write with empty list."""
        # Arrange
        mock_client = Mock()
        mock_get_client.return_value = mock_client
        ops = DynamoDBOperations()

        # Act
        result = ops._batch_write_items([], "test items")

        # Assert
        assert result == {"UnprocessedItems": {}}
        mock_client.batch_write_item.assert_not_called()

    @patch("src.core.aws.database.operations.get_dynamodb_client")
    def test_batch_write_shop_metadata_success(self, mock_get_client):
        """Test successfully batch writing shop metadata."""
        # Arrange
        mock_client = Mock()
        mock_get_client.return_value = mock_client
        mock_client.batch_write_item.return_value = {"UnprocessedItems": {}}

        ops = DynamoDBOperations()

        metadata1 = ShopMetadata(
            domain="example1.com", standards_used=["json-ld"], country="US"
        )
        metadata2 = ShopMetadata(
            domain="example2.com", standards_used=["microdata"], country="CA"
        )
        metadata_list = [metadata1, metadata2]

        # Act
        result = ops.batch_write_shop_metadata(metadata_list)

        # Assert
        assert result == {"UnprocessedItems": {}}
        mock_client.batch_write_item.assert_called_once()

    @patch("src.core.aws.database.operations.get_dynamodb_client")
    def test_batch_write_shop_metadata_empty_list(self, mock_get_client):
        """Test batch write shop metadata with empty list."""
        # Arrange
        mock_client = Mock()
        mock_get_client.return_value = mock_client
        ops = DynamoDBOperations()

        # Act
        result = ops.batch_write_shop_metadata([])

        # Assert
        assert result == {"UnprocessedItems": {}}

    @patch("src.core.aws.database.operations.get_dynamodb_client")
    def test_batch_write_url_entries_success(self, mock_get_client):
        """Test successfully batch writing URL entries."""
        # Arrange
        mock_client = Mock()
        mock_get_client.return_value = mock_client
        mock_client.batch_write_item.return_value = {"UnprocessedItems": {}}

        ops = DynamoDBOperations()

        entry1 = URLEntry(domain="example.com", url="https://example.com/1")
        entry2 = URLEntry(domain="example.com", url="https://example.com/2")
        entry3 = URLEntry(domain="example.com", url="https://example.com/3")
        url_entries = [entry1, entry2, entry3]

        # Act
        result = ops.batch_write_url_entries(url_entries)

        # Assert
        assert result == {"UnprocessedItems": {}}
        mock_client.batch_write_item.assert_called_once()

    @patch("src.core.aws.database.operations.get_dynamodb_client")
    @patch("src.core.aws.database.operations.os.getenv")
    def test_batch_write_handles_unprocessed_items(self, mock_getenv, mock_get_client):
        """Test handling of unprocessed items."""
        # Arrange
        table_name = "test-table"
        mock_getenv.return_value = table_name

        mock_client = Mock()
        mock_get_client.return_value = mock_client

        # Simulate some unprocessed items
        mock_client.batch_write_item.return_value = {
            "UnprocessedItems": {table_name: [{"PutRequest": {"Item": {}}}]}
        }

        ops = DynamoDBOperations()
        metadata = ShopMetadata(domain="example.com", country="DE")

        # Act
        result = ops.batch_write_shop_metadata([metadata])

        # Assert
        assert "UnprocessedItems" in result
        assert table_name in result["UnprocessedItems"]
        assert len(result["UnprocessedItems"][table_name]) == 1


class TestUpsertOperations:
    """Test upsert (insert/update) operations."""

    @patch("src.core.aws.database.operations.get_dynamodb_client")
    def test_upsert_shop_metadata_success(self, mock_get_client):
        """Test successfully upserting shop metadata."""
        # Arrange
        mock_client = Mock()
        mock_get_client.return_value = mock_client
        mock_client.put_item.return_value = {}

        ops = DynamoDBOperations()
        metadata = ShopMetadata(
            domain="example.com", standards_used=["json-ld"], country="FR"
        )

        # Act
        ops.upsert_shop_metadata(metadata)

        # Assert
        mock_client.put_item.assert_called_once()

    @patch("src.core.aws.database.operations.get_dynamodb_client")
    def test_upsert_shop_metadata_error(self, mock_get_client):
        """Test error handling when upserting shop metadata."""
        # Arrange
        mock_client = Mock()
        mock_get_client.return_value = mock_client
        mock_client.put_item.side_effect = ClientError(
            {"Error": {"Code": "InternalError", "Message": "Put failed"}}, "PutItem"
        )

        ops = DynamoDBOperations()
        metadata = ShopMetadata(domain="example.com", country="GB")

        # Act & Assert
        with pytest.raises(ClientError):
            ops.upsert_shop_metadata(metadata)

    @patch("src.core.aws.database.operations.get_dynamodb_client")
    def test_upsert_url_entry_success(self, mock_get_client):
        """Test successfully upserting URL entry."""
        # Arrange
        mock_client = Mock()
        mock_get_client.return_value = mock_client
        mock_client.put_item.return_value = {}

        ops = DynamoDBOperations()
        entry = URLEntry(domain="example.com", url="https://example.com/product/123")

        # Act
        ops.upsert_url_entry(entry)

        # Assert
        mock_client.put_item.assert_called_once()


class TestQueryOperations:
    """Test query operations."""

    @patch("src.core.aws.database.operations.get_dynamodb_client")
    def test_query_all_urls_for_domain_success(self, mock_get_client):
        """Test successfully querying all URLs for a domain."""
        # Arrange
        mock_client = Mock()
        mock_get_client.return_value = mock_client

        mock_client.query.return_value = {
            "Items": [
                {
                    "PK": {"S": "SHOP#example.com"},
                    "SK": {"S": "URL#https://example.com/page1"},
                    "url": {"S": "https://example.com/page1"},
                    "standards_used": {"L": []},
                    "is_product": {"BOOL": False},
                },
                {
                    "PK": {"S": "SHOP#example.com"},
                    "SK": {"S": "URL#https://example.com/page2"},
                    "url": {"S": "https://example.com/page2"},
                    "standards_used": {"L": []},
                    "is_product": {"BOOL": False},
                },
            ]
        }

        ops = DynamoDBOperations()

        # Act
        result = ops.query_all_urls_for_domain("example.com")

        # Assert
        assert len(result) == 2
        assert result[0].url == "https://example.com/page1"
        assert result[1].url == "https://example.com/page2"

    @patch("src.core.aws.database.operations.get_dynamodb_client")
    def test_query_all_urls_for_domain_empty(self, mock_get_client):
        """Test querying URLs when domain has no URLs."""
        # Arrange
        mock_client = Mock()
        mock_get_client.return_value = mock_client
        mock_client.query.return_value = {"Items": []}

        ops = DynamoDBOperations()

        # Act
        result = ops.query_all_urls_for_domain("empty.com")

        # Assert
        assert result == []

    @patch("src.core.aws.database.operations.get_dynamodb_client")
    def test_query_handles_pagination(self, mock_get_client):
        """Test query handles pagination correctly."""
        # Arrange
        mock_client = Mock()
        mock_get_client.return_value = mock_client

        # First call returns data with LastEvaluatedKey
        mock_client.query.side_effect = [
            {
                "Items": [
                    {
                        "PK": {"S": "SHOP#example.com"},
                        "SK": {"S": "URL#https://example.com/page1"},
                        "url": {"S": "https://example.com/page1"},
                        "standards_used": {"L": []},
                        "is_product": {"BOOL": False},
                    }
                ],
                "LastEvaluatedKey": {
                    "PK": {"S": "SHOP#example.com"},
                    "SK": {"S": "URL#https://example.com/page1"},
                },
            },
            # Second call returns remaining data without LastEvaluatedKey
            {
                "Items": [
                    {
                        "PK": {"S": "SHOP#example.com"},
                        "SK": {"S": "URL#https://example.com/page2"},
                        "url": {"S": "https://example.com/page2"},
                        "standards_used": {"L": []},
                        "is_product": {"BOOL": False},
                    }
                ]
            },
        ]

        ops = DynamoDBOperations()

        # Act
        result = ops.query_all_urls_for_domain("example.com")

        # Assert
        assert len(result) == 2
        assert mock_client.query.call_count == 2


class TestGlobalInstance:
    """Test global db_operations instance."""

    def test_db_operations_instance_exists(self):
        """Test that global db_operations instance exists."""
        assert db_operations is not None
        assert isinstance(db_operations, DynamoDBOperations)

    def test_db_operations_has_client(self):
        """Test that instance has client configured."""
        assert hasattr(db_operations, "client")
        assert hasattr(db_operations, "table_name")


class TestGetProductUrls:
    """Tests for get_product_urls_by_domain (refactored to match project test style)."""

    @patch("src.core.aws.database.operations.get_dynamodb_client")
    @patch("src.core.aws.database.operations.os.getenv")
    def test_get_product_urls_single_page(self, mock_getenv, mock_get_client):
        mock_getenv.return_value = "test-table"
        mock_client = Mock()
        mock_get_client.return_value = mock_client

        mock_client.query.return_value = {
            "Items": [
                {"url": {"S": "https://a.com/p1"}, "is_product": {"BOOL": True}},
                {"url": {"S": "https://a.com/p2"}, "is_product": {"BOOL": True}},
            ]
        }

        ops = DynamoDBOperations()

        urls = ops.get_product_urls_by_domain("a.com")

        assert urls == ["https://a.com/p1", "https://a.com/p2"]
        mock_client.query.assert_called_once()
        called_kwargs = mock_client.query.call_args.kwargs
        assert called_kwargs["TableName"] == "test-table"
        assert ":pk" in called_kwargs.get("ExpressionAttributeValues", {})
        assert "FilterExpression" in called_kwargs

    @patch("src.core.aws.database.operations.get_dynamodb_client")
    @patch("src.core.aws.database.operations.os.getenv")
    def test_get_product_urls_multiple_pages(self, mock_getenv, mock_get_client):
        mock_getenv.return_value = "test-table"
        mock_client = Mock()
        mock_get_client.return_value = mock_client

        mock_client.query.side_effect = [
            {
                "Items": [{"url": {"S": "https://b.com/p1"}}],
                "LastEvaluatedKey": {"PK": {"S": "x"}},
            },
            {"Items": [{"url": {"S": "https://b.com/p2"}}]},
        ]

        ops = DynamoDBOperations()
        urls = ops.get_product_urls_by_domain("b.com")

        assert urls == ["https://b.com/p1", "https://b.com/p2"]
        assert mock_client.query.call_count == 2

    @patch("src.core.aws.database.operations.get_dynamodb_client")
    @patch("src.core.aws.database.operations.os.getenv")
    def test_get_product_urls_filters_only_products(self, mock_getenv, mock_get_client):
        mock_getenv.return_value = "test-table"
        mock_client = Mock()
        mock_get_client.return_value = mock_client

        # Simulate DynamoDB already applying the filter; we still assert the FilterExpression was provided.
        mock_client.query.return_value = {
            "Items": [
                {"url": {"S": "https://c.com/p1"}, "is_product": {"BOOL": True}},
            ]
        }

        ops = DynamoDBOperations()
        urls = ops.get_product_urls_by_domain("c.com")

        assert urls == ["https://c.com/p1"]
        called_kwargs = mock_client.query.call_args.kwargs
        assert "FilterExpression" in called_kwargs
        assert ":is_product" in called_kwargs.get("ExpressionAttributeValues", {})

    @patch("src.core.aws.database.operations.get_dynamodb_client")
    @patch("src.core.aws.database.operations.os.getenv")
    def test_get_product_urls_handles_empty(self, mock_getenv, mock_get_client):
        mock_getenv.return_value = "test-table"
        mock_client = Mock()
        mock_get_client.return_value = mock_client

        mock_client.query.return_value = {"Items": []}

        ops = DynamoDBOperations()
        urls = ops.get_product_urls_by_domain("d.com")
        assert urls == []

    @patch("src.core.aws.database.operations.get_dynamodb_client")
    @patch("src.core.aws.database.operations.os.getenv")
    def test_get_product_urls_client_error(self, mock_getenv, mock_get_client):
        mock_getenv.return_value = "test-table"
        mock_client = Mock()
        mock_get_client.return_value = mock_client

        mock_client.query.side_effect = ClientError(
            {"Error": {"Message": "boom"}}, "Query"
        )

        ops = DynamoDBOperations()
        with pytest.raises(ClientError):
            ops.get_product_urls_by_domain("e.com")


class TestUpdateOperations:
    """Test UpdateItem operations."""

    @patch("src.core.aws.database.operations.get_dynamodb_client")
    def test_update_shop_metadata_success(self, mock_get_client):
        """Test successfully updating shop metadata."""
        # Arrange
        mock_client = Mock()
        mock_get_client.return_value = mock_client
        mock_client.update_item.return_value = {
            "Attributes": {"lastCrawledDate": {"S": "2025-12-14T12:00:00"}}
        }

        ops = DynamoDBOperations()
        domain = "example.com"
        crawled_date = "2025-12-14T12:00:00"

        # Act
        result = ops.update_shop_metadata(domain=domain, last_crawled_date=crawled_date)

        # Assert
        mock_client.update_item.assert_called_once_with(
            TableName=ops.table_name,
            Key={"PK": {"S": f"SHOP#{domain}"}, "SK": {"S": "META#"}},
            UpdateExpression="SET lastCrawledDate = :crawled",
            ExpressionAttributeValues={":crawled": {"S": crawled_date}},
            ReturnValues="UPDATED_NEW",
        )
        assert result == {"lastCrawledDate": {"S": "2025-12-14T12:00:00"}}

    @patch("src.core.aws.database.operations.get_dynamodb_client")
    def test_update_shop_metadata_client_error(self, mock_get_client):
        """Test ClientError handling when updating shop metadata."""
        # Arrange
        mock_client = Mock()
        mock_get_client.return_value = mock_client
        error_response = {
            "Error": {
                "Code": "ProvisionedThroughputExceededException",
                "Message": "Rate exceeded",
            }
        }
        mock_client.update_item.side_effect = ClientError(error_response, "UpdateItem")

        ops = DynamoDBOperations()
        domain = "example.com"

        # Act & Assert
        with pytest.raises(ClientError) as excinfo:
            ops.update_shop_metadata(
                domain=domain, last_scraped_date="2025-12-14T13:00:00"
            )

        assert (
            excinfo.value.response["Error"]["Code"]
            == "ProvisionedThroughputExceededException"
        )

    @patch("src.core.aws.database.operations.get_dynamodb_client")
    def test_update_shop_metadata_no_updates(self, mock_get_client):
        """Test that no update is performed if no fields are provided."""
        # Arrange
        mock_client = Mock()
        mock_get_client.return_value = mock_client
        ops = DynamoDBOperations()

        # Act
        result = ops.update_shop_metadata(domain="example.com")

        # Assert
        mock_client.update_item.assert_not_called()
        assert result == {}

    @patch("src.core.aws.database.operations.get_dynamodb_client")
    def test_update_shop_metadata_both_dates(self, mock_get_client):
        """Test that update expression is correct with both date fields."""
        # Arrange
        mock_client = Mock()
        mock_get_client.return_value = mock_client
        mock_client.update_item.return_value = {"Attributes": {}}

        ops = DynamoDBOperations()
        domain = "example.com"
        crawled_date = "2025-01-01T00:00:00"
        scraped_date = "2025-01-02T00:00:00"

        # Act
        ops.update_shop_metadata(
            domain=domain,
            last_crawled_date=crawled_date,
            last_scraped_date=scraped_date,
        )

        # Assert
        mock_client.update_item.assert_called_once_with(
            TableName=ops.table_name,
            Key={"PK": {"S": f"SHOP#{domain}"}, "SK": {"S": "META#"}},
            UpdateExpression="SET lastCrawledDate = :crawled, lastScrapedDate = :scraped",
            ExpressionAttributeValues={
                ":crawled": {"S": crawled_date},
                ":scraped": {"S": scraped_date},
            },
            ReturnValues="UPDATED_NEW",
        )
