import pytest
from unittest.mock import Mock, patch
from botocore.exceptions import ClientError

from src.core.aws.database.models import ShopMetadata, URLEntry
from src.core.aws.database.operations import DynamoDBOperations


@pytest.fixture
def mock_boto_client():
    """Fixture for a mocked boto3 DynamoDB client."""
    with patch(
        "src.core.aws.database.operations.get_dynamodb_client"
    ) as mock_get_client:
        mock_client = Mock()
        mock_get_client.return_value = mock_client
        yield mock_client


@pytest.fixture
def db_ops(mock_boto_client):
    """Fixture for DynamoDBOperations instance with a mocked client."""
    ops = DynamoDBOperations()
    ops.client = mock_boto_client
    return ops


class TestGetProductUrls:
    """Tests for get_product_urls_by_domain."""

    def test_get_product_urls_single_page(self, db_ops, mock_boto_client):
        mock_boto_client.query.return_value = {
            "Items": [
                {"url": {"S": "https://a.com/p1"}},
                {"url": {"S": "https://a.com/p2"}},
            ]
        }

        urls = db_ops.get_product_urls_by_domain("a.com")

        assert urls == ["https://a.com/p1", "https://a.com/p2"]
        mock_boto_client.query.assert_called_once()
        called_kwargs = mock_boto_client.query.call_args.kwargs
        assert called_kwargs["TableName"] == db_ops.table_name
        assert called_kwargs["IndexName"] == "IsProductIndex"
        assert (
            called_kwargs["KeyConditionExpression"]
            == "PK = :pk AND is_product = :is_product"
        )
        assert called_kwargs["ExpressionAttributeValues"] == {
            ":pk": {"S": "SHOP#a.com"},
            ":is_product": {"N": "1"},
        }
        assert called_kwargs["ProjectionExpression"] == "#url_attr"
        assert called_kwargs["ExpressionAttributeNames"] == {"#url_attr": "url"}

    def test_get_product_urls_multiple_pages(self, db_ops, mock_boto_client):
        mock_boto_client.query.side_effect = [
            {
                "Items": [{"url": {"S": "https://b.com/p1"}}],
                "LastEvaluatedKey": {"PK": {"S": "x"}},
            },
            {"Items": [{"url": {"S": "https://b.com/p2"}}]},
        ]

        urls = db_ops.get_product_urls_by_domain("b.com")

        assert urls == ["https://b.com/p1", "https://b.com/p2"]
        assert mock_boto_client.query.call_count == 2

    def test_get_product_urls_handles_empty(self, db_ops, mock_boto_client):
        mock_boto_client.query.return_value = {"Items": []}
        urls = db_ops.get_product_urls_by_domain("d.com")
        assert urls == []

    def test_get_product_urls_client_error(self, db_ops, mock_boto_client):
        mock_boto_client.query.side_effect = ClientError(
            {"Error": {"Message": "boom"}}, "Query"
        )
        with pytest.raises(ClientError):
            db_ops.get_product_urls_by_domain("e.com")


class TestBatchWriteOperations:
    """Test BatchWriteItem operations."""

    def test_batch_write_items_empty_list(self, db_ops, mock_boto_client):
        result = db_ops._batch_write_items([], "test items")
        assert result == {"UnprocessedItems": {}}
        mock_boto_client.batch_write_item.assert_not_called()

    def test_batch_write_shop_metadata_success(self, db_ops, mock_boto_client):
        mock_boto_client.batch_write_item.return_value = {"UnprocessedItems": {}}
        metadata1 = ShopMetadata(domain="example1.com", shop_country="US")
        metadata2 = ShopMetadata(domain="example2.com", shop_country="CA")
        metadata_list = [metadata1, metadata2]

        result = db_ops.batch_write_shop_metadata(metadata_list)

        assert result == {"UnprocessedItems": {}}
        mock_boto_client.batch_write_item.assert_called_once()
        # Check that the items are correctly formatted
        call_args = mock_boto_client.batch_write_item.call_args
        sent_items = call_args.kwargs["RequestItems"][db_ops.table_name]
        assert len(sent_items) == 2
        assert sent_items[0]["PutRequest"]["Item"]["domain"]["S"] == "example1.com"

    def test_batch_write_url_entries_success(self, db_ops, mock_boto_client):
        mock_boto_client.batch_write_item.return_value = {"UnprocessedItems": {}}
        entry1 = URLEntry(
            domain="example.com", url="https://example.com/1", is_product=1
        )
        entry2 = URLEntry(
            domain="example.com", url="https://example.com/2", is_product=0
        )
        url_entries = [entry1, entry2]

        result = db_ops.batch_write_url_entries(url_entries)

        assert result == {"UnprocessedItems": {}}
        mock_boto_client.batch_write_item.assert_called_once()
        sent_items = mock_boto_client.batch_write_item.call_args.kwargs["RequestItems"][
            db_ops.table_name
        ]
        assert len(sent_items) == 2
        assert sent_items[0]["PutRequest"]["Item"]["is_product"]["N"] == "1"
        assert sent_items[1]["PutRequest"]["Item"]["is_product"]["N"] == "0"

    def test_batch_write_handles_unprocessed_items(self, db_ops, mock_boto_client):
        table_name = db_ops.table_name
        mock_boto_client.batch_write_item.return_value = {
            "UnprocessedItems": {table_name: [{"PutRequest": {"Item": {}}}]}
        }
        metadata = ShopMetadata(domain="example.com", shop_country="DE")

        result = db_ops.batch_write_shop_metadata([metadata])

        assert "UnprocessedItems" in result
        assert table_name in result["UnprocessedItems"]
        assert len(result["UnprocessedItems"][table_name]) == 1


class TestUpsertOperations:
    """Test upsert (insert/update) operations."""

    @patch(
        "src.core.aws.database.operations.socket.gethostbyname", return_value="1.2.3.4"
    )
    @patch("src.core.aws.database.operations.get_country_code", return_value="US")
    def test_upsert_shop_metadata_resolves_country(
        self, mock_get_country, mock_gethost, db_ops, mock_boto_client
    ):
        """Test that country is resolved if not provided."""
        metadata = ShopMetadata(domain="example.com")
        db_ops.upsert_shop_metadata(metadata)

        mock_gethost.assert_called_once_with("example.com")
        mock_get_country.assert_called_once_with("1.2.3.4")
        # Check that the country was added to the item before putting it
        call_args = mock_boto_client.put_item.call_args
        item = call_args.kwargs["Item"]
        assert item["shop_country"]["S"] == "US"

    def test_upsert_shop_metadata_error(self, db_ops, mock_boto_client):
        mock_boto_client.put_item.side_effect = ClientError(
            {"Error": {"Code": "InternalError", "Message": "Put failed"}}, "PutItem"
        )
        metadata = ShopMetadata(domain="example.com", shop_country="GB")
        with pytest.raises(ClientError):
            db_ops.upsert_shop_metadata(metadata)

    def test_upsert_url_entry_success(self, db_ops, mock_boto_client):
        entry = URLEntry(
            domain="example.com", url="https://example.com/product/123", is_product=1
        )
        db_ops.upsert_url_entry(entry)
        mock_boto_client.put_item.assert_called_once()
        item = mock_boto_client.put_item.call_args.kwargs["Item"]
        assert item["is_product"]["N"] == "1"


class TestUpdateOperations:
    """Test UpdateItem operations."""

    def test_update_shop_metadata_success(self, db_ops, mock_boto_client):
        mock_boto_client.update_item.return_value = {
            "Attributes": {"last_crawled": {"S": "2025-12-14T12:00:00"}}
        }
        domain = "example.com"
        crawled_date = "2025-12-14T12:00:00"

        result = db_ops.update_shop_metadata(domain=domain, last_crawled=crawled_date)

        mock_boto_client.update_item.assert_called_once_with(
            TableName=db_ops.table_name,
            Key={"PK": {"S": f"SHOP#{domain}"}, "SK": {"S": db_ops.METADATA_SK}},
            UpdateExpression="SET last_crawled = :crawled",
            ExpressionAttributeValues={":crawled": {"S": crawled_date}},
            ReturnValues="UPDATED_NEW",
        )
        assert result == {"last_crawled": {"S": "2025-12-14T12:00:00"}}

    def test_update_shop_metadata_client_error(self, db_ops, mock_boto_client):
        error_response = {
            "Error": {
                "Code": "ProvisionedThroughputExceededException",
                "Message": "Rate exceeded",
            }
        }
        mock_boto_client.update_item.side_effect = ClientError(
            error_response, "UpdateItem"
        )
        with pytest.raises(ClientError):
            db_ops.update_shop_metadata(
                domain="example.com", last_scraped="2025-12-14T13:00:00"
            )

    def test_update_shop_metadata_no_updates(self, db_ops, mock_boto_client):
        result = db_ops.update_shop_metadata(domain="example.com")
        mock_boto_client.update_item.assert_not_called()
        assert result == {}

    def test_update_shop_metadata_both_dates(self, db_ops, mock_boto_client):
        mock_boto_client.update_item.return_value = {"Attributes": {}}
        domain = "example.com"
        crawled_date = "2025-01-01T00:00:00"
        scraped_date = "2025-01-02T00:00:00"

        db_ops.update_shop_metadata(
            domain=domain, last_crawled=crawled_date, last_scraped=scraped_date
        )

        mock_boto_client.update_item.assert_called_once_with(
            TableName=db_ops.table_name,
            Key={"PK": {"S": f"SHOP#{domain}"}, "SK": {"S": db_ops.METADATA_SK}},
            UpdateExpression="SET last_crawled = :crawled, last_scraped = :scraped",
            ExpressionAttributeValues={
                ":crawled": {"S": crawled_date},
                ":scraped": {"S": scraped_date},
            },
            ReturnValues="UPDATED_NEW",
        )


class TestQueryByDateGSI:
    """Tests for querying shops by date ranges using GSIs."""

    @pytest.mark.parametrize(
        "method_name, index_name, date_attr",
        [
            (
                "get_shops_by_country_and_crawled_date",
                "CountryLastCrawledIndex",
                "last_crawled",
            ),
            (
                "get_shops_by_country_and_scraped_date",
                "CountryLastScrapedIndex",
                "last_scraped",
            ),
        ],
    )
    def test_query_shops_by_date_success(
        self, db_ops, mock_boto_client, method_name, index_name, date_attr
    ):
        """Test successfully querying shops by crawled or scraped date."""
        # Arrange
        mock_boto_client.query.return_value = {
            "Items": [{"domain": {"S": "example.com"}}]
        }
        country = "DE"
        start_date = "2025-01-01T00:00:00"
        end_date = "2025-01-31T23:59:59"

        # Act
        method_to_call = getattr(db_ops, method_name)
        result = method_to_call(country, start_date, end_date)

        # Assert
        assert result == ["example.com"]
        mock_boto_client.query.assert_called_once_with(
            TableName=db_ops.table_name,
            IndexName=index_name,
            KeyConditionExpression=f"shop_country = :country AND {date_attr} BETWEEN :start AND :end",
            ExpressionAttributeValues={
                ":country": {"S": country},
                ":start": {"S": start_date},
                ":end": {"S": end_date},
            },
            ProjectionExpression="#domain_attr",
            ExpressionAttributeNames={"#domain_attr": "domain"},
        )

    def test_query_shops_by_date_pagination(self, db_ops, mock_boto_client):
        """Test that date-based queries handle pagination."""
        # Arrange
        mock_boto_client.query.side_effect = [
            {
                "Items": [{"domain": {"S": "a.com"}}],
                "LastEvaluatedKey": {"PK": {"S": "x"}},
            },
            {"Items": [{"domain": {"S": "b.com"}}]},
        ]

        # Act
        result = db_ops.get_shops_by_country_and_crawled_date("US", "d1", "d2")

        # Assert
        assert result == ["a.com", "b.com"]
        assert mock_boto_client.query.call_count == 2

    def test_query_shops_by_date_error(self, db_ops, mock_boto_client):
        """Test error handling for date-based queries."""
        # Arrange
        mock_boto_client.query.side_effect = ClientError({}, "Query")

        # Act & Assert
        with pytest.raises(ClientError):
            db_ops.get_shops_by_country_and_scraped_date("FR", "d1", "d2")
