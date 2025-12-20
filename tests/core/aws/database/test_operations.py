import pytest
import socket
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
    """Tests for get_product_urls_by_domain method."""

    def test_returns_urls_from_single_page(self, db_ops, mock_boto_client):
        """Test retrieving product URLs when results fit in a single page."""
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
        assert called_kwargs["IndexName"] == "GSI1"
        assert (
            called_kwargs["KeyConditionExpression"]
            == "gsi1_pk = :pk AND gsi1_sk = :type"
        )
        assert called_kwargs["ExpressionAttributeValues"] == {
            ":pk": {"S": "SHOP#a.com"},
            ":type": {"S": "product"},
        }
        assert called_kwargs["ProjectionExpression"] == "#url_attr"
        assert called_kwargs["ExpressionAttributeNames"] == {"#url_attr": "url"}

    def test_handles_pagination(self, db_ops, mock_boto_client):
        """Test retrieving product URLs with pagination across multiple pages."""
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

    def test_handles_empty_results(self, db_ops, mock_boto_client):
        """Test handling when no product URLs are found for a domain."""
        mock_boto_client.query.return_value = {"Items": []}

        urls = db_ops.get_product_urls_by_domain("d.com")

        assert urls == []
        mock_boto_client.query.assert_called_once()

    def test_propagates_client_error(self, db_ops, mock_boto_client):
        """Test that ClientError from DynamoDB is propagated."""
        mock_boto_client.query.side_effect = ClientError(
            {"Error": {"Message": "Query failed"}}, "Query"
        )

        with pytest.raises(ClientError):
            db_ops.get_product_urls_by_domain("e.com")


class TestBatchWriteOperations:
    """Tests for BatchWriteItem operations."""

    def test_empty_list_returns_without_calling_api(self, db_ops, mock_boto_client):
        """Test that batch_write with empty list doesn't call DynamoDB."""
        result = db_ops._batch_write_items([], "test items")

        assert result == {"UnprocessedItems": {}}
        mock_boto_client.batch_write_item.assert_not_called()

    def test_batch_write_shop_metadata_success(self, db_ops, mock_boto_client):
        """Test successfully batch writing ShopMetadata items."""
        mock_boto_client.batch_write_item.return_value = {"UnprocessedItems": {}}
        metadata1 = ShopMetadata(domain="example1.com", shop_country="US")
        metadata2 = ShopMetadata(domain="example2.com", shop_country="CA")
        metadata_list = [metadata1, metadata2]

        result = db_ops.batch_write_shop_metadata(metadata_list)

        assert result == {"UnprocessedItems": {}}
        mock_boto_client.batch_write_item.assert_called_once()
        call_args = mock_boto_client.batch_write_item.call_args
        sent_items = call_args.kwargs["RequestItems"][db_ops.table_name]
        assert len(sent_items) == 2
        assert sent_items[0]["PutRequest"]["Item"]["domain"]["S"] == "example1.com"
        assert sent_items[1]["PutRequest"]["Item"]["domain"]["S"] == "example2.com"

    def test_batch_write_url_entries_success(self, db_ops, mock_boto_client):
        """Test successfully batch writing URLEntry items."""
        mock_boto_client.batch_write_item.return_value = {"UnprocessedItems": {}}
        entry1 = URLEntry(
            domain="example.com", url="https://example.com/1", type="product"
        )
        entry2 = URLEntry(
            domain="example.com", url="https://example.com/2", type="category"
        )
        url_entries = [entry1, entry2]

        result = db_ops.batch_write_url_entries(url_entries)

        assert result == {"UnprocessedItems": {}}
        mock_boto_client.batch_write_item.assert_called_once()
        sent_items = mock_boto_client.batch_write_item.call_args.kwargs["RequestItems"][
            db_ops.table_name
        ]
        assert len(sent_items) == 2
        assert sent_items[0]["PutRequest"]["Item"]["type"]["S"] == "product"
        assert sent_items[1]["PutRequest"]["Item"]["type"]["S"] == "category"

    def test_batch_write_returns_unprocessed_items(self, db_ops, mock_boto_client):
        """Test that unprocessed items are returned when batch write is throttled."""
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
    """Tests for upsert (insert/update) operations."""

    @patch(
        "src.core.aws.database.operations.socket.gethostbyname", return_value="1.2.3.4"
    )
    @patch("src.core.aws.database.operations.get_country_code", return_value="US")
    def test_upsert_shop_metadata_resolves_country(
        self, mock_get_country, mock_gethost, db_ops, mock_boto_client
    ):
        """Test that country is automatically resolved from domain if not provided."""
        metadata = ShopMetadata(domain="example.com")

        db_ops.upsert_shop_metadata(metadata)

        mock_gethost.assert_called_once_with("example.com")
        mock_get_country.assert_called_once_with("1.2.3.4")
        call_args = mock_boto_client.put_item.call_args
        item = call_args.kwargs["Item"]
        assert item["shop_country"]["S"] == "US"

    def test_upsert_shop_metadata_error(self, db_ops, mock_boto_client):
        """Test that ClientError is propagated on upsert failure."""
        mock_boto_client.put_item.side_effect = ClientError(
            {"Error": {"Code": "InternalError", "Message": "Put failed"}}, "PutItem"
        )
        metadata = ShopMetadata(domain="example.com", shop_country="GB")

        with pytest.raises(ClientError):
            db_ops.upsert_shop_metadata(metadata)

    def test_upsert_url_entry_success(self, db_ops, mock_boto_client):
        """Test successfully upserting a URLEntry."""
        entry = URLEntry(
            domain="example.com",
            url="https://example.com/product/123",
            type="product",
        )

        db_ops.upsert_url_entry(entry)

        mock_boto_client.put_item.assert_called_once()
        item = mock_boto_client.put_item.call_args.kwargs["Item"]
        assert item["type"]["S"] == "product"
        assert item["url"]["S"] == "https://example.com/product/123"


class TestUpdateOperations:
    """Tests for UpdateItem operations."""

    def test_update_shop_metadata_success(self, db_ops, mock_boto_client):
        """Test successfully updating shop metadata with crawled date."""
        mock_boto_client.update_item.return_value = {
            "Attributes": {"last_crawled_start": {"S": "2025-12-14T12:00:00"}}
        }
        domain = "example.com"
        crawled_date = "2025-12-14T12:00:00"

        result = db_ops.update_shop_metadata(
            domain=domain, last_crawled_start=crawled_date
        )

        mock_boto_client.update_item.assert_called_once_with(
            TableName=db_ops.table_name,
            Key={"pk": {"S": f"SHOP#{domain}"}, "sk": {"S": db_ops.METADATA_SK}},
            UpdateExpression="SET last_crawled_start = :crawled_start, gsi2_sk = :crawled_start",
            ExpressionAttributeValues={":crawled_start": {"S": crawled_date}},
            ReturnValues="UPDATED_NEW",
        )
        assert result == {"last_crawled_start": {"S": "2025-12-14T12:00:00"}}

    def test_update_shop_metadata_with_both_dates(self, db_ops, mock_boto_client):
        """Test updating shop metadata with both crawled and scraped dates."""
        mock_boto_client.update_item.return_value = {"Attributes": {}}
        domain = "example.com"
        crawled_date = "2025-01-01T00:00:00"
        scraped_date = "2025-01-02T00:00:00"

        db_ops.update_shop_metadata(
            domain=domain,
            last_crawled_start=crawled_date,
            last_scraped_start=scraped_date,
        )

        mock_boto_client.update_item.assert_called_once()
        call_kwargs = mock_boto_client.update_item.call_args.kwargs
        assert "last_crawled_start = :crawled_start" in call_kwargs["UpdateExpression"]
        assert "last_scraped_start = :scraped_start" in call_kwargs["UpdateExpression"]
        assert "gsi2_sk = :crawled_start" in call_kwargs["UpdateExpression"]
        assert "gsi3_sk = :scraped_start" in call_kwargs["UpdateExpression"]

    def test_update_shop_metadata_no_updates(self, db_ops, mock_boto_client):
        """Test that update is skipped when no update fields are provided."""
        result = db_ops.update_shop_metadata(domain="example.com")

        mock_boto_client.update_item.assert_not_called()
        assert result == {}

    def test_update_shop_metadata_handles_client_error(self, db_ops, mock_boto_client):
        """Test error handling when update operation fails."""
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
                domain="example.com", last_scraped_start="2025-12-14T13:00:00"
            )


class TestQueryByDateGSI:
    """Tests for querying shops by date ranges using Global Secondary Indexes."""

    @pytest.mark.parametrize(
        "method_name,index_name,pk_attr,sk_attr",
        [
            (
                "get_shops_by_country_and_crawled_date",
                "GSI2",
                "gsi2_pk",
                "gsi2_sk",
            ),
            (
                "get_shops_by_country_and_scraped_date",
                "GSI3",
                "gsi3_pk",
                "gsi3_sk",
            ),
        ],
    )
    def test_query_shops_by_date_success(
        self, db_ops, mock_boto_client, method_name, index_name, pk_attr, sk_attr
    ):
        """Test successfully querying shops by country and date range."""
        mock_boto_client.query.return_value = {
            "Items": [{"domain": {"S": "example.com"}}]
        }
        country = "DE"
        start_date = "2025-01-01T00:00:00"
        end_date = "2025-01-31T23:59:59"

        method_to_call = getattr(db_ops, method_name)
        result = method_to_call(country, start_date, end_date)

        assert result == ["example.com"]
        mock_boto_client.query.assert_called_once_with(
            TableName=db_ops.table_name,
            IndexName=index_name,
            KeyConditionExpression=f"{pk_attr} = :country AND {sk_attr} BETWEEN :start AND :end",
            ExpressionAttributeValues={
                ":country": {"S": f"COUNTRY#{country}"},
                ":start": {"S": start_date},
                ":end": {"S": end_date},
            },
            ProjectionExpression="#domain_attr",
            ExpressionAttributeNames={"#domain_attr": "domain"},
        )

    def test_query_shops_handles_pagination(self, db_ops, mock_boto_client):
        """Test that date-based queries handle pagination correctly."""
        mock_boto_client.query.side_effect = [
            {
                "Items": [{"domain": {"S": "a.com"}}],
                "LastEvaluatedKey": {"PK": {"S": "x"}},
            },
            {"Items": [{"domain": {"S": "b.com"}}]},
        ]

        result = db_ops.get_shops_by_country_and_crawled_date("US", "d1", "d2")

        assert result == ["a.com", "b.com"]
        assert mock_boto_client.query.call_count == 2

    def test_query_shops_propagates_error(self, db_ops, mock_boto_client):
        """Test that ClientError from date-based queries is propagated."""
        mock_boto_client.query.side_effect = ClientError(
            {"Error": {"Message": "Query failed"}}, "Query"
        )

        with pytest.raises(ClientError):
            db_ops.get_shops_by_country_and_scraped_date("FR", "d1", "d2")


class TestFindShopsByCoreName:
    """Tests for find_all_domains_by_core_domain_name (GSI4)."""

    def test_find_all_domains_with_pagination_and_exclusion(
        self, db_ops, mock_boto_client
    ):
        """Test GSI4 query with pagination and the domain_to_exclude filter."""
        mock_boto_client.query.side_effect = [
            {
                "Items": [
                    {
                        "pk": {"S": "SHOP#a.com"},
                        "sk": {"S": "META#"},
                        "domain": {"S": "a.com"},
                    }
                ],
                "LastEvaluatedKey": {"pk": {"S": "key1"}},
            },
            {
                "Items": [
                    {
                        "pk": {"S": "SHOP#b.com"},
                        "sk": {"S": "META#"},
                        "domain": {"S": "b.com"},
                    }
                ]
            },
        ]

        with patch(
            "src.core.aws.database.models.ShopMetadata.from_dynamodb_item"
        ) as mock_from:
            # Mocking model conversion to avoid internal model dependency logic
            mock_from.side_effect = [Mock(domain="a.com"), Mock(domain="b.com")]

            result = db_ops.find_all_domains_by_core_domain_name(
                "example", domain_to_exclude="exclude.com"
            )

        assert len(result) == 2
        assert mock_boto_client.query.call_count == 2

        # Verify FilterExpression was added
        called_args = mock_boto_client.query.call_args.kwargs
        assert called_args["FilterExpression"] == "#d <> :domain_to_exclude"
        assert called_args["ExpressionAttributeValues"][":domain_to_exclude"] == {
            "S": "exclude.com"
        }

    def test_find_all_domains_handles_missing_gsi4_warning(
        self, db_ops, mock_boto_client
    ):
        """Test the specific ClientError branch for a missing GSI4 index."""
        error_response = {
            "Error": {"Message": "The table does not have the specified index: GSI4"}
        }
        mock_boto_client.query.side_effect = ClientError(error_response, "Query")

        result = db_ops.find_all_domains_by_core_domain_name("example")

        assert result == []  # Should catch error and return empty list

    def test_find_all_domains_unexpected_error(self, db_ops, mock_boto_client):
        """Test generic Exception catch-all in find_all_domains."""
        mock_boto_client.query.side_effect = Exception("Unknown error")

        result = db_ops.find_all_domains_by_core_domain_name("example")
        assert result == []


class TestInternalHandlersAndEdgeCases:
    """Tests for generic error handlers and remaining logic branches."""

    def test_get_product_urls_unexpected_exception(self, db_ops, mock_boto_client):
        """Test the generic Exception catch-all in get_product_urls_by_domain."""
        mock_boto_client.query.side_effect = RuntimeError("Panic")

        with pytest.raises(RuntimeError):
            db_ops.get_product_urls_by_domain("a.com")

    def test_batch_write_items_unexpected_exception(self, db_ops, mock_boto_client):
        """Test the generic Exception catch-all in _batch_write_items."""
        mock_boto_client.batch_write_item.side_effect = Exception("Batch crash")

        with pytest.raises(Exception, match="Batch crash"):
            db_ops._batch_write_items([{"pk": {"S": "1"}}], "type")

    @patch("src.core.aws.database.operations.socket.gethostbyname")
    def test_upsert_shop_metadata_socket_gaierror(
        self, mock_gethost, db_ops, mock_boto_client
    ):
        """Test handling of socket.error (DNS resolution failure)."""
        mock_gethost.side_effect = socket.gaierror()
        metadata = ShopMetadata(domain="nonexistent.local")

        db_ops.upsert_shop_metadata(metadata)

        # Should finish execution and call put_item even if DNS fails
        mock_boto_client.put_item.assert_called_once()
        assert metadata.shop_country is None

    @patch(
        "src.core.aws.database.operations.socket.gethostbyname", return_value="1.1.1.1"
    )
    @patch("src.core.aws.database.operations.get_country_code")
    def test_upsert_shop_metadata_generic_lookup_error(
        self, mock_country, mock_gethost, db_ops, mock_boto_client
    ):
        """Test handling of generic exception during country lookup."""
        mock_country.side_effect = Exception("IP lookup service down")
        metadata = ShopMetadata(domain="example.com")

        db_ops.upsert_shop_metadata(metadata)

        # Should log error but proceed to upsert item
        mock_boto_client.put_item.assert_called_once()

    def test_update_shop_metadata_all_possible_timestamps(
        self, db_ops, mock_boto_client
    ):
        """Test update logic covering every possible timestamp branch."""
        mock_boto_client.update_item.return_value = {"Attributes": {}}

        db_ops.update_shop_metadata(
            domain="a.com",
            last_crawled_start="T1",
            last_crawled_end="T2",
            last_scraped_start="T3",
            last_scraped_end="T4",
        )

        expr = mock_boto_client.update_item.call_args.kwargs["UpdateExpression"]
        assert "last_crawled_start =" in expr
        assert "last_crawled_end =" in expr
        assert "last_scraped_start =" in expr
        assert "last_scraped_end =" in expr
        assert "gsi2_sk =" in expr
        assert "gsi3_sk =" in expr

    def test_upsert_item_generic_exception(self, db_ops, mock_boto_client):
        """Test _upsert_item's generic exception handler."""
        mock_boto_client.put_item.side_effect = ValueError("Format error")

        with pytest.raises(ValueError):
            db_ops._upsert_item({}, "context")
