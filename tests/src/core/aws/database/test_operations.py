import socket
from typing import Any, Dict, cast
import pytest

from unittest.mock import Mock, patch
from botocore.exceptions import ClientError

from src.core.aws.database.operations import DynamoDBOperations, parse_gsi_sk
from src.core.aws.database.constants import STATE_DONE, STATE_NEVER, STATE_PROGRESS
from src.core.aws.database.models import ShopMetadata


def _client_error_response(message: str) -> Dict[str, Any]:
    return {
        "Error": {"Code": "ValidationException", "Message": message},
        "ResponseMetadata": {
            "RequestId": "1",
            "HTTPStatusCode": 400,
            "HTTPHeaders": {},
            "RetryAttempts": 0,
            "HostId": "host",
        },
    }


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

        urls, next_token = db_ops.get_product_urls_by_domain("a.com")

        assert urls == ["https://a.com/p1", "https://a.com/p2"]
        assert next_token is None
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
        """Test retrieving product URLs with pagination token returned."""
        mock_boto_client.query.return_value = {
            "Items": [{"url": {"S": "https://b.com/p1"}}],
            "LastEvaluatedKey": {"pk": {"S": "SHOP#b.com"}, "sk": {"S": "URL#p1"}},
        }

        urls, next_token = db_ops.get_product_urls_by_domain("b.com")

        assert urls == ["https://b.com/p1"]
        assert next_token == {"pk": {"S": "SHOP#b.com"}, "sk": {"S": "URL#p1"}}
        assert mock_boto_client.query.call_count == 1

    def test_handles_empty_results(self, db_ops, mock_boto_client):
        """Test handling when no product URLs are found for a domain."""
        mock_boto_client.query.return_value = {"Items": []}

        urls, next_token = db_ops.get_product_urls_by_domain("d.com")

        assert urls == []
        assert next_token is None
        mock_boto_client.query.assert_called_once()

    def test_propagates_client_error(self, db_ops, mock_boto_client):
        """Test that ClientError from DynamoDB is propagated."""
        mock_boto_client.query.side_effect = ClientError(
            {"Error": {"Message": "Query failed"}}, "Query"
        )

        with pytest.raises(ClientError):
            db_ops.get_product_urls_by_domain("e.com")


class TestGetAllProductUrls:
    """Tests for get_all_product_urls_by_domain method."""

    def test_returns_all_urls_from_single_page(self, db_ops, mock_boto_client):
        """Test retrieving all URLs when results fit in a single page."""
        mock_boto_client.query.return_value = {
            "Items": [
                {"url": {"S": "https://test.com/p1"}},
                {"url": {"S": "https://test.com/p2"}},
            ]
        }

        urls = db_ops.get_all_product_urls_by_domain("test.com")

        assert urls == ["https://test.com/p1", "https://test.com/p2"]
        mock_boto_client.query.assert_called_once()

    def test_returns_all_urls_from_multiple_pages(self, db_ops, mock_boto_client):
        """Test automatic pagination through multiple pages."""
        mock_boto_client.query.side_effect = [
            {
                "Items": [{"url": {"S": "https://multi.com/p1"}}],
                "LastEvaluatedKey": {"pk": {"S": "key1"}},
            },
            {
                "Items": [{"url": {"S": "https://multi.com/p2"}}],
                "LastEvaluatedKey": {"pk": {"S": "key2"}},
            },
            {
                "Items": [{"url": {"S": "https://multi.com/p3"}}],
            },
        ]

        urls = db_ops.get_all_product_urls_by_domain("multi.com")

        assert urls == [
            "https://multi.com/p1",
            "https://multi.com/p2",
            "https://multi.com/p3",
        ]
        assert mock_boto_client.query.call_count == 3

    def test_handles_empty_results(self, db_ops, mock_boto_client):
        """Test handling when no URLs are found."""
        mock_boto_client.query.return_value = {"Items": []}

        urls = db_ops.get_all_product_urls_by_domain("empty.com")

        assert urls == []
        mock_boto_client.query.assert_called_once()

    def test_propagates_errors(self, db_ops, mock_boto_client):
        """Test that errors during pagination are propagated."""
        mock_boto_client.query.side_effect = [
            {
                "Items": [{"url": {"S": "https://error.com/p1"}}],
                "LastEvaluatedKey": {"pk": {"S": "key1"}},
            },
            ClientError({"Error": {"Message": "Query failed"}}, "Query"),
        ]

        with pytest.raises(ClientError):
            db_ops.get_all_product_urls_by_domain("error.com")


class TestFindShopsByCoreName:
    """Tests for find_all_domains_by_core_domain_name (GSI4)."""

    def test_find_all_domains_with_pagination(self, db_ops, mock_boto_client):
        """Test GSI4 query with pagination."""
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

            result = db_ops.find_all_domains_by_core_domain_name("example")

        assert len(result) == 2
        assert mock_boto_client.query.call_count == 2

        # Verify query structure
        called_args = mock_boto_client.query.call_args_list[0].kwargs
        assert called_args["IndexName"] == "GSI4"
        assert called_args["KeyConditionExpression"] == "gsi4_pk = :cdn"

    def test_find_all_domains_propagates_missing_gsi4_error(
        self, db_ops, mock_boto_client
    ):
        """Test that missing GSI4 error is re-raised after logging."""
        mock_boto_client.query.side_effect = ClientError(
            cast(
                Any,
                _client_error_response(
                    "The table does not have the specified index: GSI4"
                ),
            ),
            "Query",
        )

        with pytest.raises(ClientError):
            db_ops.find_all_domains_by_core_domain_name("example")

    def test_find_all_domains_propagates_unexpected_error(
        self, db_ops, mock_boto_client
    ):
        """Test that generic Exception is re-raised."""
        mock_boto_client.query.side_effect = Exception("Unknown error")

        with pytest.raises(Exception, match="Unknown error"):
            db_ops.find_all_domains_by_core_domain_name("example")


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

        items = [{"pk": {"S": "1"}, "sk": {"S": "META#"}}]

        with pytest.raises(Exception, match="Batch crash"):
            db_ops._batch_write_items(items, "type")

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


class TestParseGsiSk:
    def test_parses_never_state(self):
        state, timestamp = parse_gsi_sk("NEVER#")

        assert state == STATE_NEVER
        assert timestamp is None

    def test_parses_progress_state(self):
        state, timestamp = parse_gsi_sk("PROGRESS#2026-01-15T12:00:00Z")

        assert state == STATE_PROGRESS
        assert timestamp == "2026-01-15T12:00:00Z"

    def test_parses_done_state(self):
        state, timestamp = parse_gsi_sk("DONE#2026-01-15T12:00:00Z")

        assert state == STATE_DONE
        assert timestamp == "2026-01-15T12:00:00Z"

    def test_raises_for_invalid_state(self):
        with pytest.raises(ValueError, match="Invalid GSI SK"):
            parse_gsi_sk("INVALID#")


class TestUpdateUrlHash:
    def test_updates_hash_with_expected_expression(self, db_ops, mock_boto_client):
        mock_boto_client.update_item.return_value = {"Attributes": {"hash": {"S": "h"}}}

        result = db_ops.update_url_hash("shop.com", "https://shop.com/p", "h")

        assert result == {"hash": {"S": "h"}}
        called_kwargs = mock_boto_client.update_item.call_args.kwargs
        assert called_kwargs["TableName"] == db_ops.table_name
        assert called_kwargs["Key"] == {
            "pk": {"S": "SHOP#shop.com"},
            "sk": {"S": "URL#https://shop.com/p"},
        }
        assert called_kwargs["UpdateExpression"] == "SET #h = :new_hash"
        assert called_kwargs["ExpressionAttributeNames"] == {"#h": "hash"}
        assert called_kwargs["ExpressionAttributeValues"] == {":new_hash": {"S": "h"}}

    def test_propagates_client_error(self, db_ops, mock_boto_client):
        mock_boto_client.update_item.side_effect = ClientError(
            cast(Any, _client_error_response("Update failed")),
            "UpdateItem",
        )

        with pytest.raises(ClientError):
            db_ops.update_url_hash("shop.com", "https://shop.com/p", "h")


class TestUpsertShopMetadata:
    def test_sets_country_from_dns_lookup(self, db_ops, mock_boto_client):
        metadata = ShopMetadata(domain="shop.com", shop_country=None)

        with (
            patch(
                "src.core.aws.database.operations.socket.gethostbyname",
                return_value="1.2.3.4",
            ),
            patch(
                "src.core.aws.database.operations.get_country_code",
                return_value="DE",
            ),
        ):
            db_ops.upsert_shop_metadata(metadata)

        assert metadata.shop_country == "DE"
        mock_boto_client.put_item.assert_called_once()

    def test_handles_dns_failure_without_country(self, db_ops, mock_boto_client):
        metadata = ShopMetadata(domain="shop.com", shop_country=None)

        with patch(
            "src.core.aws.database.operations.socket.gethostbyname",
            side_effect=socket.gaierror(),
        ):
            db_ops.upsert_shop_metadata(metadata)

        assert metadata.shop_country is None
        mock_boto_client.put_item.assert_called_once()


class TestGetShopMetadata:
    def test_returns_shop_metadata_when_found(self, db_ops, mock_boto_client):
        mock_boto_client.get_item.return_value = {
            "Item": {
                "pk": {"S": "SHOP#a.com"},
                "sk": {"S": "META#"},
                "domain": {"S": "a.com"},
            }
        }

        with patch(
            "src.core.aws.database.models.ShopMetadata.from_dynamodb_item",
            return_value=Mock(domain="a.com"),
        ):
            result = db_ops.get_shop_metadata("a.com")

        assert result.domain == "a.com"

    def test_returns_none_when_missing(self, db_ops, mock_boto_client):
        mock_boto_client.get_item.return_value = {}

        result = db_ops.get_shop_metadata("a.com")

        assert result is None


class TestGetUrlEntry:
    def test_returns_url_entry_when_found(self, db_ops, mock_boto_client):
        mock_boto_client.get_item.return_value = {
            "Item": {
                "pk": {"S": "SHOP#a.com"},
                "sk": {"S": "URL#https://a.com/p"},
                "url": {"S": "https://a.com/p"},
            }
        }

        with patch(
            "src.core.aws.database.models.URLEntry.from_dynamodb_item",
            return_value=Mock(url="https://a.com/p"),
        ):
            result = db_ops.get_url_entry("a.com", "https://a.com/p")

        assert result.url == "https://a.com/p"

    def test_returns_none_when_missing(self, db_ops, mock_boto_client):
        mock_boto_client.get_item.return_value = {}

        result = db_ops.get_url_entry("a.com", "https://a.com/p")

        assert result is None


class TestGetShopsForOrchestration:
    def test_fetches_never_and_done_shops_for_country(self, db_ops, mock_boto_client):
        mock_boto_client.query.side_effect = [
            {
                "Items": [
                    {
                        "pk": {"S": "SHOP#a.com"},
                        "sk": {"S": "META#"},
                        "domain": {"S": "a.com"},
                    }
                ]
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
            "src.core.aws.database.models.ShopMetadata.from_dynamodb_item",
            side_effect=[Mock(domain="a.com"), Mock(domain="b.com")],
        ):
            result = db_ops.get_shops_for_orchestration(
                operation_type="crawl",
                cutoff_date="2026-01-02T00:00:00Z",
                country="DE",
            )

        assert [shop.domain for shop in result] == ["a.com", "b.com"]
        assert mock_boto_client.query.call_count == 2
        first_call = mock_boto_client.query.call_args_list[0].kwargs
        second_call = mock_boto_client.query.call_args_list[1].kwargs
        assert first_call["IndexName"] == "GSI2"
        assert second_call["IndexName"] == "GSI2"

    def test_rejects_invalid_operation_type(self, db_ops):
        with pytest.raises(
            ValueError, match="operation_type must be 'crawl' or 'scrape'"
        ):
            db_ops.get_shops_for_orchestration("index", "2026-01-02T00:00:00Z")
