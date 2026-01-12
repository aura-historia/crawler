"""Tests for Spider Orchestration Lambda handler."""

from __future__ import annotations

import json
from datetime import datetime, timedelta, timezone
from unittest.mock import MagicMock, patch

import pytest

from src.core.aws.database.models import ShopMetadata


class TestSpiderOrchestrationHandler:
    """Tests for the orchestration handler."""

    @pytest.fixture
    def mock_env_vars(self, monkeypatch):
        """Set up environment variables for testing."""
        monkeypatch.setenv("SQS_PRODUCT_SPIDER_QUEUE_URL", "https://queue.url")
        monkeypatch.setenv("DYNAMODB_TABLE_NAME", "test-table")
        monkeypatch.setenv("ORCHESTRATION_CUTOFF_DAYS", "2")

    @pytest.fixture
    def sample_shops(self):
        """Create sample shop metadata objects."""
        return [
            ShopMetadata(domain="example.com", shop_name="Example Shop"),
            ShopMetadata(domain="test-shop.de", shop_name="Test Shop"),
            ShopMetadata(domain="new-shop.com"),  # New shop without name
        ]

    def test_handler_no_queue_url(self, monkeypatch):
        """Test handler behavior when queue URL is not configured."""
        from src.lambdas.orchestration_spider import orchestration_handler

        # Unset queue URL
        monkeypatch.delenv("SQS_PRODUCT_SPIDER_QUEUE_URL", raising=False)

        result = orchestration_handler.handler({}, None)

        assert result["statusCode"] == 500
        body = json.loads(result["body"])
        assert "error" in body
        assert "Queue URL not configured" in body["error"]

    @patch("src.lambdas.orchestration_spider.orchestration_handler.db_operations")
    @patch("src.lambdas.orchestration_spider.orchestration_handler._get_sqs_client")
    def test_handler_no_shops_found(self, mock_get_sqs, mock_db_ops, mock_env_vars):
        """Test handler when no shops need crawling."""
        from src.lambdas.orchestration_spider import orchestration_handler

        # Mock empty shop list
        mock_db_ops.get_last_crawled_shops.return_value = []

        result = orchestration_handler.handler({}, None)

        assert result["statusCode"] == 200
        body = json.loads(result["body"])
        assert body["shops_count"] == 0
        assert "No shops to enqueue" in body["message"]

    @patch("src.lambdas.orchestration_spider.orchestration_handler.db_operations")
    @patch("src.lambdas.orchestration_spider.orchestration_handler._get_sqs_client")
    def test_handler_success(
        self, mock_get_sqs, mock_db_ops, mock_env_vars, sample_shops
    ):
        """Test successful orchestration with shops found."""
        from src.lambdas.orchestration_spider import orchestration_handler

        # Mock shops found
        mock_db_ops.get_last_crawled_shops.return_value = sample_shops

        # Mock SQS client
        mock_sqs_client = MagicMock()
        mock_get_sqs.return_value = mock_sqs_client
        mock_sqs_client.send_message_batch.return_value = {
            "Successful": [{"Id": "0"}, {"Id": "1"}, {"Id": "2"}],
            "Failed": [],
        }

        result = orchestration_handler.handler({}, None)

        assert result["statusCode"] == 200
        body = json.loads(result["body"])
        assert body["shops_found"] == 3
        assert body["shops_enqueued"] == 3
        assert body["shops_failed"] == 0
        assert body["failed_domains"] == []
        assert "Spider orchestration completed" in body["message"]

        # Verify SQS batch send was called
        mock_sqs_client.send_message_batch.assert_called_once()

    @patch("src.lambdas.orchestration_spider.orchestration_handler.db_operations")
    @patch("src.lambdas.orchestration_spider.orchestration_handler._get_sqs_client")
    def test_handler_partial_sqs_failure(
        self, mock_get_sqs, mock_db_ops, mock_env_vars, sample_shops
    ):
        """Test handler when some SQS messages fail to send."""
        from src.lambdas.orchestration_spider import orchestration_handler

        mock_db_ops.get_last_crawled_shops.return_value = sample_shops

        # Mock partial SQS failure
        mock_sqs_client = MagicMock()
        mock_get_sqs.return_value = mock_sqs_client
        mock_sqs_client.send_message_batch.return_value = {
            "Successful": [{"Id": "0"}, {"Id": "1"}],
            "Failed": [{"Id": "2", "Message": "Rate exceeded"}],
        }

        result = orchestration_handler.handler({}, None)

        assert result["statusCode"] == 200
        body = json.loads(result["body"])
        assert body["shops_found"] == 3
        assert body["shops_enqueued"] == 2  # Only 2 succeeded
        assert body["shops_failed"] == 1
        assert len(body["failed_domains"]) == 1
        # The third domain (index 2 = new-shop.com) should have failed
        assert "new-shop.com" in body["failed_domains"]

    @patch("src.lambdas.orchestration_spider.orchestration_handler.db_operations")
    def test_handler_db_exception(self, mock_db_ops, mock_env_vars):
        """Test handler when database operation fails."""
        from src.lambdas.orchestration_spider import orchestration_handler

        # Mock database exception
        mock_db_ops.get_last_crawled_shops.side_effect = Exception("DynamoDB error")

        result = orchestration_handler.handler({}, None)

        assert result["statusCode"] == 500
        body = json.loads(result["body"])
        assert "error" in body
        assert "DynamoDB error" in body["error"]

    def test_enqueue_shops_batching(self, mock_env_vars):
        """Test that shops are enqueued in batches of 10."""
        from src.lambdas.orchestration_spider import orchestration_handler

        # Create 25 domains (should result in 3 batches: 10, 10, 5)
        domains = [f"shop{i}.com" for i in range(25)]

        mock_sqs_client = MagicMock()
        mock_sqs_client.send_message_batch.return_value = {
            "Successful": [{"Id": str(i)} for i in range(10)],
            "Failed": [],
        }

        with patch(
            "src.lambdas.orchestration_spider.orchestration_handler._get_sqs_client",
            return_value=mock_sqs_client,
        ):
            result = orchestration_handler._enqueue_shops_to_spider_queue(
                domains, "https://queue.url"
            )

        # Should have called send_message_batch 3 times
        assert mock_sqs_client.send_message_batch.call_count == 3
        assert (
            result["successful"] == 30
        )  # 10 + 10 + 10 (mock returns 10 successful each time)
        assert result["failed"] == []


class TestGetShopsForOrchestration:
    """Tests for the database operation get_last_crawled_shops."""

    @pytest.fixture
    def mock_dynamodb_client(self):
        """Mock DynamoDB client."""
        with patch("src.core.aws.database.operations.get_dynamodb_client") as mock:
            yield mock.return_value

    def test_get_last_crawled_shops_with_old_shops(self, mock_dynamodb_client):
        """Test querying shops with old last_crawled_end dates using GSI2."""
        from src.core.aws.database.operations import DynamoDBOperations

        cutoff = datetime.now(timezone.utc) - timedelta(days=2)
        cutoff_str = cutoff.isoformat()

        # Mock GSI2 query response for old shops
        mock_dynamodb_client.query.return_value = {
            "Items": [
                {
                    "pk": {"S": "SHOP#example.com"},
                    "sk": {"S": "META#"},
                    "domain": {"S": "example.com"},
                    "last_crawled_end": {"S": "2026-01-01T00:00:00Z"},
                }
            ],
            "LastEvaluatedKey": None,
        }

        ops = DynamoDBOperations()
        ops.client = mock_dynamodb_client

        result = ops.get_last_crawled_shops(cutoff_str, country="DE")

        assert len(result) >= 1
        assert any(shop.domain == "example.com" for shop in result)
        # Should have called query once (unified query with marker value)
        assert mock_dynamodb_client.query.call_count == 1

    def test_get_last_crawled_shops_with_new_shops(self, mock_dynamodb_client):
        """Test querying new shops (with marker value 1970-01-01T00:00:00Z) using GSI2."""
        from src.core.aws.database.operations import DynamoDBOperations

        cutoff = datetime.now(timezone.utc) - timedelta(days=2)
        cutoff_str = cutoff.isoformat()

        # Mock response with new shop (has marker value in gsi2_sk)
        mock_dynamodb_client.query.return_value = {
            "Items": [
                {
                    "pk": {"S": "SHOP#newshop.com"},
                    "sk": {"S": "META#"},
                    "domain": {"S": "newshop.com"},
                    "gsi2_sk": {"S": "1970-01-01T00:00:00Z"},
                    # No last_crawled_start field
                    # No last_crawled_end field
                }
            ],
            "LastEvaluatedKey": None,
        }

        ops = DynamoDBOperations()
        ops.client = mock_dynamodb_client

        result = ops.get_last_crawled_shops(cutoff_str, country="DE")

        assert len(result) >= 1
        new_shop = next((s for s in result if s.domain == "newshop.com"), None)
        assert new_shop is not None
        assert new_shop.last_crawled_end is None
        assert new_shop.last_crawled_start is None

    def test_query_uses_marker_value_for_new_shops(self, mock_dynamodb_client):
        """Test that the query correctly uses marker value to find new shops.

        New shops have gsi2_sk = "1970-01-01T00:00:00Z" which is always <= cutoff_date.
        No FilterExpression is needed.
        """
        from src.core.aws.database.operations import DynamoDBOperations

        cutoff = datetime.now(timezone.utc) - timedelta(days=2)
        cutoff_str = cutoff.isoformat()

        mock_dynamodb_client.query.return_value = {
            "Items": [],
            "LastEvaluatedKey": None,
        }

        ops = DynamoDBOperations()
        ops.client = mock_dynamodb_client

        ops.get_last_crawled_shops(cutoff_str, country="DE")

        # Verify query structure
        query_call = mock_dynamodb_client.query.call_args_list[0]
        key_condition = query_call[1]["KeyConditionExpression"]

        # Should use gsi2_sk <= cutoff to find both old and new shops
        assert "gsi2_sk <= :cutoff" in key_condition
        assert query_call[1]["IndexName"] == "GSI2"

        # Should NOT have FilterExpression (simplified)
        assert "FilterExpression" not in query_call[1]

    def test_get_last_crawled_shops_no_country(self, mock_dynamodb_client):
        """Test orchestration without country filter (queries default countries)."""
        from src.core.aws.database.operations import DynamoDBOperations

        cutoff = datetime.now(timezone.utc) - timedelta(days=2)
        cutoff_str = cutoff.isoformat()

        mock_dynamodb_client.query.return_value = {
            "Items": [],
            "LastEvaluatedKey": None,
        }

        ops = DynamoDBOperations()
        ops.client = mock_dynamodb_client

        ops.get_last_crawled_shops(cutoff_str, country=None)

        assert mock_dynamodb_client.query.call_count == 1
