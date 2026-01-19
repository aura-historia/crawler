"""Tests for unified Orchestration Lambda handler."""

from __future__ import annotations

import json
from datetime import datetime, timedelta, timezone
from unittest.mock import MagicMock, patch

import pytest

from src.core.aws.database.operations import ShopMetadata
from src.core.aws.database.constants import STATE_NEVER, STATE_PROGRESS, STATE_DONE


class TestOrchestrationHandler:
    """Tests for the unified orchestration handler."""

    @pytest.fixture
    def mock_env_vars(self, monkeypatch):
        """Set up environment variables for testing."""
        monkeypatch.setenv("SQS_PRODUCT_SPIDER_QUEUE_URL", "https://spider-queue.url")
        monkeypatch.setenv("SQS_PRODUCT_SCRAPER_QUEUE_URL", "https://scraper-queue.url")
        monkeypatch.setenv("DYNAMODB_TABLE_NAME", "test-table")
        monkeypatch.setenv("ORCHESTRATION_CUTOFF_DAYS", "2")

    @pytest.fixture
    def sample_shops(self):
        """Create sample shop metadata objects."""
        return [
            ShopMetadata(domain="example.com", shop_name="Example Shop"),
            ShopMetadata(domain="test-shop.de", shop_name="Test Shop"),
            ShopMetadata(domain="new-shop.com"),
        ]

    def test_handler_invalid_operation_type(self, mock_env_vars):
        """Test handler with invalid operation type."""
        from src.lambdas.orchestration import orchestration_handler

        result = orchestration_handler.handler({"operation": "invalid"}, None)

        assert result["statusCode"] == 400
        body = json.loads(result["body"])
        assert "error" in body
        assert "Invalid operation type" in body["error"]

    def test_handler_crawl_no_queue_url(self, monkeypatch):
        """Test crawl operation when queue URL is not configured."""
        from src.lambdas.orchestration import orchestration_handler

        monkeypatch.delenv("SQS_PRODUCT_SPIDER_QUEUE_URL", raising=False)
        monkeypatch.setenv("SQS_PRODUCT_SCRAPER_QUEUE_URL", "https://scraper-queue.url")
        monkeypatch.setenv("DYNAMODB_TABLE_NAME", "test-table")

        result = orchestration_handler.handler({"operation": "crawl"}, None)

        assert result["statusCode"] == 500
        body = json.loads(result["body"])
        assert "error" in body

    def test_handler_scrape_no_queue_url(self, monkeypatch):
        """Test scrape operation when queue URL is not configured."""
        from src.lambdas.orchestration import orchestration_handler

        monkeypatch.setenv("SQS_PRODUCT_SPIDER_QUEUE_URL", "https://spider-queue.url")
        monkeypatch.delenv("SQS_PRODUCT_SCRAPER_QUEUE_URL", raising=False)
        monkeypatch.setenv("DYNAMODB_TABLE_NAME", "test-table")

        result = orchestration_handler.handler({"operation": "scrape"}, None)

        assert result["statusCode"] == 500
        body = json.loads(result["body"])
        assert "error" in body

    @patch("src.lambdas.orchestration.orchestration_handler.db_operations")
    @patch("src.lambdas.orchestration.orchestration_handler._get_sqs_client")
    def test_handler_crawl_no_shops_found(
        self, mock_get_sqs, mock_db_ops, mock_env_vars
    ):
        """Test crawl operation when no shops need crawling."""
        from src.lambdas.orchestration import orchestration_handler

        mock_db_ops.get_shops_for_orchestration.return_value = []

        result = orchestration_handler.handler({"operation": "crawl"}, None)

        assert result["statusCode"] == 200
        body = json.loads(result["body"])
        assert body["shops_count"] == 0
        assert body["operation_type"] == "crawl"
        assert "No shops to enqueue" in body["message"]

    @patch("src.lambdas.orchestration.orchestration_handler.db_operations")
    @patch("src.lambdas.orchestration.orchestration_handler._get_sqs_client")
    def test_handler_scrape_no_shops_found(
        self, mock_get_sqs, mock_db_ops, mock_env_vars
    ):
        """Test scrape operation when no shops need scraping."""
        from src.lambdas.orchestration import orchestration_handler

        mock_db_ops.get_shops_for_orchestration.return_value = []

        result = orchestration_handler.handler({"operation": "scrape"}, None)

        assert result["statusCode"] == 200
        body = json.loads(result["body"])
        assert body["shops_count"] == 0
        assert body["operation_type"] == "scrape"

    @patch("src.lambdas.orchestration.orchestration_handler.db_operations")
    @patch("src.lambdas.orchestration.orchestration_handler._get_sqs_client")
    def test_handler_crawl_success(
        self, mock_get_sqs, mock_db_ops, mock_env_vars, sample_shops
    ):
        """Test successful crawl orchestration."""
        from src.lambdas.orchestration import orchestration_handler

        mock_db_ops.get_shops_for_orchestration.return_value = sample_shops

        mock_sqs_client = MagicMock()
        mock_get_sqs.return_value = mock_sqs_client
        mock_sqs_client.send_message_batch.return_value = {
            "Successful": [{"Id": "0"}, {"Id": "1"}, {"Id": "2"}],
            "Failed": [],
        }

        result = orchestration_handler.handler({"operation": "crawl"}, None)

        assert result["statusCode"] == 200
        body = json.loads(result["body"])
        assert body["shops_found"] == 3
        assert body["shops_enqueued"] == 3
        assert body["shops_failed"] == 0
        assert body["operation_type"] == "crawl"
        assert "Crawl orchestration completed" in body["message"]

        # Verify correct method was called
        mock_db_ops.get_shops_for_orchestration.assert_called_once()
        call_args = mock_db_ops.get_shops_for_orchestration.call_args
        assert call_args[1]["operation_type"] == "crawl"

    @patch("src.lambdas.orchestration.orchestration_handler.db_operations")
    @patch("src.lambdas.orchestration.orchestration_handler._get_sqs_client")
    def test_handler_scrape_success(self, mock_get_sqs, mock_db_ops, mock_env_vars):
        """Test successful scrape orchestration."""
        from src.lambdas.orchestration import orchestration_handler

        # Create shops with proper timestamps for scrape eligibility
        eligible_shops = [
            ShopMetadata(
                domain="example.com",
                shop_name="Example Shop",
                last_crawled_end=f"{STATE_DONE}2026-01-15T12:00:00Z",
                last_scraped_end=f"{STATE_DONE}2026-01-14T10:00:00Z",
            ),
            ShopMetadata(
                domain="test-shop.de",
                shop_name="Test Shop",
                last_crawled_end=f"{STATE_DONE}2026-01-15T14:00:00Z",
                last_scraped_end=STATE_NEVER,
            ),
            ShopMetadata(
                domain="new-shop.com",
                last_crawled_end=f"{STATE_DONE}2026-01-15T16:00:00Z",
                last_scraped_end=f"{STATE_DONE}2026-01-14T12:00:00Z",
            ),
        ]

        mock_db_ops.get_shops_for_orchestration.return_value = eligible_shops

        mock_sqs_client = MagicMock()
        mock_get_sqs.return_value = mock_sqs_client
        mock_sqs_client.send_message_batch.return_value = {
            "Successful": [{"Id": "0"}, {"Id": "1"}, {"Id": "2"}],
            "Failed": [],
        }

        result = orchestration_handler.handler({"operation": "scrape"}, None)

        assert result["statusCode"] == 200
        body = json.loads(result["body"])
        assert body["shops_found"] == 3
        assert body["shops_enqueued"] == 3
        assert body["operation_type"] == "scrape"
        assert "Scrape orchestration completed" in body["message"]

        # Verify correct method was called
        call_args = mock_db_ops.get_shops_for_orchestration.call_args
        assert call_args[1]["operation_type"] == "scrape"

    @patch("src.lambdas.orchestration.orchestration_handler.db_operations")
    @patch("src.lambdas.orchestration.orchestration_handler._get_sqs_client")
    def test_handler_default_operation_is_crawl(
        self, mock_get_sqs, mock_db_ops, mock_env_vars, sample_shops
    ):
        """Test that default operation is crawl for backward compatibility."""
        from src.lambdas.orchestration import orchestration_handler

        mock_db_ops.get_shops_for_orchestration.return_value = sample_shops
        mock_sqs_client = MagicMock()
        mock_get_sqs.return_value = mock_sqs_client
        mock_sqs_client.send_message_batch.return_value = {
            "Successful": [{"Id": "0"}, {"Id": "1"}, {"Id": "2"}],
            "Failed": [],
        }

        # Call without operation parameter
        result = orchestration_handler.handler({}, None)

        assert result["statusCode"] == 200
        body = json.loads(result["body"])
        assert body["operation_type"] == "crawl"

    @patch("src.lambdas.orchestration.orchestration_handler.db_operations")
    @patch("src.lambdas.orchestration.orchestration_handler._get_sqs_client")
    def test_handler_partial_sqs_failure(
        self, mock_get_sqs, mock_db_ops, mock_env_vars, sample_shops
    ):
        """Test handler when some SQS messages fail to send."""
        from src.lambdas.orchestration import orchestration_handler

        mock_db_ops.get_shops_for_orchestration.return_value = sample_shops

        mock_sqs_client = MagicMock()
        mock_get_sqs.return_value = mock_sqs_client
        mock_sqs_client.send_message_batch.return_value = {
            "Successful": [{"Id": "0"}, {"Id": "1"}],
            "Failed": [{"Id": "2", "Message": "Rate exceeded"}],
        }

        result = orchestration_handler.handler({"operation": "crawl"}, None)

        assert result["statusCode"] == 200
        body = json.loads(result["body"])
        assert body["shops_found"] == 3
        assert body["shops_enqueued"] == 2
        assert body["shops_failed"] == 1
        assert len(body["failed_domains"]) == 1

    @patch("src.lambdas.orchestration.orchestration_handler.db_operations")
    def test_handler_db_exception(self, mock_db_ops, mock_env_vars):
        """Test handler when database operation fails."""
        from src.lambdas.orchestration import orchestration_handler

        mock_db_ops.get_shops_for_orchestration.side_effect = Exception(
            "DynamoDB error"
        )

        result = orchestration_handler.handler({"operation": "crawl"}, None)

        assert result["statusCode"] == 500
        body = json.loads(result["body"])
        assert "error" in body
        assert "DynamoDB error" in body["error"]

    def test_enqueue_shops_batching(self, mock_env_vars):
        """Test that shops are enqueued in batches of 10."""
        from src.lambdas.orchestration import orchestration_handler

        domains = [f"shop{i}.com" for i in range(25)]

        mock_sqs_client = MagicMock()
        mock_sqs_client.send_message_batch.return_value = {
            "Successful": [{"Id": str(i)} for i in range(10)],
            "Failed": [],
        }

        with patch.object(
            orchestration_handler, "_get_sqs_client", return_value=mock_sqs_client
        ):
            result = orchestration_handler._enqueue_shops_to_queue(
                domains, "https://queue.url", "crawl"
            )

        assert mock_sqs_client.send_message_batch.call_count == 3
        assert result["successful"] == 30
        assert result["failed"] == []


class TestGetShopsForOrchestration:
    """Tests for the unified get_shops_for_orchestration method."""

    @pytest.fixture
    def mock_dynamodb_client(self):
        """Mock DynamoDB client."""
        with patch("src.core.aws.database.operations.get_dynamodb_client") as mock:
            yield mock.return_value

    def test_get_shops_for_orchestration_crawl(self, mock_dynamodb_client):
        """Test querying shops for crawl operation using GSI2 with state prefixes."""
        from src.core.aws.database.operations import DynamoDBOperations

        cutoff = datetime.now(timezone.utc) - timedelta(days=2)
        cutoff_str = cutoff.isoformat()

        # Mock response - called twice (once for NEVER#, once for DONE#)
        mock_dynamodb_client.query.side_effect = [
            {
                "Items": [
                    {
                        "pk": {"S": "SHOP#never.com"},
                        "sk": {"S": "META#"},
                        "domain": {"S": "never.com"},
                    }
                ],
                "LastEvaluatedKey": None,
            },
            {
                "Items": [
                    {
                        "pk": {"S": "SHOP#old.com"},
                        "sk": {"S": "META#"},
                        "domain": {"S": "old.com"},
                    }
                ],
                "LastEvaluatedKey": None,
            },
        ]

        ops = DynamoDBOperations()
        ops.client = mock_dynamodb_client

        shops = ops.get_shops_for_orchestration("crawl", cutoff_str, country="DE")

        assert len(shops) == 2
        assert shops[0].domain == "never.com"
        assert shops[1].domain == "old.com"

        # Verify it used GSI2 with state prefixes
        assert mock_dynamodb_client.query.call_count == 2

        # First call should query NEVER# state
        first_call_kwargs = mock_dynamodb_client.query.call_args_list[0][1]
        assert first_call_kwargs["IndexName"] == "GSI2"
        assert "gsi2_pk = :country" in first_call_kwargs["KeyConditionExpression"]
        assert "gsi2_sk = :never" in first_call_kwargs["KeyConditionExpression"]

        # Second call should query DONE# state with cutoff
        second_call_kwargs = mock_dynamodb_client.query.call_args_list[1][1]
        assert second_call_kwargs["IndexName"] == "GSI2"
        assert "gsi2_pk = :country" in second_call_kwargs["KeyConditionExpression"]
        assert "BETWEEN" in second_call_kwargs["KeyConditionExpression"]

    def test_get_shops_for_orchestration_scrape(self, mock_dynamodb_client):
        """Test querying shops for scrape operation using GSI3 with state prefixes."""
        from src.core.aws.database.operations import DynamoDBOperations

        cutoff = datetime.now(timezone.utc) - timedelta(days=2)
        cutoff_str = cutoff.isoformat()

        # Mock response - called twice (once for NEVER#, once for DONE#)
        mock_dynamodb_client.query.side_effect = [
            {
                "Items": [
                    {
                        "pk": {"S": "SHOP#never.com"},
                        "sk": {"S": "META#"},
                        "domain": {"S": "never.com"},
                    }
                ],
                "LastEvaluatedKey": None,
            },
            {
                "Items": [
                    {
                        "pk": {"S": "SHOP#old.com"},
                        "sk": {"S": "META#"},
                        "domain": {"S": "old.com"},
                    }
                ],
                "LastEvaluatedKey": None,
            },
        ]

        ops = DynamoDBOperations()
        ops.client = mock_dynamodb_client

        shops = ops.get_shops_for_orchestration("scrape", cutoff_str, country="DE")

        assert len(shops) == 2

        # Verify it used GSI3 with state prefixes
        assert mock_dynamodb_client.query.call_count == 2

        # First call should query NEVER# state
        first_call_kwargs = mock_dynamodb_client.query.call_args_list[0][1]
        assert first_call_kwargs["IndexName"] == "GSI3"
        assert "gsi3_pk = :country" in first_call_kwargs["KeyConditionExpression"]
        assert "gsi3_sk = :never" in first_call_kwargs["KeyConditionExpression"]

        # Second call should query DONE# state with cutoff
        second_call_kwargs = mock_dynamodb_client.query.call_args_list[1][1]
        assert second_call_kwargs["IndexName"] == "GSI3"
        assert "gsi3_pk = :country" in second_call_kwargs["KeyConditionExpression"]
        assert "BETWEEN" in second_call_kwargs["KeyConditionExpression"]

    def test_get_shops_for_orchestration_invalid_type(self, mock_dynamodb_client):
        """Test that invalid operation type raises ValueError."""
        from src.core.aws.database.operations import DynamoDBOperations

        cutoff = datetime.now(timezone.utc) - timedelta(days=2)
        cutoff_str = cutoff.isoformat()

        ops = DynamoDBOperations()
        ops.client = mock_dynamodb_client

        with pytest.raises(ValueError) as exc_info:
            ops.get_shops_for_orchestration("invalid", cutoff_str, country="DE")

        assert "must be 'crawl' or 'scrape'" in str(exc_info.value)

    def test_get_shops_for_orchestration_pagination(self, mock_dynamodb_client):
        """Test GSI query handles pagination with state prefixes."""
        from src.core.aws.database.operations import DynamoDBOperations

        cutoff = datetime.now(timezone.utc) - timedelta(days=2)
        cutoff_str = cutoff.isoformat()

        # Mock paginated responses for both NEVER# and DONE# queries
        mock_dynamodb_client.query.side_effect = [
            # NEVER# query - page 1
            {
                "Items": [
                    {
                        "pk": {"S": "SHOP#never1.com"},
                        "sk": {"S": "META#"},
                        "domain": {"S": "never1.com"},
                    }
                ],
                "LastEvaluatedKey": {"pk": {"S": "SHOP#never1.com"}},
            },
            # NEVER# query - page 2
            {
                "Items": [
                    {
                        "pk": {"S": "SHOP#never2.com"},
                        "sk": {"S": "META#"},
                        "domain": {"S": "never2.com"},
                    }
                ],
                "LastEvaluatedKey": None,
            },
            # DONE# query - page 1
            {
                "Items": [
                    {
                        "pk": {"S": "SHOP#old1.com"},
                        "sk": {"S": "META#"},
                        "domain": {"S": "old1.com"},
                    }
                ],
                "LastEvaluatedKey": {"pk": {"S": "SHOP#old1.com"}},
            },
            # DONE# query - page 2
            {
                "Items": [
                    {
                        "pk": {"S": "SHOP#old2.com"},
                        "sk": {"S": "META#"},
                        "domain": {"S": "old2.com"},
                    }
                ],
                "LastEvaluatedKey": None,
            },
        ]

        ops = DynamoDBOperations()
        ops.client = mock_dynamodb_client

        shops = ops.get_shops_for_orchestration("crawl", cutoff_str, country="DE")

        # Should get 2 shops from NEVER# query and 2 from DONE# query
        assert len(shops) == 4
        assert shops[0].domain == "never1.com"
        assert shops[1].domain == "never2.com"
        assert shops[2].domain == "old1.com"
        assert shops[3].domain == "old2.com"
        assert mock_dynamodb_client.query.call_count == 4


class TestFilterEligibleShopsForScrape:
    """Tests for the _filter_eligible_shops_for_scrape function."""

    def test_filter_all_eligible(self):
        """Test filtering when all shops are eligible."""
        from src.lambdas.orchestration import orchestration_handler

        shops = [
            ShopMetadata(
                domain="shop1.com",
                last_crawled_end=f"{STATE_DONE}2026-01-15T12:00:00Z",
                last_scraped_end=f"{STATE_DONE}2026-01-14T10:00:00Z",
            ),
            ShopMetadata(
                domain="shop2.com",
                last_crawled_end=f"{STATE_DONE}2026-01-15T14:00:00Z",
                last_scraped_end=STATE_NEVER,  # Never scraped
            ),
        ]

        eligible, stats = orchestration_handler._filter_eligible_shops_for_scrape(shops)

        assert len(eligible) == 2
        assert stats["total_queried"] == 2
        assert stats["eligible"] == 2
        assert stats["in_progress"] == 0
        assert stats["crawl_not_finished"] == 0
        assert stats["already_scraped"] == 0

    def test_filter_scrape_in_progress(self):
        """Test filtering out shops with scrape in progress."""
        from src.lambdas.orchestration import orchestration_handler

        shops = [
            ShopMetadata(
                domain="shop1.com",
                last_crawled_end=f"{STATE_DONE}2026-01-15T12:00:00Z",
                last_scraped_start="2026-01-15T13:00:00Z",
                last_scraped_end=f"{STATE_PROGRESS}2026-01-15T13:00:00Z",  # Scrape in progress
            ),
        ]

        eligible, stats = orchestration_handler._filter_eligible_shops_for_scrape(shops)

        assert len(eligible) == 0
        assert stats["in_progress"] == 1
        assert stats["eligible"] == 0

    def test_filter_crawl_not_finished(self):
        """Test filtering out shops where crawl is not finished."""
        from src.lambdas.orchestration import orchestration_handler

        shops = [
            ShopMetadata(
                domain="shop1.com",
                last_crawled_end=None,  # Crawl in progress
                last_scraped_end=f"{STATE_DONE}2026-01-14T10:00:00Z",
            ),
            ShopMetadata(
                domain="shop2.com",
                last_crawled_end=STATE_NEVER,  # Never crawled
                last_scraped_end=STATE_NEVER,
            ),
        ]

        eligible, stats = orchestration_handler._filter_eligible_shops_for_scrape(shops)

        assert len(eligible) == 0
        assert stats["crawl_not_finished"] == 2
        assert stats["eligible"] == 0

    def test_filter_already_scraped(self):
        """Test filtering out shops where scrape is newer than crawl."""
        from src.lambdas.orchestration import orchestration_handler

        shops = [
            ShopMetadata(
                domain="shop1.com",
                last_crawled_end=f"{STATE_DONE}2026-01-14T10:00:00Z",
                last_scraped_end=f"{STATE_DONE}2026-01-15T12:00:00Z",  # Scrape newer
            ),
        ]

        eligible, stats = orchestration_handler._filter_eligible_shops_for_scrape(shops)

        assert len(eligible) == 0
        assert stats["already_scraped"] == 1
        assert stats["eligible"] == 0

    def test_filter_mixed_scenarios(self):
        """Test filtering with mixed eligible and ineligible shops."""
        from src.lambdas.orchestration import orchestration_handler

        shops = [
            # Eligible
            ShopMetadata(
                domain="eligible1.com",
                last_crawled_end=f"{STATE_DONE}2026-01-15T12:00:00Z",
                last_scraped_end=f"{STATE_DONE}2026-01-14T10:00:00Z",
            ),
            # In progress
            ShopMetadata(
                domain="in-progress.com",
                last_crawled_end=f"{STATE_DONE}2026-01-15T12:00:00Z",
                last_scraped_start="2026-01-15T13:00:00Z",
                last_scraped_end=f"{STATE_PROGRESS}2026-01-15T13:00:00Z",
            ),
            # Crawl not finished
            ShopMetadata(
                domain="crawl-not-done.com",
                last_crawled_end=None,
                last_scraped_end=f"{STATE_DONE}2026-01-14T10:00:00Z",
            ),
            # Already scraped
            ShopMetadata(
                domain="already-scraped.com",
                last_crawled_end=f"{STATE_DONE}2026-01-14T10:00:00Z",
                last_scraped_end=f"{STATE_DONE}2026-01-15T12:00:00Z",
            ),
            # Eligible
            ShopMetadata(
                domain="eligible2.com",
                last_crawled_end=f"{STATE_DONE}2026-01-15T14:00:00Z",
                last_scraped_end=STATE_NEVER,
            ),
        ]

        eligible, stats = orchestration_handler._filter_eligible_shops_for_scrape(shops)

        assert len(eligible) == 2
        assert eligible[0].domain == "eligible1.com"
        assert eligible[1].domain == "eligible2.com"
        assert stats["total_queried"] == 5
        assert stats["eligible"] == 2
        assert stats["in_progress"] == 1
        assert stats["crawl_not_finished"] == 1
        assert stats["already_scraped"] == 1

    def test_filter_empty_list(self):
        """Test filtering with empty shop list."""
        from src.lambdas.orchestration import orchestration_handler

        eligible, stats = orchestration_handler._filter_eligible_shops_for_scrape([])

        assert len(eligible) == 0
        assert stats["total_queried"] == 0
        assert stats["eligible"] == 0


class TestHandlerWithFiltering:
    """Tests for handler with scrape filtering enabled."""

    @pytest.fixture
    def mock_env_vars(self, monkeypatch):
        """Set up environment variables for testing."""
        monkeypatch.setenv("SQS_PRODUCT_SPIDER_QUEUE_URL", "https://spider-queue.url")
        monkeypatch.setenv("SQS_PRODUCT_SCRAPER_QUEUE_URL", "https://scraper-queue.url")
        monkeypatch.setenv("DYNAMODB_TABLE_NAME", "test-table")
        monkeypatch.setenv("ORCHESTRATION_CUTOFF_DAYS", "2")

    @patch("src.lambdas.orchestration.orchestration_handler.db_operations")
    @patch("src.lambdas.orchestration.orchestration_handler._get_sqs_client")
    def test_scrape_handler_with_filtering(
        self, mock_get_sqs, mock_db_ops, mock_env_vars
    ):
        """Test scrape handler applies in-memory filtering."""
        from src.lambdas.orchestration import orchestration_handler

        # Return mix of eligible and ineligible shops
        mock_db_ops.get_shops_for_orchestration.return_value = [
            ShopMetadata(
                domain="eligible.com",
                last_crawled_end=f"{STATE_DONE}2026-01-15T12:00:00Z",
                last_scraped_end=f"{STATE_DONE}2026-01-14T10:00:00Z",
            ),
            ShopMetadata(
                domain="already-scraped.com",
                last_crawled_end=f"{STATE_DONE}2026-01-14T10:00:00Z",
                last_scraped_end=f"{STATE_DONE}2026-01-15T12:00:00Z",
            ),
        ]

        mock_sqs_client = MagicMock()
        mock_sqs_client.send_message_batch.return_value = {
            "Successful": [{"Id": "0"}],
            "Failed": [],
        }
        mock_get_sqs.return_value = mock_sqs_client

        result = orchestration_handler.handler({"operation": "scrape"}, None)

        assert result["statusCode"] == 200
        body = json.loads(result["body"])
        assert body["shops_found"] == 1  # Only eligible shop
        assert body["shops_enqueued"] == 1
        assert "filter_stats" in body
        assert body["filter_stats"]["total_queried"] == 2
        assert body["filter_stats"]["eligible"] == 1
        assert body["filter_stats"]["already_scraped"] == 1

    @patch("src.lambdas.orchestration.orchestration_handler.db_operations")
    def test_scrape_handler_all_filtered_out(self, mock_db_ops, mock_env_vars):
        """Test scrape handler when all shops are filtered out."""
        from src.lambdas.orchestration import orchestration_handler

        # Return only ineligible shops
        mock_db_ops.get_shops_for_orchestration.return_value = [
            ShopMetadata(
                domain="already-scraped.com",
                last_crawled_end=f"{STATE_DONE}2026-01-14T10:00:00Z",
                last_scraped_end=f"{STATE_DONE}2026-01-15T12:00:00Z",
            ),
        ]

        result = orchestration_handler.handler({"operation": "scrape"}, None)

        assert result["statusCode"] == 200
        body = json.loads(result["body"])
        assert body["shops_count"] == 0
        assert body["shops_queried"] == 1
        assert body["shops_filtered"] == 1
        assert "No eligible shops" in body["message"]

    @patch("src.lambdas.orchestration.orchestration_handler.db_operations")
    @patch("src.lambdas.orchestration.orchestration_handler._get_sqs_client")
    def test_crawl_handler_with_filtering(
        self, mock_get_sqs, mock_db_ops, mock_env_vars
    ):
        """Test crawl handler applies filtering and all shops are eligible."""
        from src.lambdas.orchestration import orchestration_handler

        mock_db_ops.get_shops_for_orchestration.return_value = [
            ShopMetadata(
                domain="shop1.com",
                last_crawled_start=None,
                last_crawled_end=f"{STATE_DONE}2026-01-10T10:00:00Z",
            ),
            ShopMetadata(
                domain="shop2.com",
                last_crawled_start=None,
                last_crawled_end=f"{STATE_DONE}2026-01-11T10:00:00Z",
            ),
        ]

        mock_sqs_client = MagicMock()
        mock_sqs_client.send_message_batch.return_value = {
            "Successful": [{"Id": "0"}, {"Id": "1"}],
            "Failed": [],
        }
        mock_get_sqs.return_value = mock_sqs_client

        result = orchestration_handler.handler({"operation": "crawl"}, None)

        assert result["statusCode"] == 200
        body = json.loads(result["body"])
        assert body["shops_found"] == 2
        assert body["shops_enqueued"] == 2
        # Filter stats should be present for crawl operations
        assert "filter_stats" in body
        assert body["filter_stats"]["total_queried"] == 2
        assert body["filter_stats"]["eligible"] == 2
        assert body["filter_stats"]["in_progress"] == 0

    @patch("src.lambdas.orchestration.orchestration_handler.db_operations")
    @patch("src.lambdas.orchestration.orchestration_handler._get_sqs_client")
    def test_crawl_handler_filters_in_progress(
        self, mock_get_sqs, mock_db_ops, mock_env_vars
    ):
        """Test crawl handler filters out shops that are already crawling."""
        from src.lambdas.orchestration import orchestration_handler

        mock_db_ops.get_shops_for_orchestration.return_value = [
            # Shop 1: Not crawling
            ShopMetadata(
                domain="shop1.com",
                last_crawled_start=None,
                last_crawled_end=f"{STATE_DONE}2026-01-10T10:00:00Z",
            ),
            # Shop 2: Currently crawling (start > end)
            ShopMetadata(
                domain="shop2.com",
                last_crawled_start="2026-01-15T10:00:00Z",
                last_crawled_end=f"{STATE_PROGRESS}2026-01-15T10:00:00Z",
            ),
            # Shop 3: Currently crawling (end starts with PROGRESS#)
            ShopMetadata(
                domain="shop3.com",
                last_crawled_start="2026-01-15T09:00:00Z",
                last_crawled_end=f"{STATE_PROGRESS}2026-01-15T09:00:00Z",
            ),
        ]

        mock_sqs_client = MagicMock()
        mock_sqs_client.send_message_batch.return_value = {
            "Successful": [{"Id": "0"}],
            "Failed": [],
        }
        mock_get_sqs.return_value = mock_sqs_client

        result = orchestration_handler.handler({"operation": "crawl"}, None)

        assert result["statusCode"] == 200
        body = json.loads(result["body"])
        assert body["shops_found"] == 1  # Only shop1
        assert body["shops_enqueued"] == 1
        assert body["filter_stats"]["total_queried"] == 3
        assert body["filter_stats"]["eligible"] == 1
        assert body["filter_stats"]["in_progress"] == 2

    @patch("src.lambdas.orchestration.orchestration_handler.db_operations")
    def test_crawl_handler_all_filtered_out(self, mock_db_ops, mock_env_vars):
        """Test crawl handler when all shops are filtered out (all in progress)."""
        from src.lambdas.orchestration import orchestration_handler

        mock_db_ops.get_shops_for_orchestration.return_value = [
            ShopMetadata(
                domain="shop1.com",
                last_crawled_start="2026-01-15T10:00:00Z",
                last_crawled_end=f"{STATE_PROGRESS}2026-01-15T10:00:00Z",
            ),
        ]

        result = orchestration_handler.handler({"operation": "crawl"}, None)

        assert result["statusCode"] == 200
        body = json.loads(result["body"])
        assert body["shops_count"] == 0
        assert body["shops_queried"] == 1
        assert body["shops_filtered"] == 1
        assert "No eligible shops" in body["message"]
