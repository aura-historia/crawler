import pytest
from unittest.mock import patch, MagicMock
from botocore.exceptions import ClientError

from src.core.aws.database.migrations import create_tables


@pytest.fixture
def mock_env(monkeypatch):
    """Sets the required environment variable."""
    table_name = "test-table"
    monkeypatch.setenv("DYNAMODB_TABLE_NAME", table_name)
    return table_name


@pytest.fixture
def mock_dynamo():
    """Mocks the DynamoDB client and the getter function."""
    with patch("src.core.aws.database.migrations.get_dynamodb_client") as mock_get:
        client = MagicMock()
        mock_get.return_value = client
        yield client


@pytest.fixture
def mock_logger():
    """Mocks the logger to verify output and error messages."""
    with patch("src.core.aws.database.migrations.logger") as mock_log:
        yield mock_log


class TestCreateTables:
    def test_creates_table_when_not_exists(self, mock_dynamo, mock_env, mock_logger):
        """Happy path: Table doesn't exist, so it is created and waited for."""
        # 1. Setup mocks
        mock_dynamo.describe_table.side_effect = ClientError(
            {"Error": {"Code": "ResourceNotFoundException"}}, "DescribeTable"
        )
        mock_waiter = MagicMock()
        mock_dynamo.get_waiter.return_value = mock_waiter

        create_tables()

        # 2. Verify creation logic
        mock_dynamo.create_table.assert_called_once()
        mock_waiter.wait.assert_called_once_with(TableName=mock_env)
        mock_logger.info.assert_called_with(f"Table '{mock_env}' created successfully.")

    def test_skips_creation_when_table_exists(self, mock_dynamo, mock_env, mock_logger):
        """Idempotency check: describe_table succeeds, so creation is skipped."""
        mock_dynamo.describe_table.return_value = {"Table": {"TableName": mock_env}}

        create_tables()

        mock_dynamo.create_table.assert_not_called()
        mock_logger.info.assert_called_with(f"Table '{mock_env}' already exists.")

    def test_handles_specific_client_error(self, mock_dynamo, mock_env):
        """Re-raises ClientError if it's NOT ResourceNotFoundException."""
        mock_dynamo.describe_table.side_effect = ClientError(
            {"Error": {"Code": "AccessDeniedException"}}, "DescribeTable"
        )

        with pytest.raises(ClientError):
            create_tables()

    def test_handles_unexpected_generic_exception(self, mock_dynamo, mock_logger):
        """Hits the outer 'except Exception' block and logs the error."""
        mock_dynamo.describe_table.side_effect = RuntimeError("Connection Lost")

        with pytest.raises(RuntimeError, match="Connection Lost"):
            create_tables()

        mock_logger.error.assert_called_with("Unexpected error: Connection Lost")


class TestTableConfiguration:
    def test_verifies_gsi_and_capacity_schema(self, mock_dynamo, mock_env):
        """Ensures the complex dictionary for create_table is executed correctly."""
        mock_dynamo.describe_table.side_effect = ClientError(
            {"Error": {"Code": "ResourceNotFoundException"}}, "DescribeTable"
        )

        create_tables()

        _, kwargs = mock_dynamo.create_table.call_args
        assert len(kwargs["GlobalSecondaryIndexes"]) == 4
        assert kwargs["ProvisionedThroughput"]["ReadCapacityUnits"] == 25
        # Verify specific GSI projection (e.g., GSI2 uses INCLUDE)
        gsi2 = next(
            g for g in kwargs["GlobalSecondaryIndexes"] if g["IndexName"] == "GSI2"
        )
        assert gsi2["Projection"]["ProjectionType"] == "INCLUDE"
