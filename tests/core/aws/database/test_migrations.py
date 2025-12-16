import pytest
from botocore.exceptions import ClientError
from unittest.mock import Mock, patch

from src.core.aws.database.migrations import create_tables


class TestCreateTables:
    """Tests for create_tables function."""

    @patch("src.core.aws.database.migrations.get_dynamodb_client")
    @patch("src.core.aws.database.migrations.os.getenv")
    def test_creates_table_when_not_exists(self, mock_getenv, mock_get_client):
        """Test creating tables when table doesn't exist."""
        mock_getenv.return_value = "test-table"
        mock_client = Mock()
        mock_get_client.return_value = mock_client

        mock_client.describe_table.side_effect = ClientError(
            {"Error": {"Code": "ResourceNotFoundException"}}, "DescribeTable"
        )
        mock_client.create_table.return_value = {}

        mock_waiter = Mock()
        mock_client.get_waiter.return_value = mock_waiter

        create_tables()

        mock_client.describe_table.assert_called_once()
        mock_client.create_table.assert_called_once()
        mock_waiter.wait.assert_called_once()

    @patch("src.core.aws.database.migrations.get_dynamodb_client")
    @patch("src.core.aws.database.migrations.os.getenv")
    def test_skips_creation_when_table_exists(self, mock_getenv, mock_get_client):
        """Test that table creation is skipped when table already exists."""
        mock_getenv.return_value = "existing-table"
        mock_client = Mock()
        mock_get_client.return_value = mock_client

        mock_client.describe_table.return_value = {
            "Table": {"TableName": "existing-table"}
        }

        create_tables()

        mock_client.describe_table.assert_called_once()
        mock_client.create_table.assert_not_called()

    @patch("src.core.aws.database.migrations.get_dynamodb_client")
    @patch("src.core.aws.database.migrations.os.getenv")
    def test_handles_table_creation_error(self, mock_getenv, mock_get_client):
        """Test error handling when table creation fails."""
        mock_getenv.return_value = "test-table"
        mock_client = Mock()
        mock_get_client.return_value = mock_client

        mock_client.describe_table.side_effect = ClientError(
            {"Error": {"Code": "ResourceNotFoundException"}}, "DescribeTable"
        )
        mock_client.create_table.side_effect = ClientError(
            {"Error": {"Code": "InternalError", "Message": "Table creation failed"}},
            "CreateTable",
        )

        with pytest.raises(ClientError):
            create_tables()

    @patch("src.core.aws.database.migrations.get_dynamodb_client")
    @patch("src.core.aws.database.migrations.os.getenv")
    def test_handles_describe_table_error(self, mock_getenv, mock_get_client):
        """Test error handling when describe_table fails with non-NotFound error."""
        mock_getenv.return_value = "test-table"
        mock_client = Mock()
        mock_get_client.return_value = mock_client

        mock_client.describe_table.side_effect = ClientError(
            {"Error": {"Code": "AccessDenied", "Message": "Access denied"}},
            "DescribeTable",
        )

        with pytest.raises(ClientError):
            create_tables()

    @patch("src.core.aws.database.migrations.get_dynamodb_client")
    @patch("src.core.aws.database.migrations.os.getenv")
    def test_idempotent_operation(self, mock_getenv, mock_get_client):
        """Test that create_tables is idempotent and can be called multiple times safely."""
        mock_getenv.return_value = "test-table"
        mock_client = Mock()
        mock_get_client.return_value = mock_client
        mock_client.describe_table.return_value = {"Table": {}}

        create_tables()
        create_tables()
        create_tables()

        assert mock_client.describe_table.call_count == 3
        mock_client.create_table.assert_not_called()


class TestCreateTablesConfiguration:
    """Tests for table configuration in create_tables."""

    @patch("src.core.aws.database.migrations.get_dynamodb_client")
    @patch("src.core.aws.database.migrations.os.getenv")
    def test_uses_correct_key_schema(self, mock_getenv, mock_get_client):
        """Test that create_table is called with correct key schema."""
        mock_getenv.return_value = "test-table"
        mock_client = Mock()
        mock_get_client.return_value = mock_client

        mock_client.describe_table.side_effect = ClientError(
            {"Error": {"Code": "ResourceNotFoundException"}}, "DescribeTable"
        )
        mock_client.create_table.return_value = {}
        mock_waiter = Mock()
        mock_client.get_waiter.return_value = mock_waiter

        create_tables()

        call_kwargs = mock_client.create_table.call_args[1]
        assert call_kwargs["KeySchema"] == [
            {"AttributeName": "pk", "KeyType": "HASH"},
            {"AttributeName": "sk", "KeyType": "RANGE"},
        ]

    @patch("src.core.aws.database.migrations.get_dynamodb_client")
    @patch("src.core.aws.database.migrations.os.getenv")
    def test_uses_correct_capacity_units(self, mock_getenv, mock_get_client):
        """Test that create_table is called with correct read/write capacity units."""
        mock_getenv.return_value = "test-table"
        mock_client = Mock()
        mock_get_client.return_value = mock_client

        mock_client.describe_table.side_effect = ClientError(
            {"Error": {"Code": "ResourceNotFoundException"}}, "DescribeTable"
        )
        mock_client.create_table.return_value = {}
        mock_waiter = Mock()
        mock_client.get_waiter.return_value = mock_waiter

        create_tables()

        call_kwargs = mock_client.create_table.call_args[1]
        assert call_kwargs["ProvisionedThroughput"] == {
            "ReadCapacityUnits": 25,
            "WriteCapacityUnits": 25,
        }

    @patch("src.core.aws.database.migrations.get_dynamodb_client")
    @patch("src.core.aws.database.migrations.os.getenv")
    def test_waits_for_table_creation(self, mock_getenv, mock_get_client):
        """Test that waiter is used to wait for table creation completion."""
        mock_getenv.return_value = "test-table"
        mock_client = Mock()
        mock_get_client.return_value = mock_client

        mock_client.describe_table.side_effect = ClientError(
            {"Error": {"Code": "ResourceNotFoundException"}}, "DescribeTable"
        )
        mock_client.create_table.return_value = {}
        mock_waiter = Mock()
        mock_client.get_waiter.return_value = mock_waiter

        create_tables()

        mock_client.get_waiter.assert_called_once_with("table_exists")
        mock_waiter.wait.assert_called_once_with(TableName="test-table")


class TestCreateTablesLogging:
    """Tests for logging behavior in create_tables."""

    @patch("src.core.aws.database.migrations.get_dynamodb_client")
    @patch("src.core.aws.database.migrations.os.getenv")
    @patch("src.core.aws.database.migrations.logger")
    def test_logs_success_message(self, mock_logger, mock_getenv, mock_get_client):
        """Test that success message is logged when table is created."""
        table_name = "my-test-table"
        mock_getenv.return_value = table_name
        mock_client = Mock()
        mock_get_client.return_value = mock_client

        mock_client.describe_table.side_effect = ClientError(
            {"Error": {"Code": "ResourceNotFoundException"}}, "DescribeTable"
        )
        mock_client.create_table.return_value = {}
        mock_waiter = Mock()
        mock_client.get_waiter.return_value = mock_waiter

        create_tables()

        mock_logger.info.assert_called_with(
            f"Table '{table_name}' created successfully."
        )

    @patch("src.core.aws.database.migrations.get_dynamodb_client")
    @patch("src.core.aws.database.migrations.os.getenv")
    @patch("src.core.aws.database.migrations.logger")
    def test_logs_table_exists_message(self, mock_logger, mock_getenv, mock_get_client):
        """Test that appropriate message is logged when table already exists."""
        table_name = "existing-table"
        mock_getenv.return_value = table_name
        mock_client = Mock()
        mock_get_client.return_value = mock_client

        mock_client.describe_table.return_value = {"Table": {}}

        create_tables()

        mock_logger.info.assert_called_with(f"Table '{table_name}' already exists.")

    @patch("src.core.aws.database.migrations.get_dynamodb_client")
    @patch("src.core.aws.database.migrations.os.getenv")
    @patch("src.core.aws.database.migrations.logger")
    def test_logs_unexpected_error(self, mock_logger, mock_getenv, mock_get_client):
        """Test that unexpected errors are logged."""
        mock_getenv.return_value = "test-table"
        error_msg = "Network timeout"
        mock_get_client.side_effect = Exception(error_msg)

        with pytest.raises(Exception):
            create_tables()

        mock_logger.error.assert_called_once()
