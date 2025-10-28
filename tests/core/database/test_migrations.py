import pytest
from unittest.mock import patch, Mock
from botocore.exceptions import ClientError

from src.core.database.migrations import create_tables


class TestCreateTables:
    """Test create_tables function."""

    @patch("src.core.database.migrations.get_dynamodb_client")
    @patch("src.core.database.migrations.os.getenv")
    def test_create_tables_when_table_does_not_exist(
        self, mock_getenv, mock_get_client
    ):
        """Test creating tables when table doesn't exist."""
        # Arrange
        mock_getenv.return_value = "test-table"
        mock_client = Mock()
        mock_get_client.return_value = mock_client

        # Simulate table doesn't exist
        mock_client.describe_table.side_effect = ClientError(
            {"Error": {"Code": "ResourceNotFoundException"}}, "DescribeTable"
        )
        mock_client.create_table.return_value = {}

        # Mock the waiter
        mock_waiter = Mock()
        mock_client.get_waiter.return_value = mock_waiter

        # Act
        create_tables()

        # Assert
        mock_client.describe_table.assert_called_once()
        mock_client.create_table.assert_called_once()
        mock_waiter.wait.assert_called_once()

    @patch("src.core.database.migrations.get_dynamodb_client")
    @patch("src.core.database.migrations.os.getenv")
    def test_create_tables_when_table_already_exists(
        self, mock_getenv, mock_get_client
    ):
        """Test creating tables when table already exists."""
        # Arrange
        mock_getenv.return_value = "existing-table"
        mock_client = Mock()
        mock_get_client.return_value = mock_client

        # Simulate table exists (describe_table succeeds)
        mock_client.describe_table.return_value = {
            "Table": {"TableName": "existing-table"}
        }

        # Act
        create_tables()

        # Assert
        mock_client.describe_table.assert_called_once()
        mock_client.create_table.assert_not_called()

    @patch("src.core.database.migrations.get_dynamodb_client")
    @patch("src.core.database.migrations.os.getenv")
    def test_create_tables_handles_creation_error(self, mock_getenv, mock_get_client):
        """Test error handling when table creation fails."""
        # Arrange
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

        # Act & Assert
        with pytest.raises(ClientError):
            create_tables()

    @patch("src.core.database.migrations.get_dynamodb_client")
    @patch("src.core.database.migrations.os.getenv")
    def test_create_tables_handles_describe_error(self, mock_getenv, mock_get_client):
        """Test error handling when describe_table fails with non-NotFound error."""
        # Arrange
        mock_getenv.return_value = "test-table"
        mock_client = Mock()
        mock_get_client.return_value = mock_client

        # Simulate different error (not ResourceNotFoundException)
        mock_client.describe_table.side_effect = ClientError(
            {"Error": {"Code": "AccessDenied", "Message": "Access denied"}},
            "DescribeTable",
        )

        # Act & Assert
        with pytest.raises(ClientError):
            create_tables()

    @patch("src.core.database.migrations.get_dynamodb_client")
    @patch("src.core.database.migrations.os.getenv")
    @patch("src.core.database.migrations.logger")
    def test_create_tables_logs_success_message(
        self, mock_logger, mock_getenv, mock_get_client
    ):
        """Test that success message is logged when table is created."""
        # Arrange
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

        # Act
        create_tables()

        # Assert
        mock_logger.info.assert_called_with(
            f"Table '{table_name}' created successfully."
        )

    @patch("src.core.database.migrations.get_dynamodb_client")
    @patch("src.core.database.migrations.os.getenv")
    @patch("src.core.database.migrations.logger")
    def test_create_tables_logs_already_exists_message(
        self, mock_logger, mock_getenv, mock_get_client
    ):
        """Test that appropriate message is logged when table already exists."""
        # Arrange
        table_name = "existing-table"
        mock_getenv.return_value = table_name
        mock_client = Mock()
        mock_get_client.return_value = mock_client

        mock_client.describe_table.return_value = {"Table": {}}

        # Act
        create_tables()

        # Assert
        mock_logger.info.assert_called_with(f"Table '{table_name}' already exists.")

    @patch("src.core.database.migrations.get_dynamodb_client")
    @patch("src.core.database.migrations.os.getenv")
    @patch("src.core.database.migrations.logger")
    def test_create_tables_logs_unexpected_error(
        self, mock_logger, mock_getenv, mock_get_client
    ):
        """Test that unexpected errors are logged."""
        # Arrange
        mock_getenv.return_value = "test-table"
        error_msg = "Network timeout"
        mock_get_client.side_effect = Exception(error_msg)

        # Act & Assert
        with pytest.raises(Exception):
            create_tables()

        # Check that error was logged
        mock_logger.error.assert_called_once()

    @patch("src.core.database.migrations.get_dynamodb_client")
    @patch("src.core.database.migrations.os.getenv")
    def test_create_tables_idempotent(self, mock_getenv, mock_get_client):
        """Test that create_tables is idempotent (can be called multiple times safely)."""
        # Arrange
        mock_getenv.return_value = "test-table"
        mock_client = Mock()
        mock_get_client.return_value = mock_client
        mock_client.describe_table.return_value = {"Table": {}}

        # Act - call multiple times
        create_tables()
        create_tables()
        create_tables()

        # Assert - should check existence each time but not create
        assert mock_client.describe_table.call_count == 3
        mock_client.create_table.assert_not_called()

    @patch("src.core.database.migrations.get_dynamodb_client")
    @patch("src.core.database.migrations.os.getenv")
    def test_create_tables_uses_correct_key_schema(self, mock_getenv, mock_get_client):
        """Test that create_table is called with correct key schema."""
        # Arrange
        mock_getenv.return_value = "test-table"
        mock_client = Mock()
        mock_get_client.return_value = mock_client

        mock_client.describe_table.side_effect = ClientError(
            {"Error": {"Code": "ResourceNotFoundException"}}, "DescribeTable"
        )
        mock_client.create_table.return_value = {}
        mock_waiter = Mock()
        mock_client.get_waiter.return_value = mock_waiter

        # Act
        create_tables()

        # Assert - verify key schema
        call_kwargs = mock_client.create_table.call_args[1]
        assert call_kwargs["KeySchema"] == [
            {"AttributeName": "PK", "KeyType": "HASH"},
            {"AttributeName": "SK", "KeyType": "RANGE"},
        ]

    @patch("src.core.database.migrations.get_dynamodb_client")
    @patch("src.core.database.migrations.os.getenv")
    def test_create_tables_uses_correct_capacity_units(
        self, mock_getenv, mock_get_client
    ):
        """Test that create_table is called with correct read/write capacity units."""
        # Arrange
        mock_getenv.return_value = "test-table"
        mock_client = Mock()
        mock_get_client.return_value = mock_client

        mock_client.describe_table.side_effect = ClientError(
            {"Error": {"Code": "ResourceNotFoundException"}}, "DescribeTable"
        )
        mock_client.create_table.return_value = {}
        mock_waiter = Mock()
        mock_client.get_waiter.return_value = mock_waiter

        # Act
        create_tables()

        # Assert - verify provisioned throughput
        call_kwargs = mock_client.create_table.call_args[1]
        assert call_kwargs["ProvisionedThroughput"] == {
            "ReadCapacityUnits": 25,
            "WriteCapacityUnits": 25,
        }

    @patch("src.core.database.migrations.get_dynamodb_client")
    @patch("src.core.database.migrations.os.getenv")
    def test_create_tables_waits_for_table_creation(self, mock_getenv, mock_get_client):
        """Test that waiter is used to wait for table creation."""
        # Arrange
        mock_getenv.return_value = "test-table"
        mock_client = Mock()
        mock_get_client.return_value = mock_client

        mock_client.describe_table.side_effect = ClientError(
            {"Error": {"Code": "ResourceNotFoundException"}}, "DescribeTable"
        )
        mock_client.create_table.return_value = {}
        mock_waiter = Mock()
        mock_client.get_waiter.return_value = mock_waiter

        # Act
        create_tables()

        # Assert - verify waiter was used
        mock_client.get_waiter.assert_called_once_with("table_exists")
        mock_waiter.wait.assert_called_once_with(TableName="test-table")


class TestFunctionDocumentation:
    """Test function documentation and design principles."""

    def test_function_has_proper_documentation(self):
        """Test that create_tables has comprehensive documentation."""
        doc = create_tables.__doc__
        assert doc is not None
        assert len(doc) > 0

    def test_documentation_mentions_single_table_design(self):
        """Test that documentation mentions Single-Table Design."""
        doc = create_tables.__doc__
        assert "Single-Table Design" in doc

    def test_documentation_describes_pk_structure(self):
        """Test that documentation describes PK structure."""
        doc = create_tables.__doc__
        assert "PK:" in doc or "PK " in doc
        assert "domain" in doc

    def test_documentation_describes_sk_structure(self):
        """Test that documentation describes SK structure."""
        doc = create_tables.__doc__
        assert "SK:" in doc or "SK " in doc
        assert "META#" in doc
        assert "URL#" in doc

    def test_documentation_describes_operations(self):
        """Test that documentation describes supported operations."""
        doc = create_tables.__doc__
        assert "GetItem" in doc or "Query" in doc
        assert "BatchWriteItem" in doc or "Insert" in doc

    def test_documentation_mentions_idempotency(self):
        """Test that documentation mentions idempotent behavior."""
        doc = create_tables.__doc__
        assert "idempotent" in doc.lower()
