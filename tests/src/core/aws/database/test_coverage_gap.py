import pytest
from unittest.mock import patch
from src.core.aws.database.models import (
    _validate_state,
    get_dynamodb_resource,
    get_dynamodb_client,
    ShopMetadata,
)


class TestCoverageGap:
    def test_validate_state_invalid(self):
        """Test _validate_state raises ValueError for invalid input."""
        with pytest.raises(ValueError, match="Invalid state value"):
            _validate_state("INVALID_STATE")

    @patch("src.core.aws.database.models.boto3")
    def test_get_dynamodb_resource(self, mock_boto3):
        """Test get_dynamodb_resource calls boto3.resource."""
        get_dynamodb_resource()
        mock_boto3.resource.assert_called_once()

    @patch("src.core.aws.database.models.os.getenv")
    @patch("src.core.aws.database.models.boto3")
    def test_get_dynamodb_client_with_endpoint(self, mock_boto3, mock_getenv):
        """Test get_dynamodb_client with endpoint_url env var."""
        mock_getenv.side_effect = (
            lambda key, default=None: "http://localhost:8000"
            if key == "DYNAMODB_ENDPOINT_URL"
            else default
        )

        get_dynamodb_client()

        args, kwargs = mock_boto3.client.call_args
        assert kwargs["endpoint_url"] == "http://localhost:8000"

    def test_shop_metadata_post_init_invalid_state(self):
        """Test ShopMetadata raises error if state fields are invalid."""
        with pytest.raises(ValueError, match="Invalid state value"):
            ShopMetadata(domain="test.com", last_crawled_end="INVALID")
