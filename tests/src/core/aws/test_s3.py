import pytest
from unittest.mock import patch, MagicMock
from botocore.exceptions import ClientError
from src.core.aws.s3 import S3Operations


class TestS3Operations:
    @patch("src.core.aws.s3.boto3.client")
    def test_init_sets_bucket_name(self, mock_boto_client):
        """Test that S3Operations initializes with correct bucket name."""
        mock_client = MagicMock()
        mock_boto_client.return_value = mock_client

        s3_ops = S3Operations(bucket_name="test-bucket")

        assert s3_ops.bucket_name == "test-bucket"

    @patch("src.core.aws.s3.boto3.client")
    def test_init_uses_env_bucket_name(self, mock_boto_client):
        """Test that S3Operations falls back to environment variable."""
        mock_client = MagicMock()
        mock_boto_client.return_value = mock_client

        with patch.dict("os.environ", {"BOILERPLATE_S3_BUCKET": "env-bucket"}):
            s3_ops = S3Operations()
            assert s3_ops.bucket_name == "env-bucket"

    @patch("src.core.aws.s3.boto3.client")
    def test_upload_json_success(self, mock_boto_client):
        """Test successful JSON upload to S3."""
        mock_client = MagicMock()
        mock_boto_client.return_value = mock_client

        s3_ops = S3Operations(bucket_name="test-bucket")
        test_data = {"blocks": ["block1", "block2"], "metadata": {"version": 1}}

        s3_ops.upload_json("test/file.json", test_data)

        # Verify put_object was called
        assert mock_client.put_object.called
        call_kwargs = mock_client.put_object.call_args[1]
        assert call_kwargs["Bucket"] == "test-bucket"
        assert call_kwargs["Key"] == "test/file.json"
        assert call_kwargs["ContentType"] == "application/json"

    @patch("src.core.aws.s3.boto3.client")
    def test_upload_json_handles_error(self, mock_boto_client):
        """Test that upload_json raises error on failure."""
        mock_client = MagicMock()
        mock_boto_client.return_value = mock_client

        error_response = {"Error": {"Code": "AccessDenied", "Message": "Access Denied"}}
        mock_client.put_object.side_effect = ClientError(error_response, "PutObject")

        s3_ops = S3Operations(bucket_name="test-bucket")

        with pytest.raises(ClientError):
            s3_ops.upload_json("test/file.json", {})

    @patch("src.core.aws.s3.boto3.client")
    def test_download_json_success(self, mock_boto_client):
        """Test successful JSON download from S3."""
        mock_client = MagicMock()
        mock_boto_client.return_value = mock_client

        # Mock S3 response
        test_data = {"blocks": ["block1"], "updated_at": "2024-01-01"}
        mock_response = {
            "Body": MagicMock(
                read=MagicMock(
                    return_value=b'{"blocks": ["block1"], "updated_at": "2024-01-01"}'
                )
            )
        }
        mock_client.get_object.return_value = mock_response

        s3_ops = S3Operations(bucket_name="test-bucket")
        result = s3_ops.download_json("test/file.json")

        assert result == test_data
        assert mock_client.get_object.called

    @patch("src.core.aws.s3.boto3.client")
    def test_download_json_returns_none_for_missing_key(self, mock_boto_client):
        """Test that download_json returns None for missing key."""
        mock_client = MagicMock()
        mock_boto_client.return_value = mock_client

        error_response = {"Error": {"Code": "NoSuchKey"}}
        mock_client.get_object.side_effect = ClientError(error_response, "GetObject")

        s3_ops = S3Operations(bucket_name="test-bucket")
        result = s3_ops.download_json("missing/file.json")

        assert result is None

    @patch("src.core.aws.s3.boto3.client")
    def test_download_json_handles_invalid_json(self, mock_boto_client):
        """Test that download_json handles invalid JSON gracefully."""
        mock_client = MagicMock()
        mock_boto_client.return_value = mock_client

        # Mock response with invalid JSON
        mock_response = {
            "Body": MagicMock(read=MagicMock(return_value=b"not valid json"))
        }
        mock_client.get_object.return_value = mock_response

        s3_ops = S3Operations(bucket_name="test-bucket")
        result = s3_ops.download_json("test/file.json")

        assert result is None

    @patch("src.core.aws.s3.boto3.client")
    def test_list_objects_returns_keys(self, mock_boto_client):
        """Test that list_objects returns object keys."""
        mock_client = MagicMock()
        mock_boto_client.return_value = mock_client

        # Mock paginator
        mock_paginator = MagicMock()
        mock_client.get_paginator.return_value = mock_paginator

        # Mock pages with objects
        mock_pages = [
            {
                "Contents": [
                    {"Key": "boilerplate/shop1.json"},
                    {"Key": "boilerplate/shop2.json"},
                ]
            },
            {"Contents": [{"Key": "boilerplate/shop3.json"}]},
        ]
        mock_paginator.paginate.return_value = mock_pages

        s3_ops = S3Operations(bucket_name="test-bucket")
        keys = s3_ops.list_objects(prefix="boilerplate/")

        assert len(keys) == 3
        assert "boilerplate/shop1.json" in keys
        assert "boilerplate/shop3.json" in keys

    @patch("src.core.aws.s3.boto3.client")
    def test_list_objects_returns_empty_for_no_contents(self, mock_boto_client):
        """Test that list_objects handles empty results."""
        mock_client = MagicMock()
        mock_boto_client.return_value = mock_client

        mock_paginator = MagicMock()
        mock_client.get_paginator.return_value = mock_paginator

        # No contents in response
        mock_pages = [{}]
        mock_paginator.paginate.return_value = mock_pages

        s3_ops = S3Operations(bucket_name="test-bucket")
        keys = s3_ops.list_objects(prefix="empty/")

        assert keys == []

    @patch("src.core.aws.s3.boto3.client")
    def test_list_objects_handles_error(self, mock_boto_client):
        """Test that list_objects handles errors gracefully."""
        mock_client = MagicMock()
        mock_boto_client.return_value = mock_client

        mock_paginator = MagicMock()
        mock_client.get_paginator.return_value = mock_paginator

        error_response = {"Error": {"Code": "AccessDenied"}}
        mock_paginator.paginate.side_effect = ClientError(error_response, "ListObjects")

        s3_ops = S3Operations(bucket_name="test-bucket")
        keys = s3_ops.list_objects()

        assert keys == []
