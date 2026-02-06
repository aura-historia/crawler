import logging
import os
import json
from typing import Any, Optional
import boto3
from botocore.exceptions import ClientError
from dotenv import load_dotenv

logger = logging.getLogger(__name__)

load_dotenv(verbose=True)


def _get_s3_config() -> dict:
    """Build S3 configuration from environment variables."""
    config = {
        "region_name": os.getenv("AWS_REGION", "eu-central-1"),
    }

    # Use LocalStack endpoint if configured
    endpoint_url = os.getenv("S3_ENDPOINT_URL") or os.getenv("LOCALSTACK_ENDPOINT")
    if endpoint_url:
        config["endpoint_url"] = endpoint_url

    return config


class S3Operations:
    """Handles S3 operations for boilerplate storage."""

    def __init__(self, bucket_name: Optional[str] = None):
        self.config = _get_s3_config()
        self.client = boto3.client("s3", **self.config)
        self.bucket_name = bucket_name or os.getenv(
            "BOILERPLATE_S3_BUCKET", "aura-historia-crawler-markdown-boilerplate"
        )

    def upload_json(self, key: str, data: Any) -> None:
        """Upload data as JSON to S3."""
        try:
            self.client.put_object(
                Bucket=self.bucket_name,
                Key=key,
                Body=json.dumps(data, ensure_ascii=False),
                ContentType="application/json",
            )
            logger.info(f"Uploaded {key} to S3 bucket {self.bucket_name}")
        except ClientError as e:
            logger.error(f"Error uploading to S3: {e}")
            raise

    def download_json(self, key: str) -> Optional[Any]:
        """Download JSON data from S3."""
        try:
            response = self.client.get_object(Bucket=self.bucket_name, Key=key)
            return json.loads(response["Body"].read().decode("utf-8"))
        except ClientError as e:
            if e.response["Error"]["Code"] == "NoSuchKey":
                return None
            logger.error(f"Error downloading from S3: {e}")
            raise
        except Exception as e:
            logger.error(f"Error parsing JSON from S3 {key}: {e}")
            return None

    def list_objects(self, prefix: str = "") -> list[str]:
        """List object keys with a given prefix."""
        try:
            paginator = self.client.get_paginator("list_objects_v2")
            pages = paginator.paginate(Bucket=self.bucket_name, Prefix=prefix)
            keys = []
            for page in pages:
                if "Contents" in page:
                    keys.extend([obj["Key"] for obj in page["Contents"]])
            return keys
        except ClientError as e:
            logger.error(f"Error listing objects in S3: {e}")
            return []

    def ensure_bucket_exists(self) -> None:
        """Create the bucket if it doesn't exist."""
        try:
            self.client.head_bucket(Bucket=self.bucket_name)
        except ClientError as e:
            error_code = e.response["Error"]["Code"]
            if error_code == "404":
                logger.info(f"Creating S3 bucket: {self.bucket_name}")
                kwargs = {"Bucket": self.bucket_name}
                if self.config.get("region_name") != "us-east-1":
                    kwargs["CreateBucketConfiguration"] = {
                        "LocationConstraint": self.config["region_name"]
                    }
                self.client.create_bucket(**kwargs)
            else:
                logger.error(f"Error checking S3 bucket: {e}")
                raise


if __name__ == "__main__":
    s3 = S3Operations()
    s3.ensure_bucket_exists()
