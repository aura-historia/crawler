"""CDK Stack for S3 bucket used by the crawler system."""

from __future__ import annotations

from aws_cdk import RemovalPolicy, Stack, aws_s3 as s3
from constructs import Construct


class S3Stack(Stack):
    """Stack that creates S3 bucket for the crawler system."""

    def __init__(self, scope: Construct, construct_id: str, **kwargs):
        """Initialize the S3 Stack.

        Args:
            scope: CDK app scope.
            construct_id: Stack ID.
            **kwargs: Additional stack arguments.
        """
        super().__init__(scope, construct_id, **kwargs)

        # Create the boilerplate bucket
        self.boilerplate_bucket = s3.Bucket(
            self,
            "BoilerplateBucket",
            bucket_name="aura-historia-boilerplate",
            versioned=True,
            encryption=s3.BucketEncryption.S3_MANAGED,
            block_public_access=s3.BlockPublicAccess.BLOCK_ALL,
            removal_policy=RemovalPolicy.RETAIN,
            auto_delete_objects=False,
        )
