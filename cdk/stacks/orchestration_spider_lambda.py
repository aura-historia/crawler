"""CDK Stack for Spider Orchestration Lambda.

This stack creates:
- Lambda function from Docker image (manual invocation)
- SQS queue for spider tasks (or uses existing one)
- IAM permissions for Lambda to read DynamoDB and write to SQS
"""

from __future__ import annotations

import os
from aws_cdk import (
    Duration,
    Stack,
    aws_dynamodb as dynamodb,
    aws_iam as iam,
    aws_lambda as _lambda,
    aws_sqs as sqs,
)
from constructs import Construct
from dotenv import load_dotenv

load_dotenv()


class SpiderOrchestrationLambdaConstruct(Construct):
    """Construct for Spider Orchestration Lambda (manual invocation only)."""

    def __init__(
        self,
        scope: Construct,
        id_: str,
        table: dynamodb.ITable,
        spider_queue: sqs.IQueue,
    ) -> None:
        """Initialize the Spider Orchestration Lambda construct.

        Args:
            scope: CDK construct scope.
            id_: Construct ID.
            table: DynamoDB table for shop metadata.
            spider_queue: SQS queue for spider tasks.
        """
        super().__init__(scope, id_)

        # Lambda role with permissions
        self.lambda_role = self._build_lambda_role(table, spider_queue)

        # Lambda from Docker image
        self.lambda_func = self._build_lambda_function(
            self.lambda_role, table, spider_queue
        )

        # Grant DynamoDB query permissions (for orchestration)
        table.grant_read_data(self.lambda_func)

        # Grant SQS send permissions
        spider_queue.grant_send_messages(self.lambda_func)

    def _build_lambda_role(
        self, table: dynamodb.ITable, spider_queue: sqs.IQueue
    ) -> iam.Role:
        """Create IAM role for Lambda with necessary permissions.

        Args:
            table: DynamoDB table reference.
            spider_queue: SQS queue reference.

        Returns:
            IAM Role for Lambda.
        """
        role = iam.Role(
            self,
            "SpiderOrchestrationLambdaRole",
            assumed_by=iam.ServicePrincipal("lambda.amazonaws.com"),
            managed_policies=[
                iam.ManagedPolicy.from_aws_managed_policy_name(
                    "service-role/AWSLambdaBasicExecutionRole"
                )
            ],
        )

        # DynamoDB query permission
        role.add_to_policy(
            iam.PolicyStatement(
                actions=["dynamodb:Query"],
                resources=[
                    table.table_arn,
                    f"{table.table_arn}/index/*",
                ],
            )
        )

        # SQS send permission
        role.add_to_policy(
            iam.PolicyStatement(
                actions=["sqs:SendMessage", "sqs:SendMessageBatch"],
                resources=[spider_queue.queue_arn],
            )
        )

        return role

    def _build_lambda_function(
        self, role: iam.Role, table: dynamodb.ITable, spider_queue: sqs.IQueue
    ) -> _lambda.DockerImageFunction:
        """Build Lambda function from Docker image.

        Args:
            role: IAM role for Lambda.
            table: DynamoDB table reference.
            spider_queue: SQS queue reference.

        Returns:
            Lambda function.
        """
        lambda_dir = os.path.join(os.path.dirname(__file__), "../../")

        lambda_func = _lambda.DockerImageFunction(
            self,
            "SpiderOrchestrationLambda",
            code=_lambda.DockerImageCode.from_image_asset(
                directory=lambda_dir,
                file="src/lambdas/orchestration_spider/Dockerfile",
                exclude=[
                    ".github",
                    "cdk",
                    "docs",
                    "tests",
                    "data",
                    "docker",
                    "migrations",
                    ".venv",
                    "volume",
                    ".env",
                    ".env.example",
                    ".gitignore",
                    ".pre-commit-config.yaml",
                    "blitzfilter-data.iml",
                    "conftest.py",
                    "docker-compose.yml",
                    "LICENSE",
                    "README.md",
                    "main.py",
                    "package-lock.json",
                    "pytest.ini",
                    "requirements.txt",
                    "sonar-project.properties",
                    "try_product_spider.py",
                    "try_scraper.py",
                    ".idea",
                    "src/app",
                    "src/strategies",
                    "src/core/algorithms",
                    "src/core/llms",
                    "src/core/worker",
                    "src/core/shops_finder",
                    "src/core/classifier",
                    "src/core/scraper",
                    "src/lambdas/shop_registration",
                    "local_development",
                    "scripts",
                ],
            ),
            role=role,
            memory_size=512,
            timeout=Duration.minutes(5),
            architecture=_lambda.Architecture.X86_64,
            environment={
                "DYNAMODB_TABLE_NAME": table.table_name,
                "SQS_PRODUCT_SPIDER_QUEUE_URL": spider_queue.queue_url,
                "ORCHESTRATION_CUTOFF_DAYS": "2",
                "LOG_LEVEL": "INFO",
            },
        )

        return lambda_func


class SpiderOrchestrationStack(Stack):
    """CDK Stack for Spider Orchestration Lambda."""

    def __init__(
        self,
        scope: Construct,
        id: str,
        table: dynamodb.ITable,
        spider_queue: sqs.IQueue,
        **kwargs,
    ):
        """Initialize the Spider Orchestration Stack.

        Args:
            scope: CDK app scope.
            id: Stack ID.
            table: DynamoDB table for shop metadata.
            spider_queue: SQS queue for spider tasks.
            **kwargs: Additional stack arguments.
        """
        super().__init__(scope, id, **kwargs)

        SpiderOrchestrationLambdaConstruct(
            self,
            "SpiderOrchestrationLambdaConstruct",
            table=table,
            spider_queue=spider_queue,
        )
