import os
from aws_cdk import (
    aws_iam as iam,
    aws_lambda as _lambda,
    aws_dynamodb as dynamodb,
    aws_lambda_event_sources as lambda_events,
    Stack,
)
from constructs import Construct
from dotenv import load_dotenv

load_dotenv()


class ShopRegistrationLambdaConstruct(Construct):
    def __init__(
        self, scope: Construct, id_: str, table: dynamodb.ITable, backend_api_url: str
    ) -> None:
        super().__init__(scope, id_)

        # Lambda role with permissions to read DynamoDB Streams
        self.lambda_role = self._build_lambda_role(table)

        # Lambda from Docker image
        self.lambda_func = self._build_lambda_ecr(
            self.lambda_role, backend_api_url, table
        )

        # Attach DynamoDB Stream as event source
        if table.table_stream_arn:
            self.lambda_func.add_event_source(
                lambda_events.DynamoEventSource(
                    table,
                    starting_position=_lambda.StartingPosition.LATEST,
                    batch_size=10,
                    bisect_batch_on_error=True,
                )
            )
        table.grant_read_data(self.lambda_func)

    def _build_lambda_role(self, table: dynamodb.ITable) -> iam.Role:
        role = iam.Role(
            self,
            "ShopRegistrationLambdaRole",
            assumed_by=iam.ServicePrincipal("lambda.amazonaws.com"),
            managed_policies=[
                iam.ManagedPolicy.from_aws_managed_policy_name(
                    "service-role/AWSLambdaBasicExecutionRole"
                )
            ],
        )

        # Allow Lambda to read from DynamoDB Streams
        if table.table_stream_arn:
            role.add_to_policy(
                iam.PolicyStatement(
                    actions=[
                        "dynamodb:GetRecords",
                        "dynamodb:GetShardIterator",
                        "dynamodb:DescribeStream",
                        "dynamodb:ListStreams",
                    ],
                    resources=[
                        table.table_stream_arn
                    ],  # Use the stream ARN from imported table
                )
            )
        return role

    def _build_lambda_ecr(
        self, role: iam.Role, backend_api_url: str, table: dynamodb.ITable
    ) -> _lambda.DockerImageFunction:
        lambda_dir = os.path.join(os.path.dirname(__file__), "../../")

        return _lambda.DockerImageFunction(
            self,
            "ShopRegistrationLambda",
            code=_lambda.DockerImageCode.from_image_asset(
                directory=lambda_dir,
                file="src/lambdas/shop_registration/Dockerfile",
                exclude=[
                    ".github",
                    "cdk",
                    "docs",
                    "tests",
                    "data",
                    "docker",
                    "docs",
                    "migrations",
                    ".venv",
                    "tests",
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
                    "local_development",
                    "src/lambdas/orchestration_spider",
                    "local_development",
                    "scripts",
                ],
            ),
            role=role,
            memory_size=256,
            architecture=_lambda.Architecture.X86_64,
            environment={
                "BACKEND_API_URL": backend_api_url,
                "DYNAMODB_TABLE_NAME": table.table_name,
            },
        )


class CrawlerStack(Stack):
    def __init__(self, scope: Construct, id: str, table: dynamodb.ITable, **kwargs):
        super().__init__(scope, id, **kwargs)

        backend_api_url = os.getenv("BACKEND_API_URL")

        ShopRegistrationLambdaConstruct(
            self,
            "ShopRegistrationLambdaConstruct",
            table=table,
            backend_api_url=backend_api_url,
        )
