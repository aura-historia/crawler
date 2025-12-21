import pytest
import os
from testcontainers.localstack import LocalStackContainer
from src.core.aws.database.migrations import create_tables
from src.core.aws.database.operations import DynamoDBOperations


@pytest.fixture(scope="session")
def dynamodb_setup():
    """Start LocalStack for eu-central-1 with the specific table name."""
    os.environ["TESTCONTAINERS_RYUK_DISABLED"] = "true"

    with LocalStackContainer("localstack/localstack:3.8.1").with_services(
        "dynamodb"
    ) as ls:
        endpoint_url = ls.get_url()

        os.environ["DYNAMODB_ENDPOINT_URL"] = endpoint_url
        os.environ["DYNAMODB_TABLE_NAME"] = "aura-historia-data"
        os.environ["AWS_REGION"] = "eu-central-1"
        os.environ["AWS_DEFAULT_REGION"] = "eu-central-1"
        os.environ["AWS_ACCESS_KEY_ID"] = "test"
        os.environ["AWS_SECRET_ACCESS_KEY"] = "test"

        create_tables()

        yield DynamoDBOperations()


@pytest.fixture(scope="function", autouse=True)
def cleanup_table(dynamodb_setup):
    client = dynamodb_setup.client
    table_name = os.environ["DYNAMODB_TABLE_NAME"]

    paginator = client.get_paginator("scan")
    for page in paginator.paginate(TableName=table_name, ProjectionExpression="pk, sk"):
        items = page.get("Items", [])
        if not items:
            continue

        for i in range(0, len(items), 25):
            batch = items[i : i + 25]
            requests = [
                {"DeleteRequest": {"Key": {"pk": item["pk"], "sk": item["sk"]}}}
                for item in batch
            ]
            client.batch_write_item(RequestItems={table_name: requests})
