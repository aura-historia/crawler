import logging
import os

from botocore.exceptions import ClientError
from dotenv import load_dotenv

from src.core.aws.database.models import get_dynamodb_client

logger = logging.getLogger(__name__)
logging.basicConfig(level=logging.INFO)
load_dotenv()


def create_tables():
    """
    Create the DynamoDB table required for the application (Single-Table Design).
    This is an idempotent operation.

    Table structure:
    - pk: 'SHOP#' + domain (e.g., 'SHOP#example.com')
    - sk: Two types:
        1. 'META#' - Shop metadata
           Attributes: domain, standards_used (BOOL), shop_name (optional), shop_country (COUNTRY#XX),
                      last_crawled_start/end, last_scraped_start/end (ISO 8601),
                      core_domain_name
        2. 'URL#<full_url>' - Individual URL data
           Attributes: type (category/product/etc), hash (status+price), url

    GSIs:
    - GSI1: Product type index (gsi1_pk=SHOP#domain, gsi1_sk=type)
    - GSI2: Country + crawled date (gsi2_pk=COUNTRY#XX, gsi2_sk=last_crawled_start)
    - GSI3: Country + scraped date (gsi3_pk=COUNTRY#XX, gsi3_sk=last_scraped_start)
    - GSI4: Core domain name (gsi4_pk=core_domain_name, gsi4_sk=domain)

    Operations:
    - Query: Use GetItem for direct access
    - Insert: Use BatchWriteItem for bulk operations
    """
    try:
        table_name = os.getenv("DYNAMODB_TABLE_NAME")
        dynamodb = get_dynamodb_client()

        # Check if table exists
        try:
            dynamodb.describe_table(TableName=table_name)
            logger.info(f"Table '{table_name}' already exists.")
            return
        except ClientError as e:
            if e.response["Error"]["Code"] != "ResourceNotFoundException":
                raise

        # Create table
        dynamodb.create_table(
            TableName=table_name,
            KeySchema=[
                {"AttributeName": "pk", "KeyType": "HASH"},
                {"AttributeName": "sk", "KeyType": "RANGE"},
            ],
            AttributeDefinitions=[
                {"AttributeName": "pk", "AttributeType": "S"},
                {"AttributeName": "sk", "AttributeType": "S"},
                {"AttributeName": "gsi1_pk", "AttributeType": "S"},
                {"AttributeName": "gsi1_sk", "AttributeType": "S"},
                {"AttributeName": "gsi2_pk", "AttributeType": "S"},
                {"AttributeName": "gsi2_sk", "AttributeType": "S"},
                {"AttributeName": "gsi3_pk", "AttributeType": "S"},
                {"AttributeName": "gsi3_sk", "AttributeType": "S"},
                {"AttributeName": "gsi4_pk", "AttributeType": "S"},
                {"AttributeName": "gsi4_sk", "AttributeType": "S"},
            ],
            GlobalSecondaryIndexes=[
                {
                    "IndexName": "GSI1",
                    "KeySchema": [
                        {"AttributeName": "gsi1_pk", "KeyType": "HASH"},
                        {"AttributeName": "gsi1_sk", "KeyType": "RANGE"},
                    ],
                    "Projection": {"ProjectionType": "ALL"},
                },
                {
                    "IndexName": "GSI2",
                    "KeySchema": [
                        {"AttributeName": "gsi2_pk", "KeyType": "HASH"},
                        {"AttributeName": "gsi2_sk", "KeyType": "RANGE"},
                    ],
                    "Projection": {
                        "ProjectionType": "INCLUDE",
                        "NonKeyAttributes": ["domain"],
                    },
                },
                {
                    "IndexName": "GSI3",
                    "KeySchema": [
                        {"AttributeName": "gsi3_pk", "KeyType": "HASH"},
                        {"AttributeName": "gsi3_sk", "KeyType": "RANGE"},
                    ],
                    "Projection": {
                        "ProjectionType": "INCLUDE",
                        "NonKeyAttributes": ["domain"],
                    },
                },
                {
                    "IndexName": "GSI4",
                    "KeySchema": [
                        {"AttributeName": "gsi4_pk", "KeyType": "HASH"},
                        {"AttributeName": "gsi4_sk", "KeyType": "RANGE"},
                    ],
                    "Projection": {"ProjectionType": "ALL"},
                },
            ],
            StreamSpecification={"StreamEnabled": True, "StreamViewType": "NEW_IMAGE"},
            BillingMode="PAY_PER_REQUEST",
        )

        # Wait for table to be created
        waiter = dynamodb.get_waiter("table_exists")
        waiter.wait(TableName=table_name)

        logger.info(f"Table '{table_name}' created successfully.")

    except Exception as ex:
        logger.error(f"Unexpected error: {ex}")
        raise
