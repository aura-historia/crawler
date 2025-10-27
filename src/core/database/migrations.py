import logging
import os
import sys

from dotenv import load_dotenv
from botocore.exceptions import ClientError
from src.core.database.models import get_dynamodb_client

logger = logging.getLogger(__name__)
logging.basicConfig(level=logging.INFO)

load_dotenv()


def create_tables():
    """
    Create the DynamoDB table required for the application (Single-Table Design).
    This is an idempotent operation.

    Table structure:
    - PK: domain (e.g., 'example.com')
    - SK: Two types:
        1. 'META#' - Shop metadata
           Attributes: domain, standards_used (list)
        2. 'URL#<full_url>' - Individual URL data
           Attributes: standards_used, type (category/product/etc),
                      is_product (bool), hash (status+price), url

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
                {"AttributeName": "PK", "KeyType": "HASH"},
                {"AttributeName": "SK", "KeyType": "RANGE"},
            ],
            AttributeDefinitions=[
                {"AttributeName": "PK", "AttributeType": "S"},
                {"AttributeName": "SK", "AttributeType": "S"},
            ],
            BillingMode="PROVISIONED",
            ProvisionedThroughput={"ReadCapacityUnits": 5, "WriteCapacityUnits": 5},
        )

        # Wait for table to be created
        waiter = dynamodb.get_waiter("table_exists")
        waiter.wait(TableName=table_name)

        logger.info(f"Table '{table_name}' created successfully.")

    except Exception as ex:
        logger.error(f"Unexpected error: {ex}")
        raise


if __name__ == "__main__":
    print("Creating DynamoDB table...")
    try:
        create_tables()
    except Exception as e:
        logger.error(f"Something went wrong while connecting to DynamoDB: {e}")
        sys.exit(1)
    print("Done.")
