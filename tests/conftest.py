import os
import pytest
from dotenv import load_dotenv


@pytest.fixture(scope="session", autouse=True)
def setup_environment():
    """
    Set up environment variables for tests.
    This runs once per test session before any tests are collected.
    """
    # Load environment variables from .env file
    load_dotenv()

    # Set default values for required environment variables if not present
    if not os.getenv("AWS_REGION"):
        os.environ["AWS_REGION"] = "eu-central-1"

    if not os.getenv("DYNAMODB_TABLE_NAME"):
        os.environ["DYNAMODB_TABLE_NAME"] = "aura-historia-data"

    if not os.getenv("DYNAMODB_ENDPOINT_URL"):
        os.environ["DYNAMODB_ENDPOINT_URL"] = "http://localhost:8000"

    if not os.getenv("AWS_ACCESS_KEY_ID"):
        os.environ["AWS_ACCESS_KEY_ID"] = "fakeMyKeyId"

    if not os.getenv("AWS_SECRET_ACCESS_KEY"):
        os.environ["AWS_SECRET_ACCESS_KEY"] = "fakeSecretAccessKey"

    yield
