"""
Root pytest configuration.
Ensures environment variables are loaded BEFORE any module imports.
This must be in the root directory to load before tests are collected.
"""

import os
import pytest
from dotenv import load_dotenv

# Load environment variables IMMEDIATELY before any other imports
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


@pytest.fixture(scope="session", autouse=True)
def setup_environment():
    """
    Verify environment variables are set for tests.
    """
    yield
