"""
Root pytest configuration.
Ensures environment variables are loaded BEFORE any module imports.
This must be in the root directory to load before tests are collected.
"""

import os
import pytest
from dotenv import load_dotenv
import sys
from pathlib import Path

# Load environment variables IMMEDIATELY before any other imports
load_dotenv()

# Ensure local packages like `aura_historia_backend_api_client` are importable
PROJECT_ROOT = Path(__file__).resolve().parent
CLIENT_PACKAGE = PROJECT_ROOT / "aura-historia-backend-api-client"
if CLIENT_PACKAGE.exists() and str(CLIENT_PACKAGE) not in sys.path:
    sys.path.insert(0, str(CLIENT_PACKAGE))

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

if not os.getenv("API_KEY"):
    os.environ["API_KEY"] = "test-key"

if not os.getenv("BACKEND_API_URL"):
    os.environ["BACKEND_API_URL"] = "http://localhost:8080"


@pytest.fixture(scope="session", autouse=True)
def setup_environment():
    """
    Verify environment variables are set for tests.
    """
    yield
