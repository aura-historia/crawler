from unittest.mock import Mock, patch

import pytest
import requests

from src.core.aws.database.models import ShopMetadata, METADATA_SK
from src.core.aws.lambdas.shop_registration_handler import (
    get_core_domain_name,
    find_existing_shop,
    register_or_update_shop,
    handler,
)


@pytest.fixture
def mock_backend_url(monkeypatch):
    """Set up a mock BACKEND_API_URL."""
    url = "https://api.example.com"
    monkeypatch.setenv("BACKEND_API_URL", url)
    return url


@pytest.fixture
def mock_session():
    """Create a mock requests Session."""
    session = Mock(spec=requests.Session)
    return session


@pytest.fixture
def sample_shop_metadata():
    """Create a sample ShopMetadata object."""
    return ShopMetadata(
        domain="shop.com",
        standards_used=["json-ld", "microdata"],
        shop_country="DE",
    )


@pytest.fixture
def sample_dynamodb_item():
    """Create a sample DynamoDB item for testing."""
    return {
        "pk": {"S": "SHOP#shop.com"},
        "sk": {"S": METADATA_SK},
        "domain": {"S": "shop.com"},
        "standards_used": {"L": [{"S": "json-ld"}]},
        "core_domain_name": {"S": "shop"},
    }


class TestGetCoreDomainName:
    """Tests for get_core_domain_name function."""

    @pytest.mark.parametrize(
        "domain,expected",
        [
            ("example.com", "example"),
            ("shop.example.com", "example"),
            ("sub.shop.example.com", "example"),
            ("example.co.uk", "example"),
            ("shop.example.co.uk", "example"),
            ("example.de", "example"),
            ("my-shop.fr", "my-shop"),
            ("test123.com", "test123"),
        ],
    )
    def test_extracts_core_domain_correctly(self, domain, expected):
        """Test that core domain name is extracted correctly."""
        result = get_core_domain_name(domain)
        assert result == expected

    def test_handles_simple_domain(self):
        """Test extraction from simple domain."""
        assert get_core_domain_name("shop.com") == "shop"

    def test_handles_subdomain(self):
        """Test extraction from domain with subdomain."""
        assert get_core_domain_name("www.shop.com") == "shop"

    def test_handles_country_tld(self):
        """Test extraction from domain with country TLD."""
        assert get_core_domain_name("shop.co.uk") == "shop"


class TestFindExistingShop:
    """Tests for find_existing_shop function."""

    @patch("src.core.aws.lambdas.shop_registration_handler.db_operations")
    def test_returns_none_when_no_existing_shop(self, mock_db_ops):
        """Test that None is returned when no existing shop is found."""
        mock_db_ops.find_all_domains_by_core_domain_name.return_value = []

        result = find_existing_shop("newshop.com")

        assert result is None
        mock_db_ops.find_all_domains_by_core_domain_name.assert_called_once_with(
            "newshop", domain_to_exclude="newshop.com"
        )

    @patch("src.core.aws.lambdas.shop_registration_handler.db_operations")
    def test_returns_identifier_and_domains_when_shops_exist(self, mock_db_ops):
        """Test that identifier and domains are returned when shops exist."""
        existing_shops = [
            ShopMetadata(domain="shop.com"),
            ShopMetadata(domain="shop.de"),
        ]
        mock_db_ops.find_all_domains_by_core_domain_name.return_value = existing_shops

        result = find_existing_shop("shop.fr")

        assert result is not None
        identifier, all_domains = result
        assert identifier == "shop.com"
        assert all_domains == ["shop.com", "shop.de"]

    @patch("src.core.aws.lambdas.shop_registration_handler.db_operations")
    def test_uses_first_domain_as_identifier(self, mock_db_ops):
        """Test that the first existing domain is used as identifier."""
        existing_shops = [
            ShopMetadata(domain="shop.de"),
            ShopMetadata(domain="shop.com"),
            ShopMetadata(domain="shop.fr"),
        ]
        mock_db_ops.find_all_domains_by_core_domain_name.return_value = existing_shops

        result = find_existing_shop("shop.uk")

        identifier, _ = result
        assert identifier == "shop.de"


class TestRegisterOrUpdateShop:
    """Tests for register_or_update_shop function."""

    @patch("src.core.aws.lambdas.shop_registration_handler.BACKEND_API_URL", None)
    def test_raises_error_when_backend_url_not_set(
        self, sample_shop_metadata, mock_session
    ):
        """Test that ValueError is raised when BACKEND_API_URL is not set."""
        with pytest.raises(ValueError, match="Backend API URL is not configured"):
            register_or_update_shop(sample_shop_metadata, mock_session)

    @patch(
        "src.core.aws.lambdas.shop_registration_handler.BACKEND_API_URL",
        "https://api.example.com",
    )
    @patch("src.core.aws.lambdas.shop_registration_handler.find_existing_shop")
    def test_creates_new_shop_when_none_exists(self, mock_find, mock_session):
        """Test creating a new shop when no existing shop is found."""
        mock_find.return_value = None
        shop = ShopMetadata(domain="newshop.com")

        mock_response = Mock()
        mock_response.status_code = 201
        mock_response.raise_for_status = Mock()
        mock_session.post.return_value = mock_response

        register_or_update_shop(shop, mock_session)

        expected_url = "https://api.example.com/shops"
        expected_payload = {
            "name": "Newshop",
            "domains": ["newshop.com"],
        }
        mock_session.post.assert_called_once_with(
            expected_url,
            json=expected_payload,
            headers={"Content-Type": "application/json"},
            timeout=10,
        )

    @patch(
        "src.core.aws.lambdas.shop_registration_handler.BACKEND_API_URL",
        "https://api.example.com",
    )
    @patch("src.core.aws.lambdas.shop_registration_handler.find_existing_shop")
    def test_adds_domain_to_existing_shop(self, mock_find, mock_session):
        """Test adding a domain to an existing shop."""
        mock_find.return_value = ("shop.com", ["shop.com", "shop.de"])
        shop = ShopMetadata(domain="shop.fr")

        mock_response = Mock()
        mock_response.raise_for_status = Mock()
        mock_session.patch.return_value = mock_response

        register_or_update_shop(shop, mock_session)

        expected_url = "https://api.example.com/shops/shop.com"
        expected_payload = {"domains": ["shop.com", "shop.de", "shop.fr"]}
        mock_session.patch.assert_called_once_with(
            expected_url,
            json=expected_payload,
            headers={"Content-Type": "application/json"},
            timeout=10,
        )

    @patch(
        "src.core.aws.lambdas.shop_registration_handler.BACKEND_API_URL",
        "https://api.example.com",
    )
    @patch("src.core.aws.lambdas.shop_registration_handler.find_existing_shop")
    def test_handles_api_error_on_post(self, mock_find, mock_session):
        """Test handling API errors during shop creation."""
        mock_find.return_value = None
        shop = ShopMetadata(domain="newshop.com")

        mock_session.post.side_effect = requests.exceptions.RequestException(
            "API Error"
        )

        with pytest.raises(requests.exceptions.RequestException):
            register_or_update_shop(shop, mock_session)

    @patch(
        "src.core.aws.lambdas.shop_registration_handler.BACKEND_API_URL",
        "https://api.example.com",
    )
    @patch("src.core.aws.lambdas.shop_registration_handler.find_existing_shop")
    def test_handles_api_error_on_patch(self, mock_find, mock_session):
        """Test handling API errors during domain addition."""
        mock_find.return_value = ("shop.com", ["shop.com"])
        shop = ShopMetadata(domain="shop.fr")

        mock_session.patch.side_effect = requests.exceptions.RequestException(
            "API Error"
        )

        with pytest.raises(requests.exceptions.RequestException):
            register_or_update_shop(shop, mock_session)

    @patch(
        "src.core.aws.lambdas.shop_registration_handler.BACKEND_API_URL",
        "https://api.example.com",
    )
    @patch("src.core.aws.lambdas.shop_registration_handler.find_existing_shop")
    def test_capitalizes_shop_name_correctly(self, mock_find, mock_session):
        """Test that shop name is capitalized correctly."""
        mock_find.return_value = None
        shop = ShopMetadata(domain="my-great-shop.com")

        mock_response = Mock()
        mock_response.status_code = 201
        mock_response.raise_for_status = Mock()
        mock_session.post.return_value = mock_response

        register_or_update_shop(shop, mock_session)

        call_args = mock_session.post.call_args
        payload = call_args.kwargs["json"]
        assert payload["name"] == "My-great-shop"


class TestHandler:
    """Tests for the updated Lambda handler function using partial batch failures."""

    @patch("src.core.aws.lambdas.shop_registration_handler.http_session")
    @patch("src.core.aws.lambdas.shop_registration_handler.register_or_update_shop")
    def test_processes_insert_event_successfully(
        self, mock_register, mock_http_session, sample_dynamodb_item
    ):
        """Test processing INSERT event returns empty failures."""
        event = {
            "Records": [
                {
                    "eventID": "1",
                    "eventName": "INSERT",
                    "dynamodb": {
                        "NewImage": sample_dynamodb_item,
                        "SequenceNumber": "seq-123",
                    },
                }
            ]
        }

        result = handler(event, None)

        assert result == {"batchItemFailures": []}
        mock_register.assert_called_once()

    @patch("src.core.aws.lambdas.shop_registration_handler.http_session")
    @patch("src.core.aws.lambdas.shop_registration_handler.register_or_update_shop")
    def test_reports_failure_on_exception(
        self, mock_register, mock_http_session, sample_dynamodb_item
    ):
        """Test that a failing record returns its SequenceNumber."""
        mock_register.side_effect = Exception("API Down")

        event = {
            "Records": [
                {
                    "eventID": "err-1",
                    "eventName": "INSERT",
                    "dynamodb": {
                        "NewImage": sample_dynamodb_item,
                        "SequenceNumber": "fail-seq-001",
                    },
                }
            ]
        }

        result = handler(event, None)

        # Verify the failure is reported correctly for AWS
        assert result == {"batchItemFailures": [{"itemIdentifier": "fail-seq-001"}]}

    @patch("src.core.aws.lambdas.shop_registration_handler.http_session")
    @patch("src.core.aws.lambdas.shop_registration_handler.register_or_update_shop")
    def test_partial_batch_failure_mixed_results(
        self, mock_register, mock_http_session, sample_dynamodb_item
    ):
        """Test that only the failed record's sequence number is returned."""
        # First call fails, second succeeds
        mock_register.side_effect = [Exception("First record failed"), None]

        item2 = sample_dynamodb_item.copy()
        item2["domain"] = {"S": "success-shop.com"}

        event = {
            "Records": [
                {
                    "eventID": "1",
                    "eventName": "INSERT",
                    "dynamodb": {
                        "NewImage": sample_dynamodb_item,
                        "SequenceNumber": "fail-seq",
                    },
                },
                {
                    "eventID": "2",
                    "eventName": "INSERT",
                    "dynamodb": {"NewImage": item2, "SequenceNumber": "success-seq"},
                },
            ]
        }

        result = handler(event, None)

        # Only 'fail-seq' should be in the list
        assert result == {"batchItemFailures": [{"itemIdentifier": "fail-seq"}]}
        assert mock_register.call_count == 2

    def test_skips_modify_events(self, sample_dynamodb_item):
        """
        Test that MODIFY events are skipped (as per final code logic).
        Skipped records should not be marked as failures.
        """
        event = {
            "Records": [
                {
                    "eventName": "MODIFY",
                    "dynamodb": {
                        "NewImage": sample_dynamodb_item,
                        "SequenceNumber": "mod-seq",
                    },
                }
            ]
        }

        result = handler(event, None)
        assert result == {"batchItemFailures": []}

    def test_handles_missing_new_image(self):
        """Test handling record where NewImage is missing entirely."""
        event = {
            "Records": [
                {
                    "eventName": "INSERT",
                    "dynamodb": {
                        # NewImage key is missing
                        "SequenceNumber": "missing-img-seq"
                    },
                }
            ]
        }

        result = handler(event, None)
        assert result == {"batchItemFailures": []}

    def test_handles_empty_records(self):
        """Test handling event with no records returns empty failure list."""
        assert handler({"Records": []}, None) == {"batchItemFailures": []}
        assert handler({}, None) == {"batchItemFailures": []}
