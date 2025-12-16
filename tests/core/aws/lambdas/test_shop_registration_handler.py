from unittest.mock import Mock, patch

import pytest
import requests

from src.core.aws.database.models import ShopMetadata, METADATA_SK
from src.core.aws.lambdas.shop_registration_handler import (
    get_core_domain_name,
    find_existing_shop,
    update_shop_domain,
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


class TestUpdateShopDomain:
    """Tests for update_shop_domain function."""

    @patch("src.core.aws.lambdas.shop_registration_handler.BACKEND_API_URL", None)
    def test_raises_error_when_backend_url_not_set(
        self, sample_shop_metadata, mock_session
    ):
        """Test that ValueError is raised when BACKEND_API_URL is not set."""

        with pytest.raises(ValueError, match="Backend API URL is not configured"):
            update_shop_domain(sample_shop_metadata, sample_shop_metadata, mock_session)

    @patch(
        "src.core.aws.lambdas.shop_registration_handler.BACKEND_API_URL",
        "https://api.example.com",
    )
    def test_skips_update_when_domain_unchanged(
        self, sample_shop_metadata, mock_session
    ):
        """Test that update is skipped when domain hasn't changed."""
        update_shop_domain(sample_shop_metadata, sample_shop_metadata, mock_session)

        mock_session.patch.assert_not_called()

    @patch(
        "src.core.aws.lambdas.shop_registration_handler.BACKEND_API_URL",
        "https://api.example.com",
    )
    @patch("src.core.aws.lambdas.shop_registration_handler.db_operations")
    def test_updates_domain_successfully(self, mock_db_ops, mock_session):
        """Test successful domain update."""
        old_shop = ShopMetadata(domain="shop.com")
        new_shop = ShopMetadata(domain="shop.de")

        all_shops = [
            ShopMetadata(domain="shop.de"),
            ShopMetadata(domain="shop.fr"),
        ]
        mock_db_ops.find_all_domains_by_core_domain_name.return_value = all_shops

        mock_response = Mock()
        mock_response.raise_for_status = Mock()
        mock_session.patch.return_value = mock_response

        update_shop_domain(old_shop, new_shop, mock_session)

        expected_url = "https://api.example.com/shops/shop.com"
        expected_payload = {"domains": ["shop.de", "shop.fr"]}
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
    @patch("src.core.aws.lambdas.shop_registration_handler.db_operations")
    def test_handles_no_domains_found(self, mock_db_ops, mock_session):
        """Test handling when no domains are found for the core domain name."""
        old_shop = ShopMetadata(domain="shop.com")
        new_shop = ShopMetadata(domain="shop.de")

        mock_db_ops.find_all_domains_by_core_domain_name.return_value = []

        update_shop_domain(old_shop, new_shop, mock_session)

        mock_session.patch.assert_not_called()

    @patch(
        "src.core.aws.lambdas.shop_registration_handler.BACKEND_API_URL",
        "https://api.example.com",
    )
    @patch("src.core.aws.lambdas.shop_registration_handler.db_operations")
    def test_handles_request_exception(self, mock_db_ops, mock_session):
        """Test that request exceptions are propagated."""
        old_shop = ShopMetadata(domain="shop.com")
        new_shop = ShopMetadata(domain="shop.de")

        all_shops = [ShopMetadata(domain="shop.de")]
        mock_db_ops.find_all_domains_by_core_domain_name.return_value = all_shops

        mock_session.patch.side_effect = requests.exceptions.RequestException(
            "API Error"
        )

        with pytest.raises(requests.exceptions.RequestException):
            update_shop_domain(old_shop, new_shop, mock_session)


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
    """Tests for the Lambda handler function."""

    @patch("src.core.aws.lambdas.shop_registration_handler.http_session")
    @patch("src.core.aws.lambdas.shop_registration_handler.register_or_update_shop")
    def test_processes_insert_event(
        self, mock_register, mock_http_session, sample_dynamodb_item
    ):
        """Test processing INSERT event."""
        event = {
            "Records": [
                {
                    "eventName": "INSERT",
                    "dynamodb": {
                        "NewImage": sample_dynamodb_item,
                    },
                }
            ]
        }

        result = handler(event, None)

        assert result["status"] == "success"
        assert result["processed_records"] == 1
        mock_register.assert_called_once()

    @patch("src.core.aws.lambdas.shop_registration_handler.http_session")
    @patch("src.core.aws.lambdas.shop_registration_handler.update_shop_domain")
    def test_processes_modify_event(
        self, mock_update, mock_http_session, sample_dynamodb_item
    ):
        """Test processing MODIFY event."""
        old_item = sample_dynamodb_item.copy()
        new_item = sample_dynamodb_item.copy()
        new_item["domain"] = {"S": "shop.de"}

        event = {
            "Records": [
                {
                    "eventName": "MODIFY",
                    "dynamodb": {
                        "OldImage": old_item,
                        "NewImage": new_item,
                    },
                }
            ]
        }

        result = handler(event, None)

        assert result["status"] == "success"
        assert result["processed_records"] == 1
        mock_update.assert_called_once()

    def test_skips_non_metadata_records(self):
        """Test that non-metadata records are skipped."""
        event = {
            "Records": [
                {
                    "eventName": "INSERT",
                    "dynamodb": {
                        "NewImage": {
                            "pk": {"S": "SHOP#shop.com"},
                            "sk": {"S": "URL#https://shop.com/product"},
                            "url": {"S": "https://shop.com/product"},
                        },
                    },
                }
            ]
        }

        result = handler(event, None)

        assert result["status"] == "success"
        assert result["processed_records"] == 0

    def test_skips_remove_events(self, sample_dynamodb_item):
        """Test that REMOVE events are skipped."""
        event = {
            "Records": [
                {
                    "eventName": "REMOVE",
                    "dynamodb": {
                        "OldImage": sample_dynamodb_item,
                    },
                }
            ]
        }

        result = handler(event, None)

        assert result["status"] == "success"
        assert result["processed_records"] == 0

    @patch("src.core.aws.lambdas.shop_registration_handler.http_session")
    @patch("src.core.aws.lambdas.shop_registration_handler.register_or_update_shop")
    def test_processes_multiple_records(
        self, mock_register, mock_http_session, sample_dynamodb_item
    ):
        """Test processing multiple records."""
        item2 = sample_dynamodb_item.copy()
        item2["domain"] = {"S": "shop2.com"}
        item2["pk"] = {"S": "SHOP#shop2.com"}

        event = {
            "Records": [
                {
                    "eventName": "INSERT",
                    "dynamodb": {
                        "NewImage": sample_dynamodb_item,
                    },
                },
                {
                    "eventName": "INSERT",
                    "dynamodb": {
                        "NewImage": item2,
                    },
                },
            ]
        }

        result = handler(event, None)

        assert result["status"] == "success"
        assert result["processed_records"] == 2
        assert mock_register.call_count == 2

    @patch("src.core.aws.lambdas.shop_registration_handler.http_session")
    @patch("src.core.aws.lambdas.shop_registration_handler.register_or_update_shop")
    def test_continues_on_error(
        self, mock_register, mock_http_session, sample_dynamodb_item
    ):
        """Test that processing continues even when one record fails."""
        mock_register.side_effect = [Exception("Error"), None]

        item2 = sample_dynamodb_item.copy()
        item2["domain"] = {"S": "shop2.com"}
        item2["pk"] = {"S": "SHOP#shop2.com"}

        event = {
            "Records": [
                {
                    "eventName": "INSERT",
                    "dynamodb": {
                        "NewImage": sample_dynamodb_item,
                    },
                },
                {
                    "eventName": "INSERT",
                    "dynamodb": {
                        "NewImage": item2,
                    },
                },
            ]
        }

        result = handler(event, None)

        assert result["status"] == "success"
        assert result["processed_records"] == 1
        assert mock_register.call_count == 2

    @patch("src.core.aws.lambdas.shop_registration_handler.http_session")
    @patch("src.core.aws.lambdas.shop_registration_handler.update_shop_domain")
    def test_handles_modify_event_without_old_image(
        self, mock_update, mock_http_session, sample_dynamodb_item
    ):
        """Test handling MODIFY event when OldImage is missing."""
        event = {
            "Records": [
                {
                    "eventName": "MODIFY",
                    "dynamodb": {
                        "NewImage": sample_dynamodb_item,
                    },
                }
            ]
        }

        result = handler(event, None)

        assert result["status"] == "success"
        # The record is still processed but update_shop_domain is not called
        assert result["processed_records"] == 1
        mock_update.assert_not_called()

    def test_handles_empty_records(self):
        """Test handling event with no records."""
        event = {"Records": []}

        result = handler(event, None)

        assert result["status"] == "success"
        assert result["processed_records"] == 0

    def test_handles_missing_records_key(self):
        """Test handling event without Records key."""
        event = {}

        result = handler(event, None)

        assert result["status"] == "success"
        assert result["processed_records"] == 0

    @patch("src.core.aws.lambdas.shop_registration_handler.http_session")
    @patch("src.core.aws.lambdas.shop_registration_handler.register_or_update_shop")
    def test_processes_mixed_event_types(
        self, mock_register, mock_http_session, sample_dynamodb_item
    ):
        """Test processing a mix of INSERT and other event types."""
        event = {
            "Records": [
                {
                    "eventName": "INSERT",
                    "dynamodb": {
                        "NewImage": sample_dynamodb_item,
                    },
                },
                {
                    "eventName": "REMOVE",
                    "dynamodb": {
                        "OldImage": sample_dynamodb_item,
                    },
                },
            ]
        }

        result = handler(event, None)

        assert result["status"] == "success"
        assert result["processed_records"] == 1
        mock_register.assert_called_once()


class TestIntegration:
    """Integration tests for end-to-end scenarios."""

    @patch(
        "src.core.aws.lambdas.shop_registration_handler.BACKEND_API_URL",
        "https://api.example.com",
    )
    @patch("src.core.aws.lambdas.shop_registration_handler.db_operations")
    @patch("src.core.aws.lambdas.shop_registration_handler.http_session")
    def test_full_new_shop_registration_flow(
        self, mock_http_session, mock_db_ops, sample_dynamodb_item
    ):
        """Test complete flow of registering a new shop."""
        mock_db_ops.find_all_domains_by_core_domain_name.return_value = []

        mock_response = Mock()
        mock_response.status_code = 201
        mock_response.raise_for_status = Mock()
        mock_http_session.post.return_value = mock_response

        event = {
            "Records": [
                {
                    "eventName": "INSERT",
                    "dynamodb": {
                        "NewImage": sample_dynamodb_item,
                    },
                }
            ]
        }

        result = handler(event, None)

        assert result["status"] == "success"
        assert result["processed_records"] == 1
        mock_http_session.post.assert_called_once()

    @patch(
        "src.core.aws.lambdas.shop_registration_handler.BACKEND_API_URL",
        "https://api.example.com",
    )
    @patch("src.core.aws.lambdas.shop_registration_handler.db_operations")
    @patch("src.core.aws.lambdas.shop_registration_handler.http_session")
    def test_full_add_domain_to_existing_shop_flow(
        self, mock_http_session, mock_db_ops, sample_dynamodb_item
    ):
        """Test complete flow of adding domain to existing shop."""

        existing_shops = [
            ShopMetadata(domain="shop.com"),
            ShopMetadata(domain="shop.de"),
        ]
        mock_db_ops.find_all_domains_by_core_domain_name.return_value = existing_shops

        mock_response = Mock()
        mock_response.raise_for_status = Mock()
        mock_http_session.patch.return_value = mock_response

        new_domain_item = sample_dynamodb_item.copy()
        new_domain_item["domain"] = {"S": "shop.fr"}

        event = {
            "Records": [
                {
                    "eventName": "INSERT",
                    "dynamodb": {
                        "NewImage": new_domain_item,
                    },
                }
            ]
        }

        result = handler(event, None)

        assert result["status"] == "success"
        assert result["processed_records"] == 1
        mock_http_session.patch.assert_called_once()

        call_args = mock_http_session.patch.call_args
        payload = call_args.kwargs["json"]
        assert "shop.fr" in payload["domains"]
        assert "shop.com" in payload["domains"]
        assert "shop.de" in payload["domains"]
