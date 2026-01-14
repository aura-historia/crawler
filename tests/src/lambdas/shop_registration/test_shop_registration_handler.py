from unittest.mock import Mock, patch
import pytest
import requests

from src.core.aws.database.models import ShopMetadata, METADATA_SK
from src.lambdas.shop_registration.shop_registration_handler import (
    find_existing_shop,
    handler,
    register_or_update_shop,
    resilient_http_request_sync,
)


@pytest.fixture
def sample_shop_metadata():
    return ShopMetadata(
        domain="shop.com",
        shop_country="DE",
    )


@pytest.fixture
def sample_dynamodb_item():
    return {
        "pk": {"S": "SHOP#shop.com"},
        "sk": {"S": METADATA_SK},
        "domain": {"S": "shop.com"},
        "core_domain_name": {"S": "shop"},
    }


class TestFindExistingShop:
    @patch("src.lambdas.shop_registration.shop_registration_handler.db_operations")
    def test_returns_none_when_no_existing_shop(self, mock_db_ops):
        mock_db_ops.find_all_domains_by_core_domain_name.return_value = []

        result = find_existing_shop("newshop.com", "newshop")

        assert result is None
        mock_db_ops.find_all_domains_by_core_domain_name.assert_called_once_with(
            "newshop", domain_to_exclude="newshop.com"
        )

    @patch("src.lambdas.shop_registration.shop_registration_handler.db_operations")
    def test_returns_identifier_and_domains_when_shops_exist(self, mock_db_ops):
        existing_shops = [
            ShopMetadata(domain="shop.com"),
            ShopMetadata(domain="shop.de"),
        ]
        mock_db_ops.find_all_domains_by_core_domain_name.return_value = existing_shops

        result = find_existing_shop("shop.fr", "shop")

        assert result is not None
        identifier, all_domains = result
        assert identifier == "shop.com"
        assert all_domains == ["shop.com", "shop.de"]

    @patch("src.lambdas.shop_registration.shop_registration_handler.db_operations")
    def test_uses_first_domain_as_identifier(self, mock_db_ops):
        existing_shops = [
            ShopMetadata(domain="shop.de"),
            ShopMetadata(domain="shop.com"),
            ShopMetadata(domain="shop.fr"),
        ]
        mock_db_ops.find_all_domains_by_core_domain_name.return_value = existing_shops

        result = find_existing_shop("shop.uk", "shop")

        identifier, _ = result
        assert identifier == "shop.de"


class TestRegisterOrUpdateShop:
    @patch(
        "src.lambdas.shop_registration.shop_registration_handler.resilient_http_request_sync",
        new_callable=Mock,
    )
    @patch("src.lambdas.shop_registration.shop_registration_handler.find_existing_shop")
    @patch(
        "src.lambdas.shop_registration.shop_registration_handler.BACKEND_API_URL",
        "http://test-backend",
    )
    def test_register_new_shop(
        self, mock_find_existing, mock_resilient, sample_shop_metadata
    ):
        mock_find_existing.return_value = None
        session = Mock()

        register_or_update_shop(sample_shop_metadata, session)

        mock_resilient.assert_called_once()
        _, kwargs = mock_resilient.call_args
        assert kwargs["method"] == "POST"
        assert "name" in kwargs["json_data"]
        assert kwargs["json_data"]["domains"] == [sample_shop_metadata.domain]

    @patch(
        "src.lambdas.shop_registration.shop_registration_handler.resilient_http_request_sync",
        new_callable=Mock,
    )
    @patch("src.lambdas.shop_registration.shop_registration_handler.find_existing_shop")
    @patch(
        "src.lambdas.shop_registration.shop_registration_handler.BACKEND_API_URL",
        "http://test-backend",
    )
    def test_update_existing_shop(
        self, mock_find_existing, mock_resilient, sample_shop_metadata
    ):
        mock_find_existing.return_value = ("shop.com", ["shop.com", "shop.de"])
        session = Mock()

        register_or_update_shop(sample_shop_metadata, session)

        mock_resilient.assert_called_once()
        _, kwargs = mock_resilient.call_args
        assert kwargs["method"] == "PATCH"
        assert "domains" in kwargs["json_data"]
        assert sample_shop_metadata.domain in kwargs["json_data"]["domains"]

    def test_backend_api_url_missing(self, sample_shop_metadata):
        # Temporarily patch BACKEND_API_URL to None
        with patch(
            "src.lambdas.shop_registration.shop_registration_handler.BACKEND_API_URL",
            None,
        ):
            session = Mock()
            with pytest.raises(ValueError):
                register_or_update_shop(sample_shop_metadata, session)

    @patch(
        "src.lambdas.shop_registration.shop_registration_handler.resilient_http_request_sync",
        new_callable=Mock,
    )
    @patch("src.lambdas.shop_registration.shop_registration_handler.find_existing_shop")
    def test_handles_invalid_shop_metadata(self, mock_find_existing, mock_resilient):
        shop = ShopMetadata(domain="", shop_country="DE")
        session = Mock()
        with pytest.raises(Exception):
            register_or_update_shop(shop, session)


class TestHandler:
    @patch(
        "src.lambdas.shop_registration.shop_registration_handler.register_or_update_shop",
        new_callable=Mock,
    )
    def test_processes_insert_event_successfully(
        self, mock_register, sample_dynamodb_item
    ):
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
        mock_register.return_value = None
        result = handler(event, None)
        assert result == {"batchItemFailures": []}
        mock_register.assert_called_once()
        args, _ = mock_register.call_args
        shop_arg = args[0]
        assert hasattr(shop_arg, "core_domain_name")
        assert shop_arg.core_domain_name == "shop"

    @patch(
        "src.lambdas.shop_registration.shop_registration_handler.register_or_update_shop",
        new_callable=Mock,
    )
    def test_partial_batch_failure_mixed_results(
        self, mock_register, sample_dynamodb_item
    ):
        mock_register.side_effect = [Exception("First record failed"), None]
        item2 = sample_dynamodb_item.copy()
        item2["domain"] = {"S": "success-shop.com"}
        item2.pop("core_domain_name", None)
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
        assert result == {"batchItemFailures": [{"itemIdentifier": "fail-seq"}]}
        assert mock_register.call_count == 2
        # Ensure second call got core_domain_name None (item2 has no core_domain_name)
        calls = mock_register.call_args_list
        shop1 = calls[0][0][0]
        shop2 = calls[1][0][0]
        assert shop1.core_domain_name == "shop"
        # When core_domain_name is not present in the stream, the model derives
        # it from the domain; for success-shop.com we expect 'success-shop'.
        assert getattr(shop2, "core_domain_name", None) == "success-shop"

    @patch(
        "src.lambdas.shop_registration.shop_registration_handler.register_or_update_shop",
        new_callable=Mock,
    )
    def test_skips_modify_events(self, mock_register, sample_dynamodb_item):
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

        # no calls to register_or_update_shop expected
        mock_register.assert_not_called()

    @patch(
        "src.lambdas.shop_registration.shop_registration_handler.register_or_update_shop",
        new_callable=Mock,
    )
    def test_handles_missing_new_image(self, mock_register):
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
        mock_register.assert_not_called()

    @patch(
        "src.lambdas.shop_registration.shop_registration_handler.register_or_update_shop",
        new_callable=Mock,
    )
    def test_handles_empty_records(self, mock_register):
        result = handler({"Records": []}, None)
        assert result == {"batchItemFailures": []}
        result2 = handler({}, None)
        assert result2 == {"batchItemFailures": []}
        mock_register.assert_not_called()


class TestResilientHttpRequestSync:
    def test_returns_text_on_200(self):
        """Returns response.text when status is 200 and return_json is False."""
        session = Mock()
        response = Mock()
        response.raise_for_status = Mock()
        response.text = "ok"
        response.json = Mock(side_effect=RuntimeError("json should not be called"))

        session.headers = {"User-Agent": "aura-shop-registration/1.0"}
        session.request = Mock(return_value=response)

        result = resilient_http_request_sync(
            "http://example.test/", session, method="GET", return_json=False
        )

        assert result == "ok"
        session.request.assert_called_once()
        _, kwargs = session.request.call_args
        assert kwargs["method"] == "GET"
        assert kwargs["url"] == "http://example.test/"

    def test_returns_json_on_200_and_return_json_true(self):
        """Returns parsed JSON when return_json=True."""
        session = Mock()
        response = Mock()
        response.raise_for_status = Mock()
        response.text = '{"a": 1}'
        response.json = Mock(return_value={"a": 1})

        session.headers = {}
        session.request = Mock(return_value=response)

        result = resilient_http_request_sync(
            "http://example.test/", session, method="POST", return_json=True
        )

        assert result == {"a": 1}

    def test_invalid_json_raises_when_return_json_true(self):
        """If response.json() raises ValueError, the error is propagated."""
        session = Mock()
        response = Mock()
        response.raise_for_status = Mock()
        response.text = "not json"
        response.json = Mock(side_effect=ValueError("Invalid JSON"))

        session.headers = {}
        session.request = Mock(return_value=response)

        with pytest.raises(ValueError):
            resilient_http_request_sync(
                "http://example.test/", session, return_json=True
            )

    def test_non_2xx_raises_http_error_after_raise_for_status(self):
        """HTTP errors raised by raise_for_status are propagated."""
        session = Mock()
        response = Mock()
        response.status_code = 500
        response.text = "Internal Server Error"

        # Create HTTPError with response attribute for logging
        http_error = requests.HTTPError("Server error")
        http_error.response = response

        response.raise_for_status = Mock(side_effect=http_error)

        session.headers = {}
        session.request = Mock(return_value=response)

        with pytest.raises(requests.HTTPError):
            resilient_http_request_sync("http://example.test/", session)
