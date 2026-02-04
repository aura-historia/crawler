from unittest.mock import Mock, patch, MagicMock
import pytest

from src.core.aws.database.models import METADATA_SK
from src.core.aws.database.operations import ShopMetadata
from src.lambdas.shop_registration.shop_registration_handler import (
    find_existing_shop,
    handler,
    register_or_update_shop,
    _shop_type_from_string,
)

from aura_historia_backend_api_client.models.shop_type_data import ShopTypeData


@pytest.fixture
def sample_shop_metadata():
    return ShopMetadata(
        domain="shop.com",
        shop_country="DE",
        shop_type="COMMERCIAL_DEALER",
    )


@pytest.fixture
def sample_dynamodb_item():
    return {
        "pk": {"S": "SHOP#shop.com"},
        "sk": {"S": METADATA_SK},
        "domain": {"S": "shop.com"},
        "core_domain_name": {"S": "shop"},
        "shop_type": {"S": "COMMERCIAL_DEALER"},
    }


class TestShopTypeFromString:
    def test_commercial_dealer(self):
        assert (
            _shop_type_from_string("COMMERCIAL_DEALER")
            == ShopTypeData.COMMERCIAL_DEALER
        )

    def test_auction_house(self):
        assert _shop_type_from_string("AUCTION_HOUSE") == ShopTypeData.AUCTION_HOUSE

    def test_auction_platform(self):
        assert (
            _shop_type_from_string("AUCTION_PLATFORM") == ShopTypeData.AUCTION_PLATFORM
        )

    def test_marketplace(self):
        assert _shop_type_from_string("MARKETPLACE") == ShopTypeData.MARKETPLACE

    def test_unknown_defaults_to_commercial_dealer(self):
        assert _shop_type_from_string("UNKNOWN") == ShopTypeData.COMMERCIAL_DEALER


class TestFindExistingShop:
    @patch("src.lambdas.shop_registration.shop_registration_handler.db_operations")
    def test_returns_none_when_no_existing_shop(self, mock_db_ops):
        mock_db_ops.find_all_domains_by_core_domain_name.return_value = []

        result = find_existing_shop("newshop.com", "newshop")

        assert result is None
        mock_db_ops.find_all_domains_by_core_domain_name.assert_called_once_with(
            "newshop"
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
    @patch("src.lambdas.shop_registration.shop_registration_handler.create_shop")
    @patch("src.lambdas.shop_registration.shop_registration_handler.find_existing_shop")
    def test_register_new_shop(
        self, mock_find_existing, mock_create_shop, sample_shop_metadata
    ):
        mock_find_existing.return_value = None
        mock_response = MagicMock()
        mock_response.__class__ = Mock  # Not an ApiError
        mock_create_shop.sync.return_value = mock_response
        client = Mock()

        register_or_update_shop(sample_shop_metadata, client)

        mock_create_shop.sync.assert_called_once()
        call_kwargs = mock_create_shop.sync.call_args
        assert call_kwargs.kwargs["client"] == client
        body = call_kwargs.kwargs["body"]
        assert body.name == "shop"  # core_domain_name derived from domain
        assert body.shop_type == ShopTypeData.COMMERCIAL_DEALER
        assert body.domains == [sample_shop_metadata.domain]

    @patch(
        "src.lambdas.shop_registration.shop_registration_handler.update_shop_by_domain"
    )
    @patch("src.lambdas.shop_registration.shop_registration_handler.find_existing_shop")
    def test_update_existing_shop(
        self, mock_find_existing, mock_update_shop, sample_shop_metadata
    ):
        mock_find_existing.return_value = ("shop.com", ["shop.com", "shop.de"])
        mock_response = MagicMock()
        mock_response.__class__ = Mock  # Not an ApiError
        mock_update_shop.sync.return_value = mock_response
        client = Mock()

        register_or_update_shop(sample_shop_metadata, client)

        mock_update_shop.sync.assert_called_once()
        call_kwargs = mock_update_shop.sync.call_args
        assert call_kwargs.kwargs["shop_domain"] == "shop.com"
        assert call_kwargs.kwargs["client"] == client
        body = call_kwargs.kwargs["body"]
        assert sample_shop_metadata.domain in body.domains

    @patch("src.lambdas.shop_registration.shop_registration_handler.create_shop")
    @patch("src.lambdas.shop_registration.shop_registration_handler.find_existing_shop")
    def test_handles_invalid_shop_metadata(self, mock_find_existing, mock_create_shop):
        from aura_historia_backend_api_client.models.api_error import ApiError

        mock_find_existing.return_value = None
        mock_error = Mock(spec=ApiError)
        mock_error.title = "Bad Request"
        mock_error.detail = "Invalid domain"
        mock_create_shop.sync.return_value = mock_error

        shop = ShopMetadata(domain="", shop_country="DE")
        client = Mock()

        with pytest.raises(Exception) as exc_info:
            register_or_update_shop(shop, client)
        assert "Failed to create shop" in str(exc_info.value)


class TestHandler:
    @patch(
        "src.lambdas.shop_registration.shop_registration_handler.register_or_update_shop",
        new_callable=Mock,
    )
    @patch(
        "src.lambdas.shop_registration.shop_registration_handler._get_client",
    )
    def test_processes_insert_event_successfully(
        self, mock_get_client, mock_register, sample_dynamodb_item
    ):
        mock_get_client.return_value = Mock()
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
        assert shop_arg.shop_type == "COMMERCIAL_DEALER"

    @patch(
        "src.lambdas.shop_registration.shop_registration_handler.register_or_update_shop",
        new_callable=Mock,
    )
    @patch(
        "src.lambdas.shop_registration.shop_registration_handler._get_client",
    )
    def test_partial_batch_failure_mixed_results(
        self, mock_get_client, mock_register, sample_dynamodb_item
    ):
        mock_get_client.return_value = Mock()
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
    @patch(
        "src.lambdas.shop_registration.shop_registration_handler._get_client",
    )
    def test_skips_modify_events(
        self, mock_get_client, mock_register, sample_dynamodb_item
    ):
        mock_get_client.return_value = Mock()
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
    @patch(
        "src.lambdas.shop_registration.shop_registration_handler._get_client",
    )
    def test_handles_missing_new_image(self, mock_get_client, mock_register):
        mock_get_client.return_value = Mock()
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
    @patch(
        "src.lambdas.shop_registration.shop_registration_handler._get_client",
    )
    def test_handles_empty_records(self, mock_get_client, mock_register):
        mock_get_client.return_value = Mock()
        result = handler({"Records": []}, None)
        assert result == {"batchItemFailures": []}
        result2 = handler({}, None)
        assert result2 == {"batchItemFailures": []}
        mock_register.assert_not_called()


class TestShopTypeExtraction:
    @patch(
        "src.lambdas.shop_registration.shop_registration_handler.register_or_update_shop",
        new_callable=Mock,
    )
    @patch(
        "src.lambdas.shop_registration.shop_registration_handler._get_client",
    )
    def test_extracts_shop_type_from_stream(self, mock_get_client, mock_register):
        """Test that shop_type is correctly extracted from DynamoDB stream."""
        mock_get_client.return_value = Mock()
        mock_register.return_value = None

        dynamodb_item = {
            "pk": {"S": "SHOP#test.com"},
            "sk": {"S": METADATA_SK},
            "domain": {"S": "test.com"},
            "shop_type": {"S": "AUCTION_HOUSE"},
        }

        event = {
            "Records": [
                {
                    "eventName": "INSERT",
                    "dynamodb": {
                        "NewImage": dynamodb_item,
                        "SequenceNumber": "seq-type",
                    },
                }
            ]
        }

        handler(event, None)

        mock_register.assert_called_once()
        shop_arg = mock_register.call_args[0][0]
        assert shop_arg.shop_type == "AUCTION_HOUSE"

    @patch(
        "src.lambdas.shop_registration.shop_registration_handler.register_or_update_shop",
        new_callable=Mock,
    )
    @patch(
        "src.lambdas.shop_registration.shop_registration_handler._get_client",
    )
    def test_defaults_shop_type_when_missing(self, mock_get_client, mock_register):
        """Test that shop_type defaults to COMMERCIAL_DEALER when not in stream."""
        mock_get_client.return_value = Mock()
        mock_register.return_value = None

        dynamodb_item = {
            "pk": {"S": "SHOP#test.com"},
            "sk": {"S": METADATA_SK},
            "domain": {"S": "test.com"},
            # No shop_type field
        }

        event = {
            "Records": [
                {
                    "eventName": "INSERT",
                    "dynamodb": {
                        "NewImage": dynamodb_item,
                        "SequenceNumber": "seq-default",
                    },
                }
            ]
        }

        handler(event, None)

        mock_register.assert_called_once()
        shop_arg = mock_register.call_args[0][0]
        assert shop_arg.shop_type == "COMMERCIAL_DEALER"
