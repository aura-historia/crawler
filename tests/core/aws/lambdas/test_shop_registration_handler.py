import pytest
from unittest.mock import AsyncMock, patch
from aiohttp import ClientSession

from src.core.aws.database.models import ShopMetadata, METADATA_SK
from src.core.aws.lambdas.shop_registration_handler import (
    get_core_domain_name,
    find_existing_shop,
    handler,
    register_or_update_shop,
)


@pytest.fixture
def sample_shop_metadata():
    return ShopMetadata(
        domain="shop.com",
        standards_used=["json-ld", "microdata"],
        shop_country="DE",
    )


@pytest.fixture
def sample_dynamodb_item():
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

    @pytest.mark.asyncio
    @patch(
        "src.core.aws.lambdas.shop_registration_handler.resilient_http_request",
        new_callable=AsyncMock,
    )
    @patch("src.core.aws.lambdas.shop_registration_handler.find_existing_shop")
    @patch(
        "src.core.aws.lambdas.shop_registration_handler.BACKEND_API_URL",
        "http://test-backend",
    )
    async def test_register_new_shop(
        self, mock_find_existing, mock_resilient, sample_shop_metadata
    ):
        mock_find_existing.return_value = None
        session = AsyncMock(spec=ClientSession)
        await register_or_update_shop(sample_shop_metadata, session)
        mock_resilient.assert_awaited_once()
        _, kwargs = mock_resilient.call_args
        assert kwargs["method"] == "POST"
        assert "name" in kwargs["json_data"]
        assert kwargs["json_data"]["domains"] == [sample_shop_metadata.domain]

    @pytest.mark.asyncio
    @patch(
        "src.core.aws.lambdas.shop_registration_handler.resilient_http_request",
        new_callable=AsyncMock,
    )
    @patch("src.core.aws.lambdas.shop_registration_handler.find_existing_shop")
    @patch(
        "src.core.aws.lambdas.shop_registration_handler.BACKEND_API_URL",
        "http://test-backend",
    )
    async def test_update_existing_shop(
        self, mock_find_existing, mock_resilient, sample_shop_metadata
    ):
        mock_find_existing.return_value = ("shop.com", ["shop.com", "shop.de"])
        session = AsyncMock(spec=ClientSession)
        await register_or_update_shop(sample_shop_metadata, session)
        mock_resilient.assert_awaited_once()
        _, kwargs = mock_resilient.call_args
        assert kwargs["method"] == "PATCH"
        assert "domains" in kwargs["json_data"]
        assert sample_shop_metadata.domain in kwargs["json_data"]["domains"]

    @pytest.mark.asyncio
    @patch("src.core.aws.lambdas.shop_registration_handler.BACKEND_API_URL", None)
    async def test_backend_api_url_missing(self, sample_shop_metadata):
        session = AsyncMock(spec=ClientSession)
        with pytest.raises(ValueError):
            await register_or_update_shop(sample_shop_metadata, session)

    @pytest.mark.asyncio
    @patch(
        "src.core.aws.lambdas.shop_registration_handler.resilient_http_request",
        new_callable=AsyncMock,
    )
    @patch("src.core.aws.lambdas.shop_registration_handler.find_existing_shop")
    async def test_handles_invalid_shop_metadata(
        self, mock_find_existing, mock_resilient
    ):
        from src.core.aws.database.models import ShopMetadata

        shop = ShopMetadata(domain="", standards_used=[], shop_country="DE")
        session = AsyncMock(spec=ClientSession)
        with pytest.raises(Exception):
            await register_or_update_shop(shop, session)


class TestHandler:
    """Tests for the updated Lambda handler function using partial batch failures."""

    @pytest.mark.asyncio
    @patch(
        "src.core.aws.lambdas.shop_registration_handler.register_or_update_shop",
        new_callable=AsyncMock,
    )
    @patch("src.core.utils.network.resilient_http_request", new_callable=AsyncMock)
    async def test_processes_insert_event_successfully(
        self, mock_resilient, mock_register, sample_dynamodb_item
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
        mock_register.return_value = None
        result = await handler(event, None)
        assert result == {"batchItemFailures": []}
        mock_register.assert_awaited_once()

    @pytest.mark.asyncio
    @patch(
        "src.core.aws.lambdas.shop_registration_handler.register_or_update_shop",
        new_callable=AsyncMock,
    )
    @patch("src.core.utils.network.resilient_http_request", new_callable=AsyncMock)
    async def test_partial_batch_failure_mixed_results(
        self, mock_resilient, mock_register, sample_dynamodb_item
    ):
        """Test that only the failed record's sequence number is returned."""
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
        result = await handler(event, None)
        assert result == {"batchItemFailures": [{"itemIdentifier": "fail-seq"}]}
        assert mock_register.call_count == 2

    @pytest.mark.asyncio
    @patch(
        "src.core.aws.lambdas.shop_registration_handler.register_or_update_shop",
        new_callable=AsyncMock,
    )
    @patch("src.core.utils.network.resilient_http_request", new_callable=AsyncMock)
    async def test_skips_modify_events(self, _, mock_register, sample_dynamodb_item):
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
        result = await handler(event, None)
        assert result == {"batchItemFailures": []}

    @pytest.mark.asyncio
    @patch(
        "src.core.aws.lambdas.shop_registration_handler.register_or_update_shop",
        new_callable=AsyncMock,
    )
    @patch("src.core.utils.network.resilient_http_request", new_callable=AsyncMock)
    async def test_handles_missing_new_image(self, _, mock_register):
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
        result = await handler(event, None)
        assert result == {"batchItemFailures": []}

    @pytest.mark.asyncio
    @patch(
        "src.core.aws.lambdas.shop_registration_handler.register_or_update_shop",
        new_callable=AsyncMock,
    )
    @patch("src.core.utils.network.resilient_http_request", new_callable=AsyncMock)
    async def test_handles_empty_records(self, _, mock_register):
        result = await handler({"Records": []}, None)
        assert result == {"batchItemFailures": []}
        result2 = await handler({}, None)
        assert result2 == {"batchItemFailures": []}
