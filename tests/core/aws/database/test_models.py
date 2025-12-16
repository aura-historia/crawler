import pytest

from src.core.aws.database.models import ShopMetadata, URLEntry


class TestShopMetadata:
    """Tests for ShopMetadata model."""

    def test_creation_with_all_fields(self):
        """Test creating a ShopMetadata instance with all fields."""
        metadata = ShopMetadata(
            domain="example.com",
            standards_used=["json-ld", "microdata"],
            shop_country="US",
        )

        assert metadata.pk == "SHOP#example.com"
        assert metadata.sk == "META#"
        assert metadata.domain == "example.com"
        assert metadata.standards_used == ["json-ld", "microdata"]
        assert metadata.shop_country == "COUNTRY#US"
        assert metadata.core_domain_name == "example"

    def test_creation_with_defaults(self):
        """Test that ShopMetadata has correct default values."""
        metadata = ShopMetadata(domain="example.com")

        assert metadata.sk == "META#"
        assert metadata.domain == "example.com"
        assert metadata.pk == "SHOP#example.com"
        assert metadata.core_domain_name == "example"

    def test_to_dynamodb_item(self):
        """Test converting ShopMetadata to DynamoDB item format."""
        metadata = ShopMetadata(
            domain="example.com", standards_used=["json-ld"], shop_country="CA"
        )

        item = metadata.to_dynamodb_item()

        assert item["pk"]["S"] == "SHOP#example.com"
        assert item["sk"]["S"] == "META#"
        assert item["domain"]["S"] == "example.com"
        assert item["shop_country"]["S"] == "COUNTRY#CA"
        assert "L" in item["standards_used"]
        assert len(item["standards_used"]["L"]) == 1
        assert item["standards_used"]["L"][0]["S"] == "json-ld"
        assert item["core_domain_name"]["S"] == "example"

    def test_from_dynamodb_item(self):
        """Test creating ShopMetadata from a DynamoDB item."""
        item = {
            "pk": {"S": "SHOP#example.com"},
            "sk": {"S": "META#"},
            "domain": {"S": "example.com"},
            "standards_used": {"L": [{"S": "opengraph"}]},
            "shop_country": {"S": "COUNTRY#DE"},
        }

        metadata = ShopMetadata.from_dynamodb_item(item)

        assert metadata.domain == "example.com"
        assert metadata.standards_used == ["opengraph"]
        assert metadata.shop_country == "COUNTRY#DE"
        assert metadata.pk == "SHOP#example.com"
        assert metadata.sk == "META#"

    @pytest.mark.parametrize(
        "domain,expected_pk",
        [
            ("example.com", "SHOP#example.com"),
            ("shop.de", "SHOP#shop.de"),
            ("my-store.co.uk", "SHOP#my-store.co.uk"),
        ],
    )
    def test_pk_generation(self, domain, expected_pk):
        """Test that PK is generated correctly from domain."""
        metadata = ShopMetadata(domain=domain)
        assert metadata.pk == expected_pk


class TestURLEntry:
    """Tests for URLEntry model."""

    def test_creation_with_all_fields(self):
        """Test creating a URLEntry instance with all fields."""
        url_entry = URLEntry(
            domain="example.com",
            url="https://example.com/product/123",
            standards_used=["json-ld"],
            type="product",
            hash="some_hash_value",
        )

        assert url_entry.pk == "SHOP#example.com"
        assert url_entry.sk == "URL#https://example.com/product/123"
        assert url_entry.url == "https://example.com/product/123"
        assert url_entry.standards_used == ["json-ld"]
        assert url_entry.type == "product"
        assert url_entry.hash == "some_hash_value"
        assert url_entry.domain == "example.com"

    def test_domain_property_sets_pk(self):
        """Test that URLEntry domain property sets PK correctly."""
        url_entry = URLEntry(domain="test.com", url="https://test.com/page")

        assert url_entry.domain == "test.com"
        assert url_entry.pk == "SHOP#test.com"

    def test_defaults(self):
        """Test URLEntry default values."""
        url_entry = URLEntry(
            domain="example.com", url="https://example.com/product/123"
        )

        assert url_entry.standards_used == []
        assert url_entry.type is None
        assert url_entry.hash is None

    def test_to_dynamodb_item(self):
        """Test converting URLEntry to DynamoDB item format."""
        url_entry = URLEntry(
            domain="example.com",
            url="https://example.com/product/123",
            standards_used=["json-ld"],
            type="product",
            hash="test_hash",
        )

        item = url_entry.to_dynamodb_item()

        assert item["pk"]["S"] == "SHOP#example.com"
        assert item["sk"]["S"] == "URL#https://example.com/product/123"
        assert item["url"]["S"] == "https://example.com/product/123"
        assert item["type"]["S"] == "product"
        assert item["hash"]["S"] == "test_hash"
        assert len(item["standards_used"]["L"]) == 1
        assert item["gsi1_pk"]["S"] == "SHOP#example.com"
        assert item["gsi1_sk"]["S"] == "product"

    def test_from_dynamodb_item(self):
        """Test creating URLEntry from DynamoDB item format."""
        item = {
            "pk": {"S": "SHOP#example.com"},
            "sk": {"S": "URL#https://example.com/product/123"},
            "url": {"S": "https://example.com/product/123"},
            "standards_used": {"L": [{"S": "json-ld"}]},
            "type": {"S": "product"},
            "hash": {"S": "some_hash"},
        }

        url_entry = URLEntry.from_dynamodb_item(item)

        assert url_entry.domain == "example.com"
        assert url_entry.url == "https://example.com/product/123"
        assert url_entry.standards_used == ["json-ld"]
        assert url_entry.type == "product"
        assert url_entry.hash == "some_hash"

    def test_type_creates_gsi_entries(self):
        """Test that type field creates GSI1 entries."""
        url_entry = URLEntry(
            domain="example.com",
            url="https://example.com/product",
            type="product",
        )

        item = url_entry.to_dynamodb_item()

        assert "gsi1_pk" in item
        assert "gsi1_sk" in item
        assert item["gsi1_pk"]["S"] == "SHOP#example.com"
        assert item["gsi1_sk"]["S"] == "product"


class TestURLEntryHashing:
    """Tests for URLEntry hash calculation."""

    def test_calculate_hash_with_price(self):
        """Test hash calculation with status and a valid price."""
        status = "in_stock"
        price = 99.99
        expected_hash = (
            "6e74ec8d33d1c00ffc74de5cff6ea992a58a7cdad3bad329a3cccaf89e788801"
        )

        assert URLEntry.calculate_hash(status, price) == expected_hash

    def test_calculate_hash_without_price(self):
        """Test hash calculation with status and no price."""
        status = "out_of_stock"
        price = None
        expected_hash = (
            "2c7d77ebe2c94ec6ac66eae99fd760393d37651cd6dfd1fc003c51e6ef1e5836"
        )

        assert URLEntry.calculate_hash(status, price) == expected_hash

    def test_calculate_hash_consistency(self):
        """Test that the hash is consistent for the same inputs."""
        status = "in_stock"
        price = 19.99

        hash1 = URLEntry.calculate_hash(status, price)
        hash2 = URLEntry.calculate_hash(status, price)

        assert hash1 == hash2

    @pytest.mark.parametrize(
        "status1,price1,status2,price2",
        [
            ("in_stock", 19.99, "out_of_stock", 19.99),
            ("in_stock", 19.99, "in_stock", 29.99),
            ("in_stock", 19.99, "in_stock", None),
            ("available", 10.00, "available", 10.01),
        ],
    )
    def test_calculate_hash_changes(self, status1, price1, status2, price2):
        """Test that the hash changes when inputs change."""
        hash1 = URLEntry.calculate_hash(status1, price1)
        hash2 = URLEntry.calculate_hash(status2, price2)

        assert hash1 != hash2

    def test_calculate_hash_idempotent(self):
        """Test that calling calculate_hash multiple times gives same result."""
        status = "in_stock"
        price = 29.99

        hashes = [URLEntry.calculate_hash(status, price) for _ in range(5)]

        assert all(h == hashes[0] for h in hashes)
