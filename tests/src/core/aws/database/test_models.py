import pytest

from src.core.aws.database.models import ShopMetadata, URLEntry


class TestShopMetadata:
    """Tests for ShopMetadata model."""

    def test_creation_with_all_fields(self):
        """Test creating a ShopMetadata instance with all fields."""
        metadata = ShopMetadata(
            domain="example.com",
            shop_country="US",
        )

        assert metadata.pk == "SHOP#example.com"
        assert metadata.sk == "META#"
        assert metadata.domain == "example.com"
        assert metadata.standards_used is True
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
        metadata = ShopMetadata(domain="example.com", shop_country="CA")

        item = metadata.to_dynamodb_item()

        assert item["pk"]["S"] == "SHOP#example.com"
        assert item["sk"]["S"] == "META#"
        assert item["domain"]["S"] == "example.com"
        assert item["shop_country"]["S"] == "COUNTRY#CA"
        assert item["standards_used"]["BOOL"] is True
        assert item["core_domain_name"]["S"] == "example"

    def test_from_dynamodb_item(self):
        """Test creating ShopMetadata from a DynamoDB item."""
        item = {
            "pk": {"S": "SHOP#example.com"},
            "sk": {"S": "META#"},
            "domain": {"S": "example.com"},
            "standards_used": {"BOOL": True},
            "shop_country": {"S": "COUNTRY#DE"},
        }

        metadata = ShopMetadata.from_dynamodb_item(item)

        assert metadata.domain == "example.com"
        assert metadata.standards_used is True
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
            type="product",
            hash="some_hash_value",
        )

        assert url_entry.pk == "SHOP#example.com"
        assert url_entry.sk == "URL#https://example.com/product/123"
        assert url_entry.url == "https://example.com/product/123"
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

        assert url_entry.type is None
        assert url_entry.hash is None

    def test_to_dynamodb_item(self):
        """Test converting URLEntry to DynamoDB item format."""
        url_entry = URLEntry(
            domain="example.com",
            url="https://example.com/product/123",
            type="product",
            hash="test_hash",
        )

        item = url_entry.to_dynamodb_item()

        assert item["pk"]["S"] == "SHOP#example.com"
        assert item["sk"]["S"] == "URL#https://example.com/product/123"
        assert item["url"]["S"] == "https://example.com/product/123"
        assert item["type"]["S"] == "product"
        assert item["hash"]["S"] == "test_hash"
        assert item["gsi1_pk"]["S"] == "SHOP#example.com"
        assert item["gsi1_sk"]["S"] == "product"

    def test_from_dynamodb_item(self):
        """Test creating URLEntry from DynamoDB item format."""
        item = {
            "pk": {"S": "SHOP#example.com"},
            "sk": {"S": "URL#https://example.com/product/123"},
            "url": {"S": "https://example.com/product/123"},
            "standards_used": {"BOOL": True},
            "type": {"S": "product"},
            "hash": {"S": "some_hash"},
        }

        url_entry = URLEntry.from_dynamodb_item(item)

        assert url_entry.domain == "example.com"
        assert url_entry.url == "https://example.com/product/123"
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

    def test_calculate_hash_changes_with_content(self):
        """Test that the hash changes when content changes."""
        markdown1 = "# Product A\nPrice: €19.99"
        markdown2 = "# Product B\nPrice: €19.99"
        markdown3 = "# Product A\nPrice: €29.99"

        hash1 = URLEntry.calculate_hash(markdown1)
        hash2 = URLEntry.calculate_hash(markdown2)
        hash3 = URLEntry.calculate_hash(markdown3)

        # All should be different
        assert hash1 != hash2
        assert hash1 != hash3
        assert hash2 != hash3

    def test_calculate_hash_idempotent(self):
        """Test that calling calculate_hash multiple times gives same result."""
        markdown = "# Antique Table\nPrice: €299.99\nState: available"

        hashes = [URLEntry.calculate_hash(markdown) for _ in range(5)]

        assert all(h == hashes[0] for h in hashes)

    def test_calculate_hash_with_unicode(self):
        """Test hash calculation with Unicode characters."""
        markdown = "# Möbel für Küche\nPreis: €19,99\nZustand: verfügbar"

        # Should not raise an error
        hash_result = URLEntry.calculate_hash(markdown)

        assert isinstance(hash_result, str)
        assert len(hash_result) == 64

    def test_calculate_hash_length(self):
        """Test that hash is always 64 characters (SHA256)."""
        test_cases = [
            "short",
            "a" * 1000,
            "# Product\nLong description " * 100,
            "",
        ]

        for markdown in test_cases:
            hash_result = URLEntry.calculate_hash(markdown)
            assert len(hash_result) == 64
            assert all(c in "0123456789abcdef" for c in hash_result)
