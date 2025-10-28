from src.core.database.models import ShopMetadata, URLEntry


def test_shop_metadata_creation():
    """Test creating a ShopMetadata instance."""
    metadata = ShopMetadata(
        domain="example.com", standards_used=["json-ld", "microdata"]
    )

    assert metadata.pk == "SHOP#example.com"
    assert metadata.sk == "META#"
    assert metadata.domain == "example.com"
    assert metadata.standards_used == ["json-ld", "microdata"]


def test_shop_metadata_default_sk():
    """Test that ShopMetadata has default sk value."""
    metadata = ShopMetadata(domain="example.com")

    assert metadata.sk == "META#"


def test_shop_metadata_to_dynamodb_item():
    """Test converting ShopMetadata to DynamoDB item format."""
    metadata = ShopMetadata(domain="example.com", standards_used=["json-ld"])

    item = metadata.to_dynamodb_item()

    assert item["PK"]["S"] == "SHOP#example.com"
    assert item["SK"]["S"] == "META#"
    assert item["domain"]["S"] == "example.com"
    assert len(item["standards_used"]["L"]) == 1
    assert item["standards_used"]["L"][0]["S"] == "json-ld"


def test_shop_metadata_from_dynamodb_item():
    """Test creating ShopMetadata from DynamoDB item format."""
    item = {
        "PK": {"S": "SHOP#example.com"},
        "SK": {"S": "META#"},
        "domain": {"S": "example.com"},
        "standards_used": {"L": [{"S": "json-ld"}, {"S": "microdata"}]},
    }

    metadata = ShopMetadata.from_dynamodb_item(item)

    assert metadata.domain == "example.com"
    assert metadata.pk == "SHOP#example.com"
    assert metadata.sk == "META#"
    assert metadata.standards_used == ["json-ld", "microdata"]


def test_url_entry_creation():
    """Test creating a URLEntry instance."""
    url_entry = URLEntry(
        domain="example.com",
        url="https://example.com/product/123",
        standards_used=["json-ld"],
        type="product",
        is_product=True,
        hash="some_hash_value",
    )

    assert url_entry.pk == "SHOP#example.com"
    assert url_entry.sk == "URL#https://example.com/product/123"
    assert url_entry.url == "https://example.com/product/123"
    assert url_entry.standards_used == ["json-ld"]
    assert url_entry.type == "product"
    assert url_entry.is_product is True
    assert url_entry.hash == "some_hash_value"
    assert url_entry.domain == "example.com"


def test_url_entry_domain_property():
    """Test that URLEntry domain is set correctly."""
    url_entry = URLEntry(domain="test.com", url="https://test.com/page")

    assert url_entry.domain == "test.com"
    assert url_entry.pk == "SHOP#test.com"


def test_url_entry_defaults():
    """Test URLEntry default values."""
    url_entry = URLEntry(domain="example.com", url="https://example.com/product/123")

    assert url_entry.standards_used == []
    assert url_entry.is_product is False
    assert url_entry.type is None
    assert url_entry.hash is None


def test_url_entry_to_dynamodb_item():
    """Test converting URLEntry to DynamoDB item format."""
    url_entry = URLEntry(
        domain="example.com",
        url="https://example.com/product/123",
        standards_used=["json-ld"],
        type="product",
        is_product=True,
        hash="test_hash",
    )

    item = url_entry.to_dynamodb_item()

    assert item["PK"]["S"] == "SHOP#example.com"
    assert item["SK"]["S"] == "URL#https://example.com/product/123"
    assert item["url"]["S"] == "https://example.com/product/123"
    assert item["is_product"]["BOOL"] is True
    assert item["type"]["S"] == "product"
    assert item["hash"]["S"] == "test_hash"
    assert len(item["standards_used"]["L"]) == 1


def test_url_entry_from_dynamodb_item():
    """Test creating URLEntry from DynamoDB item format."""
    item = {
        "PK": {"S": "SHOP#example.com"},
        "SK": {"S": "URL#https://example.com/product/123"},
        "url": {"S": "https://example.com/product/123"},
        "standards_used": {"L": [{"S": "json-ld"}]},
        "type": {"S": "product"},
        "is_product": {"BOOL": True},
        "hash": {"S": "some_hash"},
    }

    url_entry = URLEntry.from_dynamodb_item(item)

    assert url_entry.domain == "example.com"
    assert url_entry.url == "https://example.com/product/123"
    assert url_entry.standards_used == ["json-ld"]
    assert url_entry.type == "product"
    assert url_entry.is_product is True
    assert url_entry.hash == "some_hash"


def test_url_entry_calculate_hash():
    """Test URLEntry hash calculation."""
    hash1 = URLEntry.calculate_hash("in_stock", 29.99)
    hash2 = URLEntry.calculate_hash("in_stock", 29.99)
    hash3 = URLEntry.calculate_hash("out_of_stock", 29.99)

    # Same inputs should produce same hash
    assert hash1 == hash2
    # Different inputs should produce different hash
    assert hash1 != hash3


def test_calculate_hash_with_price():
    """Test hash calculation with status and a valid price."""
    status = "in_stock"
    price = 99.99
    # Expected hash for "in_stock|99.99"
    expected_hash = "559ceaf45a28778f6e88c3301ecd7d93"
    assert URLEntry.calculate_hash(status, price) == expected_hash


def test_calculate_hash_without_price():
    """Test hash calculation with status and no price."""
    status = "out_of_stock"
    price = None
    # Expected hash for "out_of_stock|None"
    expected_hash = "447e2ccd0cf0d38fc66e3e4a86c2e90d"
    assert URLEntry.calculate_hash(status, price) == expected_hash


def test_calculate_hash_consistency():
    """Test that the hash is consistent for the same inputs."""
    status = "in_stock"
    price = 19.99
    hash1 = URLEntry.calculate_hash(status, price)

    hash2 = URLEntry.calculate_hash(status, price)
    assert hash1 == hash2


def test_calculate_hash_changes():
    """Test that the hash changes when inputs change."""
    hash1 = URLEntry.calculate_hash("in_stock", 19.99)
    hash_status_change = URLEntry.calculate_hash("out_of_stock", 19.99)
    hash_price_change = URLEntry.calculate_hash("in_stock", 29.99)
    hash_price_none = URLEntry.calculate_hash("in_stock", None)

    assert hash1 != hash_status_change
    assert hash1 != hash_price_change
    assert hash1 != hash_price_none
