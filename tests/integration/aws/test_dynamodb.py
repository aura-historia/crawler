import pytest
import os
import random
import string
from unittest.mock import patch
from src.core.aws.database.operations import ShopMetadata, URLEntry
from src.core.aws.database.constants import STATE_NEVER, STATE_DONE, STATE_PROGRESS


class TestDynamoDBIntegration:
    @pytest.fixture(autouse=True)
    def setup_method_fixture(self, dynamodb_setup):
        """
        Injects the db_operations and table name into the test class.
        The 'dynamodb_setup' fixture handles LocalStack,
        and 'cleanup_table' ensures a fresh start for every test.
        """
        self.ops = dynamodb_setup
        self.table_name = os.getenv("DYNAMODB_TABLE_NAME", "aura-historia-data")
        self._populate_random_noise(count=5)

    def _populate_random_noise(self, count=5):
        """Adds unrelated data to verify query isolation (Noisy Neighbors)."""
        for _ in range(count):
            rand_str = "".join(random.choices(string.ascii_lowercase, k=8))
            noise_shop = ShopMetadata(
                domain=f"noise-{rand_str}.com",
                shop_country=random.choice(["US", "UK", "FR", "JP"]),
                last_crawled_start="2020-01-01T00:00:00Z",
                last_crawled_end=f"{STATE_DONE}2020-01-02T01:00:00Z",
            )
            self.ops.upsert_shop_metadata(noise_shop)

    # =========================================================================
    # Shop Metadata Tests
    # =========================================================================

    def test_shop_metadata_raw_state_and_gsis(self):
        """Verify GSI keys are correctly populated with prefixes."""
        domain = "organic-beauty.com"
        country = "DE"
        crawl_end = f"{STATE_DONE}2025-12-21T10:00:00Z"

        shop = ShopMetadata(
            domain=domain,
            shop_country=country,
            last_crawled_end=crawl_end,
        )
        self.ops.upsert_shop_metadata(shop)

        response = self.ops.client.get_item(
            TableName=self.table_name,
            Key={"pk": {"S": f"SHOP#{domain}"}, "sk": {"S": "META#"}},
        )
        assert "Item" in response
        raw_item = response["Item"]

        assert raw_item["gsi2_pk"]["S"] == "COUNTRY#DE"
        assert raw_item["gsi2_sk"]["S"] == crawl_end
        assert raw_item["gsi4_pk"]["S"] == "organic-beauty"
        assert raw_item["gsi4_sk"]["S"] == domain

    def test_get_shop_metadata_existing(self):
        """Retrieve existing shop metadata."""
        domain = "get-meta-test.com"
        shop = ShopMetadata(
            domain=domain,
            shop_country="DE",
            shop_name="Test Shop",
            last_crawled_end=f"{STATE_DONE}2025-01-01T00:00:00Z",
        )
        self.ops.upsert_shop_metadata(shop)

        retrieved = self.ops.get_shop_metadata(domain)

        assert retrieved is not None
        assert retrieved.domain == domain
        assert retrieved.shop_country == "COUNTRY#DE"
        assert retrieved.shop_name == "Test Shop"

    def test_get_shop_metadata_not_found(self):
        """Return None for non-existent shop."""
        retrieved = self.ops.get_shop_metadata("non-existent-shop.com")
        assert retrieved is None

    def test_scrape_vs_crawl_gsi_updates(self):
        """Verify Independence of GSI2 (Crawl) and GSI3 (Scrape)."""
        domain = "dual-index-shop.com"
        crawl_end_date = f"{STATE_DONE}2025-12-21T10:00:00Z"
        scrape_end_date = f"{STATE_DONE}2025-12-21T20:00:00Z"

        shop = ShopMetadata(
            domain=domain, shop_country="DE", last_crawled_end=crawl_end_date
        )
        self.ops.upsert_shop_metadata(shop)

        self.ops.update_shop_metadata(domain=domain, last_scraped_end=scrape_end_date)

        raw_item = self.ops.client.get_item(
            TableName=self.table_name,
            Key={"pk": {"S": f"SHOP#{domain}"}, "sk": {"S": "META#"}},
        )["Item"]

        assert raw_item["gsi2_sk"]["S"] == crawl_end_date
        assert raw_item["gsi3_sk"]["S"] == scrape_end_date

    def test_scenario_upsert_with_geo_lookup(self):
        """Verify geo lookup during upsert."""
        domain = "brand-new-store.com"

        with (
            patch("socket.gethostbyname") as mock_dns,
            patch("src.core.aws.database.operations.get_country_code") as mock_geo,
        ):
            mock_dns.return_value = "8.8.8.8"
            mock_geo.return_value = "US"

            shop = ShopMetadata(domain=domain)
            self.ops.upsert_shop_metadata(shop)

            results = self.ops.find_all_domains_by_core_domain_name("brand-new-store")
            assert len(results) == 1
            assert results[0].shop_country == "COUNTRY#US"

    # =========================================================================
    # URL Entry Tests
    # =========================================================================

    def test_url_entry_raw_state_and_gsi1(self):
        """Verify URL entry GSI1 keys are correctly populated."""
        domain = "shop.com"
        product_url = "https://shop.com/p/123"

        entry = URLEntry(domain=domain, url=product_url, type="product")
        self.ops.batch_write_url_entries([entry])

        raw_item = self.ops.client.get_item(
            TableName=self.table_name,
            Key={"pk": {"S": f"SHOP#{domain}"}, "sk": {"S": f"URL#{product_url}"}},
        )["Item"]

        assert raw_item["gsi1_pk"]["S"] == f"SHOP#{domain}"
        assert raw_item["gsi1_sk"]["S"] == "product"

    def test_gsi1_product_filtering_accuracy(self):
        """GSI1: Verify isolation of product types within a specific domain."""
        domain = "filter-test.com"
        other_domain = "wrong-shop.com"

        entries = [
            URLEntry(domain=domain, url=f"https://{domain}/p1", type="product"),
            URLEntry(domain=domain, url=f"https://{domain}/p2", type="product"),
            URLEntry(domain=domain, url=f"https://{domain}/p3", type="product"),
            URLEntry(domain=domain, url=f"https://{domain}/c1", type="category"),
            URLEntry(domain=domain, url=f"https://{domain}/p4", type="product"),
            URLEntry(
                domain=other_domain, url=f"https://{other_domain}/p1", type="product"
            ),
            URLEntry(
                domain=other_domain, url=f"https://{other_domain}/c1", type="category"
            ),
        ]
        self.ops.batch_write_url_entries(entries)

        products = self.ops.get_all_product_urls_by_domain(domain)

        assert len(products) == 4
        assert f"https://{domain}/p1" in products
        assert f"https://{domain}/c1" not in products
        assert f"https://{other_domain}/p1" not in products
        assert f"https://{other_domain}/c1" not in products

    def test_url_entry_idempotency_and_update(self):
        """Verify batch write is idempotent and updates existing entries."""
        domain = "idempotency-test.com"
        url = f"https://{domain}/p1"

        entry_v1 = URLEntry(domain=domain, url=url, type="product", hash="v1")
        self.ops.batch_write_url_entries([entry_v1])

        entry_v2 = URLEntry(domain=domain, url=url, type="product", hash="v2")
        self.ops.batch_write_url_entries([entry_v2])

        response = self.ops.client.query(
            TableName=self.table_name,
            KeyConditionExpression="pk = :pk AND sk = :sk",
            ExpressionAttributeValues={
                ":pk": {"S": f"SHOP#{domain}"},
                ":sk": {"S": f"URL#{url}"},
            },
        )

        assert len(response["Items"]) == 1
        assert response["Items"][0]["hash"]["S"] == "v2"

    def test_get_url_entry_existing(self):
        """Retrieve an existing URL entry."""
        domain = "url-retrieval.com"
        url = f"https://{domain}/product-123"
        original_hash = "abc123"

        entry = URLEntry(domain=domain, url=url, type="product", hash=original_hash)
        self.ops.batch_write_url_entries([entry])

        retrieved = self.ops.get_url_entry(domain, url)

        assert retrieved is not None
        assert retrieved.domain == domain
        assert retrieved.url == url
        assert retrieved.type == "product"
        assert retrieved.hash == original_hash

    def test_get_url_entry_not_found(self):
        """Return None for non-existent URL entry."""
        domain = "non-existent.com"
        url = "https://non-existent.com/missing"

        retrieved = self.ops.get_url_entry(domain, url)

        assert retrieved is None

    def test_update_url_hash_success(self):
        """Update hash for an existing URL entry."""
        domain = "hash-update.com"
        url = f"https://{domain}/product-456"
        original_hash = "hash_v1"
        new_hash = "hash_v2"

        entry = URLEntry(domain=domain, url=url, type="product", hash=original_hash)
        self.ops.batch_write_url_entries([entry])

        self.ops.update_url_hash(domain, url, new_hash)

        retrieved = self.ops.get_url_entry(domain, url)
        assert retrieved is not None
        assert retrieved.hash == new_hash

    def test_update_url_hash_preserves_other_fields(self):
        """Ensure update_url_hash only changes hash, not other attributes."""
        domain = "hash-preserve.com"
        url = f"https://{domain}/product-789"
        original_hash = "original"
        new_hash = "updated"

        entry = URLEntry(domain=domain, url=url, type="product", hash=original_hash)
        self.ops.batch_write_url_entries([entry])

        self.ops.update_url_hash(domain, url, new_hash)

        retrieved = self.ops.get_url_entry(domain, url)
        assert retrieved is not None
        assert retrieved.hash == new_hash
        assert retrieved.type == "product"
        assert retrieved.url == url

    # =========================================================================
    # Batch Write and Pagination Tests
    # =========================================================================

    def test_scenario_batch_processing_large_volume(self):
        """Test batch writing 40 URLs."""
        domain = "bulk-data.io"
        count = 40

        urls = [
            URLEntry(domain=domain, url=f"https://{domain}/item/{i}", type="product")
            for i in range(count)
        ]

        result = self.ops.batch_write_url_entries(urls)
        assert result["UnprocessedItems"] == {}

        retrieved = self.ops.get_all_product_urls_by_domain(domain)
        assert len(retrieved) == count

    def test_pagination_large_result_set(self):
        """Test pagination with 35 items."""
        domain = "pagination.io"
        total_items = 35
        urls = [
            URLEntry(domain=domain, url=f"https://{domain}/item-{i}", type="product")
            for i in range(total_items)
        ]

        self.ops.batch_write_url_entries(urls)
        retrieved = self.ops.get_all_product_urls_by_domain(domain)

        assert len(retrieved) == total_items

    def test_write_and_read_2500_urls(self):
        """Test writing 2500 URLs and reading all of them back."""
        domain = "mega-catalog.com"
        total_items = 2500

        urls = [
            URLEntry(
                domain=domain,
                url=f"https://{domain}/product-{i:05d}",
                type="product",
            )
            for i in range(total_items)
        ]

        batch_size = 100
        for i in range(0, len(urls), batch_size):
            batch = urls[i : i + batch_size]
            result = self.ops.batch_write_url_entries(batch)
            assert result["UnprocessedItems"] == {}, (
                f"Batch {i // batch_size} had unprocessed items"
            )

        retrieved = self.ops.get_all_product_urls_by_domain(domain)

        assert len(retrieved) == total_items
        assert any("product-00000" in url for url in retrieved)
        assert any("product-02499" in url for url in retrieved)

        unique_urls = set(retrieved)
        assert len(unique_urls) == total_items

    def test_batch_write_chunking_limit(self):
        """Test batch write correctly handles >25 items (DynamoDB limit)."""
        domain = "chunking.com"
        count = 30
        urls = [
            URLEntry(domain=domain, url=f"https://{domain}/u-{i}", type="product")
            for i in range(count)
        ]

        result = self.ops.batch_write_url_entries(urls)

        assert result["UnprocessedItems"] == {}

        res = self.ops.client.query(
            TableName=self.table_name,
            KeyConditionExpression="pk = :pk AND begins_with(sk, :url)",
            ExpressionAttributeValues={
                ":pk": {"S": f"SHOP#{domain}"},
                ":url": {"S": "URL#"},
            },
        )
        assert len(res["Items"]) == count

    def test_pagination_with_explicit_token(self):
        """Test explicit pagination with LastEvaluatedKey."""
        domain = "pagination-explicit.io"
        total_items = 15
        page_size = 5

        urls = [
            URLEntry(domain=domain, url=f"https://{domain}/item-{i}", type="product")
            for i in range(total_items)
        ]
        self.ops.batch_write_url_entries(urls)

        all_pages = []
        last_key = None

        while True:
            page_urls, next_key = self.ops.get_product_urls_by_domain(
                domain, max_urls=page_size, last_evaluated_key=last_key
            )
            all_pages.extend(page_urls)

            if next_key is None:
                break
            last_key = next_key

        assert len(all_pages) == total_items
        assert len(set(all_pages)) == total_items

    # =========================================================================
    # Core Domain Discovery (GSI4) Tests
    # =========================================================================

    def test_scenario_core_domain_search_with_exclusions(self):
        """GSI4: Verify core domain search and application-level filtering."""
        store = "aura"
        exclude_me = "aura.net"
        domains = [exclude_me, "aura.de", "aura.fr", "apple.aura.com", "other.com"]

        for d in domains:
            self.ops.upsert_shop_metadata(ShopMetadata(domain=d, shop_country="EU"))

        # Get all shops and filter at application level (as done in shop_registration_handler)
        all_shops = self.ops.find_all_domains_by_core_domain_name(store)
        result_domains = [r.domain for r in all_shops if r.domain != exclude_me]

        assert len(result_domains) == 3
        assert "aura.de" in result_domains
        assert "aura.fr" in result_domains
        assert "apple.aura.com" in result_domains
        assert "other.com" not in result_domains
        assert exclude_me not in result_domains

    def test_gsi4_core_domain_discovery_isolation(self):
        """GSI4: Verify brand discovery ignores partial string matches."""
        brand = "apple"
        domains = ["apple.com", "apple.de", "apple-store.fr"]

        for d in domains:
            self.ops.upsert_shop_metadata(ShopMetadata(domain=d, shop_country="EU"))

        self.ops.upsert_shop_metadata(
            ShopMetadata(domain="pineapple.com", shop_country="US")
        )

        results = self.ops.find_all_domains_by_core_domain_name(brand)
        result_domains = [r.domain for r in results]

        assert "apple.com" in result_domains
        assert "apple.de" in result_domains
        assert "pineapple.com" not in result_domains
        assert "apple-store.fr" not in result_domains

    # =========================================================================
    # Item Isolation Tests
    # =========================================================================

    def test_item_isolation_same_partition(self):
        """Verify metadata and URL entries coexist in same partition."""
        domain = "isolation.com"
        url = f"https://{domain}/p1"

        self.ops.upsert_shop_metadata(ShopMetadata(domain=domain, shop_country="US"))
        self.ops.batch_write_url_entries(
            [URLEntry(domain=domain, url=url, type="product")]
        )

        res = self.ops.client.query(
            TableName=self.table_name,
            KeyConditionExpression="pk = :pk",
            ExpressionAttributeValues={":pk": {"S": f"SHOP#{domain}"}},
        )

        assert len(res["Items"]) == 2
        sks = [item["sk"]["S"] for item in res["Items"]]
        assert "META#" in sks
        assert f"URL#{url}" in sks

    def test_cross_gsi_sparse_index_verification(self):
        """Verify sparse index behavior - only metadata in GSI2."""
        domain = "sparse-test.com"
        self.ops.upsert_shop_metadata(
            ShopMetadata(
                domain=domain,
                shop_country="DE",
                last_crawled_end=f"{STATE_DONE}2025-12-21T10:00:00Z",
            )
        )
        self.ops.batch_write_url_entries(
            [URLEntry(domain=domain, url=f"https://{domain}/p1", type="product")]
        )

        gsi2_raw = self.ops.client.query(
            TableName=self.table_name,
            IndexName="GSI2",
            KeyConditionExpression="gsi2_pk = :pk",
            ExpressionAttributeValues={":pk": {"S": "COUNTRY#DE"}},
        )

        assert len(gsi2_raw["Items"]) > 0

        for item in gsi2_raw["Items"]:
            assert item["pk"]["S"] == f"SHOP#{domain}"
            assert item["domain"]["S"] == f"{domain}"
            assert not item["sk"]["S"].startswith("URL#")

    # =========================================================================
    # get_shops_for_orchestration Tests
    # =========================================================================

    def test_get_shops_for_orchestration_crawl_never(self):
        """Verify shops with NEVER# state are returned for crawl."""
        domain = "never-crawled.com"
        shop = ShopMetadata(
            domain=domain,
            shop_country="DE",
            last_crawled_end=STATE_NEVER,
        )
        self.ops.upsert_shop_metadata(shop)

        results = self.ops.get_shops_for_orchestration(
            operation_type="crawl",
            cutoff_date="2025-01-01T00:00:00Z",
            country="DE",
        )

        result_domains = [r.domain for r in results]
        assert domain in result_domains

    def test_get_shops_for_orchestration_crawl_done_old(self):
        """Verify shops with old DONE# timestamp are returned."""
        domain = "old-crawled.com"
        old_date = "2020-01-01T00:00:00Z"
        shop = ShopMetadata(
            domain=domain,
            shop_country="DE",
            last_crawled_end=f"{STATE_DONE}{old_date}",
        )
        self.ops.upsert_shop_metadata(shop)

        results = self.ops.get_shops_for_orchestration(
            operation_type="crawl",
            cutoff_date="2025-01-01T00:00:00Z",
            country="DE",
        )

        result_domains = [r.domain for r in results]
        assert domain in result_domains

    def test_get_shops_for_orchestration_crawl_done_recent(self):
        """Verify shops with recent DONE# timestamp are NOT returned."""
        domain = "recent-crawled.com"
        recent_date = "2025-12-01T00:00:00Z"
        shop = ShopMetadata(
            domain=domain,
            shop_country="DE",
            last_crawled_end=f"{STATE_DONE}{recent_date}",
        )
        self.ops.upsert_shop_metadata(shop)

        results = self.ops.get_shops_for_orchestration(
            operation_type="crawl",
            cutoff_date="2025-01-01T00:00:00Z",
            country="DE",
        )

        result_domains = [r.domain for r in results]
        assert domain not in result_domains

    def test_get_shops_for_orchestration_crawl_progress_excluded(self):
        """Verify shops with PROGRESS# state are NOT returned."""
        domain = "in-progress.com"
        shop = ShopMetadata(
            domain=domain,
            shop_country="DE",
            last_crawled_end=f"{STATE_PROGRESS}2025-01-01T00:00:00Z",
        )
        self.ops.upsert_shop_metadata(shop)

        results = self.ops.get_shops_for_orchestration(
            operation_type="crawl",
            cutoff_date="2026-01-01T00:00:00Z",
            country="DE",
        )

        result_domains = [r.domain for r in results]
        assert domain not in result_domains

    def test_get_shops_for_orchestration_scrape(self):
        """Verify GSI3 is used correctly for scrape operation."""
        domain = "scrape-test.com"
        shop = ShopMetadata(
            domain=domain,
            shop_country="DE",
            last_crawled_end=f"{STATE_DONE}2025-12-01T00:00:00Z",
            last_scraped_end=STATE_NEVER,
        )
        self.ops.upsert_shop_metadata(shop)

        results = self.ops.get_shops_for_orchestration(
            operation_type="scrape",
            cutoff_date="2025-01-01T00:00:00Z",
            country="DE",
        )

        result_domains = [r.domain for r in results]
        assert domain in result_domains

    def test_get_shops_for_orchestration_invalid_type(self):
        """Verify ValueError raised for invalid operation type."""
        with pytest.raises(ValueError) as exc_info:
            self.ops.get_shops_for_orchestration(
                operation_type="invalid",
                cutoff_date="2025-01-01T00:00:00Z",
                country="DE",
            )

        assert "crawl" in str(exc_info.value) or "scrape" in str(exc_info.value)

    def test_get_shops_for_orchestration_country_isolation(self):
        """Verify country filter isolates results correctly."""
        de_domain = "german-shop.de"
        fr_domain = "french-shop.fr"

        self.ops.upsert_shop_metadata(
            ShopMetadata(
                domain=de_domain,
                shop_country="DE",
                last_crawled_end=STATE_NEVER,
            )
        )
        self.ops.upsert_shop_metadata(
            ShopMetadata(
                domain=fr_domain,
                shop_country="FR",
                last_crawled_end=STATE_NEVER,
            )
        )

        results = self.ops.get_shops_for_orchestration(
            operation_type="crawl",
            cutoff_date="2025-01-01T00:00:00Z",
            country="DE",
        )

        result_domains = [r.domain for r in results]
        assert de_domain in result_domains
        assert fr_domain not in result_domains
