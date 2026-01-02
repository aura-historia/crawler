import pytest
import os
import random
import string
from unittest.mock import patch
from src.core.aws.database.models import ShopMetadata, URLEntry


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
                last_crawled_end="2020-01-02T01:00:00Z",
            )
            self.ops.upsert_shop_metadata(noise_shop)

    def test_shop_metadata_raw_state_and_gsis(self):
        domain = "organic-beauty.com"
        country = "DE"
        crawl_start = "2025-12-21T10:00:00Z"

        shop = ShopMetadata(
            domain=domain,
            shop_country=country,
            last_crawled_start=crawl_start,
        )
        self.ops.upsert_shop_metadata(shop)

        response = self.ops.client.get_item(
            TableName=self.table_name,
            Key={"pk": {"S": f"SHOP#{domain}"}, "sk": {"S": "META#"}},
        )
        assert "Item" in response
        raw_item = response["Item"]

        assert raw_item["gsi2_pk"]["S"] == "COUNTRY#DE"
        assert raw_item["gsi2_sk"]["S"] == crawl_start
        assert raw_item["gsi4_pk"]["S"] == "organic-beauty"
        assert raw_item["gsi4_sk"]["S"] == domain
        assert raw_item["standards_used"]["BOOL"] is True

    def test_url_entry_raw_state_and_gsi1(self):
        domain = "shop.com"
        product_url = "https://shop.com/p/123"

        entry = URLEntry(domain=domain, url=product_url, type="product")
        self.ops.upsert_url_entry(entry)

        raw_item = self.ops.client.get_item(
            TableName=self.table_name,
            Key={"pk": {"S": f"SHOP#{domain}"}, "sk": {"S": f"URL#{product_url}"}},
        )["Item"]

        assert raw_item["gsi1_pk"]["S"] == f"SHOP#{domain}"
        assert raw_item["gsi1_sk"]["S"] == "product"

    def test_query_isolation_with_noise(self):
        target_domain = "target-germany.de"
        self.ops.upsert_shop_metadata(
            ShopMetadata(
                domain=target_domain,
                shop_country="DE",
                last_crawled_start="2024-01-20T00:00:00Z",
            )
        )

        self.ops.upsert_shop_metadata(
            ShopMetadata(
                domain="old-germany.de",
                shop_country="DE",
                last_crawled_start="2020-01-01T00:00:00Z",
            )
        )

        results = self.ops.get_shops_by_country_and_crawled_date(
            country="DE",
            start_date="2024-01-01T00:00:00Z",
            end_date="2026-01-01T00:00:00Z",
        )

        assert target_domain in results
        assert "old-germany.de" not in results
        assert len(results) == 1

    def test_scrape_vs_crawl_gsi_updates(self):
        """Verify Independence of GSI2 (Crawl) and GSI3 (Scrape)."""
        domain = "dual-index-shop.com"
        crawl_date = "2025-12-21T10:00:00Z"
        scrape_date = "2025-12-21T20:00:00Z"

        shop = ShopMetadata(
            domain=domain, shop_country="DE", last_crawled_start=crawl_date
        )
        self.ops.upsert_shop_metadata(shop)

        self.ops.update_shop_metadata(domain=domain, last_scraped_start=scrape_date)

        raw_item = self.ops.client.get_item(
            TableName=self.table_name,
            Key={"pk": {"S": f"SHOP#{domain}"}, "sk": {"S": "META#"}},
        )["Item"]

        assert raw_item["gsi2_sk"]["S"] == crawl_date
        assert raw_item["gsi3_sk"]["S"] == scrape_date

    def test_scenario_upsert_with_geo_lookup(self):
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

    def test_scenario_update_sync_and_date_filtering(self):
        domain = "germany-shop.de"
        initial_date = "2025-01-01T00:00:00Z"
        target_date = "2025-12-21T14:00:00Z"

        self.ops.upsert_shop_metadata(
            ShopMetadata(
                domain=domain, shop_country="DE", last_crawled_start=initial_date
            )
        )
        self.ops.update_shop_metadata(domain=domain, last_crawled_start=target_date)

        found_new = self.ops.get_shops_by_country_and_crawled_date(
            country="DE",
            start_date="2025-12-01T00:00:00Z",
            end_date="2025-12-31T23:59:59Z",
        )
        assert domain in found_new

        found_old = self.ops.get_shops_by_country_and_crawled_date(
            country="DE",
            start_date="2025-01-01T00:00:00Z",
            end_date="2025-01-02T00:00:00Z",
        )
        assert domain not in found_old

    def test_scenario_batch_processing_large_volume(self):
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

    def test_scenario_core_domain_search_with_exclusions(self):
        store = "aura"
        exclude_me = "aura.net"
        domains = [exclude_me, "aura.de", "aura.fr", "apple.aura.com", "other.com"]

        for d in domains:
            self.ops.upsert_shop_metadata(ShopMetadata(domain=d, shop_country="EU"))

        results = self.ops.find_all_domains_by_core_domain_name(
            store, domain_to_exclude=exclude_me
        )

        result_domains = [r.domain for r in results]
        assert len(result_domains) == 3
        assert "aura.de" in result_domains
        assert "aura.fr" in result_domains
        assert "apple.aura.com" in result_domains
        assert "other.com" not in result_domains
        assert exclude_me not in result_domains

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

    def test_gsi2_crawl_date_range_isolation(self):
        """GSI2: Verify country-based crawl date filtering ignores unrelated dates and countries."""
        country = "DE"
        target_domain = "target-crawl.de"

        self.ops.upsert_shop_metadata(
            ShopMetadata(
                domain=target_domain,
                shop_country=country,
                last_crawled_start="2025-12-21T10:00:00Z",
            )
        )

        self.ops.upsert_shop_metadata(
            ShopMetadata(
                domain="old-crawl.de",
                shop_country=country,
                last_crawled_start="2020-01-01T00:00:00Z",
            )
        )

        self.ops.upsert_shop_metadata(
            ShopMetadata(
                domain="french-shop.fr",
                shop_country="FR",
                last_crawled_start="2025-12-21T11:00:00Z",
            )
        )

        results = self.ops.get_shops_by_country_and_crawled_date(
            country=country,
            start_date="2025-12-20T00:00:00Z",
            end_date="2025-12-22T00:00:00Z",
        )

        assert target_domain in results
        assert "old-germany.de" not in results
        assert "french-shop.fr" not in results
        assert len(results) == 1

    def test_gsi3_scrape_date_range_isolation(self):
        """GSI3: Verify country-based scrape date filtering is independent of crawl data."""
        country = "DE"
        domain = "scrape-target.de"
        scrape_date = "2025-12-21T15:00:00Z"

        self.ops.upsert_shop_metadata(
            ShopMetadata(
                domain=domain,
                shop_country=country,
                last_crawled_start="2025-12-21T10:00:00Z",
            )
        )

        before_update = self.ops.get_shops_by_country_and_scraped_date(
            country, "2025-12-21T00:00:00Z", "2025-12-21T23:59:59Z"
        )
        assert domain not in before_update

        self.ops.update_shop_metadata(domain=domain, last_scraped_start=scrape_date)

        after_update = self.ops.get_shops_by_country_and_scraped_date(
            country, "2025-12-21T00:00:00Z", "2025-12-21T23:59:59Z"
        )
        assert domain in after_update

    def test_gsi4_core_domain_discovery_isolation(self):
        """GSI4: Verify brand discovery ignores partial string matches and other brands."""
        brand = "apple"
        domains = [
            "apple.com",
            "apple.de",
            "apple-store.fr",
        ]  # apple-store.fr should have different core name

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

    def test_url_entry_idempotency_and_update(self):
        domain = "idempotency-test.com"
        url = f"https://{domain}/p1"

        entry_v1 = URLEntry(domain=domain, url=url, type="product", hash="v1")
        self.ops.upsert_url_entry(entry_v1)

        entry_v2 = URLEntry(domain=domain, url=url, type="product", hash="v2")
        self.ops.upsert_url_entry(entry_v2)

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

    def test_pagination_large_result_set(self):
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
        """
        Test writing 2500 URLs and reading all of them back.

        This test verifies:
        - Batch writing large volumes (2500 URLs)
        - Automatic pagination through multiple DynamoDB pages
        - Data integrity (all URLs retrieved correctly)
        - No duplicates in results
        - Performance with large datasets
        """
        domain = "mega-catalog.com"
        total_items = 2500

        # Create 2500 URL entries with zero-padded numbers for easy verification
        urls = [
            URLEntry(
                domain=domain,
                url=f"https://{domain}/product-{i:05d}",
                type="product",
            )
            for i in range(total_items)
        ]

        # Write in batches (DynamoDB batch_write_item has a 25-item limit internally)
        batch_size = 100
        for i in range(0, len(urls), batch_size):
            batch = urls[i : i + batch_size]
            result = self.ops.batch_write_url_entries(batch)
            assert result["UnprocessedItems"] == {}, (
                f"Batch {i // batch_size} had unprocessed items"
            )

        # Retrieve ALL 2500 URLs using automatic pagination
        retrieved = self.ops.get_all_product_urls_by_domain(domain)

        # Verify exact count
        assert len(retrieved) == total_items, (
            f"Expected {total_items} URLs, got {len(retrieved)}"
        )

        # Verify first and last URLs are present
        assert any("product-00000" in url for url in retrieved), (
            "First URL (product-00000) not found"
        )
        assert any("product-02499" in url for url in retrieved), (
            "Last URL (product-02499) not found"
        )

        # Verify all URLs are unique (no duplicates from pagination)
        unique_urls = set(retrieved)
        assert len(unique_urls) == total_items, (
            f"Found duplicates: {len(retrieved)} total vs {len(unique_urls)} unique"
        )

        # Verify URL format and domain consistency
        for url in retrieved:
            assert domain in url, f"URL {url} doesn't contain domain {domain}"
            assert url.startswith("https://"), f"URL {url} doesn't start with https://"

        # Verify we can find specific URLs by index
        sample_indices = [0, 500, 1000, 1500, 2000, 2499]
        for idx in sample_indices:
            expected_url = f"https://{domain}/product-{idx:05d}"
            assert expected_url in retrieved, (
                f"Expected URL at index {idx} not found: {expected_url}"
            )

    def test_item_isolation_same_partition(self):
        domain = "isolation.com"
        url = f"https://{domain}/p1"

        self.ops.upsert_shop_metadata(ShopMetadata(domain=domain, shop_country="US"))
        self.ops.upsert_url_entry(URLEntry(domain=domain, url=url, type="product"))

        res = self.ops.client.query(
            TableName=self.table_name,
            KeyConditionExpression="pk = :pk",
            ExpressionAttributeValues={":pk": {"S": f"SHOP#{domain}"}},
        )

        assert len(res["Items"]) == 2
        sks = [item["sk"]["S"] for item in res["Items"]]
        assert "META#" in sks
        assert f"URL#{url}" in sks

    def test_batch_write_chunking_limit(self):
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

    def test_cross_gsi_sparse_index_verification(self):
        domain = "sparse-test.com"
        self.ops.upsert_shop_metadata(
            ShopMetadata(
                domain=domain,
                shop_country="DE",
                last_crawled_start="2025-12-21T10:00:00Z",
            )
        )
        self.ops.upsert_url_entry(
            URLEntry(domain=domain, url=f"https://{domain}/p1", type="product")
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

    def test_get_url_entry_existing(self):
        """Retrieve an existing URL entry."""
        domain = "url-retrieval.com"
        url = f"https://{domain}/product-123"
        original_hash = "abc123"

        entry = URLEntry(domain=domain, url=url, type="product", hash=original_hash)
        self.ops.upsert_url_entry(entry)

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
        self.ops.upsert_url_entry(entry)

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
        self.ops.upsert_url_entry(entry)

        self.ops.update_url_hash(domain, url, new_hash)

        retrieved = self.ops.get_url_entry(domain, url)
        assert retrieved is not None
        assert retrieved.hash == new_hash
        assert retrieved.type == "product"
        assert retrieved.url == url

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

        # Collect all pages
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

        # Verify total count and no duplicates
        assert len(all_pages) == total_items
        assert len(set(all_pages)) == total_items
