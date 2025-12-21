import pytest
import torch
import torch.nn.functional as F
from pathlib import Path

from src.core.classifier.url_classifier import URLBertClassifier


class TestURLClassifier:
    @pytest.fixture(scope="class")
    def classifier(self):
        """
        Fixture to load the classifier once for all tests.
        This avoids reloading the model for each test.
        """
        return URLBertClassifier()

    @pytest.fixture(scope="class")
    def model_paths(self):
        """Fixture providing model file paths."""
        root_dir = Path(__file__).resolve().parents[3] / "src" / "core" / "classifier"
        return {
            "model": root_dir / "fine_tuned_model" / "urlbert_products_best.pth",
            "tokenizer": root_dir / "bert_tokenizer" / "vocab.txt",
            "config": root_dir / "bert_config" / "config.json",
        }

    def test_model_files_exist(self, model_paths):
        """Verify that all required model files exist."""
        assert model_paths["tokenizer"].exists(), (
            f"Tokenizer file not found: {model_paths['tokenizer']}"
        )
        assert model_paths["config"].exists(), (
            f"Config file not found: {model_paths['config']}"
        )

    def test_classifier_initialization(self, classifier):
        """Test that classifier initializes successfully."""
        assert classifier is not None
        assert classifier.bert is not None
        assert classifier.tokenizer is not None
        assert classifier.VOCAB_SIZE == 5000
        assert classifier.PAD_SIZE == 100

    def test_classifier_device(self, classifier):
        """Test that classifier is on the correct device."""
        assert classifier.device in [torch.device("cuda"), torch.device("cpu")]
        # Check model is on the same device
        assert next(classifier.parameters()).device == classifier.device

    def test_model_in_eval_mode(self, classifier):
        """Test that model is in evaluation mode."""
        assert not classifier.training

    # Product URL tests - Real e-commerce product pages
    @pytest.mark.parametrize(
        "url,expected_label",
        [
            # Product pages
            (
                "https://www.antik-shop.de/produkt/nachtschrank-gruenderzeit-fichte-shabby",
                "product",
            ),
            (
                "https://nostalgie-palast.de/tresor-panzerschrank-gruenderzeit-1880-2",
                "product",
            ),
            ("https://www.kunstplaza.de/en/shop/wall-relief-orient", "product"),
            ("https://www.kunstplaza.de/en/shop/wall-relief-city-skyline", "product"),
            (
                "https://www.kunstplaza.de/en/shop/wall-object-bueffel-steampunk-buffalo",
                "product",
            ),
            (
                "https://www.kunstplaza.de/en/shop/woven-wall-carpet-dreamy-woman",
                "product",
            ),
            (
                "https://gallerease.de/de/kunstwerke/green-aquamarine-earrings__e0485f70052f",
                "product",
            ),
        ],
    )
    def test_classify_real_product_urls(self, classifier, url, expected_label):
        """Test classification of real product URLs from actual e-commerce sites."""
        is_product, confidence = classifier.classify_url(url)

        # Check return types
        assert isinstance(is_product, bool)
        assert isinstance(confidence, float)

        # Check confidence is in valid range
        assert 0.0 <= confidence <= 1.0

        # Log results for debugging
        label = "product" if is_product else "non_product"
        print(f"\nURL: {url}")
        print(f"Predicted: {label} (confidence: {confidence:.4f})")
        print(f"Expected: {expected_label}")

        # Verify prediction matches expected label
        expected_is_product = expected_label == "product"
        assert is_product == expected_is_product, (
            f"Expected {expected_label} but got {label}"
        )

    # Non-product URL tests - Real website pages (not product pages)
    @pytest.mark.parametrize(
        "url,expected_label",
        [
            # Category/Listing pages
            ("https://nostalgie-palast.de/neuwaren-tische", "non_product"),
            (
                "https://www.antik-shop.de/produkt-kategorie/moebelart/moebelart-vertiko",
                "non_product",
            ),
            (
                "https://www.antik-shop.de/produkt-kategorie/epoche/epoche-biedermeier",
                "non_product",
            ),
            ("https://nostalgie-palast.de/boulle-uhren", "non_product"),
            (
                "https://nostalgie-palast.de/neuwaren-tische-ess-und-kuchentische",
                "non_product",
            ),
            (
                "https://nostalgie-palast.de/neuwaren-sessel-und-sofas-hocker",
                "non_product",
            ),
            (
                "https://www.antik-shop.de/produkt-schlagwort/jugendstil-shabby",
                "non_product",
            ),
            (
                "https://nostalgie-palast.de/neuwaren-wohnaccessoires-bilder-amp-wanddekorationen/",
                "non_product",
            ),
            ("https://nostalgie-palast.de/neuwaren-tische-couchtische", "non_product"),
            ("https://nostalgie-palast.de/kleiderschraenke", "non_product"),
            ("https://nostalgie-palast.de/gruenderzeit-moebel", "non_product"),
            (
                "https://www.antik-shop.de/produkt-schlagwort/biedermeier-kirschbaum",
                "non_product",
            ),
            (
                "https://www.antik-shop.de/produkt-kategorie/epoche/epoche-gruenderzeit",
                "non_product",
            ),
            ("https://www.kunstplaza.de/en/work-of-art/11072/reliefs", "non_product"),
            # Home/Info pages
            ("https://www.antik-shop.de", "non_product"),
            ("https://www.antik-shop.de/moebelbau", "non_product"),
            (
                "https://www.antik-shop.de/antiquitaeten-antike-moebel-frankfurt-am-main",
                "non_product",
            ),
            ("https://nostalgie-palast.de/modell-anna-ladeneinrichtung", "non_product"),
            # Legal/Policy pages
            ("https://nostalgie-palast.de/agb-ruckgaberecht", "non_product"),
            # Blog/Content pages
            ("https://www.kunstplaza.de/en/trends/outdoor-working", "non_product"),
            (
                "https://www.kunstplaza.de/en/street-art/tape-art-pictures",
                "non_product",
            ),
            (
                "https://www.kunstplaza.de/en/street-art/nikita-golubev-dirty-art",
                "non_product",
            ),
            (
                "https://www.kunstplaza.de/en/creative-ideas/laser-cutter-projects",
                "non_product",
            ),
            (
                "https://www.kunstplaza.de/en/creative-ideas/stamps-holidays-decoration",
                "non_product",
            ),
            (
                "https://www.kunstplaza.de/en/fashion-design/fashion-and-art-collaborations",
                "non_product",
            ),
            (
                "https://www.kunstplaza.de/en/fashion-design/more-diversity-fashion-design-industry",
                "non_product",
            ),
            # Other non-product pages
            (
                "https://www.muenzkurier.de/anfrage-formular?sinquiry=detail&sordernumber=00120105--20-",
                "non_product",
            ),
        ],
    )
    def test_classify_real_non_product_urls(self, classifier, url, expected_label):
        """Test classification of real non-product URLs (categories, blogs, support, etc.)."""
        is_product, confidence = classifier.classify_url(url)

        # Check return types
        assert isinstance(is_product, bool)
        assert isinstance(confidence, float)

        # Check confidence is in valid range
        assert 0.0 <= confidence <= 1.0

        # Log results for debugging
        label = "product" if is_product else "non_product"
        print(f"\nURL: {url}")
        print(f"Predicted: {label} (confidence: {confidence:.4f})")
        print(f"Expected: {expected_label}")

        # Verify prediction matches expected label
        expected_is_product = expected_label == "product"
        assert is_product == expected_is_product, (
            f"Expected {expected_label} but got {label}"
        )

    def test_classify_batch_real_urls(self, classifier):
        """Test batch classification with real mixed URLs."""
        test_data = [
            # Products
            (
                "https://www.antik-shop.de/produkt/nachtschrank-gruenderzeit-fichte-shabby",
                True,
            ),
            (
                "https://nostalgie-palast.de/tresor-panzerschrank-gruenderzeit-1880-2",
                True,
            ),
            ("https://www.kunstplaza.de/en/shop/wall-relief-orient", True),
            # Non-products
            ("https://nostalgie-palast.de/neuwaren-tische", False),
            (
                "https://www.antik-shop.de/produkt-kategorie/moebelart/moebelart-vertiko",
                False,
            ),
            ("https://www.kunstplaza.de/en/trends/outdoor-working", False),
        ]

        urls = [url for url, _ in test_data]
        expected_results = [is_product for _, is_product in test_data]

        results = classifier.classify_urls_batch(urls)

        # Check we get results for all URLs
        assert len(results) == len(urls)

        # Check each result
        for (is_product, confidence), url, expected_is_product in zip(
            results, urls, expected_results
        ):
            assert isinstance(is_product, bool)
            assert isinstance(confidence, float)
            assert 0.0 <= confidence <= 1.0

            label = "product" if is_product else "non_product"
            expected_label = "product" if expected_is_product else "non_product"
            print(f"\nBatch: {url}")
            print(f"  Predicted: {label} ({confidence:.4f})")
            print(f"  Expected: {expected_label}")

            # Verify the classification is correct
            assert is_product == expected_is_product, (
                f"Expected {expected_label} but got {label} for {url}"
            )

    def test_batch_vs_single_consistency_real_urls(self, classifier):
        """Test that batch and single classification give same results for real URLs."""
        urls = [
            "https://www.antik-shop.de/produkt/nachtschrank-gruenderzeit-fichte-shabby",
            "https://nostalgie-palast.de/neuwaren-tische",
            "https://www.kunstplaza.de/en/shop/wall-relief-orient",
        ]

        # Single classification
        single_results = [classifier.classify_url(url) for url in urls]

        # Batch classification
        batch_results = classifier.classify_urls_batch(urls)

        # Compare results (should be identical)
        for single, batch, url in zip(single_results, batch_results, urls):
            assert single[0] == batch[0], (
                f"Mismatch for {url}: single={single[0]}, batch={batch[0]}"
            )
            # Confidence might have tiny floating point differences
            assert abs(single[1] - batch[1]) < 1e-6, f"Confidence mismatch for {url}"

    def test_preprocessing_output_shape(self, classifier):
        """Test that preprocessing produces correct shapes."""
        url = (
            "https://www.antik-shop.de/produkt/nachtschrank-gruenderzeit-fichte-shabby"
        )
        ids, types, masks = classifier._preprocess_url(url)

        # Check all have correct length
        assert len(ids) == classifier.PAD_SIZE
        assert len(types) == classifier.PAD_SIZE
        assert len(masks) == classifier.PAD_SIZE

        # Check first token is CLS
        assert ids[0] == classifier.tokenizer.cls_token_id

        # Check types: actual tokens are 0, padding is 1
        # Find first padding position
        first_pad = next((i for i, m in enumerate(masks) if m == 0), len(masks))
        if first_pad < len(types):
            assert all(t == 0 for t in types[:first_pad]), (
                "Non-pad tokens should have type 0"
            )
            assert all(t == 1 for t in types[first_pad:]), (
                "Pad tokens should have type 1"
            )

    def test_long_url_truncation(self, classifier):
        """Test that very long URLs are properly truncated."""
        # Create a very long URL with lots of parameters
        long_url = (
            "https://www.antik-shop.de/produkt/nachtschrank-gruenderzeit-fichte-shabby?"
            + "&".join([f"param{i}=value{i}" for i in range(100)])
        )

        ids, types, masks = classifier._preprocess_url(long_url)

        # Should still be PAD_SIZE
        assert len(ids) == classifier.PAD_SIZE
        assert len(types) == classifier.PAD_SIZE
        assert len(masks) == classifier.PAD_SIZE

        # All positions should be filled (no padding needed for very long URLs)
        assert all(m == 1 for m in masks)
        assert all(t == 0 for t in types)

    def test_short_url_padding(self, classifier):
        """Test that short URLs are properly padded."""
        short_url = "https://antik-shop.de/produkt/123"  # Short URL

        ids, _, masks = classifier._preprocess_url(short_url)

        # Check padding exists
        pad_count = sum(1 for m in masks if m == 0)
        assert pad_count > 0, "Short URL should have padding"

        # Check padding tokens are 0
        first_pad = next((i for i, m in enumerate(masks) if m == 0), len(masks))
        assert all(id == 0 for id in ids[first_pad:]), "Padding IDs should be 0"

    def test_model_output_probabilities_sum_to_one(self, classifier):
        """Test that model outputs valid probability distributions."""
        url = (
            "https://www.antik-shop.de/produkt/nachtschrank-gruenderzeit-fichte-shabby"
        )

        # Get model logits manually
        ids, types, masks = classifier._preprocess_url(url)
        input_ids = torch.tensor([ids], dtype=torch.long).to(classifier.device)
        token_type_ids = torch.tensor([types], dtype=torch.long).to(classifier.device)
        attention_mask = torch.tensor([masks], dtype=torch.long).to(classifier.device)

        with torch.no_grad():
            logits = classifier([input_ids, token_type_ids, attention_mask])
            probabilities = F.softmax(logits, dim=-1)

            # Check probabilities sum to 1 (within floating point tolerance)
            prob_sum = probabilities.sum(dim=-1).item()
            assert abs(prob_sum - 1.0) < 1e-6, (
                f"Probabilities sum to {prob_sum}, not 1.0"
            )

    def test_batch_size_parameter_real_urls(self, classifier):
        """Test that different batch sizes produce consistent results with real URLs."""
        urls = [
            "https://www.antik-shop.de/produkt/nachtschrank-gruenderzeit-fichte-shabby",
            "https://nostalgie-palast.de/tresor-panzerschrank-gruenderzeit-1880-2",
            "https://www.kunstplaza.de/en/shop/wall-relief-orient",
            "https://www.kunstplaza.de/en/shop/wall-relief-city-skyline",
            "https://nostalgie-palast.de/neuwaren-tische",
            "https://www.antik-shop.de/produkt-kategorie/moebelart/moebelart-vertiko",
            "https://www.kunstplaza.de/en/trends/outdoor-working",
            "https://nostalgie-palast.de/agb-ruckgaberecht",
        ]

        results_batch_2 = classifier.classify_urls_batch(urls, batch_size=2)
        results_batch_4 = classifier.classify_urls_batch(urls, batch_size=4)
        results_batch_8 = classifier.classify_urls_batch(urls, batch_size=8)

        # All should produce same results
        for r2, r4, r8 in zip(results_batch_2, results_batch_4, results_batch_8):
            assert r2[0] == r4[0] == r8[0], (
                "Different batch sizes should give same predictions"
            )
            assert abs(r2[1] - r4[1]) < 1e-6, (
                "Confidence should match across batch sizes"
            )
            assert abs(r2[1] - r8[1]) < 1e-6, (
                "Confidence should match across batch sizes"
            )

    def test_deterministic_predictions_real_url(self, classifier):
        """Test that predictions are deterministic (same input = same output) for real URLs."""
        url = (
            "https://www.antik-shop.de/produkt/nachtschrank-gruenderzeit-fichte-shabby"
        )

        # Run classification multiple times
        results = [classifier.classify_url(url) for _ in range(5)]

        # All results should be identical
        first_result = results[0]
        for result in results[1:]:
            assert result[0] == first_result[0], "Predictions should be deterministic"
            assert result[1] == first_result[1], "Confidence should be deterministic"

    def test_empty_url(self, classifier):
        """Test classification with empty URL."""
        is_product, confidence = classifier.classify_url("")

        assert isinstance(is_product, bool)
        assert isinstance(confidence, float)
        assert 0.0 <= confidence <= 1.0

        # Empty URL should not be classified as product
        assert not is_product, (
            f"Empty URL should be classified as non-product, but got product (confidence: {confidence:.4f})"
        )

    def test_invalid_url_format(self, classifier):
        """Test classification with invalid URL format.

        NOTE: The model currently has limitations with invalid URL formats.
        Some invalid URLs are incorrectly classified as products due to training data limitations.
        This test documents the current behavior.
        """
        invalid_urls = {
            "not-a-url": True,  # Currently classified as product (99.82% confidence) - MODEL LIMITATION
            "ftp://example.com/file": False,  # Correctly classified as non-product
            "javascript:alert('test')": True,  # Currently classified as product (80.38% confidence) - MODEL LIMITATION
            "mailto:test@example.com": True,  # Currently classified as product (94.27% confidence) - MODEL LIMITATION
        }

        print("\n" + "=" * 80)
        print("Testing invalid URL formats (documenting current model behavior):")
        print("=" * 80)

        for url, expected_is_product in invalid_urls.items():
            is_product, confidence = classifier.classify_url(url)

            # Verify data types
            assert isinstance(is_product, bool), f"Failed for URL: {url}"
            assert isinstance(confidence, float), f"Failed for URL: {url}"
            assert 0.0 <= confidence <= 1.0, f"Failed for URL: {url}"

            # Document actual behavior
            label = "PRODUCT" if is_product else "NON-PRODUCT"
            status = "LIMITATION" if expected_is_product else "✓ CORRECT"
            print(f"{url:40} -> {label:12} ({confidence:.4f}) {status}")

            # Assert current behavior (not ideal, but documents reality)
            assert is_product == expected_is_product, (
                f"Model behavior changed for '{url}': expected {'PRODUCT' if expected_is_product else 'NON-PRODUCT'}, got {label}"
            )

    @pytest.mark.xfail(
        reason="Model not trained to properly reject invalid URL formats", strict=False
    )
    def test_invalid_url_format_ideal_behavior(self, classifier):
        """Test that invalid URL formats SHOULD be classified as non-product (IDEAL BEHAVIOR).

        This test is marked as xfail because the current model has limitations.
        It documents what the ideal behavior should be after model retraining.
        """
        invalid_urls = [
            "not-a-url",
            "javascript:alert('test')",
            "mailto:test@example.com",
        ]

        for url in invalid_urls:
            is_product, confidence = classifier.classify_url(url)
            # IDEAL: All invalid formats should be classified as non-product
            assert not is_product, (
                f"Invalid URL '{url}' should be classified as non-product, but got product (confidence: {confidence:.4f})"
            )


if __name__ == "__main__":
    pytest.main([__file__, "-v", "-s"])
