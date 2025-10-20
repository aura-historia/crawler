import json
from unittest.mock import AsyncMock, MagicMock, patch

import pytest

from src.core.utils.spider import crawl_urls, evaluate_urls


@patch("src.core.utils.spider.client")
def test_evaluate_urls_single_url(mock_client):
    """Test evaluating a single URL returns correct structure."""
    # Mock the API response
    mock_response = MagicMock()
    mock_response.choices = [MagicMock()]
    mock_response.choices[0].message.content = json.dumps(
        {"evaluations": [{"url": "https://example.com/item1", "confidence": 85}]}
    )
    mock_client.chat.completions.create.return_value = mock_response

    urls = ["https://example.com/item1"]
    result = evaluate_urls(urls)

    # Verify the result structure
    assert "evaluations" in result
    assert len(result["evaluations"]) == 1
    assert result["evaluations"][0]["url"] == "https://example.com/item1"
    assert result["evaluations"][0]["confidence"] == 85
    assert isinstance(result["evaluations"][0]["confidence"], int)


@patch("src.core.utils.spider.client")
def test_evaluate_urls_multiple_urls(mock_client):
    """Test evaluating multiple URLs returns all evaluations."""
    mock_response = MagicMock()
    mock_response.choices = [MagicMock()]
    mock_response.choices[0].message.content = json.dumps(
        {
            "evaluations": [
                {"url": "https://example.com/item1", "confidence": 85},
                {"url": "https://example.com/item2", "confidence": 92},
                {"url": "https://example.com/item3", "confidence": 45},
            ]
        }
    )
    mock_client.chat.completions.create.return_value = mock_response

    urls = [
        "https://example.com/item1",
        "https://example.com/item2",
        "https://example.com/item3",
    ]
    result = evaluate_urls(urls)

    assert len(result["evaluations"]) == 3
    assert all("url" in item for item in result["evaluations"])
    assert all("confidence" in item for item in result["evaluations"])
    assert all(1 <= item["confidence"] <= 100 for item in result["evaluations"])


@patch("src.core.utils.spider.client")
def test_evaluate_urls_calls_api_with_correct_params(mock_client):
    """Test that the Groq API is called with correct parameters."""
    mock_response = MagicMock()
    mock_response.choices = [MagicMock()]
    mock_response.choices[0].message.content = json.dumps(
        {"evaluations": [{"url": "https://example.com/item", "confidence": 75}]}
    )
    mock_client.chat.completions.create.return_value = mock_response

    urls = ["https://example.com/item"]
    evaluate_urls(urls)

    # Verify API was called once
    assert mock_client.chat.completions.create.call_count == 1

    # Get the call arguments
    call_args = mock_client.chat.completions.create.call_args

    # Verify model
    assert call_args.kwargs["model"] == "moonshotai/kimi-k2-instruct-0905"

    # Verify messages structure
    messages = call_args.kwargs["messages"]
    assert len(messages) == 2
    assert messages[0]["role"] == "system"
    assert messages[1]["role"] == "user"
    assert "https://example.com/item" in messages[1]["content"]

    # Verify response_format is a json_schema
    assert call_args.kwargs["response_format"]["type"] == "json_schema"


@patch("src.core.utils.spider.client")
def test_evaluate_urls_confidence_boundaries(mock_client):
    """Test that confidence scores are within valid range (1-100)."""
    mock_response = MagicMock()
    mock_response.choices = [MagicMock()]
    mock_response.choices[0].message.content = json.dumps(
        {
            "evaluations": [
                {"url": "https://example.com/low", "confidence": 1},
                {"url": "https://example.com/high", "confidence": 100},
                {"url": "https://example.com/mid", "confidence": 50},
            ]
        }
    )
    mock_client.chat.completions.create.return_value = mock_response

    urls = [
        "https://example.com/low",
        "https://example.com/high",
        "https://example.com/mid",
    ]
    result = evaluate_urls(urls)

    for item in result["evaluations"]:
        assert 1 <= item["confidence"] <= 100


@patch("src.core.utils.spider.client")
def test_evaluate_urls_handles_json_string_response(mock_client):
    """Test that the function handles JSON string responses correctly."""
    # Simulate API returning a JSON string (most common case)
    mock_response = MagicMock()
    mock_response.choices = [MagicMock()]
    json_string = (
        '{"evaluations": [{"url": "https://example.com/test", "confidence": 88}]}'
    )
    mock_response.choices[0].message.content = json_string
    mock_client.chat.completions.create.return_value = mock_response

    result = evaluate_urls(["https://example.com/test"])

    assert result["evaluations"][0]["confidence"] == 88


@pytest.mark.asyncio
async def test_crawl_urls_returns_list():
    """Test that crawl_urls returns a list of URLs."""
    mock_strategy = AsyncMock()
    mock_strategy.arun.return_value = [
        "https://example.com/",
        "https://example.com/page1",
        "https://example.com/page2",
    ]

    mock_crawler = AsyncMock()

    with patch(
        "src.core.utils.spider.BFSNoCycleDeepCrawlStrategy", return_value=mock_strategy
    ):
        with patch("src.core.utils.spider.AsyncWebCrawler", return_value=mock_crawler):
            result = await crawl_urls("https://example.com/")

    assert isinstance(result, list)
    assert len(result) == 3
    assert all(isinstance(url, str) for url in result)


@pytest.mark.asyncio
async def test_crawl_urls_handles_empty_result():
    """Test that crawl_urls handles cases where no URLs are discovered."""
    mock_strategy = AsyncMock()
    mock_strategy.arun.return_value = []
    mock_crawler = AsyncMock()

    with patch(
        "src.core.utils.spider.BFSNoCycleDeepCrawlStrategy", return_value=mock_strategy
    ):
        with patch("src.core.utils.spider.AsyncWebCrawler", return_value=mock_crawler):
            result = await crawl_urls("https://example.com/")

    assert isinstance(result, list)
    assert len(result) == 0


@pytest.mark.asyncio
async def test_crawl_urls_returns_unique_urls():
    """Test that crawl_urls returns unique URLs (via BFS strategy)."""
    # The BFSNoCycleDeepCrawlStrategy should already handle uniqueness
    mock_strategy = AsyncMock()
    mock_strategy.arun.return_value = [
        "https://example.com/",
        "https://example.com/page1",
        "https://example.com/page2",
    ]
    mock_crawler = AsyncMock()

    with patch(
        "src.core.utils.spider.BFSNoCycleDeepCrawlStrategy", return_value=mock_strategy
    ):
        with patch("src.core.utils.spider.AsyncWebCrawler", return_value=mock_crawler):
            result = await crawl_urls("https://example.com/")

    # Verify all URLs are unique
    assert len(result) == len(set(result))


@pytest.mark.asyncio
async def test_crawl_and_evaluate_workflow():
    """Test the complete workflow: crawl URLs then evaluate them."""
    # Mock crawl_urls to return some URLs
    mock_strategy = AsyncMock()
    discovered_urls = [
        "https://example.com/",
        "https://example.com/item1",
        "https://example.com/item2",
        "https://example.com/about",
    ]
    mock_strategy.arun.return_value = discovered_urls
    mock_crawler = AsyncMock()

    # Mock evaluate_urls API response
    mock_client = MagicMock()
    mock_response = MagicMock()
    mock_response.choices = [MagicMock()]
    mock_response.choices[0].message.content = json.dumps(
        {
            "evaluations": [
                {"url": url, "confidence": 50 + i * 10}
                for i, url in enumerate(discovered_urls)
            ]
        }
    )
    mock_client.chat.completions.create.return_value = mock_response

    with patch(
        "src.core.utils.spider.BFSNoCycleDeepCrawlStrategy", return_value=mock_strategy
    ):
        with patch("src.core.utils.spider.AsyncWebCrawler", return_value=mock_crawler):
            with patch("src.core.utils.spider.client", mock_client):
                # Step 1: Crawl
                crawled = await crawl_urls("https://example.com/")
                assert len(crawled) == 4

                # Step 2: Evaluate
                evaluation = evaluate_urls(crawled)
                assert len(evaluation["evaluations"]) == 4
                assert all("confidence" in item for item in evaluation["evaluations"])


@pytest.mark.asyncio
async def test_batch_evaluation():
    """Test evaluating URLs in batches (as done in main function)."""
    # Simulate discovering many URLs
    mock_strategy = AsyncMock()
    discovered_urls = [f"https://example.com/item{i}" for i in range(50)]
    mock_strategy.arun.return_value = discovered_urls
    mock_crawler = AsyncMock()

    mock_client = MagicMock()

    with patch(
        "src.core.utils.spider.BFSNoCycleDeepCrawlStrategy", return_value=mock_strategy
    ):
        with patch("src.core.utils.spider.AsyncWebCrawler", return_value=mock_crawler):
            with patch("src.core.utils.spider.client", mock_client):
                crawled = await crawl_urls("https://example.com/")

                # Simulate batching (like in main)
                batch_size = 20
                all_evaluations = []

                for i in range(0, len(crawled), batch_size):
                    batch = crawled[i : i + batch_size]

                    # Mock response for each batch
                    mock_response = MagicMock()
                    mock_response.choices = [MagicMock()]
                    mock_response.choices[0].message.content = json.dumps(
                        {
                            "evaluations": [
                                {"url": url, "confidence": 75} for url in batch
                            ]
                        }
                    )
                    mock_client.chat.completions.create.return_value = mock_response

                    result = evaluate_urls(batch)
                    all_evaluations.extend(result["evaluations"])

                # Verify we evaluated all URLs in batches
                assert len(all_evaluations) == 50
                # Verify we made multiple API calls (one per batch)
                assert (
                    mock_client.chat.completions.create.call_count == 3
                )  # 50/20 = 3 batches
