import asyncio
import json
from pathlib import Path
from unittest.mock import MagicMock

import pytest

from src.core.classifier.training_data import traning_data_generator as tdg


def test_chunked_basic():
    result = tdg._chunked(range(7), 3)
    assert result == [[0, 1, 2], [3, 4, 5], [6]]


def test_load_antique_shop_urls_with_comments(tmp_path: Path):
    payload = {
        "urls_and_domains": [
            {"url": "https://example.com/1"},
            {"url": "https://example.com/2"},
        ]
    }
    content = "// This is a comment line\n" + json.dumps(
        payload, ensure_ascii=False, indent=2
    )
    p = tmp_path / "shops.json"
    p.write_text(content, encoding="utf-8")

    urls = tdg.load_antique_shop_urls(json_path=p)
    assert isinstance(urls, list)
    assert "https://example.com/1" in urls
    assert "https://example.com/2" in urls


@pytest.mark.asyncio
async def test_process_crawl_result_rate_limit():
    result = MagicMock()
    result.url = "http://shop.local/item"
    result.success = True
    result.html = "Some response... 429 Too Many Requests ..."

    out = await tdg.process_crawl_result(result)
    assert out["label"] == "failed"
    assert out["status"] == "429"


@pytest.mark.asyncio
async def test_process_crawl_result_product_and_nonproduct(monkeypatch):
    # Prepare a successful crawl with HTML
    result = MagicMock()
    result.url = "http://shop.local/item"
    result.success = True
    result.html = "<html><head></head><body>product page</body></html>"

    # Patch extruct_extract to return an empty structured payload (not used deeply)
    monkeypatch.setattr(
        tdg, "extruct_extract", lambda html_content, base_url, syntaxes: {"json-ld": []}
    )

    # Case 1: extract_standard returns truthy -> product
    async def fake_extract_standard_ok(structured, url):
        await asyncio.sleep(0)
        return {"some": "data"}

    monkeypatch.setattr(tdg, "extract_standard", fake_extract_standard_ok)

    out = await tdg.process_crawl_result(result)
    assert out["label"] == "product"
    assert out["status"] == "success"

    # Case 2: extract_standard returns falsy -> non-product
    async def fake_extract_standard_none(structured, url):
        await asyncio.sleep(0)
        return None

    monkeypatch.setattr(tdg, "extract_standard", fake_extract_standard_none)

    out2 = await tdg.process_crawl_result(result)
    assert out2["label"] == "non-product"
    assert out2["status"] == "success"

    # Case 3: invalid HTML (list empty) -> failed with Invalid HTML content
    result2 = MagicMock()
    result2.url = "http://shop.local/empty"
    result2.success = True
    result2.html = [
        ""
    ]  # non-empty list but empty string -> normalized to empty string inside function

    out3 = await tdg.process_crawl_result(result2)
    assert out3["label"] == "failed"
    assert out3["status"] == "Invalid HTML content"

    # Case 4: failed crawl (success False)
    result3 = MagicMock()
    result3.url = "http://shop.local/error"
    result3.success = False
    result3.error_message = "Timeout"

    out4 = await tdg.process_crawl_result(result3)
    assert out4["label"] == "failed"
    assert out4["status"] == "Timeout"


@pytest.mark.asyncio
async def test_writer_task_appends_and_counts(tmp_path: Path):
    q = asyncio.Queue()
    csv_path = tmp_path / "training_data.csv"

    # Prepare three results
    await q.put({"url": "u1", "label": "product", "status": "success"})
    await q.put({"url": "u2", "label": "non-product", "status": "success"})
    await q.put({"url": "u3", "label": "failed", "status": "error"})
    await q.put(None)  # sentinel

    product_count, non_product_count, failed_count = await tdg.writer_task(q, csv_path)

    assert product_count == 1
    assert non_product_count == 1
    assert failed_count == 1

    # Verify file content has header + 3 rows
    text = csv_path.read_text(encoding="utf-8")
    lines = [line for line in text.splitlines() if line.strip()]
    # header + 3 rows
    assert len(lines) == 4
    assert "url" in lines[0]
    assert "u1" in text and "u2" in text and "u3" in text
