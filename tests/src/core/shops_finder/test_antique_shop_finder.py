import json
import os
from pathlib import Path
from datetime import datetime
import math

import pytest

from src.core.shops_finder import antique_shop_finder as af


def test_extract_domain_variants():
    assert af.extract_domain("https://www.example.com/page") == "example.com"
    assert af.extract_domain("https://example.com") == "example.com"
    assert af.extract_domain("https://sub.domain.co.uk/path") == "sub.domain.co.uk"
    assert af.extract_domain("example.com/path") == "example.com/path"


def test_remove_duplicate_urls():
    results = [
        {"link": "https://a.com/1"},
        {"link": "https://b.com/1"},
        {"link": "https://a.com/1"},
    ]
    unique = af._remove_duplicate_urls(results)
    assert len(unique) == 2
    links = {r["link"] for r in unique}
    assert links == {"https://a.com/1", "https://b.com/1"}


def test_prepare_batch_for_analysis():
    batch = [
        {
            "link": "https://a.com/1",
            "snippet": "s1",
            "query": "q",
            "country": "de",
            "language": "de",
            "some_other_data": "data",
        }
    ]
    pages = af._prepare_batch_for_analysis(batch)
    assert isinstance(pages, list)
    assert pages[0]["url"] == "https://a.com/1"
    assert pages[0]["content"] == "s1"


def test_enrich_analysis_results_adds_domain_and_timestamp():
    analysis = [{"url": "https://example.com/product", "confidence": 0.8}]
    enriched = af._enrich_analysis_results(analysis)
    assert len(enriched) == 1
    r = enriched[0]
    assert "domain" in r and r["domain"] == "example.com"
    # timestamp is ISO parseable
    dt = datetime.fromisoformat(r["timestamp"])
    assert isinstance(dt, datetime)


def test_deduplicate_by_domain_keeps_first():
    rs = [
        {"domain": "a.com", "confidence": 0.1},
        {"domain": "b.com", "confidence": 0.9},
        {"domain": "a.com", "confidence": 0.8},
    ]
    dedup = af._deduplicate_by_domain(rs)
    assert len(dedup) == 2
    domains = {r["domain"] for r in dedup}
    assert domains == {"a.com", "b.com"}
    # ensure the first a.com entry (confidence 0.1) is kept
    a = next(r for r in dedup if r["domain"] == "a.com")
    assert math.isclose(a["confidence"], 0.1, rel_tol=1e-9)


@pytest.mark.asyncio
async def test_load_and_save_progress_file(tmp_path: Path):
    # create temp working dir to avoid writing repo data/
    cwd = tmp_path
    os.chdir(cwd)

    progress_path = cwd / "data" / "analysis_progress.json"
    os.makedirs(cwd / "data", exist_ok=True)

    sample = [{"url": "https://x.com/p", "domain": "x.com"}]
    progress_path.write_text(json.dumps(sample), encoding="utf-8")

    loaded_results, processed = await af._load_progress_from_file(str(progress_path))
    assert len(loaded_results) == 1
    assert "x.com" in processed

    # Test _save_progress_to_file writes deduplicated data
    more = [
        {"url": "https://x.com/p", "domain": "x.com"},
        {"url": "https://y.com/p2", "domain": "y.com"},
    ]
    await af._save_progress_to_file(str(progress_path), more, batch_num=1)
    saved = json.loads(progress_path.read_text(encoding="utf-8"))
    assert isinstance(saved, list)
    assert {r.get("domain") for r in saved} == {"x.com", "y.com"}


@pytest.mark.asyncio
async def test_analyze_pages_in_batches_monkeypatched(monkeypatch, tmp_path: Path):
    # Change CWD so data/ paths are local to tmp
    monkeypatch.chdir(tmp_path)

    # Create fake search_results of length 3
    search_results = [
        {
            "link": "https://a.com/1",
            "snippet": "s1",
            "query": "q1",
            "country": "de",
            "language": "de",
        },
        {
            "link": "https://b.com/2",
            "snippet": "s2",
            "query": "q2",
            "country": "gb",
            "language": "en",
        },
        {
            "link": "https://c.com/3",
            "snippet": "s3",
            "query": "q3",
            "country": "gb",
            "language": "en",
        },
    ]

    # Patch deepseek analyzer to return predictable results
    def fake_analyze(pages):
        return [
            {
                "url": p["url"],
                "is_antique_shop": True if "a.com" in p["url"] else False,
                "confidence": 0.5,
            }
            for p in pages
        ]

    # Patch the imported function in its module path
    from src.core.llms.deepseek import client as deep_client

    monkeypatch.setattr(deep_client, "analyze_antique_shops_batch", fake_analyze)

    results = await af._analyze_pages_in_batches(search_results, batch_size=2)

    # Expect results for a.com and b.com and c.com (since all pages analyzed across batches)
    assert any(r.get("domain") == "a.com" for r in results)
    assert any(r.get("domain") == "b.com" for r in results)
    assert any(r.get("domain") == "c.com" for r in results)

    # verify progress file exists
    progress_path = tmp_path / "data" / "analysis_progress.json"
    assert progress_path.exists()


def test_save_results_writes_file(monkeypatch, tmp_path: Path):
    # Ensure env var is set for constructor
    monkeypatch.setenv("SERPAPI_API_KEY", "fake")

    finder = af.AntiqueShopFinder()
    # Prepare results with required keys
    finder.results = [
        {
            "url": "https://a.com/1",
            "domain": "a.com",
            "is_antique_shop": True,
            "confidence": 0.7,
        },
        {
            "url": "https://b.com/2",
            "domain": "b.com",
            "is_antique_shop": False,
            "confidence": 0.1,
        },
    ]

    # Use tmp data dir
    monkeypatch.chdir(tmp_path)

    path = finder.save_results()
    assert Path(path).exists()
    content = json.loads(Path(path).read_text(encoding="utf-8"))
    assert content["total_analyzed"] == 2
    assert content["antique_shops_found"] == 1
