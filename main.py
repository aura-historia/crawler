import sys
import asyncio
import nest_asyncio
import streamlit as st
import json
from crawl4ai import BrowserConfig, CrawlerRunConfig, CacheMode, AsyncWebCrawler
from extruct import extract as extruct_extract
from w3lib.html import get_base_url
from src.app.extractor import parse_schema
from src.core.utils.standards_extractor import extract_standard

# --- Windows asyncio fix ---
if sys.platform.startswith("win"):
    # Use ProactorEventLoopPolicy for subprocess support (required by Playwright)
    asyncio.set_event_loop_policy(asyncio.WindowsProactorEventLoopPolicy())
nest_asyncio.apply()

st.set_page_config(page_title="🕸️ Web Data Extractor", page_icon="🕸️", layout="wide")
st.title("🕸️ Universal Web Data Extractor")

with st.container(border=True):
    url = st.text_input(
        "🌐 **Enter a URL**",
        placeholder="https://example.com/product/123",
        key="url_input",
    )
    strategy = st.radio(
        "🔍 Choose extraction strategy:",
        ["AI Schema (CSS)", "Standards Extractor"],
        horizontal=True,
        index=1,
    )
    extract_btn = st.button("🚀 Extract", type="primary", use_container_width=True)


def beautify_json(data):
    try:
        return json.dumps(data, indent=2, ensure_ascii=False)
    except Exception:
        return str(data)


def display_json_sections(data: dict):
    syntaxes = []
    for syntax, entries in data.items():
        if not entries:
            continue
        clean_entries = []
        for entry in entries:
            if (
                isinstance(entry, dict)
                and list(entry.keys()) == ["@id"]
                or (
                    "http://www.w3.org/1999/xhtml/vocab#role" in entry
                    and len(entry.keys()) <= 2
                )
            ):
                continue
            clean_entries.append(entry)
        if clean_entries:
            syntaxes.append((syntax, clean_entries))
    for syntax, entries in syntaxes:
        with st.expander(
            f"📦 {syntax.upper()} — {len(entries)} entries", expanded=True
        ):
            for entry in entries:
                st.code(beautify_json(entry), language="json", line_numbers=False)


async def extract_and_display_standards(url: str):
    browser_config = BrowserConfig(headless=True, verbose=False)
    run_config = CrawlerRunConfig(
        cache_mode=CacheMode.BYPASS, stream=False, check_robots_txt=True, verbose=True
    )

    async with AsyncWebCrawler(config=browser_config) as crawler:
        result = await crawler.arun(url=url, config=run_config)
        if result.success:
            base_url = get_base_url(result.html, result.url)
            html = result.html
            data = extruct_extract(
                html,
                base_url=base_url,
                syntaxes=[
                    "microdata",
                    "opengraph",
                    "json-ld",
                    "rdfa",
                ],
            )

    st.subheader("✅ Schritt 1: Kombiniertes Endergebnis (Produkt)")
    result = await extract_standard(
        data, url, preferred=["microdata", "json-ld", "rdfa", "opengraph"]
    )
    st.code(beautify_json(result), language="json")

    st.subheader("🔍 Schritt 2: Extrahierte Rohdaten pro Syntax")
    display_json_sections(data)


if extract_btn:
    if not url:
        st.error("❌ Bitte gib eine gültige URL ein.")
    else:
        st.info(f"Extrahiere Daten mit **{strategy}**...")
        with st.spinner("⏳ Daten werden geladen und analysiert..."):
            try:
                if strategy == "AI Schema (CSS)":
                    loop = asyncio.get_event_loop()
                    nest_asyncio.apply()
                    result = loop.run_until_complete(parse_schema(url))
                    st.success("✅ Extraktion abgeschlossen!")
                    st.code(beautify_json(result), language="json")
                else:
                    loop = asyncio.get_event_loop()
                    nest_asyncio.apply()
                    loop.run_until_complete(extract_and_display_standards(url))
                    st.success("✅ Extraktion abgeschlossen!")
            except Exception as e:
                st.error(f"⚠️ Extraktion fehlgeschlagen: {e}")
