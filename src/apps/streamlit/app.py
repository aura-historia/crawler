import os
import sys
import streamlit as st
import asyncio
import nest_asyncio
import time
import importlib

# Add the project root to the Python path
sys.path.append(
    os.path.abspath(os.path.join(os.path.dirname(__file__), "..", "..", ".."))
)

import src.core.scraper.base as scrap_base
import src.core.scraper.cleaning.boilerplate_discovery as discovery_mod
from src.core.scraper.cleaning.boilerplate_remover import BoilerplateRemover

# Set ProactorEventLoopPolicy on Windows for subprocess support (needed by Playwright)
if sys.platform == "win32":
    asyncio.set_event_loop_policy(asyncio.WindowsProactorEventLoopPolicy())

# Allow Streamlit to work with other async tasks
nest_asyncio.apply()

try:
    importlib.reload(scrap_base)
    importlib.reload(discovery_mod)

    get_markdown = scrap_base.get_markdown
    get_markdowns = scrap_base.get_markdowns
    BoilerplateDiscovery = discovery_mod.BoilerplateDiscovery
except Exception as e:
    st.error(f"Failed to load core modules: {e}")
    st.stop()

st.set_page_config(page_title="Boilerplate Tester", layout="wide")

# Initialize session state to avoid AttributeErrors
if "discovery_urls" not in st.session_state:
    st.session_state.discovery_urls = []
if "discovery_markdowns" not in st.session_state:
    st.session_state.discovery_markdowns = []
if "discovered_blocks" not in st.session_state:
    st.session_state.discovered_blocks = []
if "discovery_run_id" not in st.session_state:
    st.session_state.discovery_run_id = None

st.title("🛡️ Boilerplate Discovery & Removal Tester")
st.caption(f"Debug: Discovery module loaded from {discovery_mod.__file__}")
st.markdown("""
This tool uses a **block-based structural analysis** to learn a site's boilerplate 'frame'.
1. **Safety Buffer**: Blocks with very few words or containing critical data (Prices, Images) are ignored.
2. **Block Matching**: Uses `difflib.SequenceMatcher` on normalized (whitespace-agnostic) lines to find common blocks.
""")

# Sidebar settings
st.sidebar.header("Discovery Settings")
remove_noise = st.sidebar.checkbox(
    "Remove Noise Sections (Related Products, Social Media, etc.)", value=True
)

if st.sidebar.button("🗑️ Clear Session Data"):
    for key in st.session_state.keys():
        del st.session_state[key]
    st.rerun()

# Main UI
col1, col2 = st.columns([1, 1])

with col1:
    st.header("1. Discovery Phase")
    urls_input = st.text_area(
        "Enter discovery URLs (one per line)",
        height=150,
        placeholder="https://example.com/product1\nhttps://example.com/product2\n...",
    )
    discovery_urls = [u.strip() for u in urls_input.split("\n") if u.strip()]

    if st.button("🔍 Discover Boilerplate"):
        if len(discovery_urls) < 2:
            st.error("Please provide at least 2 URLs for comparison.")
        else:
            # Clear previous results to avoid showing old data
            st.session_state.discovered_blocks = []
            # Initialize session state immediately to avoid AttributeErrors later
            st.session_state.discovery_urls = discovery_urls
            st.session_state.discovery_run_id = int(
                time.time()
            )  # Unique ID for this run to force UI refresh

            # Progress bar container for fetching
            fetch_progress = st.progress(0, text="Ready to fetch...")

            try:
                # Sequential Fetching with progress tracking
                fetch_start = time.perf_counter()

                def on_fetch_progress(curr, total):
                    fetch_progress.progress(
                        curr / total,
                        text=f"Fetched {curr}/{total} pages sequentially...",
                    )

                # Add timestamp to URLs to force fresh fetch and bypass any caching
                discovery_urls = [u + f"?t={int(time.time())}" for u in discovery_urls]

                markdowns = asyncio.run(get_markdowns(discovery_urls))
                fetch_time = time.perf_counter() - fetch_start

                fetch_progress.progress(
                    1.0, text=f"Fetched all {len(markdowns)} pages."
                )
                st.session_state.discovery_markdowns = markdowns

                # Overview of fetched markdowns
                with st.expander("📄 Overview of Fetched Markdowns"):
                    for i, md in enumerate(markdowns):
                        st.subheader(f"Sample {i + 1}: {discovery_urls[i]}")
                        st.metric("Length", f"{len(md)} chars")
                        st.text_area(
                            "Markdown Content", md, height=300, key=f"md_overview_{i}"
                        )

                if not any(markdowns):
                    st.error(
                        "No markdowns were successfully fetched. Check URLs and network."
                    )
                    st.stop()

                # Discovery matching phase
                match_progress = st.progress(0, text="Starting pairwise matching...")
                discovery = BoilerplateDiscovery()
                match_start = time.perf_counter()

                detailed_results = discovery.find_common_blocks_detailed(markdowns)

                # Debug: Check detailed_results structure
                if not isinstance(detailed_results, list):
                    st.error(
                        f"detailed_results is not a list: {type(detailed_results)}"
                    )
                    raise ValueError("Invalid detailed_results format")

                # detailed_results is now List[List[str]] of common blocks
                unique_blocks = detailed_results

                match_time = time.perf_counter() - match_start

                st.session_state.discovered_blocks = unique_blocks
                total_blocks_length = sum(len(block) for block in unique_blocks)
                total_time = time.perf_counter() - fetch_start
                st.success(
                    f"Discovered {len(unique_blocks)} unique boilerplate blocks from {len(markdowns)} pages (total {total_blocks_length} lines)! Performance: Fetch {fetch_time:.4f}s, Match {match_time:.4f}s, Total {total_time:.4f}s"
                )
            except Exception as e:
                st.error(f"Discovery failed: {e}")

            if "discovered_blocks" in st.session_state:
                st.subheader("Discovered Blocks Overview")
                blocks_data = [
                    {
                        "Index": i + 1,
                        "Length (lines)": len(block),
                        "Preview": (
                            "\n".join(block)[:100] + "..."
                            if len("\n".join(block)) > 100
                            else "\n".join(block)
                        ),
                    }
                    for i, block in enumerate(st.session_state.discovered_blocks)
                ]
                st.dataframe(blocks_data, use_container_width=True)

                with st.expander("🔍 See Full Block Details"):
                    for i, block in enumerate(st.session_state.discovered_blocks):
                        st.text_area(
                            f"Block {i + 1} ({len(block)} lines)",
                            "\n".join(block),
                            height=100,
                        )

with col2:
    st.header("2. Testing Phase")
    test_url = st.text_input(
        "Enter a URL to clean", placeholder="https://example.com/product-to-test"
    )

    if st.button("✨ Clean Markdown") and "discovered_blocks" in st.session_state:
        if not test_url:
            st.error("Please enter a test URL.")
        else:
            with st.spinner("Fetching and cleaning..."):
                try:
                    fetch_start = time.perf_counter()
                    # Add timestamp to URL to force fresh fetch
                    test_url_with_ts = test_url + f"?t={int(time.time())}"
                    raw_md = asyncio.run(get_markdown(test_url_with_ts))
                    fetch_time = time.perf_counter() - fetch_start

                    remover = BoilerplateRemover()
                    clean_start = time.perf_counter()
                    clean_md = remover.clean(
                        raw_md,
                        st.session_state.discovered_blocks,
                        remove_noise=remove_noise,
                    )
                    clean_time = time.perf_counter() - clean_start

                    deletion_rate = len(clean_md) / len(raw_md) if raw_md else 0
                    total_time = time.perf_counter() - fetch_start
                    st.metric(
                        "Processing Time",
                        f"Fetch: {fetch_time:.4f}s, Clean: {clean_time:.4f}s",
                    )
                    st.metric("Deletion coefficient", f"{deletion_rate * 100:.2f}%")

                    # Show original and cleaned stacked
                    st.subheader("Original")
                    st.markdown(f"**Length:** {len(raw_md)} chars")
                    st.text_area("Raw Markdown", raw_md, height=400)

                    st.subheader("Cleaned")
                    st.markdown(f"**Length:** {len(clean_md)} chars")
                    st.text_area("Cleaned Markdown", clean_md, height=400)

                except Exception as e:
                    st.error(f"Cleaning failed: {e}")
    elif "discovered_blocks" not in st.session_state:
        st.info("Discover some boilerplate first on the left!")

st.divider()
st.caption("Aura Historia Scraping Tools - Boilerplate Verification App")
