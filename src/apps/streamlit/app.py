import os
import sys
import streamlit as st
import asyncio
import nest_asyncio
import time
import importlib
import src.core.scraper.base as scrap_base
import src.core.scraper.cleaning.boilerplate_discovery as discovery_mod
from src.core.scraper.cleaning.processor import BoilerplateRemover

# Ensure the project root is at the front of the path
project_root = os.getcwd()
if project_root not in sys.path:
    sys.path.insert(0, project_root)

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

st.title("🛡️ Boilerplate Discovery & Removal Tester")
st.caption(f"Debug: Discovery module loaded from {discovery_mod.__file__}")
st.markdown("""
This tool uses a **line-based structural analysis** to learn a site's boilerplate 'frame'.
1. **Safety Buffer**: Lines with very few words are ignored to protect product data (Price, Add to Cart).
2. **Line Matching**: Uses `difflib.SequenceMatcher` on normalized (whitespace-agnostic) lines.
""")

# Sidebar settings
st.sidebar.header("Discovery Settings")
min_words = st.sidebar.slider("Safety Buffer (Min Words)", 1, 20, 5)

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
            # Initialize session state immediately to avoid AttributeErrors later
            st.session_state.discovery_urls = discovery_urls

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

                markdowns = asyncio.run(
                    get_markdowns(discovery_urls, progress_callback=on_fetch_progress)
                )
                fetch_time = time.perf_counter() - fetch_start

                fetch_progress.progress(
                    1.0, text=f"Fetched all {len(markdowns)} pages."
                )
                st.session_state.discovery_markdowns = markdowns

                if not any(markdowns):
                    st.error(
                        "No markdowns were successfully fetched. Check URLs and network."
                    )
                    st.stop()

                # Discovery matching phase
                match_progress = st.progress(0, text="Starting pairwise matching...")
                discovery = BoilerplateDiscovery()
                match_start = time.perf_counter()

                def on_match_progress(curr, total):
                    match_progress.progress(
                        curr / total, text=f"Pairwise matching: {curr}/{total}"
                    )

                detailed_results = discovery.find_common_blocks_detailed(
                    markdowns, progress_callback=on_match_progress
                )

                # Compute final unique blocks for the remover
                final_blocks = []
                for res in detailed_results:
                    final_blocks.extend(res["blocks"])
                # Remove duplicates and sort by length descending
                unique_blocks = sorted(set(final_blocks), key=len, reverse=True)
                match_time = time.perf_counter() - match_start

                st.session_state.detailed_results = detailed_results

                st.session_state.discovered_blocks = unique_blocks
                total_blocks_length = sum(len(block) for block in unique_blocks)
                total_time = time.perf_counter() - fetch_start
                st.success(
                    f"Discovered {len(unique_blocks)} unique boilerplate lines from {len(markdowns)} pages (total {total_blocks_length} chars)! Performance: Fetch {fetch_time:.4f}s, Match {match_time:.4f}s, Total {total_time:.4f}s"
                )
            except Exception as e:
                st.error(f"Discovery failed: {e}")

    if "discovered_blocks" in st.session_state:
        with st.expander("🔍 See Final Unique Lines"):
            for i, block in enumerate(st.session_state.discovered_blocks):
                st.text_area(
                    f"Unique Line {i + 1} ({len(block)} chars)", block, height=100
                )

    if (
        "detailed_results" in st.session_state
        and "discovery_urls" in st.session_state
        and "discovery_markdowns" in st.session_state
    ):
        with st.expander("🧬 Pairwise Discovery Details"):
            for res in st.session_state.detailed_results:
                i, j = res["pair"]
                if i >= len(st.session_state.discovery_urls) or j >= len(
                    st.session_state.discovery_urls
                ):
                    continue

                url_i = st.session_state.discovery_urls[i]
                url_j = st.session_state.discovery_urls[j]

                st.markdown("**Match between Pair:**")
                st.caption(f"1. {url_i}")
                st.caption(f"2. {url_j}")
                st.write(f"Found **{res['count']}** common lines.")

                # Show full markdowns side-by-side
                pair_col1, pair_col2 = st.columns(2)
                with pair_col1:
                    st.caption("Full Markdown (1)")
                    st.text_area(
                        f"MD 1 ({len(st.session_state.discovery_markdowns[i])} chars)",
                        st.session_state.discovery_markdowns[i],
                        height=400,
                        key=f"md_{i}_{j}_1",
                    )
                    with st.expander("👁️ Preview (1)"):
                        st.markdown(st.session_state.discovery_markdowns[i])
                with pair_col2:
                    st.caption("Full Markdown (2)")
                    st.text_area(
                        f"MD 2 ({len(st.session_state.discovery_markdowns[j])} chars)",
                        st.session_state.discovery_markdowns[j],
                        height=400,
                        key=f"md_{i}_{j}_2",
                    )
                    with st.expander("👁️ Preview (2)"):
                        st.markdown(st.session_state.discovery_markdowns[j])

                # Show visual Diff
                with st.expander("👁️ Show Visual Diff (Unified Format)"):
                    import difflib

                    diff = difflib.unified_diff(
                        st.session_state.discovery_markdowns[i].splitlines(),
                        st.session_state.discovery_markdowns[j].splitlines(),
                        fromfile=f"Page {i + 1}",
                        tofile=f"Page {j + 1}",
                        lineterm="",
                    )
                    diff_text = "\n".join(diff)
                    if diff_text:
                        st.code(diff_text, language="diff")
                    else:
                        st.info(
                            "No line-by-line differences found (pages might be identical)."
                        )

                for k, block in enumerate(res["blocks"]):
                    st.text_area(
                        f"Pair Line {k + 1} ({len(block)} chars)",
                        block,
                        height=60,
                        key=f"pair_{i}_{j}_{k}",
                    )
                st.divider()

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
                    raw_md = asyncio.run(get_markdown(test_url))
                    fetch_time = time.perf_counter() - fetch_start

                    remover = BoilerplateRemover()
                    clean_start = time.perf_counter()
                    clean_md, hit_rate = remover.clean(
                        raw_md, st.session_state.discovered_blocks
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
