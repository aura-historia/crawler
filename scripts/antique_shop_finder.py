import os
import sys
import json
import asyncio
import time
import re
from typing import List, Dict, Optional
from datetime import datetime

import aiofiles
from serpapi import GoogleSearch
from dotenv import load_dotenv

# Add src directory to path for imports
sys.path.insert(0, os.path.join(os.path.dirname(os.path.dirname(__file__)), "src"))

# Load environment variables
load_dotenv()


def load_cached_search_results(
    filename="data/cached_search_results.json",
) -> Optional[List[Dict]]:
    if os.path.exists(filename):
        with open(filename, "r", encoding="utf-8") as f:
            results = json.load(f)
        print(f"Loaded {len(results)} cached search results from {filename}")
        return results
    else:
        print("No cached search results found.")
        return None


def extract_domain(url: str) -> str:
    """Extract domain from URL."""
    match = re.search(r"https?://(?:www\.)?([^/]+)", url)
    return match.group(1) if match else url


def _remove_duplicate_urls(search_results):
    """Remove duplicate URLs from search results."""
    seen_urls = set()
    unique_results = []

    for result in search_results:
        url = result["link"]
        if url not in seen_urls:
            seen_urls.add(url)
            unique_results.append(result)

    return unique_results


async def _cache_search_results(results):
    """Save search results to cache file."""
    os.makedirs("data", exist_ok=True)
    search_cache_path = os.path.join("data", "cached_search_results.json")

    async with aiofiles.open(search_cache_path, "w", encoding="utf-8") as f:
        await f.write(json.dumps(results, indent=2, ensure_ascii=False))
    print(f"Cached search results saved to: {search_cache_path}")


def _prepare_batch_for_analysis(batch):
    """Prepare a batch of search results for analysis."""
    pages_to_analyze = []

    for search_result in batch:
        pages_to_analyze.append(
            {
                "url": search_result["link"],
                "content": search_result.get("snippet", "(No snippet available)"),
                "query": search_result.get("query", ""),
                "country": search_result.get("country", ""),
                "language": search_result.get("language", ""),
            }
        )

    return pages_to_analyze


def _enrich_analysis_results(batch_analysis):
    """Add timestamp and domain to analysis results."""
    enriched_results = []

    for result in batch_analysis:
        result["timestamp"] = datetime.now().isoformat()
        result["domain"] = extract_domain(result["url"])
        enriched_results.append(result)

    return enriched_results


def _deduplicate_by_domain(results):
    """Deduplicate results by domain, keeping highest confidence score."""
    unique_results_by_domain = {}

    for r in results:
        domain = r["domain"]
        if domain not in unique_results_by_domain:
            unique_results_by_domain[domain] = r

    return list(unique_results_by_domain.values())


async def _analyze_pages_in_batches(search_results, batch_size: int = 5):
    """Analyze pages in batches using DeepSeek."""
    from src.core.llms.deepseek.client import analyze_antique_shops_batch

    print("\n=== Phase 2: Analyzing pages with DeepSeek (batched) ===\n")
    analysis_results = []

    for start in range(0, len(search_results), batch_size):
        batch = search_results[start : start + batch_size]
        print(f"\nAnalyzing batch {start // batch_size + 1} ({len(batch)} URLs)...")

        pages_to_analyze = _prepare_batch_for_analysis(batch)
        batch_analysis = analyze_antique_shops_batch(pages_to_analyze)

        analysis_results.extend(_enrich_analysis_results(batch_analysis))
        await asyncio.sleep(1)

    return analysis_results


class AntiqueShopFinder:
    """Finds and validates antique online shops using SerpAPI and DeepSeek."""

    def __init__(self, serpapi_key: Optional[str] = None):
        """
        Initialize the Antique Shop Finder.

        Args:
            serpapi_key: SerpAPI key. If None, will try to load from environment.
        """
        self.serpapi_key = serpapi_key or os.getenv("SERPAPI_API_KEY")
        if not self.serpapi_key:
            raise ValueError("SERPAPI_API_KEY not found in environment variables")

        # Search queries for finding antique shops
        # English queries
        self.search_queries_en = [
            "antique shop online",
            "buy antiques online",
            "vintage antique store",
            "antique furniture online shop",
            "antique collectibles online",
            "rare antiques for sale",
            "antique dealer online",
            "antique marketplace online",
            "militaria shop online",
            "buy militaria online",
            "military antiques for sale",
            "military collectibles shop",
            "war memorabilia online store",
            "vintage military items shop",
        ]

        # German queries
        self.search_queries_de = [
            "antik shop online",
            "antiquitäten online kaufen",
            "vintage antiquitäten geschäft",
            "antike möbel online shop",
            "antike sammlerstücke online",
            "seltene antiquitäten kaufen",
            "antiquitätenhändler online",
            "antiquitäten marktplatz online",
            "militaria shop online",
            "militaria online kaufen",
            "militärische antiquitäten kaufen",
            "militärische sammlerstücke shop",
            "kriegserinnerungen online shop",
            "vintage militär artikel shop",
        ]

        self.results = []

    def search_google(
        self, query: str, num_pages: int = 3, country: str = "us", language: str = "en"
    ) -> List[Dict]:
        """
        Search Google using SerpAPI with pagination.

        Args:
            query: Search query
            num_pages: Number of pages to retrieve (default 3)
            country: Country code (default "us")
            language: Language code (default "en")

        Returns:
            List of search results with URLs and metadata
        """
        all_results = []

        for page in range(num_pages):
            print(
                f"Searching: '{query}' ({country}/{language}) - Page {page + 1}/{num_pages}"
            )

            params = {
                "q": query,
                "api_key": self.serpapi_key,
                "num": 10,  # Results per page
                "start": page * 10,  # Pagination offset
                "gl": country,  # Country
                "hl": language,  # Language
            }

            try:
                search = GoogleSearch(params)
                results = search.get_dict()

                if "organic_results" in results:
                    for result in results["organic_results"]:
                        all_results.append(
                            {
                                "title": result.get("title", ""),
                                "link": result.get("link", ""),
                                "snippet": result.get("snippet", ""),
                                "query": query,
                                "page": page + 1,
                                "country": country,
                                "language": language,
                            }
                        )

                # Be respectful with API calls
                time.sleep(1)

            except Exception as e:
                print(
                    f"Error searching '{query}' ({country}/{language}) page {page + 1}: {e}"
                )
                continue

        return all_results

    async def process_searches(
        self, num_pages_per_query: int = 3, max_urls_to_analyze: Optional[int] = None
    ):
        """
        Process all search queries and analyze the results.

        Args:
            num_pages_per_query: Number of result pages per query
            max_urls_to_analyze: Maximum number of URLs to analyze (None for all)
        """
        # Phase 1: Get unique search results (cached or fresh)
        unique_results = await self._get_unique_search_results(num_pages_per_query)

        # Limit URLs if specified
        if max_urls_to_analyze:
            unique_results = unique_results[:max_urls_to_analyze]

        # Phase 2: Analyze pages
        analysis_results = await _analyze_pages_in_batches(unique_results)

        # Deduplicate and store results
        self.results = _deduplicate_by_domain(analysis_results)
        self.print_summary()

    async def _get_unique_search_results(self, num_pages_per_query: int):
        """Get unique search results from cache or by performing searches."""
        cached = load_cached_search_results()
        if cached:
            print("✓ Using cached search results — skipping SerpAPI queries.")
            return cached

        # Perform searches and cache results
        all_search_results = self._perform_all_searches(num_pages_per_query)
        unique_results = _remove_duplicate_urls(all_search_results)

        print(f"\nTotal unique URLs found: {len(unique_results)}")
        await _cache_search_results(unique_results)

        return unique_results

    def _perform_all_searches(self, num_pages_per_query: int):
        """Perform all searches across different locales."""
        locales = self._get_locale_configurations()
        all_search_results = []

        print("\n=== Phase 1: Collecting URLs from SerpAPI ===\n")
        for locale in locales:
            locale_results = self._search_locale(locale, num_pages_per_query)
            all_search_results.extend(locale_results)

        return all_search_results

    def _get_locale_configurations(self):
        """Define country/language combinations with their respective queries."""
        return [
            {
                "country": "de",
                "language": "de",
                "name": "Germany (German)",
                "queries": self.search_queries_de,
            },
            {
                "country": "gb",
                "language": "en",
                "name": "UK (English)",
                "queries": self.search_queries_en,
            },
        ]

    def _search_locale(self, locale: dict, num_pages_per_query: int):
        """Search all queries for a given locale."""
        print(f"\nSearching in {locale['name']}:")
        print(f"Using {len(locale['queries'])} queries (including militaria searches)")

        locale_results = []
        for query in locale["queries"]:
            results = self.search_google(
                query,
                num_pages=num_pages_per_query,
                country=locale["country"],
                language=locale["language"],
            )
            locale_results.extend(results)
            print(f"  Found {len(results)} results for '{query}'")

        return locale_results

    def print_summary(self):
        """Print a summary of the findings."""
        print("\n" + "=" * 80)
        print("=== SUMMARY ===")
        print("=" * 80 + "\n")

        antique_shops = [r for r in self.results if r.get("is_antique_shop")]

        # Ensure each entry has 'confidence_score'
        for shop in antique_shops:
            if "confidence_score" not in shop and "confidence" in shop:
                shop["confidence_score"] = int(shop["confidence"] * 100)

        print(f"Analysis Method: {'DeepSeek LLM'}")
        print(f"Total URLs analyzed: {len(self.results)}")
        print(f"Antique shops found: {len(antique_shops)}")
        if len(self.results) > 0:
            print(
                f"Success rate: {len(antique_shops) / len(self.results) * 100:.1f}%\n"
            )

    def save_results(self, filename: str = "antique_shops_found.json"):
        """Save results to JSON files."""
        # Ensure data directory exists
        os.makedirs("data", exist_ok=True)

        # Save full results
        output_path = os.path.join("data", filename)
        with open(output_path, "w", encoding="utf-8") as f:
            json.dump(
                {
                    "timestamp": datetime.now().isoformat(),
                    "total_analyzed": len(self.results),
                    "antique_shops_found": len(
                        [r for r in self.results if r["is_antique_shop"]]
                    ),
                    "results": self.results,
                },
                f,
                indent=2,
                ensure_ascii=False,
            )

        print(f"\n✓ Full results saved to: {output_path}")

        # Save URLs and domains only
        urls_domains_path = os.path.join("data", "antique_shops_urls_domains.json")
        urls_domains_data = []

        for result in self.results:
            urls_domains_data.append(
                {
                    "url": result["url"],
                    "domain": result["domain"],
                    "is_antique_shop": result["is_antique_shop"],
                }
            )

        with open(urls_domains_path, "w", encoding="utf-8") as f:
            json.dump(
                {
                    "timestamp": datetime.now().isoformat(),
                    "total_analyzed": len(urls_domains_data),
                    "antique_shops_found": len(
                        [r for r in urls_domains_data if r["is_antique_shop"]]
                    ),
                    "urls_and_domains": urls_domains_data,
                },
                f,
                indent=2,
                ensure_ascii=False,
            )

        print(f"URLs and domains saved to: {urls_domains_path}")

        return output_path, urls_domains_path


async def main():
    """Main function to run the antique shop finder."""
    print("=" * 80)
    print("ANTIQUE SHOP FINDER")
    print("=" * 80)
    print("\nThis script will:")
    print("1. Search Google for antique shops using SerpAPI")
    print("2. Collect URLs from multiple search queries")
    print("3. Analyze each page with  DeepSeek LLM")
    print("4. Save results to a JSON file\n")

    try:
        # Initialize finder
        finder = AntiqueShopFinder()

        await finder.process_searches(num_pages_per_query=4, max_urls_to_analyze=1)

        # Save results
        finder.save_results()

        print("\n" + "=" * 80)
        print("Done! Check the following files:")
        print("  - data/antique_shops_found.json (full results)")
        print("  - data/antique_shops_urls_domains.json (URLs and domains)")
        print("=" * 80)

    except Exception as e:
        print(f"\nERROR: {e}")
        import traceback

        traceback.print_exc()


if __name__ == "__main__":
    asyncio.run(main())
