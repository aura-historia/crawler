# DynamoDB Schema - Single Table Design

## Table Overview

**Table Name:** `aura-historia-data` (configured via `DYNAMODB_TABLE_NAME`)

**Design Pattern:** Single-table design with composite keys

## Key Schema

| Attribute | Type | Description |
|-----------|------|-------------|
| `pk` | String (HASH) | Partition key: 'SHOP#' + domain (e.g., 'SHOP#example.com') |
| `sk` | String (RANGE) | Sort key: 'META#' or 'URL#<full_url>' |

## Item Types

### 1. Shop Metadata (META#)

Stores information about a shop/domain and whether structured-data standards
were detected during crawls.

**Sort Key:** `META#`

**Attributes:**
```json
{
  "pk": "SHOP#example.com",
  "sk": "META#",
  "domain": "example.com",
  "core_domain_name": "example",
  "shop_name": "Example Shop",
  "shop_country": "COUNTRY#DE",
  "last_crawled_start": "2023-10-27T00:00:00Z",
  "last_crawled_end": "2023-10-27T23:59:59Z",
  "last_scraped_start": "2023-10-27T12:00:00Z",
  "last_scraped_end": "2023-10-27T23:59:59Z"
}
```

| Attribute | Type | Description |
|-----------|------|-------------|
| `pk` | String | "SHOP#" + domain name |
| `sk` | String | Fixed value: "META#" |
| `domain` | String | Domain name (duplicate for convenience) |
| `core_domain_name` | String | The core domain name, extracted (e.g., 'example' from 'www.example.co.uk'). Used by a GSI to find related domains. |
| `shop_name` | String (optional) | Human-readable shop name, if available. This is a convenience field for downstream systems. |
| `shop_country` | String | Country identifier prefixed with `COUNTRY#` (e.g., `COUNTRY#DE`). |
| `last_crawled_start` | String | ISO 8601 timestamp marking when the latest crawl began. |
| `last_crawled_end` | String | ISO 8601 timestamp marking when the latest crawl finished. |
| `last_scraped_start` | String | ISO 8601 timestamp marking when the latest scrape began. |
| `last_scraped_end` | String | ISO 8601 timestamp marking when the latest scrape finished. |

### 2. URL Entry (URL#)

Stores information about individual URLs from a domain.

**Sort Key:** `URL#<full_url>`

**Attributes:**
```json
{
  "pk": "SHOP#example.com",
  "sk": "URL#https://example.com/products/item-123",
  "url": "https://example.com/products/item-123",
  "type": "product",
  "hash": "a1b2c3d4..."
}
```

| Attribute | Type | Description |
|-----------|------|-------------|
| `pk` | String | "SHOP#" + domain name |
| `sk` | String | "URL#" + full URL |
| `url` | String | Full URL |
| `type` | String | Type of page (category, product, listing, etc.). Used for product discovery queries. |
| `hash` | String | SHA256 hash of status+price to detect changes |

## Global Secondary Indexes (GSIs)

Each GSI uses dedicated attributes (`gsi<n>_pk`, `gsi<n>_sk`) so items only project into the index they target.

### GSI1 – ProductTypeIndex

- **Purpose:** Query all URLs of a specific type (e.g., products) within a shop.
- **Key Schema:**
    - **HASH Key (`gsi1_pk`):** Mirrors the item's `pk` (e.g., `SHOP#example.com`).
    - **RANGE Key (`gsi1_sk`):** Mirrors the item's `type` (e.g., `product`).
- **Projection:** `ALL`

### GSI2 – CountryLastCrawledIndex

- **Purpose:** Find shops from a specific country that were crawled within a date range, or find new shops that have never been crawled.
- **Key Schema:**
    - **HASH Key (`gsi2_pk`):** `shop_country` (e.g., `COUNTRY#DE`). Set for all shops with a country.
    - **RANGE Key (`gsi2_sk`):** State-prefixed timestamp for lexicographic sorting.
- **Projection:** `domain`, `last_crawled_start`, `last_crawled_end`

#### State Prefixes in `gsi2_sk`:

| State | Prefix | Example | Query Pattern |
|-------|--------|---------|---------------|
| **Never Crawled** | `NEVER#` | `NEVER#` | `begins_with(gsi2_sk, 'NEVER#')` |
| **In Progress** | `PROGRESS#` | `PROGRESS#2026-01-15T10:00:00Z` | `gsi2_sk BETWEEN 'PROGRESS#' AND 'PROGRESS#~'` |
| **Completed** | `DONE#` | `DONE#2026-01-15T12:00:00Z` | `gsi2_sk BETWEEN 'DONE#' AND 'DONE#2026-01-13...'` |

#### Query Strategies:

**Find shops needing crawl (old or never crawled):**
```python
# Get never-crawled shops
begins_with(gsi2_sk, 'NEVER#')

# Get shops crawled before cutoff
gsi2_sk BETWEEN 'DONE#' AND 'DONE#2026-01-13T00:00:00Z'
```

**Exclude in-progress shops automatically:**
- In-progress shops have `PROGRESS#` prefix
- Queries for `NEVER#` or `DONE#` naturally exclude them

**Monitor stuck crawls:**
```python
# Find crawls in-progress > 2 hours
gsi2_sk BETWEEN 'PROGRESS#' AND 'PROGRESS#2026-01-15T08:00:00Z'
```

#### Lifecycle Example:

```python
# 1. New shop registered
gsi2_sk = "NEVER#"

# 2. Crawl starts
gsi2_sk = "PROGRESS#2026-01-15T10:00:00Z"  # When started

# 3. Crawl completes
gsi2_sk = "DONE#2026-01-15T12:00:00Z"      # When finished
```

**Benefits:**
- ✅ Shops never disappear from index
- ✅ State is explicit and queryable
- ✅ Easy to find stuck/orphaned crawls
- ✅ Lexicographic sorting works naturally
- ✅ Minimal storage - NEVER# has no timestamp


### GSI3 – CountryLastScrapedIndex

- **Purpose:** Find shops from a specific country that were scraped for product data within a date range, or find shops that have never been scraped.
- **Key Schema:**
    - **HASH Key (`gsi3_pk`):** `shop_country` (e.g., `COUNTRY#DE`). Set for all shops with a country.
    - **RANGE Key (`gsi3_sk`):** State-prefixed timestamp for lexicographic sorting.
- **Projection:** `domain`, `last_scraped_start`, `last_scraped_end`, `last_crawled_end`

#### State Prefixes in `gsi3_sk`:

| State | Prefix | Example | Query Pattern |
|-------|--------|---------|---------------|
| **Never Scraped** | `NEVER#` | `NEVER#` | `begins_with(gsi3_sk, 'NEVER#')` |
| **In Progress** | `PROGRESS#` | `PROGRESS#2026-01-15T11:00:00Z` | `gsi3_sk BETWEEN 'PROGRESS#' AND 'PROGRESS#~'` |
| **Completed** | `DONE#` | `DONE#2026-01-15T13:00:00Z` | `gsi3_sk BETWEEN 'DONE#' AND 'DONE#2026-01-13...'` |

#### Query Strategies:

**Find shops needing scrape:**
```python
# Get never-scraped shops (after crawl completed)
begins_with(gsi3_sk, 'NEVER#')
# + Filter: last_crawled_end exists (crawl completed)

# Get shops scraped before cutoff
gsi3_sk BETWEEN 'DONE#' AND 'DONE#2026-01-13T00:00:00Z'
# + Filter: last_crawled_end > last_scraped_end (new crawl data available)
```

**Exclude in-progress scrapes automatically:**
- In-progress scrapes have `PROGRESS#` prefix
- Queries for `NEVER#` or `DONE#` naturally exclude them

**Monitor stuck scrapes:**
```python
# Find scrapes in-progress > 2 hours
gsi3_sk BETWEEN 'PROGRESS#' AND 'PROGRESS#2026-01-15T09:00:00Z'
```

#### Lifecycle Example:

```python
# 1. Shop crawled, ready for scraping
gsi3_sk = "NEVER#"

# 2. Scrape starts
gsi3_sk = "PROGRESS#2026-01-15T11:00:00Z"  # When started

# 3. Scrape completes
gsi3_sk = "DONE#2026-01-15T13:00:00Z"      # When finished

# 4. New crawl happens → eligible for re-scrape
gsi3_sk = "DONE#2026-01-15T13:00:00Z"      # Unchanged
# But last_crawled_end is newer → eligible
```

**Benefits:**
- ✅ Shops never disappear from index
- ✅ State is explicit and queryable
- ✅ Easy to find stuck/orphaned scrapes
- ✅ Can monitor all in-progress operations
- ✅ Eligibility check uses projected `last_crawled_end`
- ✅ Minimal storage - NEVER# has no timestamp

### GSI4 – CoreDomainNameIndex

- **Purpose:** Associate different domains that share the same core domain name.
- **Key Schema:**
    - **HASH Key (`gsi4_pk`):** `core_domain_name`.
    - **RANGE Key (`gsi4_sk`):** `domain` (ensures the domain itself is part of the index key).
- **Projection:** Includes nothing

## How to start local DynamoDB + DynamoDB Admin

```bash
docker-compose up
```
This will start DynamoDB Local on `http://localhost:8000` and DynamoDB Admin on `http://localhost:8001`.