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

Stores information about a shop/domain and the standards it uses.

**Sort Key:** `META#`

**Attributes:**
```json
{
  "pk": "SHOP#example.com",
  "sk": "META#",
  "domain": "example.com",
  "core_domain_name": "example",
  "standards_used": ["json-ld", "microdata"],
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
| `standards_used` | List[String] | List of standards used by this shop (e.g., json-ld, microdata, opengraph) |
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
  "standards_used": ["json-ld", "microdata"],
  "type": "product",
  "hash": "a1b2c3d4..."
}
```

| Attribute | Type | Description |
|-----------|------|-------------|
| `pk` | String | "SHOP#" + domain name |
| `sk` | String | "URL#" + full URL |
| `url` | String | Full URL |
| `standards_used` | List[String] | List of standards used for this URL (json-ld, microdata, etc.) |
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

- **Purpose:** Find shops from a specific country that were crawled within a date range.
- **Key Schema:**
    - **HASH Key (`gsi2_pk`):** `shop_country` (e.g., `COUNTRY#DE`).
    - **RANGE Key (`gsi2_sk`):** `last_crawled_start`.
- **Projection:** `domain`

### GSI3 – CountryLastScrapedIndex

- **Purpose:** Find shops from a specific country that were scraped for product data within a date range.
- **Key Schema:**
    - **HASH Key (`gsi3_pk`):** `shop_country` (e.g., `COUNTRY#DE`).
    - **RANGE Key (`gsi3_sk`):** `last_scraped_start`.
- **Projection:** `domain`

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