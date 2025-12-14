# DynamoDB Schema - Single Table Design

## Table Overview

**Table Name:** `aura-historia-data` (configured via `DYNAMODB_TABLE_NAME`)

**Design Pattern:** Single-table design with composite keys

## Key Schema

| Attribute | Type | Description |
|-----------|------|-------------|
| `PK` | String (HASH) | Partition key: 'SHOP#' + domain (e.g., 'SHOP#example.com') |
| `SK` | String (RANGE) | Sort key: 'META#' or 'URL#<full_url>' |

## Item Types

### 1. Shop Metadata (META#)

Stores information about a shop/domain and the standards it uses.

**Sort Key:** `META#`

**Attributes:**
```json
{
  "PK": "SHOP#example.com",
  "SK": "META#",
  "domain": "example.com",
  "standards_used": ["json-ld", "microdata"],
  "shop_country": "US",
  "last_crawled": "2023-10-27T10:00:00Z",
  "last_scraped": "2023-10-27T12:00:00Z"
}
```

| Attribute | Type | Description |
|-----------|------|-------------|
| `PK` | String | "SHOP#" + domain name |
| `SK` | String | Fixed value: "META#" |
| `domain` | String | Domain name (duplicate for convenience) |
| `standards_used` | List[String] | List of standards used by this shop (e.g., json-ld, microdata, opengraph) |
| `shop_country` | String | ISO 3166-1 alpha-2 country code for the shop. |
| `last_crawled` | String | ISO 8601 timestamp of the last crawl. |
| `last_scraped` | String | ISO 8601 timestamp of the last scrape. |

### 2. URL Entry (URL#)

Stores information about individual URLs from a domain.

**Sort Key:** `URL#<full_url>`

**Attributes:**
```json
{
  "PK": "SHOP#example.com",
  "SK": "URL#https://example.com/products/item-123",
  "url": "https://example.com/products/item-123",
  "standards_used": ["json-ld", "microdata"],
  "type": "product",
  "is_product": 1,
  "hash": "a1b2c3d4..."
}
```

| Attribute | Type | Description |
|-----------|------|-------------|
| `PK` | String | "SHOP#" + domain name |
| `SK` | String | "URL#" + full URL |
| `url` | String | Full URL |
| `standards_used` | List[String] | List of standards used for this URL (json-ld, microdata, etc.) |
| `type` | String | Type of page (category, product, listing, etc.) |
| `is_product` | Number | Whether this is a product page (1 for true, 0 for false). Used for GSI. |
| `hash` | String | SHA256 hash of status+price to detect changes |

## Local Secondary Indexes (LSIs)

### 1. `IsProductIndex`

- **Purpose:** Efficiently query for all product URLs within a specific domain.
- **Key Schema:**
    - **HASH Key:** `PK` (String)
    - **RANGE Key:** `is_product` (Number)
- **Projection:** Includes `url`, `standards_used`.
- **Example Query:** Find all URLs where `PK` is `SHOP#example.com` and `is_product = 1`.

## Global Secondary Indexes (GSIs)

### 2. `CountryLastCrawledIndex`

- **Purpose:** Find shops from a specific country that were crawled within a given date range.
- **Key Schema:**
    - **HASH Key:** `shop_country` (String)
    - **RANGE Key:** `last_crawled` (String, ISO 8601)
- **Projection:** Includes `domain`.
- **Example Query:** Find all domains where `shop_country = 'US'` and `last_crawled` is between `2023-01-01` and `2023-01-31`.

### 3. `CountryLastScrapedIndex`

- **Purpose:** Find shops from a specific country that were scraped for product data within a given date range.
- **Key Schema:**
    - **HASH Key:** `shop_country` (String)
    - **RANGE Key:** `last_scraped` (String, ISO 8601)
- **Projection:** Includes `domain`.
- **Example Query:** Find all domains where `shop_country = 'DE'` and `last_scraped` is between `2023-02-01` and `2023-02-28`.

## How to start local DynamoDB + DynamoDB Admin

```bash
docker-compose up
```
This will start DynamoDB Local on `http://localhost:8000` and DynamoDB Admin on `http://localhost:8001`.