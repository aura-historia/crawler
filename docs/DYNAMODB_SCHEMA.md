# DynamoDB Schema - Single Table Design

## Table Overview

**Table Name:** `aura-historia-data` (configured via `DYNAMODB_TABLE_NAME`)

**Design Pattern:** Single-table design with composite keys

## Key Schema

| Attribute | Type | Description |
|-----------|------|-------------|
| `pk` | String (HASH) | Partition key: domain (e.g., 'example.com') |
| `sk` | String (RANGE) | Sort key: 'META#' or 'URL#<full_url>' |

## Item Types

### 1. Shop Metadata (META#)

Stores information about a shop/domain and the standards it uses.

**Sort Key:** `META#`

**Attributes:**
```json
{
  "pk": "example.com",
  "sk": "META#",
  "domain": "example.com",
  "standards_used": ["json-ld", "microdata"]
}
```

| Attribute | Type | Description |
|-----------|------|-------------|
| `pk` | String | Domain name |
| `sk` | String | Fixed value: "META#" |
| `domain` | String | Domain name (duplicate for convenience) |
| `standards_used` | List[String] | List of standards used by this shop (e.g., json-ld, microdata, opengraph) |

### 2. URL Entry (URL#)

Stores information about individual URLs from a domain.

**Sort Key:** `URL#<full_url>`

**Attributes:**
```json
{
  "pk": "example.com",
  "sk": "URL#https://example.com/products/item-123",
  "url": "https://example.com/products/item-123",
  "standards_used": ["json-ld", "microdata"],
  "type": "product",
  "is_product": true,
  "hash": "5d41402abc4b2a76b9719d911017c592"
}
```

| Attribute | Type | Description |
|-----------|------|-------------|
| `pk` | String | Domain name |
| `sk` | String | "URL#" + full URL |
| `url` | String | Full URL |
| `standards_used` | List[String] | List of standards used for this URL (json-ld, microdata, etc.) |
| `type` | String | Type of page (category, product, listing, etc.) |
| `is_product` | Boolean | Whether this is a product page |
| `hash` | String | MD5 hash of status+price to detect changes |
