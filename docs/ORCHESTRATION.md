# Unified Orchestration Lambda

## Overview

The unified Orchestration Lambda handles both **crawl** and **scrape** operations for the crawler system. It determines which shops need processing based on their last crawl/scrape timestamps and enqueues them to the appropriate SQS queue.

## Architecture

```
EventBridge/Manual → Orchestration Lambda
                             ↓
                     Operation Type Check
                    (crawl or scrape)
                             ↓
                   ┌─────────┴─────────┐
                   ↓                   ↓
            Query GSI2           Query GSI3
         (last_crawled_end)   (last_scraped_end)
                   ↓                   ↓
            Spider Queue         Scraper Queue
                   ↓                   ↓
            Product Spider       Product Scraper
```

## Features

- ✅ **Unified Handler**: Single Lambda function for both crawl and scrape operations
- ✅ **Event-Driven**: Operation type controlled by event parameter
- ✅ **Flexible Queuing**: Routes to appropriate SQS queue based on operation
- ✅ **Batch Processing**: Sends up to 10 messages per SQS batch
- ✅ **Error Handling**: Tracks and logs failed domains
- ✅ **Structured Logging**: JSON format for CloudWatch

## Event Structure

### Crawl Operation
```json
{
  "operation": "crawl",
  "country": "DE",
  "cutoff_days": 2
}
```

### Scrape Operation
```json
{
  "operation": "scrape",
  "country": "DE",
  "cutoff_days": 2
}
```

### Parameters

| Parameter | Type | Required | Default | Description |
|-----------|------|----------|---------|-------------|
| `operation` | string | No | `"crawl"` | Operation type: `"crawl"` or `"scrape"` |
| `country` | string | No | `"DE"` | Country code for shop filtering |
| `cutoff_days` | integer | No | `2` | Days since last operation |

## Environment Variables

| Variable | Description | Required |
|----------|-------------|----------|
| `SQS_PRODUCT_SPIDER_QUEUE_URL` | URL of spider queue (for crawl) | Yes |
| `SQS_PRODUCT_SCRAPER_QUEUE_URL` | URL of scraper queue (for scrape) | Yes |
| `DYNAMODB_TABLE_NAME` | Name of DynamoDB table | Yes |
| `ORCHESTRATION_CUTOFF_DAYS` | Default cutoff days | No (default: 2) |
| `LOG_LEVEL` | Logging level | No (default: INFO) |

## Operation Types

### Crawl Operation
- **Purpose**: Find new product URLs
- **GSI**: GSI2 (CountryLastCrawledIndex)
- **Attribute**: `last_crawled_end`
- **Queue**: `spider_product_queue`
- **Worker**: Product Spider

### Scrape Operation
- **Purpose**: Scrape product data
- **GSI**: GSI3 (CountryLastScrapedIndex)
- **Attribute**: `last_scraped_end`
- **Queue**: `product_scraper_queue`
- **Worker**: Product Scraper

## Response Format

### Success (200)
```json
{
  "statusCode": 200,
  "body": {
    "message": "Crawl orchestration completed",
    "operation_type": "crawl",
    "shops_found": 150,
    "shops_enqueued": 148,
    "shops_failed": 2,
    "failed_domains": ["shop1.com", "shop2.com"],
    "cutoff_date": "2026-01-12T10:00:00+00:00",
    "country": "DE"
  }
}
```

### Error (400 - Invalid Operation)
```json
{
  "statusCode": 400,
  "body": {
    "error": "Invalid operation type: invalid",
    "allowed_values": ["crawl", "scrape"]
  }
}
```

### Error (500 - Configuration/Runtime)
```json
{
  "statusCode": 500,
  "body": {
    "error": "Error message details",
    "operation_type": "crawl"
  }
}
```

## Deployment

### With CDK

```bash
cd cdk
cdk deploy OrchestrationStack
```

This will create:
- Lambda function from Docker image
- IAM role with DynamoDB Query and SQS SendMessage permissions
- Environment variables configured automatically

## Manual Invocation

### Crawl Operation
```bash
aws lambda invoke \
  --function-name OrchestrationLambda \
  --payload '{"operation":"crawl","country":"DE","cutoff_days":2}' \
  response.json
```

### Scrape Operation
```bash
aws lambda invoke \
  --function-name OrchestrationLambda \
  --payload '{"operation":"scrape","country":"DE","cutoff_days":3}' \
  response.json
```

## Testing

Run all tests:
```bash
pytest tests/src/lambdas/orchestration/test_orchestration_handler.py -v
```

### Test Coverage

✅ **Handler Tests** (11 tests)
- Invalid operation type validation
- Queue URL configuration checks
- Success scenarios for both operations
- Partial SQS failures
- Database exceptions
- Default operation behavior
- Batch processing

✅ **Database Tests** (4 tests)
- GSI2 queries (crawl)
- GSI3 queries (scrape)
- Invalid operation type handling
- Pagination support

## EventBridge Integration (Future)

To enable scheduled orchestration, create EventBridge rules:

### Daily Crawl (6 AM UTC)
```json
{
  "schedule": "cron(0 6 * * ? *)",
  "target": "OrchestrationLambda",
  "input": {
    "operation": "crawl",
    "country": "DE",
    "cutoff_days": 2
  }
}
```

### Daily Scrape (8 AM UTC)
```json
{
  "schedule": "cron(0 8 * * ? *)",
  "target": "OrchestrationLambda",
  "input": {
    "operation": "scrape",
    "country": "DE",
    "cutoff_days": 2
  }
}
```

## Monitoring

### CloudWatch Logs

The Lambda uses structured JSON logging:

```json
{
  "timestamp": "2026-01-14T10:00:00Z",
  "level": "INFO",
  "message": "Found shops requiring crawl",
  "shops_count": 150,
  "domains_preview": ["shop1.com", "shop2.com", "..."],
  "cutoff_date": "2026-01-12T10:00:00+00:00",
  "operation_type": "crawl"
}
```

### Key Metrics

- **Shops Found**: Total shops meeting cutoff criteria
- **Shops Enqueued**: Successfully sent to SQS
- **Shops Failed**: Failed to enqueue
- **Success Rate**: Percentage of successful enqueues
- **Operation Type**: crawl or scrape

## Code Structure

```
src/lambdas/orchestration/
├── orchestration_handler.py  # Main handler
├── Dockerfile                # Lambda container image
├── requirements.txt          # Python dependencies
└── README.md                # This file
```

## Database Methods

### Unified Method
```python
db_operations.get_shops_for_orchestration(
    operation_type="crawl",  # or "scrape"
    cutoff_date="2026-01-12T10:00:00+00:00",
    country="DE"
)
```

This single method replaces the old separate methods:
- ~~`get_last_crawled_shops()`~~ (removed)
- ~~`get_last_scraped_shops()`~~ (removed)

## Troubleshooting

### No Shops Found
**Cause**: All shops recently processed

**Solution**: 
- Adjust `cutoff_days` parameter
- Check `last_crawled_end` / `last_scraped_end` in DynamoDB

### Queue URL Not Configured
**Cause**: Missing environment variable

**Solution**:
- Verify Lambda configuration
- Ensure CDK stack deployed correctly
- Check environment variables in AWS Console

### High SQS Failure Rate
**Cause**: Rate limiting or throttling

**Solution**:
- Increase SQS queue limits
- Implement exponential backoff
- Review CloudWatch Logs for details

## Migration from Separate Orchestrators

The system was previously split into:
- `orchestration_spider` (for crawling)
- `orchestration_scraper` (for scraping)

These have been **unified** into a single orchestration Lambda controlled by the `operation` event parameter. This eliminates code duplication and simplifies deployment.

### Benefits
- ✅ Single Lambda to maintain
- ✅ Shared infrastructure and monitoring
- ✅ Consistent error handling
- ✅ Easier to extend with new operation types
- ✅ Reduced deployment complexity

