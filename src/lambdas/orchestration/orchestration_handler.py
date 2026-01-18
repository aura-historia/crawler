from __future__ import annotations

import json
import logging
import os
import sys
from datetime import datetime, timedelta, timezone
from typing import Any, Dict, List, Optional

import boto3
from botocore.exceptions import ClientError
from pythonjsonlogger import json as pythonjson

from src.core.aws.database.operations import (
    DynamoDBOperations,
    parse_gsi_sk,
    STATE_NEVER,
    STATE_DONE,
)

# Configure JSON logging for CloudWatch
logger = logging.getLogger(__name__)
if not logger.handlers:
    handler = logging.StreamHandler(sys.stdout)
    formatter = pythonjson.JsonFormatter(
        "%(asctime)s %(levelname)s %(name)s %(message)s",
        rename_fields={"levelname": "level", "asctime": "timestamp"},
    )
    handler.setFormatter(formatter)
    logger.addHandler(handler)
    log_level = os.getenv("LOG_LEVEL", "INFO")
    logger.setLevel(log_level)

db_operations = DynamoDBOperations()

# Global SQS client for connection reuse across warm Lambda invocations
_GLOBAL_SQS_CLIENT: Optional[Any] = None


def _get_sqs_client() -> Any:
    """Get or create a module-level SQS client for reuse across invocations.

    Returns:
        boto3 SQS client instance.
    """
    global _GLOBAL_SQS_CLIENT
    if _GLOBAL_SQS_CLIENT is None:
        endpoint_url = os.getenv("SQS_ENDPOINT_URL")
        if endpoint_url:
            _GLOBAL_SQS_CLIENT = boto3.client("sqs", endpoint_url=endpoint_url)
        else:
            _GLOBAL_SQS_CLIENT = boto3.client("sqs")
    return _GLOBAL_SQS_CLIENT


def _send_batch_to_sqs(
    sqs_client: Any, queue_url: str, messages: List[Dict[str, Any]]
) -> int:
    """Send a batch of messages to SQS queue.

    SQS batch send supports max 10 messages per request.

    Args:
        sqs_client: boto3 SQS client.
        queue_url: SQS queue URL.
        messages: List of message dicts with 'Id' and 'MessageBody' keys.

    Returns:
        Number of successfully sent messages.

    Raises:
        ClientError: If SQS batch send fails.
    """
    if not messages:
        return 0

    try:
        logger.info(
            "Sending batch to SQS",
            extra={"batch_size": len(messages), "queue_url": queue_url},
        )

        response = sqs_client.send_message_batch(QueueUrl=queue_url, Entries=messages)

        successful = len(response.get("Successful", []))
        failed = response.get("Failed", [])

        if failed:
            logger.error(
                "Failed to send messages to SQS",
                extra={
                    "failed_count": len(failed),
                    "failures": failed,
                    "successful_count": successful,
                },
            )

        logger.info(
            "SQS batch send completed",
            extra={"successful": successful, "failed": len(failed)},
        )

        return successful

    except ClientError as e:
        logger.error(
            "SQS batch send failed",
            extra={
                "error": str(e),
                "error_code": e.response.get("Error", {}).get("Code"),
                "queue_url": queue_url,
            },
        )
        raise


def _enqueue_shops_to_queue(
    domains: List[str], queue_url: str, operation_type: str
) -> Dict[str, Any]:
    """Enqueue shop domains to the SQS queue in batches.

    Args:
        domains: List of shop domains to enqueue.
        queue_url: SQS queue URL.
        operation_type: Type of operation ("crawl" or "scrape").

    Returns:
        Dict with 'successful' count and 'failed' list of domains.
    """

    sqs_client = _get_sqs_client()
    total_sent = 0
    failed_domains: List[str] = []

    # Process in batches of 10 (SQS limit)
    batch_count = (len(domains) + 9) // 10  # Round up division
    for i in range(0, len(domains), 10):
        batch_num = (i // 10) + 1
        batch = domains[i : i + 10]

        logger.info(
            f"Processing batch {batch_num}/{batch_count}",
            extra={
                "batch_domains": batch,
                "batch_size": len(batch),
                "operation_type": operation_type,
            },
        )

        messages = [
            {
                "Id": str(idx),
                "MessageBody": json.dumps({"domain": domain}),
            }
            for idx, domain in enumerate(batch)
        ]

        try:
            sent = _send_batch_to_sqs(sqs_client, queue_url, messages)
            total_sent += sent

            # Track failed domains if not all were sent
            if sent < len(batch):
                batch_failed = batch[sent:]
                failed_domains.extend(batch_failed)
                logger.warning(
                    f"Batch {batch_num}: partial failure",
                    extra={
                        "sent": sent,
                        "failed": len(batch_failed),
                        "failed_domains": batch_failed,
                    },
                )
        except Exception as e:
            logger.error(
                f"Batch {batch_num}: complete failure",
                extra={
                    "error": str(e),
                    "error_type": type(e).__name__,
                    "failed_domains": batch,
                },
            )
            failed_domains.extend(batch)

    logger.info(
        "Finished enqueueing domains",
        extra={
            "total_domains": len(domains),
            "successful": total_sent,
            "failed": len(failed_domains),
            "operation_type": operation_type,
            "success_rate": f"{(total_sent / len(domains) * 100):.1f}%"
            if domains
            else "N/A",
        },
    )

    if failed_domains:
        logger.warning(
            "Some domains failed to enqueue",
            extra={
                "failed_count": len(failed_domains),
                "failed_domains": failed_domains,
            },
        )

    return {
        "successful": total_sent,
        "failed": failed_domains,
    }


def _is_scrape_eligible(
    last_crawled_end: str,
    last_scraped_end: str,
) -> bool:
    """Check if a shop is eligible for scraping.

    Args:
        last_crawled_end: When the crawl ended (State: DONE#...)
        last_scraped_end: When the scrape ended (State: DONE#...)
    Returns:
        True if eligible for scraping, False otherwise.
    """
    if last_scraped_end is None or last_crawled_end is None:
        return False

    scrape_state, scrape_timestamp = parse_gsi_sk(last_scraped_end)
    crawl_state, crawl_timestamp = parse_gsi_sk(last_crawled_end)

    if scrape_state == STATE_NEVER and crawl_state == STATE_DONE:
        return True
    elif scrape_state == STATE_DONE and crawl_state == STATE_DONE:
        return scrape_timestamp < crawl_timestamp
    return False


def _filter_eligible_shops_for_crawl(
    shops: List[Any],
) -> tuple[List[Any], Dict[str, int]]:
    """Filter shops to include only those eligible for crawling.

    Applies in-memory filtering after GSI query to reduce duplicate crawl jobs.
    GSI2 query may return shops that are currently being crawled.

    Args:
        shops: List of ShopMetadata objects from GSI query.

    Returns:
        Tuple of (eligible_shops, filter_stats).
    """
    eligible = []
    stats = {
        "total_queried": len(shops),
        "in_progress": 0,
        "eligible": 0,
    }

    for shop in shops:
        # Check if crawl already in progress
        if shop.last_crawled_end.startswith("PROGRESS#"):
            stats["in_progress"] += 1
            logger.debug(
                "Skipping shop - crawl in progress",
                extra={
                    "domain": shop.domain,
                    "last_crawled_start": shop.last_crawled_start,
                    "last_crawled_end": shop.last_crawled_end,
                },
            )
            continue

        eligible.append(shop)
        stats["eligible"] += 1

    logger.info(
        "Crawl eligibility filtering completed",
        extra={
            "total_queried": stats["total_queried"],
            "eligible": stats["eligible"],
            "filtered_out": stats["total_queried"] - stats["eligible"],
            "in_progress": stats["in_progress"],
            "filter_rate": f"{((stats['total_queried'] - stats['eligible']) / stats['total_queried'] * 100):.1f}%"
            if stats["total_queried"] > 0
            else "0.0%",
        },
    )

    return eligible, stats


def _filter_eligible_shops_for_scrape(
    shops: List[Any],
) -> tuple[List[Any], Dict[str, int]]:
    """Filter shops to include only those eligible for scraping.

    Applies in-memory filtering after GSI query to reduce no-op scrape jobs.
    GSI3 query may return shops that are:
    - Already scraped (scrape is newer than crawl)
    - Currently being scraped (scrape in progress)
    - Not yet crawled (crawl in progress)

    Args:
        shops: List of ShopMetadata objects from GSI query.

    Returns:
        Tuple of (eligible_shops, filter_stats).
    """
    eligible = []
    stats = {
        "total_queried": len(shops),
        "in_progress": 0,
        "crawl_not_finished": 0,
        "already_scraped": 0,
        "eligible": 0,
    }

    for shop in shops:
        # Check if scrape already in progress
        if shop.last_scraped_end.startswith("PROGRESS#"):
            stats["in_progress"] += 1
            logger.debug(
                "Skipping shop - scrape in progress",
                extra={
                    "domain": shop.domain,
                    "last_scraped_start": shop.last_scraped_start,
                    "last_scraped_end": shop.last_scraped_end,
                },
            )
            continue

        # Check eligibility (crawl finished and newer than scrape)
        if not _is_scrape_eligible(shop.last_crawled_end, shop.last_scraped_end):
            # Determine reason for ineligibility
            if (
                shop.last_crawled_end is None
                or shop.last_crawled_end == "1970-01-01T00:00:00Z"
            ):
                stats["crawl_not_finished"] += 1
            else:
                stats["already_scraped"] += 1

            logger.debug(
                "Skipping shop - not eligible",
                extra={
                    "domain": shop.domain,
                    "last_crawled_end": shop.last_crawled_end,
                    "last_scraped_end": shop.last_scraped_end,
                },
            )
            continue

        eligible.append(shop)
        stats["eligible"] += 1

    logger.info(
        "Scrape eligibility filtering completed",
        extra={
            "total_queried": stats["total_queried"],
            "eligible": stats["eligible"],
            "filtered_out": stats["total_queried"] - stats["eligible"],
            "in_progress": stats["in_progress"],
            "crawl_not_finished": stats["crawl_not_finished"],
            "already_scraped": stats["already_scraped"],
            "filter_rate": f"{((stats['total_queried'] - stats['eligible']) / stats['total_queried'] * 100):.1f}%"
            if stats["total_queried"] > 0
            else "0.0%",
        },
    )

    return eligible, stats


def handler(event: Dict[str, Any], context: object) -> Dict[str, Any]:
    """AWS Lambda handler for unified orchestration (crawl or scrape).

    Supports two operation modes controlled by event parameter:
    - "crawl": Uses GSI2 and spider queue (finds new product URLs)
    - "scrape": Uses GSI3 and scraper queue (scrapes product data)

    Event structure:
    {
        "operation": "crawl" or "scrape",  # Required
        "country": "DE",                    # Optional, defaults to "DE"
        "cutoff_days": 2                    # Optional, defaults to 2
    }

    Args:
        event: Event with operation type, or EventBridge scheduled event.
        context: Lambda context object.

    Returns:
        Dict with status and summary of enqueued shops.
    """
    # Determine operation type from event
    operation_type = event.get("operation", "crawl")  # Default to crawl

    # Validate operation type
    if operation_type not in ("crawl", "scrape"):
        logger.error(
            f"Invalid operation type: {operation_type}",
            extra={"allowed_values": ["crawl", "scrape"], "event": event},
        )
        return {
            "statusCode": 400,
            "body": json.dumps(
                {
                    "error": f"Invalid operation type: {operation_type}",
                    "allowed_values": ["crawl", "scrape"],
                }
            ),
        }

    logger.info(
        f"{operation_type.capitalize()} orchestration lambda invoked",
        extra={
            "event_source": event.get("source", "manual"),
            "operation_type": operation_type,
            "event": event,
        },
    )

    # Determine queue URL based on operation type
    queue_env_var = (
        "SQS_PRODUCT_SPIDER_QUEUE_URL"
        if operation_type == "crawl"
        else "SQS_PRODUCT_SCRAPER_QUEUE_URL"
    )
    queue_url = os.getenv(queue_env_var)

    if not queue_url:
        logger.error(f"{queue_env_var} environment variable not set")
        return {
            "statusCode": 500,
            "body": json.dumps({"error": f"{queue_env_var} not configured"}),
        }

    # Calculate cutoff date
    cutoff_days = event.get(
        "cutoff_days", int(os.getenv("ORCHESTRATION_CUTOFF_DAYS", "2"))
    )
    cutoff_date = datetime.now(timezone.utc) - timedelta(days=cutoff_days)
    cutoff_date_str = cutoff_date.isoformat()

    # Get country from event or default to DE
    country = event.get("country", "DE")

    logger.info(
        f"Querying shops needing {operation_type}",
        extra={
            "cutoff_date": cutoff_date_str,
            "cutoff_days": cutoff_days,
            "country": country,
            "operation_type": operation_type,
        },
    )

    try:
        # Get shops using unified method
        shops = db_operations.get_shops_for_orchestration(
            operation_type=operation_type,
            cutoff_date=cutoff_date_str,
            country=country,
        )

        if not shops:
            logger.info(
                f"No shops found requiring {operation_type}",
                extra={
                    "cutoff_date": cutoff_date_str,
                    "country": country,
                    "operation_type": operation_type,
                },
            )
            return {
                "statusCode": 200,
                "body": json.dumps(
                    {
                        "message": f"No shops to enqueue for {operation_type}",
                        "operation_type": operation_type,
                        "shops_count": 0,
                        "cutoff_date": cutoff_date_str,
                    }
                ),
            }

        # Apply in-memory filtering for both operations
        # GSI queries may return shops that are currently in progress
        filter_stats = None
        if operation_type == "crawl":
            shops, filter_stats = _filter_eligible_shops_for_crawl(shops)

            if not shops:
                logger.info(
                    "No eligible shops after filtering",
                    extra={
                        "cutoff_date": cutoff_date_str,
                        "country": country,
                        "operation_type": operation_type,
                        "filter_stats": filter_stats,
                    },
                )
                return {
                    "statusCode": 200,
                    "body": json.dumps(
                        {
                            "message": "No eligible shops to enqueue for crawl",
                            "operation_type": operation_type,
                            "shops_count": 0,
                            "shops_queried": filter_stats["total_queried"],
                            "shops_filtered": filter_stats["total_queried"]
                            - filter_stats["eligible"],
                            "filter_stats": filter_stats,
                            "cutoff_date": cutoff_date_str,
                        }
                    ),
                }

        elif operation_type == "scrape":
            shops, filter_stats = _filter_eligible_shops_for_scrape(shops)

            if not shops:
                logger.info(
                    "No eligible shops after filtering",
                    extra={
                        "cutoff_date": cutoff_date_str,
                        "country": country,
                        "operation_type": operation_type,
                        "filter_stats": filter_stats,
                    },
                )
                return {
                    "statusCode": 200,
                    "body": json.dumps(
                        {
                            "message": "No eligible shops to enqueue for scrape",
                            "operation_type": operation_type,
                            "shops_count": 0,
                            "shops_queried": filter_stats["total_queried"],
                            "shops_filtered": filter_stats["total_queried"]
                            - filter_stats["eligible"],
                            "filter_stats": filter_stats,
                            "cutoff_date": cutoff_date_str,
                        }
                    ),
                }

        domains = [shop.domain for shop in shops]
        logger.info(
            f"Found shops requiring {operation_type}",
            extra={
                "shops_count": len(domains),
                "domains_preview": domains[:5],
                "cutoff_date": cutoff_date_str,
                "operation_type": operation_type,
                "filter_stats": filter_stats,
            },
        )

        # Enqueue domains to appropriate SQS queue
        enqueue_result = _enqueue_shops_to_queue(domains, queue_url, operation_type)

        response_body = {
            "summary": f"{operation_type.capitalize()} orchestration completed",
            "operation_type": operation_type,
            "shops_found": len(domains),
            "shops_enqueued": enqueue_result["successful"],
            "shops_failed": len(enqueue_result["failed"]),
            "failed_domains": enqueue_result["failed"],
            "cutoff_date": cutoff_date_str,
            "country": country,
        }

        # Add filter stats for scrape operations
        if filter_stats:
            response_body["filter_stats"] = filter_stats

        logger.info(
            f"{operation_type.capitalize()} orchestration completed successfully",
            extra=response_body,
        )

        result_body = {
            "message": f"{operation_type.capitalize()} orchestration completed",
            "operation_type": operation_type,
            "shops_found": response_body["shops_found"],
            "shops_enqueued": response_body["shops_enqueued"],
            "shops_failed": response_body["shops_failed"],
            "failed_domains": response_body["failed_domains"],
            "cutoff_date": response_body["cutoff_date"],
            "country": response_body["country"],
        }

        # Add filter stats to response for scrape operations
        if filter_stats:
            result_body["filter_stats"] = filter_stats

        return {
            "statusCode": 200,
            "body": json.dumps(result_body),
        }

    except Exception as e:
        logger.error(
            f"{operation_type.capitalize()} orchestration failed",
            extra={
                "error": str(e),
                "error_type": type(e).__name__,
                "cutoff_date": cutoff_date_str,
                "operation_type": operation_type,
            },
            exc_info=True,
        )
        return {
            "statusCode": 500,
            "body": json.dumps({"error": str(e), "operation_type": operation_type}),
        }
